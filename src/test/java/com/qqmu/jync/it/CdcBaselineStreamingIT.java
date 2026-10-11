package com.qqmu.jync.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.qqmu.jync.model.CaptureStrategy;
import com.qqmu.jync.model.Project;
import com.qqmu.jync.dto.SyncConfig;
import com.qqmu.jync.service.cdc.CdcConsumerManager;
import com.qqmu.jync.service.cdc.CdcPositionStore;

/**
 * 验收 B①(小数据版,十万条版在 Task 15)+ Review Focus #4(基线中途崩溃整体重跑)。
 *
 * <p>Runs against the shared real MySQL 8 binlog source of
 * {@link CdcMySqlContainerHarnessIT}; named {@code *IT} so surefire's default includes
 * skip it — run explicitly with
 * {@code mvn test -Dtest=CdcBaselineStreamingIT -Dapi.version=1.44}.
 */
class CdcBaselineStreamingIT extends CdcMySqlContainerHarnessIT {

    @Autowired private CdcConsumerManager manager;
    @Autowired private CdcPositionStore positionStore;

    @Test
    void baselineThenStreamingReachesConsistency() throws Exception {
        try (Connection src = sourceJdbc(); Statement st = src.createStatement()) {
            st.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, name VARCHAR(64), amount DECIMAL(10,2))");
            for (int i = 1; i <= 500; i++) {
                st.execute("INSERT INTO orders VALUES (" + i + ", 'seed-" + i + "', " + i + ".5)");
            }
        }
        try (Connection tgt = targetJdbc(); Statement st = tgt.createStatement()) {
            st.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, name VARCHAR(64), amount DECIMAL(10,2))");
        }
        Project project = newProject("cdc-basic", cdcConfig("orders"));
        manager.ensureStarted(project);

        waitFor("baseline done", 60_000, () -> positionStore.find(project.getId())
                .map(p -> p.isBaselineDone()).orElse(false));
        waitFor("500 seeded rows", 30_000, () -> countTarget("orders") == 500);

        // 流式阶段:混合 DML 全部落到目标(含 UPDATE 改主键 = Review Focus #2 的真库版)
        try (Connection src = sourceJdbc(); Statement st = src.createStatement()) {
            st.execute("INSERT INTO orders VALUES (501, 'streamed', 1.00)");
            st.execute("UPDATE orders SET name = 'changed' WHERE id = 7");
            st.execute("DELETE FROM orders WHERE id = 13");
            st.execute("UPDATE orders SET id = 600 WHERE id = 20");   // 改主键
        }
        // 行数账:500 + 1(插501) - 1(删13) - 1(删20) + 1(增600) = 500。基线后计数本来就是
        // 500,单靠计数会在流事件落库前提前通过——用改主键后的 600 行做锚点,它只可能在全部
        // 四个事务应用后存在(消费者按提交序单线程应用)。
        waitFor("stream applied", 30_000, () -> countTarget("orders") == 500
                && queryTarget("SELECT id FROM orders WHERE id = 600").size() == 1);
        List<Map<String, Object>> moved = queryTarget("SELECT id, name FROM orders WHERE id IN (20, 600)");
        assertThat(moved).hasSize(1);
        assertThat(moved.get(0).get("id")).isEqualTo(600L);           // 旧主键行被删掉
        assertThat(queryTarget("SELECT name FROM orders WHERE id = 7").get(0).get("name"))
                .isEqualTo("changed");
        assertThat(queryTarget("SELECT COUNT(*) c FROM orders WHERE id = 13").get(0))
                .hasFieldOrPropertyWithValue("c", 0L);

        manager.ensureStopped(project.getId());
        // 位点已持久化:停后再写,重启消费者应从位点续传不丢(重放幂等)。
        // 续传证明:先只在目标端删掉 id=5(源端不动,也不产生 src 库的 binlog 事件)。
        // 从位点续传只会回放新的 700 事件 → 499+1=500 行且 id=5 保持缺失;若消费者
        // 悄悄改跑了整体基线,resetProject+全量重载会把源端全部 501 行(含 id=5)灌回来
        // ——CdcConsumer 的两条路径由此可区分,不再只靠行数撞对。
        try (Connection tgt = targetJdbc(); Statement st = tgt.createStatement()) {
            st.execute("DELETE FROM orders WHERE id = 5");
        }
        try (Connection src = sourceJdbc(); Statement st = src.createStatement()) {
            st.execute("INSERT INTO orders VALUES (700, 'after-stop', 2.00)");
        }
        // reload:从 repository 读最新 project(harness 工具)。1s reconcile tick 可能已抢先
        // 重启了消费者(enabled=true),那时这里幂等 no-op——断言集合与谁先启动无关。
        manager.ensureStarted(reload(project));
        waitFor("post-restart row", 30_000, () -> countTarget("orders") == 500
                && queryTarget("SELECT id FROM orders WHERE id = 700").size() == 1
                && queryTarget("SELECT id FROM orders WHERE id = 5").isEmpty());
        manager.ensureStopped(project.getId());
        disableProject(project);   // 别让 1s reconcile tick 在共享上下文余生里反复重启消费者
    }

    @Test
    void crashMidBaselineRerunsWholeBaseline() throws Exception {
        // Review Focus #4:baseline_done=false 的残缺位点 → 重新 ensureStarted 整体重跑基线
        try (Connection src = sourceJdbc(); Statement st = src.createStatement()) {
            st.execute("CREATE TABLE crash_t (id BIGINT PRIMARY KEY, v VARCHAR(32))");
            st.execute("INSERT INTO crash_t VALUES (1, 'x')");
        }
        try (Connection tgt = targetJdbc(); Statement st = tgt.createStatement()) {
            st.execute("CREATE TABLE crash_t (id BIGINT PRIMARY KEY, v VARCHAR(32))");
        }
        // 先以 enabled=false 落库:伪造位点行就位之前,1s reconcile tick 不得抢先启动
        // 消费者——否则「假写在其 markBaselineDone 之后落库」的交错会把已在流式消费的
        // 项目翻回 baseline_done=false 且永远没人再置 true(60s 超时 flake)。
        Project project = newProject("cdc-crash", cdcConfig("crash_t"), false);
        // 手工伪造「基线中途崩溃」现场:位点行存在但 baseline_done=false
        positionStore.recordBaselineStart(project.getId(), "mysql-bin.000001", 4L, null, 612000L + project.getId());
        project.setEnabled(true);
        projectRepository.save(project);

        // ensureStarted 不查 enabled 标志;reconciler 此刻起也可能抢先启动——幂等,且
        // 无论谁启动,消费者看到的都是伪造的残缺位点(整体重跑语义成立)。
        manager.ensureStarted(reload(project));   // 消费者发现 !baselineDone → 整体重跑基线
        waitFor("baseline redone", 60_000, () -> positionStore.find(project.getId())
                .map(p -> p.isBaselineDone()).orElse(false));
        waitFor("crash_t row present", 30_000, () -> countTargetTable("crash_t") == 1);
        manager.ensureStopped(project.getId());
        disableProject(project);   // 同前:测毕即关,共享上下文里不留活消费者
    }

    private SyncConfig cdcConfig(String... tables) {
        SyncConfig config = new SyncConfig();
        config.setCaptureStrategy(CaptureStrategy.BINLOG_CDC);
        for (String t : tables) {
            config.getTables().add(t);
        }
        return config;
    }
}

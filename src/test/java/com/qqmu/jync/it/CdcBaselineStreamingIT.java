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
        // 位点已持久化:停后再写,重启消费者应从位点续传不丢(重放幂等)
        try (Connection src = sourceJdbc(); Statement st = src.createStatement()) {
            st.execute("INSERT INTO orders VALUES (700, 'after-stop', 2.00)");
        }
        manager.ensureStarted(reload(project));   // reload:从 repository 读最新 project(harness 工具)
        waitFor("post-restart row", 30_000, () -> countTarget("orders") == 501);
        manager.ensureStopped(project.getId());
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
        Project project = newProject("cdc-crash", cdcConfig("crash_t"));
        // 手工伪造「基线中途崩溃」现场:位点行存在但 baseline_done=false
        positionStore.recordBaselineStart(project.getId(), "mysql-bin.000001", 4L, null, 612000L + project.getId());

        manager.ensureStarted(project);   // 消费者发现 !baselineDone → 整体重跑基线
        waitFor("baseline redone", 60_000, () -> positionStore.find(project.getId())
                .map(p -> p.isBaselineDone()).orElse(false));
        waitFor("crash_t row present", 30_000, () -> countTargetTable("crash_t") == 1);
        manager.ensureStopped(project.getId());
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

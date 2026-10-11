package com.qqmu.jync.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.containers.MySQLContainer;

import com.github.dockerjava.api.DockerClient;
import com.qqmu.jync.dto.SyncConfig;
import com.qqmu.jync.model.CaptureStrategy;
import com.qqmu.jync.model.Project;
import com.qqmu.jync.model.SyncTask;
import com.qqmu.jync.model.TaskStatus;
import com.qqmu.jync.service.cdc.CdcConsumerManager;
import com.qqmu.jync.service.cdc.CdcPositionStore;
import com.qqmu.jync.service.cdc.CdcRuntimeStats;
import com.qqmu.jync.service.connection.DataSourceManager;
import com.qqmu.jync.service.task.SyncLockService;
import com.qqmu.jync.service.task.SyncLockStore;

import org.testcontainers.DockerClientFactory;

/**
 * 验收 B②(断线重连零丢失)+ B③(binlog 被 purge → ERROR + 一键重建基线)+ B⑤(双实例接管 ≤60s)。
 *
 * <p>This class runs against its OWN {@code mysql:8.0} container ({@link #createContainer()}
 * override, built with the family's {@link #configuredMySql()} flags): PURGE BINARY LOGS
 * and {@code docker pause} mutate/freeze server state destructively, which would poison
 * every other IT class sharing the family container (Task 14 included). Ryuk reclaims the
 * instance at JVM exit like any other; nothing ever calls {@code stop()} on either
 * container. Same two schemas (src/tgt) as the shared harness, different instance.
 *
 * <p>Fixture-staging discipline (same as Task 14's crash test): the 1s reconcile AND
 * supervise ticks relaunch enabled BINLOG_CDC projects, so every stop-then-stage sequence
 * here is disableProject FIRST, then ensureStopped (which joins the consumer thread — its
 * finally block released the lease), then the fixture (purge / forged lock), then
 * re-enable. Liveness is never asserted via sync_task.status (a clean stop leaves RUNNING
 * by design); status reads only ever check the ERROR hat.
 *
 * <p>Run explicitly with
 * {@code mvn test -Dtest='CdcVolumeAndTypesIT,CdcResilienceIT' -Dapi.version=1.44}.
 */
class CdcResilienceIT extends CdcMySqlContainerHarnessIT {

    private static final Logger log = LoggerFactory.getLogger(CdcResilienceIT.class);

    /** This class's private replication source — see the class javadoc for why. */
    private static final MySQLContainer<?> OWN_MYSQL = configuredMySql();

    @Override
    protected MySQLContainer<?> createContainer() {
        return OWN_MYSQL;
    }

    @Autowired private CdcConsumerManager manager;
    @Autowired private CdcPositionStore positionStore;
    @Autowired private DataSourceManager dataSourceManager;
    @Autowired private SyncLockStore lockStore;
    @Autowired private SyncLockService lockService;

    // ================= 验收 B② — pause/unpause 重连,零丢失 =================

    @Test
    void pauseUnpauseReconnectsWithZeroLoss() throws Exception {
        Project project = null;
        try {
            createTable("resil_pause");
            insertRows("resil_pause", 1, 200);
            project = newProject("cdc-pause", cdcConfig("resil_pause"));
            final Long projectId = project.getId();   // lambda capture (project is reassigned)
            manager.ensureStarted(project);
            waitFor("pause-test baseline done", 90_000, () -> baselineDone(projectId));
            waitFor("200 seeded rows on target", 30_000, () -> countTarget("resil_pause") == 200);
            // Streaming proof before the freeze: one row through the live binlog path.
            insertRow("resil_pause", 201, "pre-pause");
            waitFor("stream proof row on target", 30_000, () -> countTarget("resil_pause") == 201);
            final long n0 = 201;

            String containerId = createContainer().getContainerId();
            DockerClient docker = DockerClientFactory.instance().client();
            log.info("Pausing MySQL container {}", containerId);
            docker.pauseContainerCmd(containerId).exec();
            long pauseStart = System.nanoTime();
            boolean sawDisconnect = false;
            try {
                // 15s freeze — longer than the first reconnect backoff tiers (1s, 2s, 4s,
                // 8s), so the loop rolls into the ~16s tier per 验收 B②. The connected
                // flag is sampled as LOGGED EVIDENCE only: the client runs with
                // keepAlive(false), so the TCP connection may survive the pause without
                // the client ever noticing; the zero-loss assertions below hold either
                // way (and must not require observing a disconnect).
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (System.nanoTime() < deadline) {
                    CdcRuntimeStats s = manager.statsFor(projectId);
                    if (s != null && !s.isConnected()) {
                        sawDisconnect = true;
                    }
                    Thread.sleep(500);
                }
            } finally {
                docker.unpauseContainerCmd(containerId).exec();
                log.info("MEASURED pause window: {} ms; consumer observed disconnected during pause: {}",
                        ms(pauseStart), sawDisconnect);
            }

            // 100 more rows right after the unfreeze — the source connection needs retry
            // while mysqld thaws (each attempt is one transaction, so a broken attempt
            // rolls back and the retry cannot duplicate rows).
            insertRowsWithRetry("resil_pause", 202, 100);

            waitFor("zero-loss convergence after pause/unpause (N0+100)", 300_000,
                    () -> countTarget("resil_pause") == n0 + 100);
            log.info("MEASURED reconnect+catchup (pause start -> N0+100 consistent): {} ms",
                    ms(pauseStart));

            waitFor("consumer connected again", 60_000, () -> {
                CdcRuntimeStats s = manager.statsFor(projectId);
                return s != null && Boolean.TRUE.equals(s.snapshot().get("connected"));
            });
            // TRANSIENT failures never park a project: no ERROR hat across a reconnect.
            // (Not a liveness check — liveness is the converged count above.)
            SyncTask task = taskStore.find(projectId).orElseThrow();
            assertThat(task.getStatus())
                    .describedAs("lastSyncResult=%s", task.getLastSyncResult())
                    .isNotEqualTo(TaskStatus.ERROR);
        } finally {
            if (project != null) {
                finish(project);
            }
        }
    }

    // ================= 验收 B③ — purge → ERROR,一键重建基线 =================

    @Test
    void purgedBinlogSurfacesErrorAndRebaselineRecovers() throws Exception {
        Project project = null;
        try {
            createTable("purge_t");
            insertRows("purge_t", 1, 50);
            project = newProject("cdc-purge", cdcConfig("purge_t"));
            final Long projectId = project.getId();   // lambda capture (project is reassigned)
            manager.ensureStarted(project);
            waitFor("purge-test baseline done", 90_000, () -> baselineDone(projectId));
            waitFor("50 seeded rows on target", 30_000, () -> countTarget("purge_t") == 50);
            insertRow("purge_t", 51, "stream-proof");
            waitFor("stream proof row on target", 30_000, () -> countTarget("purge_t") == 51);

            // Fixture staging must be consumer-free: disable FIRST (ticks would relaunch),
            // then join our consumer. Its persisted position (GTID set included) now sits
            // strictly behind everything we are about to purge away.
            disableProject(project);
            manager.ensureStopped(projectId);

            // REAL PURGE path (择稳者用 — safe here because this class owns its container;
            // the hand-edited position-row fallback is not needed): advance the source
            // past the persisted position, roll to a fresh binlog file, then purge every
            // file before it. On resume the client's GTID set lacks transactions now in
            // gtid_purged → errno 1236 → CdcFailureClassifier.PURGED → fatal park.
            try (Connection src = sourceJdbc(); Statement st = src.createStatement()) {
                for (long id = 52; id <= 56; id++) {
                    st.execute("INSERT INTO purge_t VALUES (" + id + ", 'post-stop')");
                }
                st.execute("FLUSH BINARY LOGS");
                String newest = null;
                try (ResultSet rs = st.executeQuery("SHOW BINARY LOGS")) {
                    while (rs.next()) {
                        newest = rs.getString(1);   // listing is ordered; last = current
                    }
                }
                if (newest == null) {
                    throw new IllegalStateException("SHOW BINARY LOGS returned no files");
                }
                st.execute("PURGE BINARY LOGS TO '" + newest + "'");
                log.info("Purged all binary logs before {} on own container", newest);
            }

            Project enabled = reload(project);
            enabled.setEnabled(true);
            projectRepository.save(enabled);
            long reenableNs = System.nanoTime();
            // Idempotent with the 1s ticks: whoever launches first, the consumer resumes
            // from the stale persisted position and must die on the purged GTIDs.
            manager.ensureStarted(reload(project));

            waitFor("purged binlog surfaces ERROR hat", 90_000,
                    () -> taskStatus(projectId) == TaskStatus.ERROR);
            SyncTask parked = taskStore.find(projectId).orElseThrow();
            String result = String.valueOf(parked.getLastSyncResult()).toLowerCase(Locale.ROOT);
            assertThat(result)
                    .as("lastSyncResult must carry purge semantics")
                    .containsAnyOf("purged", "could not find first log");
            log.info("MEASURED purge -> ERROR hat: {} ms; lastSyncResult: {}",
                    ms(reenableNs), parked.getLastSyncResult());

            // Consumer thread stopped. T13 pins that a fatal exit does NOT remove the
            // registration itself (the UI keeps showing counters), but the next supervise
            // tick (1s) R2-prunes dead threads — so the honest assertion is: pruned, or
            // registered-but-not-connected. Never "statsFor != null" alone.
            waitFor("consumer thread stopped after fatal park", 15_000, () -> {
                CdcRuntimeStats s = manager.statsFor(projectId);
                return s == null || !s.isConnected();
            });

            // One-click rebaseline — the service-layer equivalent of Task 17's
            // POST /api/projects/{id}/cdc-rebaseline, pre-authorized sequence:
            //   positionStore.delete → markCdcRunning (lifts the ERROR hat, else
            //   ensureStarted silently skips) → ensureStarted. The delete happens while
            //   the hat is still on, so no tick can launch a consumer mid-sequence.
            long rebaselineNs = System.nanoTime();
            positionStore.delete(projectId);
            taskStore.markCdcRunning(projectId);
            manager.ensureStarted(reload(project));
            waitFor("rebaseline done", 120_000, () -> baselineDone(projectId));
            waitFor("target complete after rebaseline", 60_000, () -> countTarget("purge_t") == 56);
            assertThat(queryTarget("SELECT COUNT(*) c FROM purge_t WHERE id BETWEEN 52 AND 56")
                    .get(0).get("c")).isEqualTo(5L);
            waitFor("ERROR hat lifted (status back to RUNNING)", 30_000,
                    () -> taskStatus(projectId) == TaskStatus.RUNNING);
            log.info("MEASURED rebaseline (position delete -> baseline_done + 56 rows consistent): {} ms",
                    ms(rebaselineNs));
        } finally {
            if (project != null) {
                finish(project);
            }
        }
    }

    // ================= 验收 B⑤ — 双实例接管 ≤60s =================

    @Test
    void dualInstanceFailoverWithin60s() throws Exception {
        Project project = null;
        try {
            createTable("failover_t");
            insertRows("failover_t", 1, 100);
            project = newProject("cdc-failover", cdcConfig("failover_t"));
            final Long projectId = project.getId();   // lambda capture (project is reassigned)
            manager.ensureStarted(project);
            waitFor("failover-test baseline done", 90_000, () -> baselineDone(projectId));
            waitFor("100 seeded rows on target", 30_000, () -> countTarget("failover_t") == 100);
            insertRow("failover_t", 101, "stream-proof");
            waitFor("stream proof row on target", 30_000, () -> countTarget("failover_t") == 101);
            final long n0 = 101;

            // Stage the "other node crashed while holding the lease": disable FIRST so the
            // 1s ticks cannot relaunch our consumer, join it (its finally released the DB
            // lease → lockOwner null), then forge a foreign-owner lease with a 3s TTL —
            // acquire's WHERE clause accepts it because the row is unowned.
            disableProject(project);
            manager.ensureStopped(projectId);
            String deadNode = "dead-node:8080:ffffffffffff";
            assertThat(lockStore.acquire(projectId, deadNode, 3_000L))
                    .as("forged dead-node lease must land on the released lock row")
                    .isTrue();

            // Resume-vs-rebaseline proof (Task 14 idiom): a row deleted ONLY on the target
            // must stay deleted after the takeover — a silent rebaseline would refill it.
            try (Connection tgt = targetJdbc(); Statement st = tgt.createStatement()) {
                st.execute("DELETE FROM failover_t WHERE id = 5");
            }

            Project enabled = reload(project);
            enabled.setEnabled(true);
            projectRepository.save(enabled);
            long enableNs = System.nanoTime();

            Thread.sleep(4_000);   // the dead-node lease (ttl 3s) expires during this sleep

            waitFor("supervise takeover within 60s", 60_000, () -> {
                CdcRuntimeStats s = manager.statsFor(projectId);
                if (s == null || !Boolean.TRUE.equals(s.snapshot().get("connected"))) {
                    return false;
                }
                String owner = taskStore.find(projectId).map(SyncTask::getLockOwner).orElse(null);
                return lockService.getOwnerId().equals(owner);
            });
            long takeoverMs = ms(enableNs);
            log.info("MEASURED failover takeover (re-enable -> lease reacquired + binlog connected):"
                    + " {} ms (includes the 4s sleep past the 3s forged-lease TTL)", takeoverMs);
            assertThat(takeoverMs).as("takeover latency").isLessThan(60_000L);
            assertThat(taskStore.find(projectId).map(SyncTask::getLockOwner).orElse(null))
                    .isNotEqualTo(deadNode);

            // Zero-loss resume: 50 more rows land from the persisted position. Target
            // count = n0 + 50 - 1: the target-side id=5 we deleted stays absent (the
            // source never lost it, but a position resume does not re-send old events —
            // a rebaseline would have refilled it).
            insertRows("failover_t", 102, 50);
            waitFor("post-takeover zero-loss convergence", 60_000,
                    () -> countTarget("failover_t") == n0 + 50 - 1
                            && queryTarget("SELECT id FROM failover_t WHERE id = 151").size() == 1
                            && queryTarget("SELECT id FROM failover_t WHERE id = 5").isEmpty());
            assertThat(taskStatus(projectId)).isNotEqualTo(TaskStatus.ERROR);
        } finally {
            if (project != null) {
                finish(project);
            }
        }
    }

    // ================= helpers =================

    /** {@code (id BIGINT PK, v VARCHAR(64))} on BOTH schemas; DROP first for idempotent reruns. */
    private void createTable(String table) throws SQLException {
        String ddl = "CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, v VARCHAR(64))";
        try (Connection src = sourceJdbc(); Statement st = src.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute(ddl);
        }
        try (Connection tgt = targetJdbc(); Statement st = tgt.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute(ddl);
        }
    }

    private void insertRow(String table, long id, String v) throws SQLException {
        try (Connection src = sourceJdbc(); Statement st = src.createStatement()) {
            st.execute("INSERT INTO " + table + " VALUES (" + id + ", '" + v + "')");
        }
    }

    private void insertRows(String table, long fromId, long count) throws SQLException {
        try (Connection src = sourceJdbc(); Statement st = src.createStatement()) {
            for (long id = fromId; id < fromId + count; id++) {
                st.execute("INSERT INTO " + table + " VALUES (" + id + ", 'row-" + id + "')");
            }
        }
    }

    /**
     * Insert with connection retry for the post-unpause window, where mysqld may still be
     * thawing. Each attempt is ONE transaction: a broken attempt rolls back implicitly
     * when its connection dies, so retrying cannot duplicate rows.
     */
    private void insertRowsWithRetry(String table, long fromId, long count) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (true) {
            try (Connection src = sourceJdbc()) {
                src.setAutoCommit(false);
                try (Statement st = src.createStatement()) {
                    for (long id = fromId; id < fromId + count; id++) {
                        st.execute("INSERT INTO " + table + " VALUES (" + id + ", 'post-pause-" + id + "')");
                    }
                }
                src.commit();
                return;
            } catch (SQLException e) {
                if (System.nanoTime() >= deadline) {
                    throw e;
                }
                Thread.sleep(1_000);
            }
        }
    }

    private boolean baselineDone(Long projectId) {
        return positionStore.find(projectId)
                .map(p -> Boolean.TRUE.equals(p.isBaselineDone()))
                .orElse(false);
    }

    private TaskStatus taskStatus(Long projectId) {
        return taskStore.find(projectId).map(SyncTask::getStatus).orElse(null);
    }

    /** Stop, then DISABLE (ticks must not relaunch), then evict both Hikari pools. */
    private void finish(Project project) {
        manager.ensureStopped(project.getId());
        disableProject(project);
        dataSourceManager.evict(project.getSourceDbId());
        dataSourceManager.evict(project.getTargetDbId());
    }

    private SyncConfig cdcConfig(String... tables) {
        SyncConfig config = new SyncConfig();
        config.setCaptureStrategy(CaptureStrategy.BINLOG_CDC);
        for (String t : tables) {
            config.getTables().add(t);
        }
        return config;
    }

    private static long ms(long startNs) {
        return (System.nanoTime() - startNs) / 1_000_000;
    }
}

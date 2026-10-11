package com.qqmu.jync.it;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.BeforeAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.qqmu.jync.dto.SyncConfig;
import com.qqmu.jync.model.ConnectionRole;
import com.qqmu.jync.model.DatabaseConfig;
import com.qqmu.jync.model.DatabaseType;
import com.qqmu.jync.model.Project;
import com.qqmu.jync.repository.DatabaseConfigRepository;
import com.qqmu.jync.repository.ProjectRepository;
import com.qqmu.jync.service.task.SyncContextFactory;
import com.qqmu.jync.service.task.SyncTaskStore;
import com.qqmu.jync.util.CryptoUtil;

/**
 * Shared real-MySQL harness for the binlog CDC integration tests (task book D2 / 验收 B).
 *
 * <p>One {@code mysql:8.0} container for the whole IT family: started once in
 * {@code @BeforeAll} and deliberately NOT managed by {@code @Container} (the Testcontainers
 * extension would stop it after every class); Ryuk reclaims it when the JVM exits. The
 * command flags turn the stock image into a replication source: ROW-format binlog with the
 * FULL row image (the CDC mapper needs every column of the before-image for PK moves),
 * GTID mode on (positions resume through the executed set), and a fixed UTC clock so
 * binlog timestamps never wobble with the host zone.
 *
 * <p>Two schemas on that one instance — {@code src} (source) and {@code tgt} (target) —
 * created at startup; each side's JDBC URL simply points at one schema, so the whole
 * source→target pipeline runs against real binlog traffic without a second container.
 * Table structures are hand-created by each test (no StructureSyncService): these ITs
 * exercise the data plane only.
 *
 * <p>Both endpoints use the container's {@code root} account: a binlog consumer must
 * present REPLICATION SLAVE / REPLICATION CLIENT, which root has by definition. The
 * password travels through {@link CryptoUtil#encrypt} exactly like a UI-saved connection.
 *
 * <p>Spring side: the full application context on the default in-memory H2 metadata store
 * (test convention per {@code SecurityConfigTest} — never the file store, so the suite
 * leaves no {@code ./data} behind; {@code ddl-auto:update} creates {@code cdc_position}),
 * MOCK web environment (no port 8080 binding), and fast lease/supervise/reconcile
 * intervals because the takeover-oriented tests cannot wait out production 30s windows.
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
@TestPropertySource(properties = {
        "sync.cdc.lock-ttl-ms=3000",
        "sync.cdc.supervise-interval-ms=1000",
        "sync.lock-ttl-ms=3000",
        "sync.reconcile-interval-ms=1000",
        "sync.shutdown-wait-ms=5000",
        // Metadata store: the app's default H2, in-memory per the existing @SpringBootTest
        // convention. AI and the GitHub update check stay off — nothing reaches the network.
        "spring.datasource.url=jdbc:h2:mem:cdcit;MODE=MySQL;DB_CLOSE_DELAY=-1",
        "sync.ai.enabled=false",
        "app.update-check.enabled=false"
})
public abstract class CdcMySqlContainerHarnessIT {

    protected static final String SRC_SCHEMA = "src";
    protected static final String TGT_SCHEMA = "tgt";

    /** The single shared MySQL 8 replication source; see the class javadoc for the flags. */
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0")
            .withCommand("--server-id=1",
                    "--log-bin=mysql-bin",
                    "--binlog-format=ROW",
                    "--binlog-row-image=FULL",
                    "--gtid-mode=ON",
                    "--enforce-gtid-consistency=ON",
                    "--default-time-zone=+00:00");

    @Autowired
    protected ProjectRepository projectRepository;

    @Autowired
    protected DatabaseConfigRepository databaseConfigRepository;

    @Autowired
    protected SyncContextFactory contextFactory;

    @Autowired
    protected SyncTaskStore taskStore;

    @Autowired
    protected CryptoUtil cryptoUtil;

    @BeforeAll
    static void startMySql() throws SQLException {
        if (!MYSQL.isRunning()) {
            MYSQL.start();
        }
        try (Connection conn = DriverManager.getConnection(
                jdbcUrl(MYSQL.getDatabaseName()), "root", MYSQL.getPassword());
             Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE IF NOT EXISTS " + SRC_SCHEMA);
            st.execute("CREATE DATABASE IF NOT EXISTS " + TGT_SCHEMA);
        }
    }

    /** Direct root connection to one schema of the shared container (plain JDBC, no pool). */
    protected static String jdbcUrl(String schema) {
        return "jdbc:mysql://" + MYSQL.getHost() + ":"
                + MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT) + "/" + schema
                + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    }

    protected Connection sourceJdbc() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(SRC_SCHEMA), "root", MYSQL.getPassword());
    }

    protected Connection targetJdbc() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(TGT_SCHEMA), "root", MYSQL.getPassword());
    }

    /**
     * A fresh saved {@link DatabaseConfig} pointing at the container's {@code src} schema,
     * password encrypted like the UI saves it. One row per call on purpose: every project
     * gets its own config id, so {@code DataSourceManager}'s per-id pool cache never mixes
     * two projects' connections.
     */
    protected DatabaseConfig sourceConfig() {
        return savedConfig("cdc-it-src", SRC_SCHEMA, ConnectionRole.SOURCE);
    }

    /** The {@code tgt}-schema twin of {@link #sourceConfig()}. */
    protected DatabaseConfig targetConfig() {
        return savedConfig("cdc-it-tgt", TGT_SCHEMA, ConnectionRole.TARGET);
    }

    private DatabaseConfig savedConfig(String name, String schema, ConnectionRole role) {
        DatabaseConfig config = new DatabaseConfig();
        config.setName(name);
        config.setType(DatabaseType.MYSQL);
        config.setRole(role);
        config.setHost(MYSQL.getHost());
        config.setPort(MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT));
        config.setDatabaseName(schema);
        config.setSchemaName(schema);
        config.setUsername("root");                       // binlog needs REPLICATION SLAVE/CLIENT
        config.setPassword(cryptoUtil.encrypt(MYSQL.getPassword()));
        return databaseConfigRepository.save(config);
    }

    /**
     * Saves an enabled BINLOG_CDC-ready project wired to fresh src/tgt configs, with the
     * given {@link SyncConfig} serialized exactly the way {@code SyncContextFactory}
     * stores and parses it.
     *
     * <p>{@code enabled=true} is load-bearing: the 1s reconcile/supervise ticks converge
     * against that flag — a disabled project's consumer would be stopped by the very next
     * tick. {@code taskStore.ensureTask} mirrors {@code ProjectService.save}: without the
     * sync_task row, {@code markCdcRunning} / {@code recordCdcFailure} (and with it the B7
     * ERROR hat) would silently no-op for repository-created projects.
     */
    protected Project newProject(String name, SyncConfig config) {
        Project project = new Project();
        project.setName(name);
        project.setSourceDbId(sourceConfig().getId());
        project.setTargetDbId(targetConfig().getId());
        project.setSyncConfig(contextFactory.serializeConfig(config));
        project.setEnabled(true);
        Project saved = projectRepository.save(project);
        taskStore.ensureTask(saved);
        return saved;
    }

    /** Re-reads the project row — entities handed around a test go stale after restarts. */
    protected Project reload(Project project) {
        return projectRepository.findById(project.getId())
                .orElseThrow(() -> new AssertionError(
                        "Project " + project.getId() + " vanished from the repository"));
    }

    /**
     * Polls {@code condition} every 200ms until it holds; {@link AssertionError} on
     * timeout. No Awaitility — the project does not carry that dependency.
     */
    protected void waitFor(String what, long timeoutMs, BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            if (condition.getAsBoolean()) {
                return;
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new AssertionError(
                        "Timed out after " + timeoutMs + " ms waiting for: " + what);
            }
            try {
                Thread.sleep(Math.min(200L, remaining));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for: " + what, e);
            }
        }
    }

    /** Row count of one table in the target ({@code tgt}) schema. */
    protected long countTarget(String table) {
        return countTargetTable(table);
    }

    /** Same as {@link #countTarget}; second name kept for the brief's call sites. */
    protected long countTargetTable(String table) {
        String sql = "SELECT COUNT(*) FROM " + table;
        try (Connection conn = targetJdbc();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next()) {
                throw new IllegalStateException("COUNT(*) returned no row: " + sql);
            }
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException("Target count failed: " + sql, e);
        }
    }

    /** Runs a query against the target schema; rows are label-keyed maps of raw JDBC values. */
    protected List<Map<String, Object>> queryTarget(String sql) {
        try (Connection conn = targetJdbc()) {
            return queryAll(conn, sql);
        } catch (SQLException e) {
            throw new IllegalStateException("Target query failed: " + sql, e);
        }
    }

    /** Assertion utility: every row as a column-label → value map, in result-set order. */
    protected List<Map<String, Object>> queryAll(Connection conn, String sql) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                rows.add(row);
            }
        }
        return rows;
    }
}

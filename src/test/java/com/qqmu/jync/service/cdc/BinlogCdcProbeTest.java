package com.qqmu.jync.service.cdc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * B2 能力探测:七项检查,任何一项不过 => passed()==false 且 failures() 指出缺什么。
 * 探测失败不抛异常、不阻断保存项目(任务书 B2)。
 *
 * <p>Mock 意图:setUp 用 conn/stmt 两个 mock 拼出一台"一切正常"的 MySQL 8 源库
 * (元数据 + SHOW VARIABLES + SHOW GRANTS 全部健康),各测试再翻转其中一项,
 * 断言只有对应检查项失败——探测逻辑只依赖字符串 SQL,不需要真实数据库。
 */
class BinlogCdcProbeTest {

    private final BinlogCdcProbe probe = new BinlogCdcProbe();
    private Connection conn;
    private Statement stmt;

    @BeforeEach
    void setUp() throws SQLException {
        conn = mock(Connection.class);
        stmt = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt);
        DatabaseMetaData md = mock(DatabaseMetaData.class);
        when(md.getDatabaseProductName()).thenReturn("MySQL");
        when(md.getDatabaseProductVersion()).thenReturn("8.0.36");
        when(conn.getMetaData()).thenReturn(md);
        // 默认:一切正常
        variable("log_bin", "ON");
        variable("binlog_format", "ROW");
        variable("binlog_row_image", "FULL");
        variable("server_id", "1");
        variable("binlog_expire_logs_seconds", "2592000");
        grants("GRANT ALL PRIVILEGES ON *.* TO 'sync'@'%'");
    }

    /**
     * Mock 一行 SHOW VARIABLES 结果:next() 一次 true 后 false,getString(2) 给出变量值
     * (null 模拟列缺失)。每次 executeQuery 返回一份新 ResultSet——像真实服务器一样,
     * 同一测试内多次 probe() 每次都能读到相同的值。
     */
    private void variable(String name, String value) throws SQLException {
        when(stmt.executeQuery("SHOW VARIABLES LIKE '" + name + "'")).thenAnswer(invocation -> {
            ResultSet rs = mock(ResultSet.class);
            when(rs.next()).thenReturn(true, false);
            when(rs.getString(2)).thenReturn(value);
            return rs;
        });
    }

    /**
     * Mock SHOW GRANTS 结果集:next() 共 rows.length 次 true 然后 false;
     * getString(1) 按调用次序返回 rows[0], rows[1], ...
     * (Mockito varargs 语义:thenReturn(首值, 后续序列),序列耗尽后重复末值)。
     * 每次 executeQuery 返回一份新结果集,支持重复 probe()。
     */
    private void grants(String... rows) throws SQLException {
        when(stmt.executeQuery("SHOW GRANTS")).thenAnswer(invocation -> {
            ResultSet rs = mock(ResultSet.class);
            Boolean[] tail = new Boolean[rows.length];
            Arrays.fill(tail, Boolean.TRUE);
            tail[rows.length - 1] = Boolean.FALSE;
            when(rs.next()).thenReturn(Boolean.TRUE, tail);
            when(rs.getString(1)).thenReturn(rows[0], Arrays.copyOfRange(rows, 1, rows.length));
            return rs;
        });
    }

    private CdcProbeResult.Check check(CdcProbeResult result, String name) {
        return result.getChecks().stream()
                .filter(c -> c.getName().equals(name)).findFirst().orElseThrow();
    }

    @Test
    void healthySourcePassesAllChecks() {
        CdcProbeResult result = probe.probe(conn, 612001L);
        assertThat(result.passed()).isTrue();
        assertThat(result.getChecks()).hasSize(7);
    }

    @Test
    void binlogOffFailsLogBinOnly() throws SQLException {
        variable("log_bin", "OFF");
        CdcProbeResult result = probe.probe(conn, 612001L);
        assertThat(result.passed()).isFalse();
        assertThat(result.failures()).extracting(CdcProbeResult.Check::getName)
                .containsExactly("LOG_BIN");
    }

    @Test
    void statementFormatFails() throws SQLException {
        variable("binlog_format", "STATEMENT");
        CdcProbeResult result = probe.probe(conn, 612001L);
        assertThat(check(result, "BINLOG_FORMAT").isPassed()).isFalse();
        assertThat(check(result, "BINLOG_FORMAT").getDetail()).contains("STATEMENT");
    }

    @Test
    void minimalRowImageFails() throws SQLException {
        variable("binlog_row_image", "MINIMAL");
        assertThat(probe.probe(conn, 612001L).passed()).isFalse();
    }

    @Test
    void serverIdZeroOrConflictFails() throws SQLException {
        variable("server_id", "0");
        assertThat(check(probe.probe(conn, 612001L), "SERVER_ID").isPassed()).isFalse();

        variable("server_id", "612001"); // 与本工具伪装 id 撞车
        CdcProbeResult conflict = probe.probe(conn, 612001L);
        assertThat(check(conflict, "SERVER_ID").isPassed()).isFalse();
        assertThat(check(conflict, "SERVER_ID").getDetail()).contains("612001");

        // plannedServerId 未知(连接页探测)时只查非 0
        variable("server_id", "612001");
        assertThat(check(probe.probe(conn, 0L), "SERVER_ID").isPassed()).isTrue();
    }

    @Test
    void serverIdConflictDetailPinsTheCollidesWording() throws SQLException {
        // CdcApiController.SERVER_ID_COLLISION_MARKER 靠 detail 里的 "collides" 一词
        // 推导 serverIdConflict;改掉措辞会让冲突标记静默变 false(前端不再给出
        // 调 server-id-offset 的指引)。这枚钉子保证探测侧与控制器侧同步。
        variable("server_id", "612001");
        CdcProbeResult.Check conflict = check(probe.probe(conn, 612001L), "SERVER_ID");
        assertThat(conflict.isPassed()).isFalse();
        assertThat(conflict.getDetail()).contains("collides");

        // 非冲突的 SERVER_ID 失败(id 为 0)绝不能带这个词,否则控制器误判为冲突。
        variable("server_id", "0");
        CdcProbeResult.Check zero = check(probe.probe(conn, 612001L), "SERVER_ID");
        assertThat(zero.isPassed()).isFalse();
        assertThat(zero.getDetail()).doesNotContain("collides");
    }

    @Test
    void missingReplicationGrantsFail() throws SQLException {
        grants("GRANT SELECT, INSERT ON `app`.* TO 'sync'@'%'");
        CdcProbeResult result = probe.probe(conn, 612001L);
        assertThat(check(result, "REPLICATION_GRANTS").isPassed()).isFalse();
    }

    @Test
    void replicationSlaveOnlyIsNotEnough() throws SQLException {
        grants("GRANT REPLICATION SLAVE ON *.* TO 'sync'@'%'");
        assertThat(check(probe.probe(conn, 612001L), "REPLICATION_GRANTS").isPassed()).isFalse();
    }

    @Test
    void shortRetentionFailsWithAdvice() throws SQLException {
        variable("binlog_expire_logs_seconds", "3600"); // 1h,撑不过停机窗口
        CdcProbeResult.Check retention = check(probe.probe(conn, 612001L), "BINLOG_RETENTION");
        assertThat(retention.isPassed()).isFalse();
        assertThat(retention.getDetail()).contains("3600");
    }

    @Test
    void legacyExpireLogsDaysUnderstood() throws SQLException {
        variable("binlog_expire_logs_seconds", null);
        variable("expire_logs_days", "7");
        CdcProbeResult.Check retention = check(probe.probe(conn, 612001L), "BINLOG_RETENTION");
        assertThat(retention.isPassed()).isTrue();
        assertThat(retention.getDetail()).contains("7");
    }

    @Test
    void mariadbRejectedEvenUnderMysqlType() throws SQLException {
        DatabaseMetaData md = mock(DatabaseMetaData.class);
        when(md.getDatabaseProductName()).thenReturn("MySQL");
        when(md.getDatabaseProductVersion()).thenReturn("5.5.5-10.11.6-MariaDB");
        when(conn.getMetaData()).thenReturn(md);
        assertThat(check(probe.probe(conn, 0L), "VERSION").isPassed()).isFalse();
    }

    @Test
    void ancientVersionRejected() throws SQLException {
        DatabaseMetaData md = mock(DatabaseMetaData.class);
        when(md.getDatabaseProductName()).thenReturn("MySQL");
        when(md.getDatabaseProductVersion()).thenReturn("5.6.51-log");
        when(conn.getMetaData()).thenReturn(md);
        assertThat(check(probe.probe(conn, 0L), "VERSION").isPassed()).isFalse();
    }
}

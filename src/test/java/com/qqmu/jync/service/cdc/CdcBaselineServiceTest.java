package com.qqmu.jync.service.cdc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import com.qqmu.jync.dto.SyncResult;
import com.qqmu.jync.model.DatabaseConfig;
import com.qqmu.jync.model.DatabaseType;
import com.qqmu.jync.service.connection.DataSourceManager;
import com.qqmu.jync.service.sync.SyncContext;
import com.qqmu.jync.service.sync.SyncEngine;
import com.qqmu.jync.service.sync.SyncStateWriter;
import com.qqmu.jync.service.task.SyncTaskStore;
import com.qqmu.jync.util.CryptoUtil;

class CdcBaselineServiceTest {

    private final DataSourceManager dataSourceManager = mock(DataSourceManager.class);
    private final SyncEngine engine = mock(SyncEngine.class);
    private final CdcPositionStore positionStore = mock(CdcPositionStore.class);
    private final SyncStateWriter stateWriter = mock(SyncStateWriter.class);
    private final SyncTaskStore taskStore = mock(SyncTaskStore.class);
    private final CryptoUtil crypto = new CryptoUtil(props());   // @Component,构造吃 SyncProperties(默认 key/salt)
    private final BinlogClientFactory clientFactory =
            new BinlogClientFactory(crypto, props());
    private final CdcBaselineService service = new CdcBaselineService(
            positionStore, clientFactory, stateWriter, taskStore, dataSourceManager, engine);

    private static com.qqmu.jync.config.SyncProperties props() {
        return new com.qqmu.jync.config.SyncProperties();
    }

    /** SHOW BINARY LOG STATUS 可用(8.4+)、gtid_mode=ON 的源库。 */
    private Connection stubSource(String file, long pos, String gtidMode, String gtidExecuted)
            throws SQLException {
        Connection conn = mock(Connection.class);
        Statement st = mock(Statement.class);
        when(conn.createStatement()).thenReturn(st);
        ResultSet rs = mock(ResultSet.class);
        when(st.executeQuery(any(String.class))).thenAnswer(inv -> {
            String sql = inv.getArgument(0);
            if (sql.contains("SHOW BINARY LOG STATUS") || sql.contains("SHOW MASTER STATUS")) {
                when(rs.next()).thenReturn(true, false);
                when(rs.getString("File")).thenReturn(file);
                when(rs.getLong("Position")).thenReturn(pos);
            } else if (sql.contains("gtid_mode")) {
                when(rs.next()).thenReturn(true, false);
                when(rs.getString(1)).thenReturn(gtidMode);
            } else if (sql.contains("gtid_executed")) {
                when(rs.next()).thenReturn(true, false);
                when(rs.getString(1)).thenReturn(gtidExecuted);
            }
            return rs;
        });
        return conn;
    }

    private DatabaseConfig source() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType(DatabaseType.MYSQL);
        config.setHost("db.local");
        config.setPort(3306);
        config.setUsername("repl");
        config.setPassword(crypto.encrypt("secret"));
        return config;
    }

    private SyncContext ctx(long projectId) {
        com.qqmu.jync.model.Project project = new com.qqmu.jync.model.Project();
        project.setId(projectId);
        project.setName("cdc-test");
        // Project 实体没有 sourceConfig 字段(只有 sourceDbId 外键);ctx 的 sourceConfig
        // 由 SyncContextFactory 解析注入,测试直接塞 builder
        return SyncContext.builder().project(project).sourceConfig(source()).build();
    }

    @Test
    void readsPositionFromModernStatement() throws SQLException {
        // stubSource 先于 when(...) 求值:它自己也在打桩,塞进 thenReturn 实参位会撞上
        // Mockito 的 UnfinishedStubbing(嵌套打桩),语义不变、只求值顺序提前。
        Connection sourceConn = stubSource("mysql-bin.000003", 154L, "ON", "uuid:1-9");
        when(dataSourceManager.getConnection(any())).thenReturn(sourceConn);
        CdcPositionMark mark = service.readCurrentPosition(source());
        assertThat(mark.getFile()).isEqualTo("mysql-bin.000003");
        assertThat(mark.getPos()).isEqualTo(154L);
        assertThat(mark.getGtidSet()).isEqualTo("uuid:1-9");
    }

    @Test
    void fallsBackToMasterStatusWhenModernMissing() throws SQLException {
        Connection conn = mock(Connection.class);
        Statement st = mock(Statement.class);
        when(conn.createStatement()).thenReturn(st);
        when(st.executeQuery("SHOW BINARY LOG STATUS"))
                .thenThrow(new SQLException("unknown statement"));
        ResultSet rs = mock(ResultSet.class);
        when(st.executeQuery("SHOW MASTER STATUS")).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("File")).thenReturn("mysql-bin.000001");
        when(rs.getLong("Position")).thenReturn(4L);
        ResultSet gtidOff = mock(ResultSet.class);
        when(st.executeQuery("SELECT @@GLOBAL.gtid_mode")).thenReturn(gtidOff);
        when(gtidOff.next()).thenReturn(true, false);
        when(gtidOff.getString(1)).thenReturn("OFF");
        when(dataSourceManager.getConnection(any())).thenReturn(conn);

        CdcPositionMark mark = service.readCurrentPosition(source());
        assertThat(mark.getFile()).isEqualTo("mysql-bin.000001");
        assertThat(mark.getPos()).isEqualTo(4L);
        assertThat(mark.getGtidSet()).isNull();
    }

    @Test
    void baselineSequenceIsPositionResetRunMarkDone() throws SQLException {
        Connection sourceConn = stubSource("mysql-bin.000003", 154L, "OFF", null);
        when(dataSourceManager.getConnection(any())).thenReturn(sourceConn);
        // 真实 runCycle 不抛异常、把失败折进 result:成功的基线必须显式给出 success 结果
        when(engine.runCycle(any(SyncContext.class))).thenReturn(new SyncResult());

        service.runBaseline(ctx(7L), () -> true);

        InOrder order = inOrder(positionStore, stateWriter, engine);
        order.verify(positionStore).recordBaselineStart(eq(7L), eq("mysql-bin.000003"),
                eq(154L), org.mockito.ArgumentMatchers.isNull(), anyLong());
        order.verify(stateWriter).resetProject(7L);
        order.verify(engine).runCycle(any(SyncContext.class));
        order.verify(positionStore).markBaselineDone(7L);
        // 关键顺序:位点先落库再全量——全量期间发生的变更不会漏(B4)
    }

    @Test
    void failureRecordsErrorAndDeletesHalfWrittenPosition() throws SQLException {
        Connection sourceConn = stubSource("mysql-bin.000003", 154L, "OFF", null);
        when(dataSourceManager.getConnection(any())).thenReturn(sourceConn);
        doThrow(new IllegalStateException("full load exploded"))
                .when(engine).runCycle(any(SyncContext.class));

        assertThatThrownBy(() -> service.runBaseline(ctx(7L), () -> true))
                .isInstanceOf(IllegalStateException.class);
        verify(positionStore).delete(7L);                       // 残缺位点必须清掉
        verify(positionStore, never()).markBaselineDone(anyLong());
        verify(taskStore).recordCdcFailure(eq(7L), org.mockito.ArgumentMatchers.contains("exploded"));
    }

    @Test
    void failedFullLoadDoesNotMarkBaselineDone() throws SQLException {
        Connection sourceConn = stubSource("mysql-bin.000003", 154L, "OFF", null);
        when(dataSourceManager.getConnection(any())).thenReturn(sourceConn);
        // runCycle 把 SQLException/LockLost/RuntimeException 折进 result 而不外抛
        // (SyncEngine.java:144-159):失败的全量必须按失败处理,否则残缺快照 +
        // baseline_done=true,流从记下的位点回放,漏掉的行永久丢失(任务书 B4:
        // 全量失败整体重来)。
        SyncResult failed = new SyncResult();
        failed.addError("lease lost mid-load");
        when(engine.runCycle(any(SyncContext.class))).thenReturn(failed);

        assertThatThrownBy(() -> service.runBaseline(ctx(7L), () -> true))
                .isInstanceOf(IllegalStateException.class);
        verify(positionStore).delete(7L);                       // 残缺位点必须清掉
        verify(positionStore, never()).markBaselineDone(anyLong());
        verify(taskStore).recordCdcFailure(eq(7L),
                org.mockito.ArgumentMatchers.contains("lease lost"));
    }
}

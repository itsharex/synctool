package com.qqmu.jync.service.cdc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.qqmu.jync.config.SyncProperties;
import com.qqmu.jync.dto.SyncConfig;
import com.qqmu.jync.dto.meta.ColumnMeta;
import com.qqmu.jync.dto.meta.TableMeta;
import com.qqmu.jync.model.CdcPosition;
import com.qqmu.jync.model.ChangeType;
import com.qqmu.jync.model.ObjectType;
import com.qqmu.jync.model.Project;
import com.qqmu.jync.service.connection.DataSourceManager;
import com.qqmu.jync.service.converter.GenericSqlDialect;
import com.qqmu.jync.service.sync.SyncContext;
import com.qqmu.jync.service.sync.SyncStateWriter;
import com.qqmu.jync.service.task.SyncLockService;
import com.qqmu.jync.service.task.SyncTaskStore;

/**
 * B6/B7 消费者线程的两条控制器裁定:
 *
 * <ul>
 *   <li>R1 —— 租约丢失必须安静退出(INFO 日志 + 释放锁),绝不 recordCdcFailure/ERROR:
 *       接管节点可能已把状态标成 RUNNING,ERROR 会把它踩掉并让所有节点的 supervise
 *       因 ERROR-skip 冻结。</li>
 *   <li>R3 —— 消费者构造 applier 时必须带上 DataSyncService 同源的查询超时
 *       (SyncProperties.queryTimeoutSeconds),经 3 参构造传给每条语句。</li>
 * </ul>
 */
class CdcConsumerTest {

    private static Project project(long id) {
        Project project = new Project();
        project.setId(id);
        project.setName("cdc-" + id);
        return project;
    }

    private static CdcPosition baselineDonePosition(long projectId, String file) {
        CdcPosition position = new CdcPosition();
        position.setProjectId(projectId);
        position.setBinlogFile(file);
        position.setBinlogPos(4L);
        position.setBaselineDone(true);
        return position;
    }

    @Test
    void leaseLossExitsQuietlyWithoutError() throws Exception {
        Project project = project(7L);
        SyncLockService.LockHandle lock = mock(SyncLockService.LockHandle.class);
        when(lock.renew()).thenReturn(false);                 // 租约已被他节点接管
        SyncContext ctx = SyncContext.builder()
                .project(project).config(new SyncConfig()).build();
        BinlogClientFactory clientFactory = mock(BinlogClientFactory.class);
        CdcPositionStore positionStore = mock(CdcPositionStore.class);
        when(positionStore.find(7L))
                .thenReturn(Optional.of(baselineDonePosition(7L, "mysql-bin.000004")));
        CdcBaselineService baselineService = mock(CdcBaselineService.class);
        SyncTaskStore taskStore = mock(SyncTaskStore.class);
        SyncStateWriter stateWriter = mock(SyncStateWriter.class);
        DataSourceManager dataSourceManager = mock(DataSourceManager.class);

        CdcConsumer consumer = new CdcConsumer(project, lock, ctx, clientFactory, positionStore,
                baselineService, taskStore, stateWriter, dataSourceManager,
                new CdcRuntimeStats(), new SyncProperties());

        Thread thread = new Thread(consumer, "cdc-test-7");
        thread.start();
        thread.join(10_000);

        assertThat(thread.isAlive()).isFalse();               // 快速安静退出
        verify(baselineService, never()).runBaseline(any(), any()); // 基线已完成,不重跑
        verify(clientFactory, never()).createClient(any(), anyLong(), any()); // 不再建流
        verify(taskStore, never()).recordCdcFailure(anyLong(), any());        // 绝不是致命错
        verify(stateWriter, never()).recordLog(anyLong(), any(), anyString(),
                eq(ChangeType.ERROR), anyString(), anyBoolean(), anyLong());  // 绝不写 ERROR
        verify(stateWriter).recordLog(7L, ObjectType.PROJECT, "cdc-7", ChangeType.INFO,
                "CDC consumer stopped: lease lost", true, 0L);
        verify(lock).close();                                  // 租约句柄仍按 owner 守卫释放
    }

    @Test
    void consumerBuildsItsApplierWithTheConfiguredQueryTimeout() throws SQLException {
        // R3:applier 的超时值与 DataSyncService 同源——SyncProperties.queryTimeoutSeconds。
        SyncProperties properties = new SyncProperties();
        properties.setQueryTimeoutSeconds(45);
        Project project = project(8L);
        SyncContext ctx = SyncContext.builder()
                .project(project)
                .config(new SyncConfig())
                .targetDialect(new GenericSqlDialect())
                .batchSize(100)
                .build();
        CdcConsumer consumer = new CdcConsumer(project, mock(SyncLockService.LockHandle.class),
                ctx, mock(BinlogClientFactory.class), mock(CdcPositionStore.class),
                mock(CdcBaselineService.class), mock(SyncTaskStore.class),
                mock(SyncStateWriter.class), mock(DataSourceManager.class),
                new CdcRuntimeStats(), properties);

        Connection target = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        when(target.prepareStatement(anyString())).thenReturn(ps);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 1L);
        row.put("name", "a");
        consumer.applierForTest().applyUpserts(target, pkBinding(), List.of(row));

        verify(ps, atLeastOnce()).setQueryTimeout(45);
    }

    // —— FIX B(评审 Important):supplier 不得吞掉瞬态元数据失败 ——
    // 返回 null =「表不受支持」→ handler 跳过该表行事件,事务照常提交、位点照常前进
    // = 静默永久丢行(at-least-once 被打破)。瞬态失败必须抛出:异常经监听器 →
    // onCommunicationFailure → TRANSIENT 分类 → 断连重连,从上次位点整事务重放。

    @Test
    void supplierPropagatesTransientMetadataFailureInsteadOfReturningNull() throws Exception {
        Project project = project(9L);
        SyncContext ctx = SyncContext.builder().project(project).config(new SyncConfig()).build();
        DataSourceManager dataSourceManager = mock(DataSourceManager.class);
        SQLException poolDown = new SQLException("pool exhausted");
        when(dataSourceManager.getConnection(any())).thenThrow(poolDown);
        CdcConsumer consumer = new CdcConsumer(project, mock(SyncLockService.LockHandle.class),
                ctx, mock(BinlogClientFactory.class), mock(CdcPositionStore.class),
                mock(CdcBaselineService.class), mock(SyncTaskStore.class),
                mock(SyncStateWriter.class), dataSourceManager,
                new CdcRuntimeStats(), new SyncProperties());

        assertThatThrownBy(() -> consumer.supplierForTest().binding("src", "orders"))
                .isInstanceOf(RuntimeException.class)
                .hasCause(poolDown);                             // 抛出(带原因),绝不返回 null
    }

    @Test
    void supplierStillReturnsNullForAGenuinelyAbsentTable() throws Exception {
        // 唯一合法的 null:表真的不在(information_schema 查无,例如流中途被 DROP)
        // → handler 按 unsupported 跳过该表行事件,流继续。
        Project project = project(10L);
        SyncContext ctx = SyncContext.builder().project(project).config(new SyncConfig()).build();
        DataSourceManager dataSourceManager = mock(DataSourceManager.class);
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(dataSourceManager.getConnection(any())).thenReturn(conn);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(false);                       // 表不存在
        CdcConsumer consumer = new CdcConsumer(project, mock(SyncLockService.LockHandle.class),
                ctx, mock(BinlogClientFactory.class), mock(CdcPositionStore.class),
                mock(CdcBaselineService.class), mock(SyncTaskStore.class),
                mock(SyncStateWriter.class), dataSourceManager,
                new CdcRuntimeStats(), new SyncProperties());

        assertThat(consumer.supplierForTest().binding("src", "gone")).isNull();
    }

    private static CdcTableBinding pkBinding() {
        TableMeta meta = new TableMeta();
        meta.setName("orders");
        ColumnMeta id = new ColumnMeta();
        id.setName("id");
        id.setJdbcType(Types.BIGINT);
        id.setPrimaryKey(true);
        ColumnMeta name = new ColumnMeta();
        name.setName("name");
        name.setJdbcType(Types.VARCHAR);
        meta.getColumns().add(id);
        meta.getColumns().add(name);
        meta.getPrimaryKeys().add("id");
        return CdcTableBinding.of("src", "orders", meta, "orders", null);
    }
}

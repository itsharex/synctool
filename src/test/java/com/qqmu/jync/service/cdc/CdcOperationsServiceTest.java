package com.qqmu.jync.service.cdc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.qqmu.jync.model.DatabaseConfig;
import com.qqmu.jync.model.DatabaseType;
import com.qqmu.jync.model.Project;
import com.qqmu.jync.repository.ChangeLogRepository;
import com.qqmu.jync.repository.DatabaseConfigRepository;
import com.qqmu.jync.repository.ProjectRepository;
import com.qqmu.jync.service.AdminAuditService;
import com.qqmu.jync.service.connection.DataSourceManager;
import com.qqmu.jync.service.sync.SyncStateWriter;
import com.qqmu.jync.service.task.SyncTaskStore;

/**
 * probeProject 三用例(任务书 Task 16 Step 1);rebaseline 两用例由 Task 17 追加到本文件。
 *
 * <p>构造一次到位:除探测五件外还吃 CdcConsumerManager/CdcPositionStore/SyncStateWriter/
 * SyncTaskStore/AdminAuditService/ChangeLogRepository,全是 Task 17 rebaseline 的依赖,
 * 避免两个任务之间再改构造。源库 stub 照抄 CdcBaselineServiceTest 的做法(mock
 * Connection + DatabaseConfig 实体直填)。
 */
@ExtendWith(MockitoExtension.class)
class CdcOperationsServiceTest {

    @Mock private ProjectRepository projectRepository;
    @Mock private DatabaseConfigRepository databaseConfigRepository;
    @Mock private DataSourceManager dataSourceManager;
    @Mock private BinlogCdcProbe probe;
    @Mock private BinlogClientFactory clientFactory;
    @Mock private CdcConsumerManager cdcManager;
    @Mock private CdcPositionStore positionStore;
    @Mock private SyncStateWriter stateWriter;
    @Mock private SyncTaskStore taskStore;
    @Mock private AdminAuditService auditService;
    @Mock private ChangeLogRepository changeLogRepository;

    private CdcOperationsService service;

    @BeforeEach
    void setUp() {
        service = new CdcOperationsService(projectRepository, databaseConfigRepository,
                dataSourceManager, probe, clientFactory, cdcManager, positionStore,
                stateWriter, taskStore, auditService, changeLogRepository);
    }

    private Project project() {
        Project p = new Project();
        p.setId(1L);
        p.setName("proj");
        p.setSourceDbId(5L);
        when(projectRepository.findById(1L)).thenReturn(Optional.of(p));
        return p;
    }

    private DatabaseConfig source(DatabaseType type) {
        DatabaseConfig config = new DatabaseConfig();
        config.setId(5L);
        config.setType(type);
        config.setHost("db.local");
        config.setPort(3306);
        config.setUsername("repl");
        when(databaseConfigRepository.findById(5L)).thenReturn(Optional.of(config));
        return config;
    }

    /** 5 过 2 败(SERVER_ID 冲突 + 权限缺失),探测逐项返回、总判定为不可用。 */
    private static CdcProbeResult fivePassTwoFail() {
        return CdcProbeResult.of(List.of(
                CdcProbeResult.Check.of(CdcProbeResult.VERSION, true, "MySQL 8.0.36"),
                CdcProbeResult.Check.of(CdcProbeResult.LOG_BIN, true, "ON"),
                CdcProbeResult.Check.of(CdcProbeResult.BINLOG_FORMAT, true, "ROW"),
                CdcProbeResult.Check.of(CdcProbeResult.BINLOG_ROW_IMAGE, true, "FULL"),
                CdcProbeResult.Check.of(CdcProbeResult.SERVER_ID, false,
                        "source server_id 612001 collides with the id Jync would present (612001)"
                                + " — change sync.cdc.server-id-offset"),
                CdcProbeResult.Check.of(CdcProbeResult.REPLICATION_GRANTS, false,
                        "missing grant(s): REPLICATION SLAVE"),
                CdcProbeResult.Check.of(CdcProbeResult.BINLOG_RETENTION, true,
                        "binlog_expire_logs_seconds=604800")));
    }

    @Test
    void probeProjectReturnsItemizedChecks() throws SQLException {
        project();
        DatabaseConfig source = source(DatabaseType.MYSQL);
        Connection conn = org.mockito.Mockito.mock(Connection.class);
        when(dataSourceManager.getConnection(any(DatabaseConfig.class))).thenReturn(conn);
        when(clientFactory.serverIdFor(1L)).thenReturn(612001L);
        when(probe.probe(any(Connection.class), anyLong())).thenReturn(fivePassTwoFail());

        CdcProbeResult result = service.probeProject(1L);

        assertThat(result.passed()).isFalse();
        assertThat(result.getChecks()).hasSize(7);
        // serverIdConflict 由 SERVER_ID 项推导:该项失败且 detail 指出与计划 id 相撞。
        CdcProbeResult.Check serverId = result.getChecks().stream()
                .filter(c -> CdcProbeResult.SERVER_ID.equals(c.getName()))
                .findFirst().orElseThrow();
        assertThat(serverId.isPassed()).isFalse();
        assertThat(serverId.getDetail()).contains("612001");
        // 探测收到的 plannedServerId 必须就是 serverIdFor(projectId)。
        ArgumentCaptor<Long> plannedServerId = ArgumentCaptor.forClass(Long.class);
        verify(probe).probe(same(conn), plannedServerId.capture());
        assertThat(plannedServerId.getValue()).isEqualTo(612001L);
        assertThat(plannedServerId.getValue()).isEqualTo(clientFactory.serverIdFor(1L));
        assertThat(source.getType()).isEqualTo(DatabaseType.MYSQL);
    }

    @Test
    void probeProjectRejectsNonMysql() throws SQLException {
        project();
        source(DatabaseType.POSTGRESQL);

        assertThatThrownBy(() -> service.probeProject(1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("cdc.probe.not.mysql");
        verify(dataSourceManager, never()).getConnection(any());
    }

    @Test
    void probeProjectSwallowsConnectionFailure() throws SQLException {
        project();
        source(DatabaseType.MYSQL);
        when(dataSourceManager.getConnection(any(DatabaseConfig.class)))
                .thenThrow(new SQLException("connection refused by db.local:3306"));

        // 与 ConnectionTestService.test 永不抛的既有约定一致:探测失败是结果,不是异常。
        CdcProbeResult[] result = new CdcProbeResult[1];
        assertThatCode(() -> result[0] = service.probeProject(1L)).doesNotThrowAnyException();

        assertThat(result[0].passed()).isFalse();
        CdcProbeResult.Check version = result[0].getChecks().stream()
                .filter(c -> CdcProbeResult.VERSION.equals(c.getName()))
                .findFirst().orElseThrow();
        assertThat(version.isPassed()).isFalse();
        assertThat(version.getDetail()).contains("connection refused by db.local:3306");
    }
}

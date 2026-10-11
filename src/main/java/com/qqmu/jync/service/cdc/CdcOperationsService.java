package com.qqmu.jync.service.cdc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.springframework.stereotype.Service;

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

import lombok.extern.slf4j.Slf4j;

/**
 * On-demand CDC operations against an existing project (task book v1.5.0): the per-project
 * capability probe now, "rebuild baseline" (rebaseline, Task 17) next.
 *
 * <p>The constructor already takes every collaborator Task 17's rebaseline needs
 * (consumer manager, position store, state writer, task store, audit, change log) so the
 * follow-up task adds a method without touching the wiring again.
 *
 * <p>{@link #probeProject} never throws for a merely-unready or unreachable source — it
 * degrades to a failed {@link CdcProbeResult}, matching {@code BinlogCdcProbe} and
 * {@code ConnectionTestService.test}: a broken probe must neither block the detail page
 * nor the save flow (B2).
 */
@Service
@Slf4j
public class CdcOperationsService {

    private final ProjectRepository projectRepository;
    private final DatabaseConfigRepository databaseConfigRepository;
    private final DataSourceManager dataSourceManager;
    private final BinlogCdcProbe probe;
    private final BinlogClientFactory clientFactory;
    private final CdcConsumerManager cdcManager;
    private final CdcPositionStore positionStore;
    private final SyncStateWriter stateWriter;
    private final SyncTaskStore taskStore;
    private final AdminAuditService auditService;
    private final ChangeLogRepository changeLogRepository;

    public CdcOperationsService(ProjectRepository projectRepository,
                                DatabaseConfigRepository databaseConfigRepository,
                                DataSourceManager dataSourceManager,
                                BinlogCdcProbe probe,
                                BinlogClientFactory clientFactory,
                                CdcConsumerManager cdcManager,
                                CdcPositionStore positionStore,
                                SyncStateWriter stateWriter,
                                SyncTaskStore taskStore,
                                AdminAuditService auditService,
                                ChangeLogRepository changeLogRepository) {
        this.projectRepository = projectRepository;
        this.databaseConfigRepository = databaseConfigRepository;
        this.dataSourceManager = dataSourceManager;
        this.probe = probe;
        this.clientFactory = clientFactory;
        this.cdcManager = cdcManager;
        this.positionStore = positionStore;
        this.stateWriter = stateWriter;
        this.taskStore = taskStore;
        this.auditService = auditService;
        this.changeLogRepository = changeLogRepository;
    }

    /**
     * Runs the itemized B2 capability probe against a project's real source, presenting
     * the same server id the consumer would later use ({@link BinlogClientFactory#serverIdFor}),
     * so a collision shows up here rather than at connect time.
     *
     * @throws IllegalArgumentException with the bare key {@code cdc.probe.not.mysql} when
     *         the project or its source is missing or not strictly MySQL (Task 17's API
     *         controller turns that into a 400)
     */
    public CdcProbeResult probeProject(Long projectId) {
        Project project = projectRepository.findById(projectId)
                .orElseThrow(() -> new IllegalArgumentException("error.project.not.found"));
        DatabaseConfig source = project.getSourceDbId() == null ? null
                : databaseConfigRepository.findById(project.getSourceDbId()).orElse(null);
        // Strict equality on purpose: MariaDB/OceanBase speak a different binlog dialect.
        if (source == null || source.getType() != DatabaseType.MYSQL) {
            throw new IllegalArgumentException("cdc.probe.not.mysql");
        }
        long plannedServerId = clientFactory.serverIdFor(projectId);
        try (Connection conn = dataSourceManager.getConnection(source)) {
            return probe.probe(conn, plannedServerId);
        } catch (SQLException | RuntimeException e) {
            // Never an exception to the caller: degrade to a failed check carrying the
            // driver's message, mirroring BinlogCdcProbe.checkVersion's own SQLException path.
            log.debug("CDC probe could not reach the source of project {}: {}",
                    projectId, e.toString());
            return CdcProbeResult.of(List.of(CdcProbeResult.Check.of(CdcProbeResult.VERSION,
                    false, String.valueOf(e.getMessage()))));
        }
    }
}

package com.qqmu.jync.service.cdc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.BooleanSupplier;

import org.springframework.stereotype.Service;

import com.qqmu.jync.model.DatabaseConfig;
import com.qqmu.jync.service.connection.DataSourceManager;
import com.qqmu.jync.service.sync.SyncContext;
import com.qqmu.jync.service.sync.SyncEngine;
import com.qqmu.jync.service.sync.SyncStateWriter;
import com.qqmu.jync.service.task.SyncTaskStore;

import lombok.extern.slf4j.Slf4j;

/**
 * Records the position, re-seeds the target with a chunked full load, then hands off
 * to streaming (task book B4: 记位点 → 分块全量 → 回放). The overlap between the
 * snapshot and the stream is absorbed by idempotent upserts — nothing in this class
 * needs to be exactly-once.
 *
 * <p>The position row is written BEFORE the full load starts, so events that happen
 * during the load are already covered by the resume point. baseline_done flips to
 * true only after the load completes; a crash mid-baseline leaves it false and the
 * whole baseline reruns (Review Focus #4).
 */
@Slf4j
@Service
public class CdcBaselineService {

    private final CdcPositionStore positionStore;
    private final BinlogClientFactory clientFactory;
    private final SyncStateWriter stateWriter;
    private final SyncTaskStore taskStore;
    private final DataSourceManager dataSourceManager;
    private final SyncEngine engine;

    public CdcBaselineService(CdcPositionStore positionStore, BinlogClientFactory clientFactory,
                              SyncStateWriter stateWriter, SyncTaskStore taskStore,
                              DataSourceManager dataSourceManager, SyncEngine engine) {
        this.positionStore = positionStore;
        this.clientFactory = clientFactory;
        this.stateWriter = stateWriter;
        this.taskStore = taskStore;
        this.dataSourceManager = dataSourceManager;
        this.engine = engine;
    }

    /** SHOW BINARY LOG STATUS (8.4+) with SHOW MASTER STATUS fallback; GTID set when gtid_mode=ON. */
    public CdcPositionMark readCurrentPosition(DatabaseConfig source) {
        try (Connection conn = dataSourceManager.getConnection(source)) {
            String file = null;
            long pos = 4L;
            try (Statement st = conn.createStatement()) {
                ResultSet rs = firstNonEmpty(st, "SHOW BINARY LOG STATUS", "SHOW MASTER STATUS");
                if (rs != null) {
                    file = rs.getString("File");
                    pos = rs.getLong("Position");
                    rs.close();
                }
            }
            String gtidSet = null;
            try (Statement st = conn.createStatement()) {
                ResultSet rs = st.executeQuery("SELECT @@GLOBAL.gtid_mode");
                String mode = rs.next() ? rs.getString(1) : "OFF";
                rs.close();
                if ("ON".equalsIgnoreCase(mode)) {
                    rs = st.executeQuery("SELECT @@GLOBAL.gtid_executed");
                    if (rs.next()) {
                        gtidSet = rs.getString(1);
                    }
                    rs.close();
                }
            }
            if (file == null || file.isBlank()) {
                throw new IllegalStateException(
                        "Could not read binary log position — is log_bin enabled?");
            }
            return new CdcPositionMark(file, pos, gtidSet);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read binlog position: " + e.getMessage(), e);
        }
    }

    private ResultSet firstNonEmpty(Statement st, String... statements) {
        for (String sql : statements) {
            try {
                ResultSet rs = st.executeQuery(sql);
                if (rs.next()) {
                    return rs;
                }
                rs.close();
            } catch (SQLException ignored) {
                // 8.0-: SHOW BINARY LOG STATUS 不存在,落到下一个
            }
        }
        return null;
    }

    public void runBaseline(SyncContext ctx, BooleanSupplier lockRenewer) {
        long projectId = ctx.getProject().getId();
        try {
            CdcPositionMark mark = readCurrentPosition(ctx.getSourceConfig());
            positionStore.recordBaselineStart(projectId, mark.getFile(), mark.getPos(),
                    mark.getGtidSet(), clientFactory.serverIdFor(projectId));
            stateWriter.resetProject(projectId);
            log.info("CDC baseline for project {}: full load from position {}:{}",
                    projectId, mark.getFile(), mark.getPos());
            engine.runCycle(ctx.toBuilder().cdcBaselineRun(true).build());
            positionStore.markBaselineDone(projectId);
            log.info("CDC baseline for project {} completed", projectId);
        } catch (RuntimeException | Error e) {
            positionStore.delete(projectId);
            taskStore.recordCdcFailure(projectId, CdcFailureClassifier.describe(e));
            throw e;
        }
    }
}

package com.qqmu.jync.service.cdc;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.qqmu.jync.dto.SyncConfig;

import lombok.extern.slf4j.Slf4j;

/**
 * Turns the raw binlog event stream into buffered, per-table row changes and flushes
 * them at transaction (XID) boundaries (task book B5).
 *
 * <p>Buffering is per-transaction: one XID = one target commit = one position persist.
 * A failing flush keeps the buffer and rethrows — the client drops the connection, the
 * consumer backs off and reconnects, and the whole transaction replays. Idempotent
 * upserts make the replay safe; that is the at-least-once contract end to end.
 */
@Slf4j
public final class CdcEventStreamHandler implements BinaryLogClient.EventListener {

    /** Receives one binlog transaction's changes plus the position to persist right after. */
    public interface TransactionSink {
        void apply(List<TableChange> changes, CdcPositionMark mark) throws SQLException;
    }

    /** One source table's buffered changes inside the current transaction. */
    public static final class TableChange {
        public final CdcTableBinding binding;
        public final List<Map<String, Object>> upserts = new ArrayList<>();
        public final List<Map<String, Object>> deletes = new ArrayList<>();

        TableChange(CdcTableBinding binding) {
            this.binding = binding;
        }
    }

    private static final int DDL_LOG_SQL_CHARS = 200;

    private final SyncConfig config;
    private final String sourceSchema;
    private final CdcTableMetaSupplier supplier;
    private final CdcRowMapper mapper;
    private final CdcRuntimeStats stats;
    private final Supplier<String> gtidSetSupplier;
    private final TransactionSink sink;

    /** tableId -> TABLE_MAP payload; kept only to name tables in ignored-row diagnostics. */
    private final Map<Long, TableMapEventData> tableMaps = new LinkedHashMap<>();
    /** tableId -> binding; filtered ids (wrong schema, unselected, no PK) are absent. */
    private final Map<Long, CdcTableBinding> bindings = new LinkedHashMap<>();
    /** "database.table" -> current transaction's buffered changes. */
    private final Map<String, TableChange> buffered = new LinkedHashMap<>();
    private String currentFile;
    private long lastNextPosition;

    public CdcEventStreamHandler(SyncConfig config, String sourceSchema, CdcTableMetaSupplier supplier,
                                 CdcRowMapper mapper, CdcRuntimeStats stats,
                                 Supplier<String> gtidSetSupplier, TransactionSink sink) {
        this.config = config;
        this.sourceSchema = sourceSchema;
        this.supplier = supplier;
        this.mapper = mapper;
        this.stats = stats;
        this.gtidSetSupplier = gtidSetSupplier;
        this.sink = sink;
    }

    @Override
    public void onEvent(Event event) {
        EventHeaderV4 header = event.getHeader();
        if (header.getTimestamp() > 0) {
            stats.touchEvent(header.getTimestamp());
        }
        if (header.getNextPosition() > 0) {
            lastNextPosition = header.getNextPosition();
        }
        switch (header.getEventType()) {
            case TABLE_MAP:
                onTableMap(event.getData());
                break;
            case WRITE_ROWS: case EXT_WRITE_ROWS:
                onWrite(event.getData());
                break;
            case UPDATE_ROWS: case EXT_UPDATE_ROWS:
                onUpdate(event.getData());
                break;
            case DELETE_ROWS: case EXT_DELETE_ROWS:
                onDelete(event.getData());
                break;
            case QUERY:
                onQuery(event.getData());
                break;
            case ROTATE:
                RotateEventData rotate = event.getData();
                currentFile = rotate.getBinlogFilename();
                break;
            case XID:
                onXid(header);
                break;
            default:
                break; // GTID/HEARTBEAT/…: touchEvent above is all they need; no buffer semantics
        }
    }

    /** Contract 1: record the map, then filter — schema, selection, applicability (B4). */
    private void onTableMap(TableMapEventData data) {
        long tableId = data.getTableId();
        tableMaps.put(tableId, data);
        String database = data.getDatabase();
        String table = data.getTable();
        if (database == null || !database.equalsIgnoreCase(sourceSchema)) {
            bindings.remove(tableId);
            return;
        }
        if (!config.includesTable(table)) {
            bindings.remove(tableId);
            return;
        }
        CdcTableBinding binding = supplier.binding(database, table);
        if (binding == null || !binding.isApplicable()) {
            // No PK (or unreadable table): stays out of CDC on one-shot seeding (B4).
            // The stats set de-dupes, so repeat TABLE_MAPs report it only once.
            stats.addUnsupportedTable(database + "." + table);
            bindings.remove(tableId);
            return;
        }
        bindings.put(tableId, binding);
    }

    /** Contract 2: after-images buffer as upserts; one poison row never stalls the stream. */
    private void onWrite(WriteRowsEventData data) {
        CdcTableBinding binding = bindings.get(data.getTableId());
        if (binding == null) {
            logIgnoredRows(data.getTableId(), data.getRows().size());
            return;
        }
        TableChange change = changeFor(binding);
        for (Serializable[] row : data.getRows()) {
            try {
                appendUpsert(change, mapper.mapRow(row, binding, stats));
            } catch (CdcUnmappableValueException e) {
                countAndLogUnmappable(binding, "INSERT", e);
            }
        }
    }

    /** Contract 3 / Review Focus #2: after-image upserts; a moved PK also deletes the old row. */
    private void onUpdate(UpdateRowsEventData data) {
        CdcTableBinding binding = bindings.get(data.getTableId());
        if (binding == null) {
            logIgnoredRows(data.getTableId(), data.getRows().size());
            return;
        }
        TableChange change = changeFor(binding);
        for (Map.Entry<Serializable[], Serializable[]> entry : data.getRows()) {
            try {
                Map<String, Object> before = mapper.mapRow(entry.getKey(), binding, stats);
                Map<String, Object> after = mapper.mapRow(entry.getValue(), binding, stats);
                appendUpsert(change, after);
                if (primaryKeyChanged(before, after, binding)) {
                    appendDelete(change, before);
                }
            } catch (CdcUnmappableValueException e) {
                // Either image unmappable: skip the whole pair — upserting the after-image
                // without deleting the old key would orphan a row on a PK change.
                countAndLogUnmappable(binding, "UPDATE", e);
            }
        }
    }

    /** Contract 4: full row maps; the applier binds only the PK columns of each. */
    private void onDelete(DeleteRowsEventData data) {
        CdcTableBinding binding = bindings.get(data.getTableId());
        if (binding == null) {
            logIgnoredRows(data.getTableId(), data.getRows().size());
            return;
        }
        TableChange change = changeFor(binding);
        for (Serializable[] row : data.getRows()) {
            try {
                appendDelete(change, mapper.mapRow(row, binding, stats));
            } catch (CdcUnmappableValueException e) {
                countAndLogUnmappable(binding, "DELETE", e);
            }
        }
    }

    /** Contract 5: BEGIN/COMMIT are no-ops; anything else is DDL — invalidate and discard. */
    private void onQuery(QueryEventData data) {
        String sql = data.getSql();
        String normalized = sql == null ? "" : sql.trim();
        while (normalized.endsWith(";")) {
            normalized = normalized.substring(0, normalized.length() - 1).trim();
        }
        if (normalized.isEmpty()) {
            return;
        }
        String upper = normalized.toUpperCase(Locale.ROOT);
        if ("BEGIN".equals(upper) || "COMMIT".equals(upper)) {
            return; // transaction control, not DDL: the buffer stays
        }
        stats.addSkippedDdl(1);
        log.warn("Skipping DDL in CDC stream (database {}): {}", data.getDatabase(),
                normalized.length() > DDL_LOG_SQL_CHARS
                        ? normalized.substring(0, DDL_LOG_SQL_CHARS) + "..." : normalized);
        // QUERY events carry no table (0.41.5 QueryEventData has database+sql only) and SQL
        // parsing is out (contract 5), so null = invalidate every cached binding (Review
        // Focus #1). Over-invalidation costs one metadata re-read per table at the next
        // TABLE_MAP; a stale binding would mis-map every row of the altered table.
        supplier.invalidate(null, null);
        // Buffered rows were mapped with the pre-DDL column layout — untrustworthy. Drop
        // them; the position has not advanced, so a reconnect replays the transaction.
        buffered.clear();
    }

    /** Contract 7: one XID = one sink call = one position mark; empty buffers never flush. */
    private void onXid(EventHeaderV4 header) {
        List<TableChange> changes = new ArrayList<>();
        for (TableChange change : buffered.values()) {
            if (!change.upserts.isEmpty() || !change.deletes.isEmpty()) {
                changes.add(change);
            }
        }
        if (changes.isEmpty()) {
            // The transaction touched nothing we track (other schemas, unselected or
            // unsupported tables, only unmappable rows): never call the sink — an empty
            // apply would push the position forward with no data behind it.
            buffered.clear();
            return;
        }
        long pos = header.getNextPosition() > 0 ? header.getNextPosition() : lastNextPosition;
        CdcPositionMark mark = new CdcPositionMark(currentFile, pos, gtidSetSupplier.get());
        try {
            sink.apply(changes, mark);
        } catch (SQLException e) {
            log.error("CDC flush failed at {}:{}", mark.getFile(), mark.getPos(), e);
            // Keep the buffer, do not advance the position, rethrow so the client drops the
            // connection: the consumer classifies, backs off, reconnects, and the whole
            // transaction replays — idempotent upserts absorb it (at-least-once).
            throw new RuntimeException("CDC transaction flush failed at "
                    + mark.getFile() + ":" + mark.getPos(), e);
        }
        buffered.clear();
    }

    private TableChange changeFor(CdcTableBinding binding) {
        String key = binding.getDatabase() + "." + binding.getTable();
        return buffered.computeIfAbsent(key, k -> new TableChange(binding));
    }

    private static boolean primaryKeyChanged(Map<String, Object> before, Map<String, Object> after,
                                             CdcTableBinding binding) {
        for (String pk : binding.pkNames()) {
            if (!Objects.equals(before.get(pk), after.get(pk))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Same-PK last-op-wins folding within one transaction (FIX C). The sink applies
     * upserts BEFORE deletes — that order is what makes PK chains (A→B, B→C) safe — so an
     * unfilled buffer holding both an upsert and a delete for the SAME primary key (a
     * row-based {@code REPLACE INTO} emits Delete_rows + Write_rows in one XID) would
     * upsert the row and then delete it: silent row loss. Folding rule: appending an
     * upsert drops any buffered delete of the same PK; appending a delete drops any
     * buffered upsert of the same PK.
     *
     * <p>Cost: one O(n) scan of the opposite list per append — fine for typical small CDC
     * transactions; a rare bulk {@code REPLACE ... SELECT} pays a one-time quadratic.
     *
     * <p>Limitation: PK columns that map to {@code byte[]} never fold (list equality uses
     * element {@code equals}, arrays compare by identity) — such tables degrade to the
     * previous naive buffering, which is acceptable: correctness then matches pre-fix
     * behavior rather than regressing.
     */
    private static void appendUpsert(TableChange change, Map<String, Object> row) {
        List<Object> key = pkKey(row, change.binding);
        change.deletes.removeIf(buffered -> pkKey(buffered, change.binding).equals(key));
        change.upserts.add(row);
    }

    private static void appendDelete(TableChange change, Map<String, Object> row) {
        List<Object> key = pkKey(row, change.binding);
        change.upserts.removeIf(buffered -> pkKey(buffered, change.binding).equals(key));
        change.deletes.add(row);
    }

    /** The row's primary-key tuple, extracted exactly like {@link #primaryKeyChanged} reads it. */
    private static List<Object> pkKey(Map<String, Object> row, CdcTableBinding binding) {
        List<Object> key = new ArrayList<>(binding.pkNames().size());
        for (String pk : binding.pkNames()) {
            key.add(row.get(pk));
        }
        return key;
    }

    private void countAndLogUnmappable(CdcTableBinding binding, String operation,
                                       CdcUnmappableValueException e) {
        stats.addUnmappable(1);
        log.error("Skipping {} row on {}.{}: unmappable column '{}' (type {})",
                operation, binding.getDatabase(), binding.getTable(), e.getColumn(), e.getTypeName(), e);
    }

    private void logIgnoredRows(long tableId, int rowCount) {
        if (log.isDebugEnabled()) {
            TableMapEventData map = tableMaps.get(tableId);
            log.debug("Ignoring {} row(s) for tableId {} ({}): filtered or unsupported",
                    rowCount, tableId,
                    map == null ? "unknown table" : map.getDatabase() + "." + map.getTable());
        }
    }
}

package com.qqmu.jync.service.cdc;

/** Where the stream handler gets per-table bindings from; the consumer caches them. */
public interface CdcTableMetaSupplier {

    /** Binding for a source table, cached; null when the table cannot be read (dropped mid-stream). */
    CdcTableBinding binding(String database, String table);

    /**
     * Drop the cached binding (stream DDL saw the table change — Review Focus #1). Both
     * arguments may be null: a null {@code database} invalidates every cached binding,
     * which is what the handler passes on DDL — binlog QUERY events identify no table
     * and SQL parsing is out, so everything is dropped. Over-invalidation only costs a
     * metadata re-read at the next TABLE_MAP; a stale binding would mis-map every row.
     */
    void invalidate(String database, String table);
}

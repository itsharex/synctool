package com.qqmu.jync.service.cdc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.qqmu.jync.dto.meta.ColumnMeta;
import com.qqmu.jync.dto.meta.TableMeta;

import lombok.Getter;

/**
 * Everything the CDC pipeline knows about one tracked source table: metadata, per-column
 * charsets (binlog ships raw bytes in the column's own charset), the mapped target name,
 * and whether row events can be applied at all (no PK => updates/deletes cannot locate
 * the row; task book B4 keeps such tables on one-shot seeding and the UI marks them).
 */
@Getter
public final class CdcTableBinding {

    private final String database;
    private final String table;
    private final TableMeta meta;
    private final String targetTable;
    /** Column name (upper-case) -> MySQL charset name, e.g. "utf8mb4"; absent = binary/unknown. */
    private final Map<String, String> charsetByUpperColumn;
    private final boolean applicable;

    private CdcTableBinding(String database, String table, TableMeta meta, String targetTable,
                            Map<String, String> charsetByUpperColumn, boolean applicable) {
        this.database = database;
        this.table = table;
        this.meta = meta;
        this.targetTable = targetTable;
        this.charsetByUpperColumn = charsetByUpperColumn;
        this.applicable = applicable;
    }

    public static CdcTableBinding of(String database, String table, TableMeta meta,
                                     String targetTable, Map<String, String> charsetByUpperColumn) {
        boolean applicable = meta != null && meta.hasPrimaryKey();
        return new CdcTableBinding(database, table, meta, targetTable,
                charsetByUpperColumn == null ? new LinkedHashMap<>() : charsetByUpperColumn,
                applicable);
    }

    public List<String> columnNames() {
        List<String> names = new ArrayList<>();
        for (ColumnMeta column : meta.getColumns()) {
            names.add(column.getName());
        }
        return names;
    }

    public List<String> pkNames() {
        return meta.getPrimaryKeys();
    }

    public Map<String, Integer> typeByColumn() {
        Map<String, Integer> types = new LinkedHashMap<>();
        for (ColumnMeta column : meta.getColumns()) {
            types.put(column.getName(), column.getJdbcType());
        }
        return types;
    }
}

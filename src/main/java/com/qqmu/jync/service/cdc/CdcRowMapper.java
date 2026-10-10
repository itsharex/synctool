package com.qqmu.jync.service.cdc;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import com.qqmu.jync.dto.meta.ColumnMeta;

import lombok.extern.slf4j.Slf4j;

/**
 * Maps physical binlog cell values (as delivered by mysql-binlog-connector-java with
 * the compatibility modes pinned in BinlogClientFactory) to the Java values
 * JdbcRowMapper.bind expects. This is the single place where wire format meets the
 * JDBC type system — task book risk R1 lives and dies here, so every branch has a
 * matrix test.
 */
@Slf4j
public final class CdcRowMapper {

    /** Zero dates arrive as Long.MIN_VALUE under INVALID_DATE_AND_TIME_AS_MIN_VALUE. */
    static final long ZERO_DATE_SENTINEL = Long.MIN_VALUE;

    public Map<String, Object> mapRow(Serializable[] binlogRow, CdcTableBinding binding,
                                      CdcRuntimeStats stats) throws CdcUnmappableValueException {
        java.util.List<ColumnMeta> columns = binding.getMeta().getColumns();
        if (binlogRow.length != columns.size()) {
            throw new CdcUnmappableValueException(binding.getTable(), "row-image",
                    "binlog row has " + binlogRow.length + " cells but metadata has "
                            + columns.size() + " columns (stale binding after DDL?)");
        }
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < binlogRow.length; i++) {
            ColumnMeta column = columns.get(i);
            String charset = binding.getCharsetByUpperColumn()
                    .get(column.getName().toUpperCase(Locale.ROOT));
            row.put(column.getName(), mapValue(binlogRow[i], column, charset, stats));
        }
        return row;
    }

    static Object mapValue(Serializable raw, ColumnMeta column, String charsetName,
                           CdcRuntimeStats stats) throws CdcUnmappableValueException {
        if (raw == null) {
            return null;
        }
        int jdbcType = column.getJdbcType();

        if (raw instanceof Long && isTemporal(jdbcType)) {
            long micros = (Long) raw;
            if (micros == ZERO_DATE_SENTINEL) {
                if (stats != null) {
                    stats.addZeroDateCoerced(1);
                }
                log.warn("Zero date in column '{}' coerced to NULL (counted)", column.getName());
                return null;
            }
            switch (jdbcType) {
                case Types.DATE:
                    return java.sql.Date.valueOf(Instant.ofEpochMilli(micros / 1000L)
                            .atZone(java.time.ZoneOffset.UTC).toLocalDate());
                case Types.TIME:
                    // java.sql.Time is millis-based: keeps MySQL's >24h and negative times
                    return new java.sql.Time(micros / 1000L);
                case Types.TIMESTAMP:
                default:
                    // floorDiv keeps the seconds base aligned with the floored (always >= 0)
                    // nanos for pre-1970 fractions; truncating division lands a second late.
                    java.sql.Timestamp ts =
                            new java.sql.Timestamp(Math.floorDiv(micros, 1_000_000L) * 1000L);
                    ts.setNanos((int) (Math.floorMod(micros, 1_000_000L) * 1000L));
                    return ts;
            }
        }

        if (raw instanceof byte[]) {
            byte[] bytes = (byte[]) raw;
            if (isCharLike(jdbcType, column.getTypeName())) {
                return new String(bytes, resolveCharset(charsetName, column));
            }
            if (jdbcType == Types.BIT) {
                if (column.getSize() != null && column.getSize() == 1) {
                    return Boolean.valueOf(bytes.length > 0 && bytes[0] != 0);
                }
                return bytes; // wider BIT: hand the raw bits to setBytes, targets cope
            }
            if (isBinaryLike(jdbcType)) {
                return bytes;
            }
            throw new CdcUnmappableValueException(column.getName(), column.getTypeName(), "byte[" + bytes.length + "]");
        }

        if (raw instanceof Integer || raw instanceof Long || raw instanceof java.math.BigDecimal
                || raw instanceof Double || raw instanceof Float || raw instanceof Boolean
                || raw instanceof Short || raw instanceof Byte || raw instanceof BigInteger) {
            return raw;
        }

        throw new CdcUnmappableValueException(column.getName(), column.getTypeName(), raw);
    }

    private static boolean isTemporal(int jdbcType) {
        return jdbcType == Types.DATE || jdbcType == Types.TIME || jdbcType == Types.TIMESTAMP;
    }

    private static boolean isCharLike(int jdbcType, String typeName) {
        if (typeName != null && typeName.toUpperCase(Locale.ROOT).contains("JSON")) {
            return true; // MySQL JSON is reported as LONGVARCHAR; it is text to us
        }
        return jdbcType == Types.CHAR || jdbcType == Types.VARCHAR || jdbcType == Types.LONGVARCHAR
                || jdbcType == Types.NCHAR || jdbcType == Types.NVARCHAR || jdbcType == Types.LONGNVARCHAR
                || jdbcType == Types.CLOB;
    }

    private static boolean isBinaryLike(int jdbcType) {
        return jdbcType == Types.BINARY || jdbcType == Types.VARBINARY || jdbcType == Types.LONGVARBINARY
                || jdbcType == Types.BLOB;
    }

    /** MySQL charset name -> Java Charset, with a conservative fallback (Review Focus #3). */
    static Charset resolveCharset(String charsetName, ColumnMeta column) {
        if (charsetName == null || charsetName.trim().isEmpty()) {
            return StandardCharsets.UTF_8;
        }
        String normalized = charsetName.trim().toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "utf8mb4": case "utf8": case "utf8mb3":
                return StandardCharsets.UTF_8;
            case "latin1": case "binary":
                return StandardCharsets.ISO_8859_1; // byte-preserving for "binary"
            case "ascii":
                return StandardCharsets.US_ASCII;
            default:
                try {
                    return Charset.forName(normalized); // gbk, big5, sjis, euckr, ...
                } catch (RuntimeException e) {
                    log.warn("Unknown charset '{}' on column {}; falling back to UTF-8",
                            charsetName, column.getName());
                    return StandardCharsets.UTF_8;
                }
        }
    }
}

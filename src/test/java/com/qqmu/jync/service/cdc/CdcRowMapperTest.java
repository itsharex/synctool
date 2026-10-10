package com.qqmu.jync.service.cdc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

import com.qqmu.jync.dto.meta.ColumnMeta;

class CdcRowMapperTest {

    private final CdcRowMapper mapper = new CdcRowMapper();
    private final CdcRuntimeStats stats = new CdcRuntimeStats();

    private static ColumnMeta column(String name, int jdbcType, String typeName, Integer size) {
        ColumnMeta column = new ColumnMeta();
        column.setName(name);
        column.setJdbcType(jdbcType);
        column.setTypeName(typeName);
        column.setSize(size);
        return column;
    }

    private Object map(Serializable raw, ColumnMeta column, String charset) throws Exception {
        return CdcRowMapper.mapValue(raw, column, charset, stats);
    }

    // —— 数值透传(整型/浮点/DECIMAL 家族)——

    @Test
    void numericsPassThrough() throws Exception {
        assertThat(map(Integer.valueOf(7), column("c_int", Types.INTEGER, "int", null), null))
                .isEqualTo(7);
        assertThat(map(Long.valueOf(-9_000_000_000L), column("c_bigint", Types.BIGINT, "bigint", null), null))
                .isEqualTo(-9_000_000_000L);
        assertThat(map(new BigDecimal("12.3400"), column("c_dec", Types.DECIMAL, "decimal", null), null))
                .isEqualTo(new BigDecimal("12.3400"));
        assertThat(map(Double.valueOf(1.5d), column("c_dbl", Types.DOUBLE, "double", null), null))
                .isEqualTo(1.5d);
        assertThat(map(Boolean.TRUE, column("c_bool", Types.BOOLEAN, "tinyint", 1), null))
                .isEqualTo(Boolean.TRUE);
    }

    // —— TEMPORAL(epoch-micros Long)——

    @Test
    void dateMapsToSqlDate() throws Exception {
        long micros = LocalDate.of(2026, 10, 10).atStartOfDay(ZoneOffset.UTC)
                .toInstant().toEpochMilli() * 1000L;
        Object out = map(micros, column("c_date", Types.DATE, "date", null), null);
        assertThat(out).isEqualTo(java.sql.Date.valueOf("2026-10-10"));
    }

    @Test
    void datetimeKeepsWallClockWithMicros() throws Exception {
        // 2026-10-10 12:34:56.789012,DATETIME 在 binlog 里是"墙钟当 UTC"的 epoch-micros
        long micros = Instant.parse("2026-10-10T12:34:56Z").getEpochSecond() * 1_000_000L + 789_012L;
        Object out = map(micros, column("c_dt", Types.TIMESTAMP, "datetime", null), null);
        java.sql.Timestamp ts = (java.sql.Timestamp) out;
        assertThat(ts.toInstant().getEpochSecond())
                .isEqualTo(Instant.parse("2026-10-10T12:34:56Z").getEpochSecond());
        assertThat(ts.getNanos()).isEqualTo(789_012_000);
    }

    @Test
    void timeSupportsNegativeAndOver24h() throws Exception {
        long micros838h = 838L * 3600 * 1_000_000L; // MySQL TIME 上限 838:59:59
        Object out = map(micros838h, column("c_time", Types.TIME, "time", null), null);
        assertThat(((java.sql.Time) out).getTime()).isEqualTo(838L * 3600 * 1000L);

        Object neg = map(-3_600_000_000L, column("c_time", Types.TIME, "time", null), null);
        assertThat(((java.sql.Time) neg).getTime()).isEqualTo(-3_600_000L);
    }

    @Test
    void zeroDateBecomesNullAndCounts() throws Exception {
        Object out = map(Long.MIN_VALUE, column("c_date", Types.DATE, "date", null), null);
        assertThat(out).isNull();
        assertThat(stats.snapshot().get("zeroDateCoerced")).isEqualTo(1L);
    }

    // —— 字符串 / JSON / 字符集 ——

    @Test
    void varcharDecodesWithColumnCharset() throws Exception {
        byte[] utf8 = "café☕".getBytes(StandardCharsets.UTF_8);
        assertThat(map(utf8, column("c_vc", Types.VARCHAR, "varchar", 64), "utf8mb4"))
                .isEqualTo("café☕");

        byte[] latin1 = "café".getBytes(Charset.forName("ISO-8859-1"));
        assertThat(map(latin1, column("c_vc", Types.VARCHAR, "varchar", 64), "latin1"))
                .isEqualTo("café"); // Review Focus #3:按列字符集解码,不盲猜 UTF-8

        byte[] gbk = "中文".getBytes(Charset.forName("GBK"));
        assertThat(map(gbk, column("c_vc", Types.VARCHAR, "varchar", 64), "gbk"))
                .isEqualTo("中文");
    }

    @Test
    void unknownCharsetFallsBackToUtf8() throws Exception {
        byte[] utf8 = "ok".getBytes(StandardCharsets.UTF_8);
        assertThat(map(utf8, column("c_vc", Types.VARCHAR, "varchar", 64), "no_such_charset"))
                .isEqualTo("ok");
    }

    @Test
    void jsonArrivesAsString() throws Exception {
        byte[] json = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
        assertThat(map(json, column("c_json", Types.LONGVARCHAR, "json", null), "utf8mb4"))
                .isEqualTo("{\"a\":1}");
    }

    // —— BIT / BLOB ——

    @Test
    void bit1BecomesBooleanWiderStaysBytes() throws Exception {
        assertThat(map(new byte[] {1}, column("c_bit1", Types.BIT, "bit", 1), null))
                .isEqualTo(Boolean.TRUE);
        assertThat(map(new byte[] {0}, column("c_bit1", Types.BIT, "bit", 1), null))
                .isEqualTo(Boolean.FALSE);
        byte[] wide = new byte[] {(byte) 0b1010_0101, (byte) 0xFF};
        assertThat(map(wide, column("c_bit16", Types.BIT, "bit", 16), null))
                .isEqualTo(wide);
    }

    @Test
    void blobPassesThrough() throws Exception {
        byte[] blob = new byte[] {0, 1, 2, (byte) 255};
        assertThat(map(blob, column("c_blob", Types.BLOB, "blob", null), null))
                .isEqualTo(blob);
    }

    // —— 不静默丢:B5 ——

    @Test
    void unmappableValueThrowsWithColumnName() {
        assertThatThrownBy(() -> map(new java.util.Date(0L),
                column("c_struct", Types.STRUCT, "geometry", null), null))
                .isInstanceOf(CdcUnmappableValueException.class)
                .hasMessageContaining("c_struct");
    }

    // —— mapRow:列对齐 + 统计 ——

    @Test
    void mapRowAlignsByColumnOrder() throws Exception {
        CdcTableBinding binding = CdcTableBinding.of("app", "t", tableOf(
                column("id", Types.BIGINT, "bigint", null),
                column("name", Types.VARCHAR, "varchar", 64)), "t", null);
        java.util.Map<String, Object> row = mapper.mapRow(
                new Serializable[] {5L, "bob".getBytes(StandardCharsets.UTF_8)}, binding, stats);
        assertThat(row).containsEntry("id", 5L).containsEntry("name", "bob");
        assertThat(new java.util.ArrayList<>(row.keySet())).containsExactly("id", "name");
    }

    @Test
    void mapRowUsesPerColumnCharsetFromBinding() throws Exception {
        java.util.Map<String, String> charsets = new java.util.LinkedHashMap<>();
        charsets.put("NAME", "latin1"); // 键是大写列名(CdcTableBinding 契约)
        CdcTableBinding binding = CdcTableBinding.of("app", "t", tableOf(
                column("id", Types.BIGINT, "bigint", null),
                column("name", Types.VARCHAR, "varchar", 64)), "t", charsets);
        byte[] latin1 = "café".getBytes(Charset.forName("ISO-8859-1"));
        java.util.Map<String, Object> row = mapper.mapRow(
                new Serializable[] {1L, latin1}, binding, stats);
        assertThat(row).containsEntry("name", "café");
    }

    @Test
    void mapRowRejectsLengthMismatch() {
        // binlog 列数 ≠ 元数据列数(典型:流中 DDL 后缓存过期)→ 显式异常,不硬套错位
        CdcTableBinding binding = CdcTableBinding.of("app", "t", tableOf(
                column("id", Types.BIGINT, "bigint", null),
                column("name", Types.VARCHAR, "varchar", 64),
                column("extra", Types.INTEGER, "int", null)), "t", null);
        assertThatThrownBy(() -> mapper.mapRow(
                new Serializable[] {5L, "bob".getBytes(StandardCharsets.UTF_8)}, binding, stats))
                .isInstanceOf(CdcUnmappableValueException.class)
                .hasMessageContaining("3 columns");
    }

    @Test
    void bindingWithoutPkIsNotApplicable() {
        com.qqmu.jync.dto.meta.TableMeta noPk = new com.qqmu.jync.dto.meta.TableMeta();
        noPk.setName("log_lines");
        noPk.getColumns().add(column("msg", Types.VARCHAR, "varchar", 64));
        CdcTableBinding binding = CdcTableBinding.of("app", "log_lines", noPk, "log_lines", null);
        assertThat(binding.isApplicable()).isFalse(); // B4:无 PK 表 CDC 不生效,UI 标注
    }

    private static com.qqmu.jync.dto.meta.TableMeta tableOf(ColumnMeta... columns) {
        com.qqmu.jync.dto.meta.TableMeta meta = new com.qqmu.jync.dto.meta.TableMeta();
        meta.setName("t");
        for (ColumnMeta c : columns) {
            meta.getColumns().add(c);
        }
        meta.getPrimaryKeys().add("id");
        return meta;
    }
}

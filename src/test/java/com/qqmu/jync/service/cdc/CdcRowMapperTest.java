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
    void datetimeBefore1970KeepsFractionalSecondAlignment() throws Exception {
        // 1969-12-31T23:59:58.999999Z: truncating millis + floored (always >= 0) nanos
        // would disagree by a full second — millis must floor-divide to stay aligned.
        Object out = map(-1_000_001L, column("c_dt_old", Types.TIMESTAMP, "datetime", null), null);
        assertThat(((java.sql.Timestamp) out).toInstant())
                .isEqualTo(Instant.ofEpochSecond(-2, 999_999_000));
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

    // —— 裁定 R2:binlog 里 JSON 列是 MySQL 内部二进制格式(类型码 + 偏移表),
    // 按字符集解码只会得到乱码;必须走客户端库自带的 JsonBinary 编解码器转成
    // 规范文本。以下向量与 MySQL 8.0.46 真实 binlog 捕获逐字节一致(服务器写的
    // size 字段 = 文档总字节数减类型字节;偏移相对类型字节后的文档起点)。——

    /** Parses a spaced hex literal into bytes (hand-built binary-JSON vectors). */
    private static byte[] hex(String spaced) {
        String s = spaced.replaceAll("\\s+", "");
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    /**
     * Builds an opaque custom-data value: {@code [0x0F][enum_field_types code][len 8]}
     * followed by the packed temporal as an 8-byte little-endian long (MySQL packs
     * JSON temporals as {@code (fields << 24) | micros}).
     */
    private static byte[] opaqueTemporal(int fieldTypeCode, long packedRaw) {
        byte[] out = new byte[11];
        out[0] = 0x0F;
        out[1] = (byte) fieldTypeCode;
        out[2] = 8;
        for (int i = 0; i < 8; i++) {
            out[3 + i] = (byte) (packedRaw >>> (8 * i));
        }
        return out;
    }

    private static ColumnMeta jsonColumn() {
        return column("c_json", Types.LONGVARCHAR, "json", null);
    }

    @Test
    void binaryJsonObjectWithStringValueDecodesToCanonicalText() throws Exception {
        // IT insert 形状 {"k":"值"}:small object,键 "k",值是 3 字节 UTF-8 的 varint 字符串。
        // 布局:[00 类型][01 00 元素数][10 00 size=总17-1][0B 00 01 00 键偏移/长度]
        //       [0C 0C 00 值类型 STRING + 载荷偏移][6B 'k'][03 E5 80 BC varint 长度 + UTF-8]
        byte[] doc = hex("00 01 00 10 00 0B 00 01 00 0C 0C 00 6B 03 E5 80 BC");
        assertThat(map(doc, jsonColumn(), "utf8mb4")).isEqualTo("{\"k\":\"值\"}");
    }

    @Test
    void binaryJsonObjectWithInlineIntAndLiteralDecodes() throws Exception {
        // {"n":42,"ok":true}:INT16 值内联在值条目里,LITERAL true 同样内联(1 字节 + 1 填充)
        byte[] doc = hex("00 02 00 15 00"
                + " 12 00 01 00"   // 键 "n":偏移 18,长 1
                + " 13 00 02 00"   // 键 "ok":偏移 19,长 2
                + " 05 2A 00"      // INT16 内联 42(LE)
                + " 04 01 00"      // LITERAL 内联 true(0x01)+ 填充
                + " 6E 6F 6B");    // 'n' 'o' 'k'
        assertThat(map(doc, jsonColumn(), "utf8mb4")).isEqualTo("{\"n\":42,\"ok\":true}");
    }

    @Test
    void binaryJsonMatchesItUpdateShape() throws Exception {
        // IT update 形状(与 8.0.46 binlog 捕获逐字节一致):MySQL 按(键长,字节)排序键
        // → "n"(1) 在 "k2"(2) 前,值 42 内联、"值2" 是 4 字节 UTF-8 字符串载荷
        byte[] doc = hex("00 02 00 1A 00"
                + " 12 00 01 00"   // "n"
                + " 13 00 02 00"   // "k2"
                + " 05 2A 00"      // INT16 42
                + " 0C 15 00"      // STRING,载荷在偏移 21
                + " 6E 6B 32"      // 'n' 'k' '2'
                + " 04 E5 80 BC 32"); // varint 长度 4 + "值2"
        assertThat(map(doc, jsonColumn(), "utf8mb4")).isEqualTo("{\"n\":42,\"k2\":\"值2\"}");
    }

    @Test
    void binaryJsonArraysAndScalarsDecode() throws Exception {
        // small array [1,2,3](服务器捕获原样):三个 INT16 内联值条目
        assertThat(map(hex("02 03 00 0D 00 05 01 00 05 02 00 05 03 00"), jsonColumn(), "utf8mb4"))
                .isEqualTo("[1,2,3]");
        // 顶层标量:STRING(varint 长度 + UTF-8,规范文本带引号)
        assertThat(map(hex("0C 02 68 69"), jsonColumn(), "utf8mb4")).isEqualTo("\"hi\"");
        // INT64 -5(8 字节 LE 补码)/ DOUBLE 1.5(LE longBits)
        assertThat(map(hex("09 FB FF FF FF FF FF FF FF"), jsonColumn(), "utf8mb4"))
                .isEqualTo("-5");
        assertThat(map(hex("0B 00 00 00 00 00 00 F8 3F"), jsonColumn(), "utf8mb4"))
                .isEqualTo("1.5");
        // 字面量:0x00 null / 0x01 true / 0x02 false
        assertThat(map(hex("04 00"), jsonColumn(), "utf8mb4")).isEqualTo("null");
        assertThat(map(hex("04 01"), jsonColumn(), "utf8mb4")).isEqualTo("true");
        assertThat(map(hex("04 02"), jsonColumn(), "utf8mb4")).isEqualTo("false");
    }

    @Test
    void binaryJsonNestedDocumentDecodes() throws Exception {
        // {"a":[1]}(服务器捕获原样):嵌套值载荷位于偏移处且【不带】自己的类型字节
        // (类型在父值条目里),内层 size 也就不含类型字节
        byte[] doc = hex("00 01 00 13 00 0B 00 01 00 02 0C 00 61"
                + " 01 00 07 00 05 01 00"); // 内层 small array:1 元素,7 字节,INT16 内联 1
        assertThat(map(doc, jsonColumn(), "utf8mb4")).isEqualTo("{\"a\":[1]}");
    }

    @Test
    void binaryJsonOpaqueTemporalsDecode() throws Exception {
        // JSON 里的日期时间是 opaque custom-data:[0F][字段类型码][长度 8][int64 LE];
        // value = raw >> 24,micros = raw % 2^24;DATE: yearMonth = 年*13+月。
        // 三个向量与 8.0.46 binlog 捕获逐字节一致。已知上游局限(报告披露):
        // 负 TIME / >24h TIME 的 JSON 内编码,MySQL 服务器与 0.41.5 编解码器都有缺陷
        long yearMonth = 2026L * 13 + 10;
        long datePacked = (yearMonth << 22) | (10L << 17);
        assertThat(map(opaqueTemporal(0x0A, datePacked << 24), jsonColumn(), "utf8mb4"))
                .isEqualTo("\"2026-10-10\"");

        long datetimePacked = datePacked | (12L << 12) | (34L << 6) | 56L;
        assertThat(map(opaqueTemporal(0x0C, (datetimePacked << 24) | 789_012L), jsonColumn(), "utf8mb4"))
                .isEqualTo("\"2026-10-10 12:34:56.789012\"");

        long timePacked = (12L << 12) | (34L << 6) | 56L;
        assertThat(map(opaqueTemporal(0x0B, timePacked << 24), jsonColumn(), "utf8mb4"))
                .isEqualTo("\"12:34:56\"");
    }

    @Test
    void undecodableBinaryJsonFailsLoudlyInsteadOfPassingGarbage() {
        // 裁定 R2:未知类型码必须大声失败(经 R1 choke 路径进 strike-out/ERROR 分类),
        // 绝不把二进制垃圾当文本透传。0x0D/0x0E 是未赋值的 ValueType 码(且 <= 0x0f,
        // 不落入文本透传区间)→ JsonBinary 抛 IOException → mapper 包成 IllegalStateException
        assertThatThrownBy(() -> map(hex("0D 00"), jsonColumn(), "utf8mb4"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot decode binary JSON")
                .hasMessageContaining("c_json")
                .hasCauseInstanceOf(java.io.IOException.class);
        // 截断的合法文档同样大声失败
        assertThatThrownBy(() -> map(hex("00 01 00 10 00 0B"), jsonColumn(), "utf8mb4"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot decode binary JSON");
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

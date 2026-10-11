package com.qqmu.jync.service.cdc;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.qqmu.jync.dto.meta.ColumnMeta;

/**
 * TIME2 is offset-binary on the wire and the pinned 0.41.5 client ignores the sign
 * bit, so negative durations must be decoded by our own deserializers before the
 * row mapper ever sees them (ruling R5 completion).
 */
class CdcEventDeserializersTest {

    // —— TIME(0) 线格式:wire = 0x800000 ± (h<<12 | m<<6 | s),服务器精确字节 ——

    @Test
    void decodesTimeV2WireBytesToSignedMicros() {
        assertThat(decode(0, 0xB4, 0x6E, 0xFB))          // 838:59:59
                .isEqualTo((838L * 3600 + 59 * 60 + 59) * 1_000_000L);
        assertThat(decode(0, 0x7F, 0x58, 0x80))          // -10:30:00
                .isEqualTo(-(10L * 3600 + 30 * 60) * 1_000_000L);
        assertThat(decode(0, 0x80, 0xC8, 0xB8))          // 12:34:56
                .isEqualTo((12L * 3600 + 34 * 60 + 56) * 1_000_000L);
        assertThat(decode(0, 0x80, 0x00, 0x00))          // 00:00:00
                .isZero();
        assertThat(decode(0, 0x4B, 0x91, 0x05))          // -838:59:59
                .isEqualTo(-(838L * 3600 + 59 * 60 + 59) * 1_000_000L);
    }

    // —— 小数秒:整段(3+frac 字节)按同一个偏移二进制数折叠,负小数才可判别 ——

    @Test
    void decodesFractionalTimeV2WithSign() {
        assertThat(decodeWire(1, false, 12, 34, 56, 700_000L))
                .isEqualTo(45_296_700_000L);                              // TIME(1) 12:34:56.7
        assertThat(decodeWire(3, false, 12, 34, 56, 789_000L))
                .isEqualTo(45_296_789_000L);                              // TIME(3) 12:34:56.789
        assertThat(decodeWire(3, true, 10, 30, 0, 500_000L))
                .isEqualTo(-37_800_500_000L);                             // TIME(3) -10:30:00.500
        assertThat(decodeWire(6, true, 10, 30, 0, 500_000L))
                .isEqualTo(-37_800_500_000L);                             // TIME(6) -10:30:00.500000
        assertThat(decodeWire(6, true, 0, 0, 0, 400_000L))
                .isEqualTo(-400_000L);                                    // TIME(6) -00:00:00.400000
        assertThat(decodeWire(6, false, 838, 59, 59, 999_999L))
                .isEqualTo(3_020_399_999_999L);                           // TIME(6) 838:59:59.999999
    }

    // —— 证据锁定:0.41.5 原始解码把 -10:30:00(wire 0x7F5880)读成 +1013:34:00,
    // 正是验收 IT 观察到的 "Incorrect time value: '1013:34:00'" 截断错误 ——

    @Test
    void stockDecoderGarblesNegativeTimeExactlyAsObservedInTheAcceptanceRun() {
        long wire = 0x7F5880L; // server bytes for TIME '-10:30:00'
        int hour = (int) ((wire >> 12) & 0x3FF);   // stock: sign bit ignored, unsigned slices
        int minute = (int) ((wire >> 6) & 0x3F);
        int second = (int) (wire & 0x3F);
        assertThat(hour).isEqualTo(1013);
        assertThat(minute).isEqualTo(34);
        assertThat(second).isZero();
        long stockMicros = (hour * 3600L + minute * 60 + second) * 1_000_000L;
        assertThat(stockMicros).isEqualTo(3_648_840_000_000L);
        // 旧值一路流到目标端就是这个非法字面量(>838:59:59 → Data truncation)
        assertThat(CdcRowMapper.formatTimeLiteral(stockMicros)).isEqualTo("1013:34:00");
        // 而静默腐败更险:最大的负值解码后落在合法域内,不会触发任何错误
        long stockForNegativeMax = ((0x4B9105L >> 12) & 0x3FF);
        assertThat(stockForNegativeMax).isLessThanOrEqualTo(838); // -838:59:59 → 185:xx,看似合法
    }

    // —— 端到端:wire → 有符号 micros → mapper 字面量,与基线 getString 文本一致 ——

    @Test
    void decodedMicrosFlowThroughRowMapperToServerLiterals() throws Exception {
        ColumnMeta time = timeColumn();
        assertThat(CdcRowMapper.mapValue(decodeWire(0, true, 10, 30, 0, 0L), time, null, null))
                .isEqualTo("-10:30:00");
        assertThat(CdcRowMapper.mapValue(decodeWire(0, false, 838, 59, 59, 0L), time, null, null))
                .isEqualTo("838:59:59");
        assertThat(CdcRowMapper.mapValue(decodeWire(6, true, 10, 30, 0, 500_000L), time, null, null))
                .isEqualTo("-10:30:00.500000");
        assertThat(CdcRowMapper.mapValue(decodeWire(0, false, 12, 34, 56, 0L), time, null, null))
                .isEqualTo("12:34:56");
    }

    // —— 注册表镜像:除六个 rows 类型外,必须与库默认注册逐类一致(防升级漂移)——

    @Test
    @SuppressWarnings("unchecked")
    void registrationMirrorMatchesLibraryDefaults() throws Exception {
        Map<EventType, ?> stock = registrations(new EventDeserializer());
        EventDeserializer custom = CdcEventDeserializers.newDeserializer();
        Map<EventType, ?> ours = registrations(custom);

        assertThat(ours.keySet()).isEqualTo(stock.keySet());
        Set<EventType> rowTypes = Set.of(EventType.WRITE_ROWS, EventType.UPDATE_ROWS,
                EventType.DELETE_ROWS, EventType.EXT_WRITE_ROWS, EventType.EXT_UPDATE_ROWS,
                EventType.EXT_DELETE_ROWS);
        for (EventType type : stock.keySet()) {
            if (!rowTypes.contains(type)) {
                assertThat(ours.get(type).getClass())
                        .describedAs("registration drift for %s", type)
                        .isEqualTo(stock.get(type).getClass());
            }
            // EXT_* 行事件靠 mayContainExtraInformation=true 才会跳过 extra-info 头;
            // 镜像注册若与库默认不一致,行事件会整体解析错位。类比对盖不住这个布尔
            // (六个 rows 类型的类本来就不同),逐类型单独钉死 —— 库升级改默认值即触发。
            if (stock.get(type) instanceof AbstractRowsEventDataDeserializer) {
                assertThat(ours.get(type))
                        .describedAs("row deserializer drift for %s", type)
                        .isInstanceOf(AbstractRowsEventDataDeserializer.class);
                assertThat(mayContainExtraInformation(ours.get(type)))
                        .describedAs("mayContainExtraInformation drift for %s", type)
                        .isEqualTo(mayContainExtraInformation(stock.get(type)));
            }
        }
        assertThat(ours.get(EventType.WRITE_ROWS))
                .isInstanceOf(CdcEventDeserializers.SignedTimeV2WriteRows.class);
        assertThat(ours.get(EventType.EXT_UPDATE_ROWS))
                .isInstanceOf(CdcEventDeserializers.SignedTimeV2UpdateRows.class);
        assertThat(ours.get(EventType.DELETE_ROWS))
                .isInstanceOf(CdcEventDeserializers.SignedTimeV2DeleteRows.class);
    }

    private static Map<EventType, ?> registrations(EventDeserializer deserializer) throws Exception {
        Field field = EventDeserializer.class.getDeclaredField("eventDataDeserializers");
        field.setAccessible(true);
        return (Map<EventType, ?>) field.get(deserializer);
    }

    /**
     * The library keeps no getter, and the flag is declared on each CONCRETE row
     * deserializer (not on AbstractRowsEventDataDeserializer), so walk the hierarchy —
     * our subclasses inherit the field. Reflection failure stays loud: if a library
     * upgrade renames or removes it, this guard trips instead of silently passing.
     */
    private static boolean mayContainExtraInformation(Object deserializer) throws Exception {
        for (Class<?> c = deserializer.getClass(); c != null; c = c.getSuperclass()) {
            try {
                Field field = c.getDeclaredField("mayContainExtraInformation");
                field.setAccessible(true);
                return field.getBoolean(deserializer);
            } catch (NoSuchFieldException ignored) {
                // keep walking up
            }
        }
        throw new NoSuchFieldException("mayContainExtraInformation not found on "
                + deserializer.getClass().getName());
    }

    private static long decode(int meta, int... unsignedBytes) {
        byte[] wire = new byte[unsignedBytes.length];
        for (int i = 0; i < wire.length; i++) {
            wire[i] = (byte) unsignedBytes[i];
        }
        return CdcEventDeserializers.decodeTimeV2SignedMicros(meta, wire);
    }

    /** Builds server-exact TIME2 wire bytes: offset-binary over (h,m,s + fraction). */
    private static long decodeWire(int meta, boolean negative, int hour, int minute, int second,
                                   long fractionMicros) {
        int fractionLength = (meta + 1) / 2;
        int fractionBits = 8 * fractionLength;
        long scaled = fractionLength == 0
                ? 0L
                : fractionMicros / (long) Math.pow(100, 3 - fractionLength);
        long signed = ((((long) hour << 12) | ((long) minute << 6) | second) << fractionBits) + scaled;
        if (negative) {
            signed = -signed;
        }
        long full = signed + (0x800000L << fractionBits);
        byte[] wire = new byte[3 + fractionLength];
        for (int i = wire.length - 1; i >= 0; i--) {
            wire[i] = (byte) (full & 0xFF);
            full >>= 8;
        }
        return CdcEventDeserializers.decodeTimeV2SignedMicros(meta, wire);
    }

    private static ColumnMeta timeColumn() {
        ColumnMeta column = new ColumnMeta();
        column.setName("c_time");
        column.setJdbcType(Types.TIME);
        column.setTypeName("time");
        return column;
    }
}

package com.qqmu.jync.model;

/**
 * How a project captures source-side changes (task book v1.5.0, C1 三策略卡片).
 *
 * <p>Stored inside the project's {@code sync_config} JSON. Absent/null deserializes to
 * {@link #POLLING} so projects saved by v1.4.0 or older keep their exact behavior.
 */
public enum CaptureStrategy {
    /** Periodic cursor-based polling (timestamp / identity / ORA_ROWSCN, auto-resolved). */
    POLLING,
    /** Periodic full-table compare: every cycle re-reads both sides and reconciles. */
    FULL_COMPARE,
    /**
     * MySQL binlog CDC: a long-running consumer thread replays row events. Only offered
     * when the source {@code DatabaseType == MYSQL} and the capability probe passes.
     */
    BINLOG_CDC
}

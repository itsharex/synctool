package com.qqmu.jync.service.cdc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import lombok.Getter;

/**
 * Itemized result of the CDC capability probe (task book B2). Never an exception — a
 * failed probe means "CDC unavailable, here is what is missing", not an error state.
 * Check names double as i18n key suffixes: cdc.check.&lt;NAME&gt;.
 */
@Getter
public final class CdcProbeResult {

    public static final String VERSION = "VERSION";
    public static final String LOG_BIN = "LOG_BIN";
    public static final String BINLOG_FORMAT = "BINLOG_FORMAT";
    public static final String BINLOG_ROW_IMAGE = "BINLOG_ROW_IMAGE";
    public static final String SERVER_ID = "SERVER_ID";
    public static final String REPLICATION_GRANTS = "REPLICATION_GRANTS";
    public static final String BINLOG_RETENTION = "BINLOG_RETENTION";

    public static final class Check {
        private final String name;
        private final boolean passed;
        private final String detail;

        private Check(String name, boolean passed, String detail) {
            this.name = name;
            this.passed = passed;
            this.detail = detail;
        }

        public static Check of(String name, boolean passed, String detail) {
            return new Check(name, passed, detail);
        }

        public String getName() { return name; }
        public boolean isPassed() { return passed; }
        public String getDetail() { return detail; }
    }

    private final List<Check> checks;

    private CdcProbeResult(List<Check> checks) {
        this.checks = Collections.unmodifiableList(new ArrayList<>(checks));
    }

    public static CdcProbeResult of(List<Check> checks) {
        return new CdcProbeResult(checks);
    }

    public boolean passed() {
        return checks.stream().allMatch(Check::isPassed);
    }

    public List<Check> failures() {
        return checks.stream().filter(c -> !c.isPassed()).collect(Collectors.toList());
    }
}

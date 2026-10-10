package com.qqmu.jync.service.cdc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.github.shyiko.mysql.binlog.network.AuthenticationException;

class CdcFailureClassifierTest {

    @Test
    void purgedBinlogClassified() {
        assertThat(CdcFailureClassifier.classify(new IOException(
                "Could not find first log file name in binary log index file (errno: 1236)")))
                .isEqualTo(CdcFailureClassifier.Kind.PURGED);
        assertThat(CdcFailureClassifier.classify(new IOException(
                "The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1, "
                        + "but the master has purged binary logs containing GTIDs that the slave requires")))
                .isEqualTo(CdcFailureClassifier.Kind.PURGED);
    }

    @Test
    void serverIdConflictClassified() {
        assertThat(CdcFailureClassifier.classify(new IOException(
                "A fatal error occurred while trying to connect as a slave: "
                        + "A slave with the same server_uuid/server_id as this slave has connected to the master")))
                .isEqualTo(CdcFailureClassifier.Kind.SERVER_ID_CONFLICT);
    }

    @Test
    void authFailureClassified() {
        assertThat(CdcFailureClassifier.classify(
                new AuthenticationException("Access denied for user 'repl'@'10.0.0.2'")))
                .isEqualTo(CdcFailureClassifier.Kind.AUTH_FAILURE);
    }

    @Test
    void nestedCauseFound() {
        RuntimeException wrapper = new RuntimeException("connect failed",
                new IOException("stream ended", new IOException("errno: 1236 purged")));
        assertThat(CdcFailureClassifier.classify(wrapper))
                .isEqualTo(CdcFailureClassifier.Kind.PURGED);
    }

    @Test
    void everythingElseTransient() {
        assertThat(CdcFailureClassifier.classify(new IOException("Connection reset")))
                .isEqualTo(CdcFailureClassifier.Kind.TRANSIENT);
        assertThat(CdcFailureClassifier.classify(new IllegalStateException("no message at all")))
                .isEqualTo(CdcFailureClassifier.Kind.TRANSIENT);
    }

    @Test
    void describeTruncatesAndReachesRootCause() {
        String longMessage = "x".repeat(900);
        String described = CdcFailureClassifier.describe(
                new RuntimeException("outer", new IOException(longMessage)));
        assertThat(described).contains("x".repeat(10));
        assertThat(described.length()).isLessThanOrEqualTo(520); // 500 + 省略标记
    }

    @Test
    void backoffDoublesAndCaps() {
        CdcReconnectBackoff backoff = new CdcReconnectBackoff(1000L, 60_000L);
        assertThat(backoff.nextDelayMs()).isEqualTo(1000L);
        assertThat(backoff.nextDelayMs()).isEqualTo(2000L);
        assertThat(backoff.nextDelayMs()).isEqualTo(4000L);
        for (int i = 0; i < 20; i++) {
            assertThat(backoff.nextDelayMs()).isLessThanOrEqualTo(60_000L);
        }
        backoff.reset();
        assertThat(backoff.nextDelayMs()).isEqualTo(1000L);
    }
}

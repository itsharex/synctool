package com.qqmu.jync.service.cdc;

/** Exponential reconnect delay, 1s doubling up to 60s (task book B7). Single-threaded by contract. */
public final class CdcReconnectBackoff {

    private final long initialMs;
    private final long maxMs;
    private int attempt;

    public CdcReconnectBackoff(long initialMs, long maxMs) {
        this.initialMs = Math.max(1L, initialMs);
        this.maxMs = Math.max(this.initialMs, maxMs);
    }

    public long nextDelayMs() {
        long delay = Math.min(maxMs, initialMs * (1L << Math.min(attempt, 40)));
        attempt++;
        return delay;
    }

    public void reset() {
        attempt = 0;
    }

    /** Escalation step reached so far; package-private for tests (ruling R3b). */
    int attemptForTest() {
        return attempt;
    }
}

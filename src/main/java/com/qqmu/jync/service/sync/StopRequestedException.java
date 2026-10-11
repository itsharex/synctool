package com.qqmu.jync.service.sync;

/**
 * Raised when a long-running load aborts at a safe boundary because its thread was
 * INTERRUPTED — a stop was requested (CDC disable / rebaseline / node shutdown), which is
 * not a failure.
 *
 * <p>Like {@link LockLostException} it aborts the whole cycle rather than the current
 * table, and already-committed chunks stay (idempotent upserts, checkpoints kept, so the
 * load resumes where it stopped). Unlike a real failure it must not put an ERROR hat on
 * the task: {@code SyncEngine.runCycle} folds it into the result without an ERROR
 * change-log row, and the CDC baseline/consumer path reads the still-set interrupt flag
 * to skip {@code recordCdcFailure} and leave the baseline not-done, so the next start
 * redoes the full load.
 */
public class StopRequestedException extends RuntimeException {

    public StopRequestedException(String message) {
        super(message);
    }
}

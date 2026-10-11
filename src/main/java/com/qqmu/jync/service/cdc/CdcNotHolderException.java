package com.qqmu.jync.service.cdc;

/**
 * Refusal of a CDC operation that must run on the lease-holding node (final review FIX 1):
 * the project's daemon lease is currently held by ANOTHER live owner and this node runs no
 * local consumer, so executing here would mutate the holder's live state (position delete)
 * while the start/stop legs silently no-op — a false success.
 *
 * <p>The message is the bare i18n key ({@code cdc.rebaseline.not.holder}); the API
 * controller maps it to HTTP 409 with {@code {error:"<key>"}} — a conflict with the current
 * lease holder, distinct from the 400-shaped {@link IllegalArgumentException} refusals.
 */
public class CdcNotHolderException extends RuntimeException {

    public CdcNotHolderException(String messageKey) {
        super(messageKey);
    }
}

package com.jrobertgardzinski.offboarding.domain;

/**
 * Who asked for the closure — the basis the whole case is carried out under.
 *
 * <p>It arrives as a word on identity's fact and is ferried to the participants untouched, so it
 * stays a String rather than becoming an enum here: this service does not own the vocabulary, it
 * only has to know which word licenses conditions. Exactly one does.
 *
 * <p>It lives in the domain, and it took a rename to notice it should: the words were constants on
 * the orchestrator while the SAGA STORE needed them too — an administrator's closure that the
 * account's own owner then asks for becomes the owner's, and the store is where that happens. A
 * rule two layers need is not the switchboard's property.
 */
public final class RequestedBy {

    /** The data subject exercising the right to erasure. No condition may survive this. */
    public static final String SELF = "SELF";

    /** The controller of the data acting on its own decision — a ban, house rules. */
    public static final String ADMIN = "ADMIN";

    private RequestedBy() {
    }

    /**
     * Anything that is not exactly {@link #ADMIN} is the account's own owner.
     *
     * <p>Deliberately not a symmetrical test. A missing value is the honest reading of a fact from
     * before the field existed — until then identity had one deletion route and only the owner
     * could walk it — and a garbage value must never be the reason somebody's content survives
     * their own erasure request.
     */
    public static String normalised(String raw) {
        return ADMIN.equals(raw) ? ADMIN : SELF;
    }
}

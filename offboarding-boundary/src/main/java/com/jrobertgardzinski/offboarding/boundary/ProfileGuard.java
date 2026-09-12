package com.jrobertgardzinski.offboarding.boundary;

import java.util.Set;

/**
 * Refuses a start without a declared deployment profile — the same boot-time honesty the stall
 * envs already get (range-checked, refused by name): a start that names no profile is a start
 * nobody decided, and the most common way it happens is a FORGOTTEN variable, which is exactly
 * when strictness matters (default-deny). Tests never enter {@code main}, so they pass untouched.
 */
final class ProfileGuard {

    private static final Set<String> DEPLOYMENT_PROFILES = Set.of("dev", "test", "prod");

    private ProfileGuard() {
    }

    static void requireDeclaredProfile(String variable, String value) {
        if (value == null || value.isBlank())
            throw new IllegalStateException("no deployment profile declared - set " + variable
                    + "=dev|prod; a start nobody decided must not happen");
        if (!DEPLOYMENT_PROFILES.contains(value))
            throw new IllegalStateException("unknown deployment profile '" + value + "' in "
                    + variable + " - declare one of dev|test|prod");
    }
}

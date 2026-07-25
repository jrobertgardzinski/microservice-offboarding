package com.jrobertgardzinski.offboarding.infrastructure;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Main's boot-time configuration hygiene: a mangled numeric env refuses to boot with a message
 * that names the variable (not a bare NumberFormatException), and the sweeper's stall tolerance
 * is floored at the sweep interval — below it /health would call a healthy sweeper stalled.
 */
class MainConfigTest {

    @Test
    void an_absent_or_blank_numeric_env_falls_back_to_the_default() {
        assertEquals(120, Main.parseLongOrRefuse("OFFBOARDING_PURGE_TIMEOUT_SEC", null, 120));
        assertEquals(120, Main.parseLongOrRefuse("OFFBOARDING_PURGE_TIMEOUT_SEC", "  ", 120));
    }

    @Test
    void a_parsable_numeric_env_wins_over_the_default() {
        assertEquals(45, Main.parseLongOrRefuse("OFFBOARDING_CONSUMER_STALL_SEC", " 45 ", 60));
    }

    @Test
    void a_mangled_numeric_env_refuses_to_boot_naming_the_variable() {
        IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> Main.parseLongOrRefuse("OFFBOARDING_PORT", "80 94", 8094));
        assertTrue(refusal.getMessage().contains("OFFBOARDING_PORT"),
                "the refusal must name the variable to fix: " + refusal.getMessage());
        assertTrue(refusal.getMessage().contains("80 94"),
                "and echo the value it choked on: " + refusal.getMessage());
    }

    @Test
    void a_sweeper_stall_below_the_sweep_interval_is_floored_to_the_interval() {
        assertEquals(Main.SWEEP_EVERY,
                Main.flooredSweeperStall(Duration.ofSeconds(1), Main.SWEEP_EVERY));
    }

    @Test
    void a_sweeper_stall_at_or_above_the_interval_is_kept() {
        assertEquals(Duration.ofSeconds(60),
                Main.flooredSweeperStall(Duration.ofSeconds(60), Main.SWEEP_EVERY));
        assertEquals(Main.SWEEP_EVERY,
                Main.flooredSweeperStall(Main.SWEEP_EVERY, Main.SWEEP_EVERY));
    }
}

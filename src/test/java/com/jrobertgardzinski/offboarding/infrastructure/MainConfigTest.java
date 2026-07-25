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
    void a_value_inside_its_range_passes_through() {
        assertEquals(8094, Main.inRangeOrRefuse("OFFBOARDING_PORT", 8094, 1, 65535));
        assertEquals(0, Main.inRangeOrRefuse("OFFBOARDING_MAX_PURGE_RETRIES", 0, 0, 100));
    }

    @Test
    void a_port_outside_1_to_65535_refuses_to_boot_naming_variable_and_value() {
        for (long nonsense : new long[]{0, -1, 65536}) {
            IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                    () -> Main.inRangeOrRefuse("OFFBOARDING_PORT", nonsense, 1, 65535));
            assertTrue(refusal.getMessage().contains("OFFBOARDING_PORT"),
                    "the refusal must name the variable: " + refusal.getMessage());
            assertTrue(refusal.getMessage().contains(String.valueOf(nonsense)),
                    "and echo the value it refused: " + refusal.getMessage());
            assertTrue(refusal.getMessage().contains("65535"),
                    "and state the legal range: " + refusal.getMessage());
        }
    }

    @Test
    void a_retry_budget_outside_0_to_100_refuses_to_boot() {
        assertThrows(IllegalArgumentException.class,
                () -> Main.inRangeOrRefuse("OFFBOARDING_MAX_PURGE_RETRIES", -1, 0, 100));
        assertThrows(IllegalArgumentException.class,
                () -> Main.inRangeOrRefuse("OFFBOARDING_MAX_PURGE_RETRIES", 101, 0, 100));
    }

    @Test
    void a_non_positive_timeout_refuses_to_boot_without_reciting_long_max() {
        // an unbounded maximum must read "at least 1", not "between 1 and 9223372036854775807"
        IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> Main.inRangeOrRefuse("OFFBOARDING_PURGE_TIMEOUT_SEC", 0, 1, Long.MAX_VALUE));
        assertTrue(refusal.getMessage().contains("OFFBOARDING_PURGE_TIMEOUT_SEC"));
        assertTrue(refusal.getMessage().contains("at least 1"),
                "the refusal must state the floor readably: " + refusal.getMessage());
    }

    @Test
    void a_stall_below_the_sweep_interval_is_floored_to_the_interval() {
        assertEquals(Main.SWEEP_EVERY, Main.flooredStall("OFFBOARDING_SWEEPER_STALL_SEC",
                Duration.ofSeconds(1), Main.SWEEP_EVERY));
        assertEquals(Main.SWEEP_EVERY, Main.flooredStall("OFFBOARDING_ALIVE_STALL_SEC",
                Duration.ofSeconds(1), Main.SWEEP_EVERY));
    }

    @Test
    void a_stall_at_or_above_the_interval_is_kept() {
        assertEquals(Duration.ofSeconds(60), Main.flooredStall("OFFBOARDING_SWEEPER_STALL_SEC",
                Duration.ofSeconds(60), Main.SWEEP_EVERY));
        assertEquals(Main.SWEEP_EVERY, Main.flooredStall("OFFBOARDING_SWEEPER_STALL_SEC",
                Main.SWEEP_EVERY, Main.SWEEP_EVERY));
    }
}

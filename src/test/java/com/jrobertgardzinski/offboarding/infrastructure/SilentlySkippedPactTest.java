package com.jrobertgardzinski.offboarding.infrastructure;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The guard on the guards, in the repo that taught the workspace why it is needed: <b>skipping a
 * pact verification is allowed only when the neighbouring repository is genuinely absent.</b>
 *
 * <p>All four provider tests here carry an {@code @EnabledIf}, so that a lone checkout of this
 * service still builds. That guard is reasonable, and it is also how
 * {@link SecurityOutcomePactProviderTest} came to verify nothing at all: security moved to the
 * shared kernel in the 2026-07-12 split, the path followed it to {@code ../../shared/…}, and CI
 * kept cloning every repo flat — so the directory was never there, {@code @EnabledIf} did its job,
 * and JUnit reported SKIPPED, which reads green. A contract test that never runs is
 * indistinguishable from one that passes.
 *
 * <p>So the skip needs a witness. If the neighbour IS checked out and the provider test still
 * cannot find its pact, that is a stale path rather than a lone checkout, and it fails HERE instead
 * of hiding. This class carries no {@code @EnabledIf} of its own on purpose: it has to run in every
 * layout. Modelled on the tests of the same name in microservice-memes and microservice-comments,
 * with one difference that is the whole point — the neighbours are not all siblings. The three
 * content participants sit next to this repo inside {@code portal/}; security sits one level up in
 * {@code shared/}, and that asymmetry is precisely what the flat CI layout flattened away.
 */
class SilentlySkippedPactTest {

    /** Relative to the REPO directory, which is what surefire makes the working directory. */
    private record Neighbour(String root, String repo, String pactFile, String providerTest) {

        Path repoDir() {
            return Path.of(root, repo);
        }

        Path pact() {
            return Path.of(root, repo, "pacts", pactFile);
        }
    }

    private static final List<Neighbour> NEIGHBOURS = List.of(
            new Neighbour("..", "microservice-memes",
                    "microservice-memes-microservice-offboarding.json",
                    "MemesCommandPactProviderTest"),
            new Neighbour("..", "microservice-comments",
                    "microservice-comments-microservice-offboarding.json",
                    "CommentsCommandPactProviderTest"),
            new Neighbour("..", "microservice-user-collections",
                    "microservice-user-collections-microservice-offboarding.json",
                    "CollectionsCommandPactProviderTest"),
            // the one that was skipping: shared kernel, one level above the portal siblings
            new Neighbour("../../shared", "microservice-security",
                    "microservice-security-microservice-offboarding.json",
                    "SecurityOutcomePactProviderTest"));

    @Test
    @DisplayName("every checked-out neighbour's pact is where its provider test looks for it")
    void a_checked_out_neighbour_is_never_skipped() {
        for (Neighbour neighbour : NEIGHBOURS) {
            if (!Files.isDirectory(neighbour.repoDir())) {
                continue;   // genuinely not cloned — the skip is honest and this test has no opinion
            }
            assertTrue(Files.isRegularFile(neighbour.pact()),
                    neighbour.repo() + " IS checked out, so its pact must be readable at "
                            + neighbour.pact() + ". It is not — which means "
                            + neighbour.providerTest() + " skips without verifying anything and"
                            + " reports that as success. Either the path drifted (as it did after"
                            + " the workspace split) or the neighbour stopped recording the pact.");
        }
    }
}

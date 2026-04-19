/**
 * Live profile integration test — verifies that COALESCE_PROFILE wiring works
 * against a real ~/.coa/config with real Coalesce credentials.
 *
 * Excluded from the default vitest run (see vitest.config.ts).
 *
 * Gates:
 *   - The file opts-in when ~/.coa/config exists AND contains a profile named
 *     `dev_testing_workspace` OR `dev_testing_environment`. No env var opt-in
 *     is required — presence of the profile is the opt-in signal.
 *   - Individual tests further gate on whether the profile has a `token=` and
 *     which fields are populated.
 *
 * To run:
 *   npx vitest run tests/integration/live-profile.test.ts
 *
 * What's covered:
 *   - resolveCoalesceAuth() returns a token when ONLY the profile provides one
 *     (env vars cleared in-process).
 *
 * What's NOT covered (intentionally):
 *   - No destructive operations (coa_create/run/deploy/refresh) even though
 *     the dev_testing_* profiles are safe for them — running writes in CI
 *     against real warehouses is worth its own test file with much louder gates.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { resolveCoalesceAuth } from "../../src/services/config/credentials.js";
import {
  loadCoaProfile,
  __resetForTests,
} from "../../src/services/config/coa-config.js";

const CONFIG_PATH = join(homedir(), ".coa", "config");
const CANDIDATE_PROFILES = ["dev_testing_workspace", "dev_testing_environment"];

function detectProfile(): string | null {
  if (!existsSync(CONFIG_PATH)) return null;
  const raw = readFileSync(CONFIG_PATH, "utf8");
  for (const name of CANDIDATE_PROFILES) {
    if (raw.includes(`[${name}]`)) return name;
  }
  return null;
}

const ACTIVE_PROFILE = detectProfile();

describe.skipIf(ACTIVE_PROFILE === null)(
  `Live profile auth — using ${ACTIVE_PROFILE}`,
  { timeout: 60_000 },
  () => {
    beforeEach(() => {
      // Isolate every test from the developer's ambient shell env so that the
      // profile is the ONLY credential source — otherwise env always wins and
      // we can't observe profile-driven auth.
      vi.stubEnv("COALESCE_ACCESS_TOKEN", "");
      vi.stubEnv("COALESCE_BASE_URL", "");
      vi.stubEnv("COALESCE_PROFILE", ACTIVE_PROFILE!);
      __resetForTests();
    });

    afterEach(() => {
      vi.unstubAllEnvs();
      __resetForTests();
    });

    it("loads the profile from ~/.coa/config", () => {
      const profile = loadCoaProfile(ACTIVE_PROFILE!);
      expect(profile).not.toBeNull();
      // Token and domain are what enable Cloud REST auth. Report cleanly if
      // the profile is missing them so the failure points at the config, not
      // at us.
      expect(profile!.token, "profile must have token=").toBeTruthy();
      expect(profile!.domain, "profile must have domain=").toBeTruthy();
    });

    it("resolveCoalesceAuth picks up the profile's token (source=profile:...)", () => {
      const auth = resolveCoalesceAuth();
      expect(auth.accessToken.length).toBeGreaterThan(10);
      expect(auth.sources.accessToken).toBe(`profile:${ACTIVE_PROFILE}`);
      expect(auth.baseUrl).toMatch(/^https:\/\//);
    });
  }
);

// When the config is missing or neither dev_testing_* profile exists, emit a
// single informational "skipped" marker so running this file doesn't silently
// produce zero tests (which reads as "everything passed" in CI logs).
describe.skipIf(ACTIVE_PROFILE !== null)("Live profile auth (skipped)", () => {
  it(
    `skipped — no ${CANDIDATE_PROFILES.join(" or ")} profile found in ~/.coa/config`,
    () => {
      expect(true).toBe(true);
    }
  );
});

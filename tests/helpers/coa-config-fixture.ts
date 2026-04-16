import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { vi } from "vitest";

import { __resetForTests } from "../../src/services/config/coa-config.js";

export interface TempHomeHandle {
  home: string;
  writeConfig: (contents: string) => void;
  cleanup: () => void;
}

/**
 * Creates an empty temp directory and stubs HOME/USERPROFILE so the coa-config
 * loader treats `~/.coa/config` as absent by default. Tests that want a specific
 * profile file call `writeConfig()`.
 */
export function setupTempHome(): TempHomeHandle {
  const home = mkdtempSync(join(tmpdir(), "coalesce-test-home-"));
  mkdirSync(join(home, ".coa"));
  vi.stubEnv("HOME", home);
  vi.stubEnv("USERPROFILE", home);
  __resetForTests();

  return {
    home,
    writeConfig: (contents: string) => {
      writeFileSync(join(home, ".coa", "config"), contents);
      __resetForTests();
    },
    cleanup: () => {
      rmSync(home, { recursive: true, force: true });
      __resetForTests();
    },
  };
}

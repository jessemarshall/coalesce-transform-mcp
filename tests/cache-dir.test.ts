import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { getCacheBaseDir } from "../src/cache-dir.js";
import { setupTempHome, type TempHomeHandle } from "./helpers/coa-config-fixture.js";

describe("getCacheBaseDir precedence", () => {
  let handle: TempHomeHandle;

  beforeEach(() => {
    handle = setupTempHome();
    vi.stubEnv("COALESCE_CACHE_DIR", "");
  });

  afterEach(() => {
    handle.cleanup();
    vi.unstubAllEnvs();
  });

  it("prefers an explicit baseDir argument over env and profile", () => {
    vi.stubEnv("COALESCE_CACHE_DIR", "/from/env");
    handle.writeConfig(`[default]\ntoken=T\ncacheDir=/from/profile\n`);
    expect(getCacheBaseDir("/explicit")).toBe("/explicit");
  });

  it("uses COALESCE_CACHE_DIR when set, ignoring the profile", () => {
    vi.stubEnv("COALESCE_CACHE_DIR", "/from/env");
    handle.writeConfig(`[default]\ntoken=T\ncacheDir=/from/profile\n`);
    expect(getCacheBaseDir()).toBe("/from/env");
  });

  it("falls back to cacheDir from the active profile when env is unset", () => {
    handle.writeConfig(`[default]\ntoken=T\ncacheDir=/from/profile\n`);
    expect(getCacheBaseDir()).toBe("/from/profile");
  });

  it("defaults to cwd when neither env nor profile has a value", () => {
    handle.writeConfig(`[default]\ntoken=T\n`);
    expect(getCacheBaseDir()).toBe(process.cwd());
  });
});

import { describe, it, expect, beforeEach } from "vitest";
import {
  resolveCoaBinary,
  resetCoaBinaryCache,
  CoaNotFoundError,
} from "../../../src/services/coa/resolver.js";

describe("resolveCoaBinary (integration with bundled @coalescesoftware/coa)", () => {
  beforeEach(() => {
    resetCoaBinaryCache();
  });

  it("resolves the bundled coa.js from node_modules", () => {
    const resolved = resolveCoaBinary();
    expect(resolved.source).toBe("bundled");
    expect(resolved.binaryPath).toMatch(/@coalescesoftware[\/\\]coa[\/\\]coa\.js$/);
    // Version may be null on machines where the probe fails (e.g., missing Python runtime),
    // but when the probe succeeds the string must look like a version tag.
    if (resolved.version !== null) {
      expect(resolved.version.length).toBeGreaterThan(0);
    }
  });

  it("caches the result across calls", () => {
    const first = resolveCoaBinary();
    const second = resolveCoaBinary();
    expect(second).toBe(first);
  });

  it("resets via resetCoaBinaryCache", () => {
    const first = resolveCoaBinary();
    resetCoaBinaryCache();
    const second = resolveCoaBinary();
    expect(second).not.toBe(first);
    expect(second.binaryPath).toBe(first.binaryPath);
  });
});

describe("CoaNotFoundError", () => {
  it("has the expected name", () => {
    const err = new CoaNotFoundError("missing");
    expect(err.name).toBe("CoaNotFoundError");
    expect(err.message).toBe("missing");
    expect(err).toBeInstanceOf(Error);
  });
});

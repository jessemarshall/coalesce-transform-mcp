import { describe, it, expect } from "vitest";
import {
  redactSensitive,
  REDACTED_PLACEHOLDER,
} from "../../../src/services/coa/redact.js";

describe("redactSensitive", () => {
  it("leaves plain data untouched and reports didRedact=false", () => {
    const input = { foo: "bar", arr: [1, 2, 3], nested: { ok: true } };
    const result = redactSensitive(input);
    expect(result.didRedact).toBe(false);
    expect(result.value).toEqual(input);
    // Structural clone (not same reference) — caller safety.
    expect(result.value).not.toBe(input);
  });

  it("redacts top-level token-shaped keys", () => {
    const input = { accessToken: "SECRET-X", apiKey: "K", other: "fine" };
    const result = redactSensitive(input);
    expect(result.didRedact).toBe(true);
    expect(result.value.accessToken).toBe(REDACTED_PLACEHOLDER);
    expect(result.value.apiKey).toBe(REDACTED_PLACEHOLDER);
    expect(result.value.other).toBe("fine");
  });

  it("redacts the `coa doctor` cloud-checks detail pattern", () => {
    // Shape verified against real `coa doctor --json` output 2026-04-16.
    const input = {
      data: {
        cloud: {
          checks: [
            { name: "domain", status: "pass", detail: "https://x.coalescesoftware.io" },
            { name: "token", status: "pass", detail: "…ZhQA" },
            { name: "environmentID", status: "pass", detail: "9" },
          ],
        },
      },
    };
    const result = redactSensitive(input);
    expect(result.didRedact).toBe(true);
    const checks = result.value.data.cloud.checks;
    expect(checks[0].detail).toBe("https://x.coalescesoftware.io");
    expect(checks[1].detail).toBe(REDACTED_PLACEHOLDER);
    expect(checks[2].detail).toBe("9");
  });

  it("is case-insensitive on sensitive key names", () => {
    const input = { PASSWORD: "p", Refresh_Token: "r", SECRET: "s" };
    const result = redactSensitive(input);
    expect(result.value.PASSWORD).toBe(REDACTED_PLACEHOLDER);
    expect(result.value.Refresh_Token).toBe(REDACTED_PLACEHOLDER);
    expect(result.value.SECRET).toBe(REDACTED_PLACEHOLDER);
  });

  it("does not redact empty-string values (no false positive on blank detail)", () => {
    const input = { token: "" };
    const result = redactSensitive(input);
    expect(result.didRedact).toBe(false);
    expect(result.value.token).toBe("");
  });

  it("does not mutate the input", () => {
    const input = { token: "SECRET" };
    const snapshot = JSON.parse(JSON.stringify(input));
    redactSensitive(input);
    expect(input).toEqual(snapshot);
  });

  it("walks arrays of objects", () => {
    const input = [
      { name: "a", detail: "public" },
      { name: "token", detail: "SECRET" },
    ];
    const result = redactSensitive(input);
    expect(result.didRedact).toBe(true);
    expect(result.value[0].detail).toBe("public");
    expect(result.value[1].detail).toBe(REDACTED_PLACEHOLDER);
  });

  it("handles non-object scalars (string, number, null) without crashing", () => {
    expect(redactSensitive("plain").value).toBe("plain");
    expect(redactSensitive(42).value).toBe(42);
    expect(redactSensitive(null).value).toBe(null);
    expect(redactSensitive(undefined).value).toBe(undefined);
  });
});

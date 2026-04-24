import { describe, it, expect } from "vitest";
import {
  DOCUMENTED_RUN_STATUSES,
  formatRunStatusForMessage,
  isTerminalRunStatus,
  validateRunStatus,
} from "../../src/workflows/run-status.js";

describe("run-status", () => {
  describe("DOCUMENTED_RUN_STATUSES", () => {
    it("exposes every documented status in the order non-terminal → terminal", () => {
      expect(DOCUMENTED_RUN_STATUSES).toEqual([
        "waitingToRun",
        "running",
        "completed",
        "failed",
        "canceled",
      ]);
    });
  });

  describe("formatRunStatusForMessage", () => {
    it("returns the string when runStatus is a string", () => {
      expect(formatRunStatusForMessage("running")).toBe("running");
      expect(formatRunStatusForMessage("completed")).toBe("completed");
      // Accepts any string — it's a display helper, not a validator
      expect(formatRunStatusForMessage("anythingGoes")).toBe("anythingGoes");
      expect(formatRunStatusForMessage("")).toBe("");
    });

    it("returns 'unknown' for non-string inputs", () => {
      expect(formatRunStatusForMessage(undefined)).toBe("unknown");
      expect(formatRunStatusForMessage(null)).toBe("unknown");
      expect(formatRunStatusForMessage(123)).toBe("unknown");
      expect(formatRunStatusForMessage(true)).toBe("unknown");
      expect(formatRunStatusForMessage({})).toBe("unknown");
      expect(formatRunStatusForMessage([])).toBe("unknown");
    });
  });

  describe("validateRunStatus", () => {
    it("returns the status when it is one of the documented values", () => {
      expect(validateRunStatus(1, "waitingToRun")).toBe("waitingToRun");
      expect(validateRunStatus(1, "running")).toBe("running");
      expect(validateRunStatus(1, "completed")).toBe("completed");
      expect(validateRunStatus(1, "failed")).toBe("failed");
      expect(validateRunStatus(1, "canceled")).toBe("canceled");
    });

    it("throws with a message naming the runCounter for non-string inputs", () => {
      expect(() => validateRunStatus(42, undefined)).toThrow(
        /Run 42 returned a non-string runStatus \(undefined\)/
      );
      expect(() => validateRunStatus(42, null)).toThrow(
        /Run 42 returned a non-string runStatus \(object\)/
      );
      expect(() => validateRunStatus(42, 123)).toThrow(
        /Run 42 returned a non-string runStatus \(number\)/
      );
      expect(() => validateRunStatus(42, {})).toThrow(
        /Run 42 returned a non-string runStatus \(object\)/
      );
    });

    it("throws when runStatus is a string but not a documented value", () => {
      expect(() => validateRunStatus(7, "unknownStatus")).toThrow(
        /Run 7 returned unexpected runStatus 'unknownStatus'/
      );
      // Empty string is a string but not documented — still rejected
      expect(() => validateRunStatus(7, "")).toThrow(
        /Run 7 returned unexpected runStatus ''/
      );
    });

    it("includes the list of expected statuses in the error message", () => {
      expect(() => validateRunStatus(1, "bogus")).toThrow(
        /Expected one of: waitingToRun, running, completed, failed, canceled/
      );
      expect(() => validateRunStatus(1, undefined)).toThrow(
        /Expected one of: waitingToRun, running, completed, failed, canceled/
      );
    });

    it("rejects documented values with differing case (status match is case-sensitive)", () => {
      expect(() => validateRunStatus(1, "RUNNING")).toThrow(
        /Run 1 returned unexpected runStatus 'RUNNING'/
      );
      expect(() => validateRunStatus(1, "Completed")).toThrow(
        /Run 1 returned unexpected runStatus 'Completed'/
      );
    });
  });

  describe("isTerminalRunStatus", () => {
    it("returns true for terminal statuses", () => {
      expect(isTerminalRunStatus("completed")).toBe(true);
      expect(isTerminalRunStatus("failed")).toBe(true);
      expect(isTerminalRunStatus("canceled")).toBe(true);
    });

    it("returns false for non-terminal statuses", () => {
      expect(isTerminalRunStatus("waitingToRun")).toBe(false);
      expect(isTerminalRunStatus("running")).toBe(false);
    });

    it("returns false for unknown strings", () => {
      expect(isTerminalRunStatus("queued")).toBe(false);
      expect(isTerminalRunStatus("")).toBe(false);
      expect(isTerminalRunStatus("RUNNING")).toBe(false);
      expect(isTerminalRunStatus("Completed")).toBe(false);
    });
  });
});

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  isCortexAvailable,
  resetCortexAvailabilityCache,
  askCortex,
  listConnections,
  searchObjects,
  analystQuery,
  runCortexCommand,
} from "../../src/services/cortex/executor.js";
import { execFile } from "node:child_process";

vi.mock("node:child_process", () => ({
  execFile: vi.fn(),
}));

vi.mock("node:util", () => ({
  promisify: (fn: Function) => {
    return (...args: unknown[]) => {
      return new Promise((resolve, reject) => {
        fn(...args, (err: Error | null, result: unknown) => {
          if (err) reject(err);
          else resolve(result);
        });
      });
    };
  },
}));

const mockExecFile = vi.mocked(execFile);

function mockExecFileSuccess(stdout: string, stderr = "") {
  mockExecFile.mockImplementation(
    ((_cmd: string, _args: unknown, _opts: unknown, cb: Function) => {
      cb(null, { stdout, stderr });
    }) as any
  );
}

function mockExecFileFailure(
  code: number | string,
  stdout = "",
  stderr = "",
  killed = false,
  signal?: string
) {
  mockExecFile.mockImplementation(
    ((_cmd: string, _args: unknown, _opts: unknown, cb: Function) => {
      const err = Object.assign(new Error("Command failed"), {
        code,
        stdout,
        stderr,
        killed,
        signal,
      });
      cb(err);
    }) as any
  );
}

describe("Cortex CLI Executor", () => {
  beforeEach(() => {
    resetCortexAvailabilityCache();
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe("isCortexAvailable", () => {
    it("returns true when cortex CLI responds", async () => {
      mockExecFileSuccess("1.2.3");
      const result = await isCortexAvailable();
      expect(result).toBe(true);
    });

    it("returns false when cortex is not found", async () => {
      mockExecFileFailure("ENOENT", "", "command not found");
      const result = await isCortexAvailable();
      expect(result).toBe(false);
    });

    it("caches true permanently", async () => {
      mockExecFileSuccess("1.2.3");
      await isCortexAvailable();
      await isCortexAvailable();
      expect(mockExecFile).toHaveBeenCalledTimes(1);
    });

    it("caches false with TTL", async () => {
      mockExecFileFailure("ENOENT");
      const result = await isCortexAvailable();
      expect(result).toBe(false);
      // Second call within TTL should use cache
      await isCortexAvailable();
      expect(mockExecFile).toHaveBeenCalledTimes(1);
    });
  });

  describe("runCortexCommand", () => {
    it("passes arguments correctly and appends --no-auto-update", async () => {
      mockExecFileSuccess("output");
      await runCortexCommand(["search", "object", "foo"]);
      const call = mockExecFile.mock.calls[0];
      expect(call[0]).toBe("cortex");
      expect(call[1]).toEqual(["search", "object", "foo", "--no-auto-update"]);
    });

    it("appends --connection flag when provided", async () => {
      mockExecFileSuccess("output");
      await runCortexCommand(["search", "object", "foo"], {
        connection: "dev",
      });
      const call = mockExecFile.mock.calls[0];
      expect(call[1]).toEqual([
        "search", "object", "foo",
        "--connection", "dev",
        "--no-auto-update",
      ]);
    });

    it("sets NO_COLOR=1 in env", async () => {
      mockExecFileSuccess("output");
      await runCortexCommand(["--version"]);
      const call = mockExecFile.mock.calls[0];
      const opts = call[2] as { env: Record<string, string> };
      expect(opts.env.NO_COLOR).toBe("1");
    });

    it("strips ANSI codes from output", async () => {
      mockExecFileSuccess("\x1B[32mgreen\x1B[0m text");
      const result = await runCortexCommand(["test"]);
      expect(result.stdout).toBe("green text");
    });

    it("throws on timeout (killed process)", async () => {
      mockExecFileFailure(1, "", "", true);
      await expect(runCortexCommand(["test"])).rejects.toThrow(
        "timed out"
      );
    });

    it("throws on SIGTERM", async () => {
      mockExecFileFailure(1, "", "", false, "SIGTERM");
      await expect(runCortexCommand(["test"])).rejects.toThrow(
        "timed out"
      );
    });

    it("throws on ENOENT (binary not found)", async () => {
      mockExecFileFailure("ENOENT");
      await expect(runCortexCommand(["test"])).rejects.toThrow(
        "Cortex CLI binary not found"
      );
    });

    it("throws on maxBuffer exceeded", async () => {
      mockExecFileFailure("ERR_CHILD_PROCESS_STDIO_MAXBUFFER");
      await expect(runCortexCommand(["test"])).rejects.toThrow(
        "exceeded buffer limit"
      );
    });

    it("throws on other system errors (string code)", async () => {
      mockExecFileFailure("EPERM");
      await expect(runCortexCommand(["test"])).rejects.toThrow(
        "system error EPERM"
      );
    });

    it("returns result with exitCode for normal non-zero exits", async () => {
      mockExecFileFailure(1, "", "some error");
      const result = await runCortexCommand(["test"]);
      expect(result.exitCode).toBe(1);
      expect(result.stderr).toBe("some error");
    });
  });

  describe("askCortex", () => {
    it("passes question with -p and --no-mcp flags", async () => {
      mockExecFileSuccess("the answer");
      await askCortex("what tables exist?");
      const call = mockExecFile.mock.calls[0];
      expect(call[1]).toContain("-p");
      expect(call[1]).toContain("what tables exist?");
      expect(call[1]).toContain("--no-mcp");
    });

    it("returns answer from stdout", async () => {
      mockExecFileSuccess("the answer");
      const result = await askCortex("question");
      expect(result.answer).toBe("the answer");
    });

    it("falls back to stderr when stdout is empty", async () => {
      mockExecFileSuccess("", "answer from stderr");
      const result = await askCortex("question");
      expect(result.answer).toBe("answer from stderr");
    });

    it("throws on non-zero exit code", async () => {
      mockExecFileFailure(1, "", "auth error");
      await expect(askCortex("question")).rejects.toThrow(
        "Cortex query failed (exit 1): auth error"
      );
    });

    it("forwards connection option", async () => {
      mockExecFileSuccess("answer");
      await askCortex("question", { connection: "prod" });
      const call = mockExecFile.mock.calls[0];
      expect(call[1]).toContain("--connection");
      expect(call[1]).toContain("prod");
    });
  });

  describe("searchObjects", () => {
    it("passes query with correct args", async () => {
      mockExecFileSuccess("results");
      await searchObjects("customer tables");
      const call = mockExecFile.mock.calls[0];
      expect(call[1]).toContain("search");
      expect(call[1]).toContain("object");
      expect(call[1]).toContain("customer tables");
    });

    it("includes --max-results when provided", async () => {
      mockExecFileSuccess("results");
      await searchObjects("foo", { maxResults: 5 });
      const call = mockExecFile.mock.calls[0];
      expect(call[1]).toContain("--max-results");
      expect(call[1]).toContain("5");
    });

    it("includes --types when provided", async () => {
      mockExecFileSuccess("results");
      await searchObjects("foo", { types: "TABLE,VIEW" });
      const call = mockExecFile.mock.calls[0];
      expect(call[1]).toContain("--types");
      expect(call[1]).toContain("TABLE,VIEW");
    });

    it("throws on non-zero exit code", async () => {
      mockExecFileFailure(1, "", "search failed");
      await expect(searchObjects("foo")).rejects.toThrow(
        "Cortex search failed"
      );
    });
  });

  describe("analystQuery", () => {
    it("passes question with analyst query args", async () => {
      mockExecFileSuccess("analyst result");
      await analystQuery("top customers");
      const call = mockExecFile.mock.calls[0];
      expect(call[1]).toContain("analyst");
      expect(call[1]).toContain("query");
      expect(call[1]).toContain("top customers");
    });

    it("includes --view when provided", async () => {
      mockExecFileSuccess("result");
      await analystQuery("question", { view: "my_model" });
      const call = mockExecFile.mock.calls[0];
      expect(call[1]).toContain("--view");
      expect(call[1]).toContain("my_model");
    });

    it("includes --model when provided", async () => {
      mockExecFileSuccess("result");
      await analystQuery("question", { model: "claude-3" });
      const call = mockExecFile.mock.calls[0];
      expect(call[1]).toContain("--model");
      expect(call[1]).toContain("claude-3");
    });

    it("throws on non-zero exit code", async () => {
      mockExecFileFailure(1, "", "analyst error");
      await expect(analystQuery("question")).rejects.toThrow(
        "Cortex analyst query failed"
      );
    });
  });

  describe("listConnections", () => {
    it("parses connection list JSON", async () => {
      const connectionJson = JSON.stringify({
        active_connection: "dev",
        connections: {
          dev: { account: "fka56740", user: "JESSEM", role: "SYSADMIN" },
          prod: { account: "abc123", user: "PROD_USER" },
        },
      });
      mockExecFileSuccess(connectionJson);

      const result = await listConnections();
      expect(result.activeConnection).toBe("dev");
      expect(result.connections.dev.account).toBe("fka56740");
      expect(result.connections.prod.user).toBe("PROD_USER");
    });

    it("throws on failure", async () => {
      mockExecFileFailure(1, "", "auth error");
      await expect(listConnections()).rejects.toThrow(
        "Failed to list cortex connections"
      );
    });

    it("throws descriptive error on malformed JSON", async () => {
      mockExecFileSuccess("Warning: login required\nPlease authenticate");
      await expect(listConnections()).rejects.toThrow(
        "Failed to parse cortex connections output as JSON"
      );
    });
  });
});

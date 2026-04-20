import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { chmodSync, mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import {
  saveSubgraphToCache,
  findSubgraphInCache,
  removeSubgraphFromCache,
} from "../../src/services/subgraphs/cache.js";
import {
  scanRepoSubgraphs,
  findRepoSubgraphByName,
} from "../../src/services/subgraphs/repo-scan.js";
import { resolveSubgraphByName } from "../../src/services/subgraphs/resolve.js";
import {
  createSubgraphWithCache,
  updateSubgraphResolved,
  deleteSubgraphByID,
} from "../../src/services/subgraphs/operations.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({}),
    post: vi.fn().mockResolvedValue({}),
    put: vi.fn().mockResolvedValue({}),
    delete: vi.fn().mockResolvedValue({}),
  };
}

let tmpCacheDir: string;

let savedRepoPath: string | undefined;

beforeEach(() => {
  tmpCacheDir = mkdtempSync(join(tmpdir(), "subgraph-cache-test-"));
  process.env.COALESCE_CACHE_DIR = tmpCacheDir;
  savedRepoPath = process.env.COALESCE_REPO_PATH;
  delete process.env.COALESCE_REPO_PATH;
});

afterEach(() => {
  delete process.env.COALESCE_CACHE_DIR;
  if (savedRepoPath !== undefined) {
    process.env.COALESCE_REPO_PATH = savedRepoPath;
  }
  rmSync(tmpCacheDir, { recursive: true, force: true });
});

describe("subgraph cache", () => {
  it("saves and retrieves a subgraph by workspace + name", () => {
    saveSubgraphToCache({
      workspaceID: "ws-1",
      id: "sg-uuid-1",
      name: "Staging",
      steps: ["n-1", "n-2"],
    });

    const found = findSubgraphInCache({ workspaceID: "ws-1", name: "Staging" });
    expect(found?.id).toBe("sg-uuid-1");
    expect(found?.steps).toEqual(["n-1", "n-2"]);
  });

  it("returns null when no match exists", () => {
    expect(findSubgraphInCache({ workspaceID: "ws-1", name: "Missing" })).toBeNull();
  });

  it("scopes cache to workspace — same name in different workspace does not match", () => {
    saveSubgraphToCache({
      workspaceID: "ws-1",
      id: "sg-1",
      name: "Staging",
      steps: [],
    });
    expect(
      findSubgraphInCache({ workspaceID: "ws-2", name: "Staging" })
    ).toBeNull();
  });

  it("overwrites on save when the same name reappears", () => {
    saveSubgraphToCache({
      workspaceID: "ws-1",
      id: "sg-old",
      name: "Staging",
      steps: ["n-1"],
    });
    saveSubgraphToCache({
      workspaceID: "ws-1",
      id: "sg-new",
      name: "Staging",
      steps: ["n-1", "n-2"],
    });
    expect(findSubgraphInCache({ workspaceID: "ws-1", name: "Staging" })?.id).toBe("sg-new");
  });

  it("save does not throw when the cache directory is unwritable", () => {
    const stderrSpy = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    process.env.COALESCE_CACHE_DIR = "/proc/1/root/definitely-not-writable-here";
    try {
      expect(() =>
        saveSubgraphToCache({
          workspaceID: "ws-1",
          id: "sg-1",
          name: "Staging",
          steps: [],
        })
      ).not.toThrow();
      expect(stderrSpy).toHaveBeenCalled();
    } finally {
      process.env.COALESCE_CACHE_DIR = tmpCacheDir;
      stderrSpy.mockRestore();
    }
  });

  it("removes entries on delete", () => {
    saveSubgraphToCache({
      workspaceID: "ws-1",
      id: "sg-1",
      name: "Staging",
      steps: [],
    });
    removeSubgraphFromCache({ workspaceID: "ws-1", id: "sg-1" });
    expect(findSubgraphInCache({ workspaceID: "ws-1", name: "Staging" })).toBeNull();
  });

  it("logs a diagnostic to stderr when the cache file is corrupt", () => {
    const stderrSpy = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    // Seed a valid cache so the on-disk directory + file exist, then corrupt the file.
    saveSubgraphToCache({ workspaceID: "ws-1", id: "sg-1", name: "Seed", steps: [] });
    const cachePath = join(tmpCacheDir, "coalesce_transform_mcp_data_cache", "subgraphs.json");
    writeFileSync(cachePath, "{not valid json");
    try {
      expect(findSubgraphInCache({ workspaceID: "ws-1", name: "Seed" })).toBeNull();
      const joined = stderrSpy.mock.calls
        .map((c) => (typeof c[0] === "string" ? c[0] : String(c[0])))
        .join("");
      expect(joined).toContain("[subgraph-cache]");
      expect(joined).toContain("Corrupt cache");
      expect(joined).toContain("treating as empty");
    } finally {
      stderrSpy.mockRestore();
    }
  });

  it("logs a diagnostic to stderr when the cache file has an unexpected shape", () => {
    const stderrSpy = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    saveSubgraphToCache({ workspaceID: "ws-1", id: "sg-1", name: "Seed", steps: [] });
    const cachePath = join(tmpCacheDir, "coalesce_transform_mcp_data_cache", "subgraphs.json");
    writeFileSync(cachePath, JSON.stringify({ version: 2, entries: [] }));
    try {
      expect(findSubgraphInCache({ workspaceID: "ws-1", name: "Seed" })).toBeNull();
      const joined = stderrSpy.mock.calls
        .map((c) => (typeof c[0] === "string" ? c[0] : String(c[0])))
        .join("");
      expect(joined).toContain("[subgraph-cache]");
      expect(joined).toContain("unexpected shape");
    } finally {
      stderrSpy.mockRestore();
    }
  });
});

describe("repo subgraph scan", () => {
  let repoDir: string;

  beforeEach(() => {
    repoDir = mkdtempSync(join(tmpdir(), "subgraph-repo-test-"));
    mkdirSync(join(repoDir, "subgraphs"), { recursive: true });
    writeFileSync(
      join(repoDir, "subgraphs", "staging.yml"),
      `id: sg-repo-1\nname: Staging\nsteps:\n  - n-1\n  - n-2\nversion: 1\n`
    );
    writeFileSync(
      join(repoDir, "subgraphs", "marts.yaml"),
      `id: sg-repo-2\nname: Marts\nsteps: []\nversion: 1\n`
    );
    writeFileSync(
      join(repoDir, "subgraphs", "not-yaml.txt"),
      `ignored\n`
    );
  });

  afterEach(() => {
    rmSync(repoDir, { recursive: true, force: true });
  });

  it("reads .yml and .yaml files in subgraphs/", () => {
    const subgraphs = scanRepoSubgraphs(repoDir);
    expect(subgraphs).toHaveLength(2);
    expect(subgraphs.map((s) => s.name).sort()).toEqual(["Marts", "Staging"]);
  });

  it("returns empty when subgraphs/ does not exist", () => {
    const emptyRepo = mkdtempSync(join(tmpdir(), "empty-repo-"));
    try {
      expect(scanRepoSubgraphs(emptyRepo)).toEqual([]);
    } finally {
      rmSync(emptyRepo, { recursive: true, force: true });
    }
  });

  it("findRepoSubgraphByName returns the matching entry", () => {
    const match = findRepoSubgraphByName(repoDir, "Staging");
    expect(match?.id).toBe("sg-repo-1");
    expect(match?.steps).toEqual(["n-1", "n-2"]);
  });

  it("findRepoSubgraphByName returns null when no match", () => {
    expect(findRepoSubgraphByName(repoDir, "NoSuch")).toBeNull();
  });

  it("skips YAML files without id or name", () => {
    writeFileSync(
      join(repoDir, "subgraphs", "bad.yml"),
      `name: OnlyName\nsteps: []\n`
    );
    const subgraphs = scanRepoSubgraphs(repoDir);
    // still just the 2 valid entries
    expect(subgraphs).toHaveLength(2);
  });

  it("logs a diagnostic to stderr when a YAML file fails to parse", () => {
    const writes: string[] = [];
    const originalWrite = process.stderr.write.bind(process.stderr);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    process.stderr.write = ((chunk: any) => {
      writes.push(typeof chunk === "string" ? chunk : String(chunk));
      return true;
    }) as typeof process.stderr.write;
    writeFileSync(
      join(repoDir, "subgraphs", "broken.yml"),
      `id: sg-broken\nname: Broken\nsteps:\n  - n-1\n  -- invalid: syntax\n`
    );
    try {
      const subgraphs = scanRepoSubgraphs(repoDir);
      expect(subgraphs.map((s) => s.name).sort()).toEqual(["Marts", "Staging"]);
      const joined = writes.join("");
      expect(joined).toContain("[subgraphs]");
      expect(joined).toContain("broken.yml");
      expect(joined).toContain("YAML parse error");
    } finally {
      process.stderr.write = originalWrite;
    }
  });

  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)(
    "logs a diagnostic to stderr when a YAML file cannot be read",
    () => {
      const writes: string[] = [];
      const originalWrite = process.stderr.write.bind(process.stderr);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      process.stderr.write = ((chunk: any) => {
        writes.push(typeof chunk === "string" ? chunk : String(chunk));
        return true;
      }) as typeof process.stderr.write;
      const unreadable = join(repoDir, "subgraphs", "unreadable.yml");
      writeFileSync(unreadable, `id: sg-u\nname: U\nsteps: []\n`);
      chmodSync(unreadable, 0o000);
      try {
        const subgraphs = scanRepoSubgraphs(repoDir);
        expect(subgraphs.map((s) => s.name).sort()).toEqual(["Marts", "Staging"]);
        const joined = writes.join("");
        expect(joined).toContain("[subgraphs]");
        expect(joined).toContain("unreadable.yml");
        expect(joined).toContain("Failed to read");
      } finally {
        chmodSync(unreadable, 0o600);
        process.stderr.write = originalWrite;
      }
    }
  );

  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)(
    "logs a diagnostic and keeps scanning when a subdirectory cannot be listed",
    () => {
      const writes: string[] = [];
      const originalWrite = process.stderr.write.bind(process.stderr);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      process.stderr.write = ((chunk: any) => {
        writes.push(typeof chunk === "string" ? chunk : String(chunk));
        return true;
      }) as typeof process.stderr.write;
      const lockedDir = join(repoDir, "subgraphs", "locked");
      mkdirSync(lockedDir);
      writeFileSync(
        join(lockedDir, "hidden.yml"),
        `id: sg-hidden\nname: Hidden\nsteps: []\n`
      );
      chmodSync(lockedDir, 0o000);
      try {
        const subgraphs = scanRepoSubgraphs(repoDir);
        expect(subgraphs.map((s) => s.name).sort()).toEqual(["Marts", "Staging"]);
        const joined = writes.join("");
        expect(joined).toContain("[subgraphs]");
        expect(joined).toContain("locked");
        expect(joined).toContain("Failed to list");
      } finally {
        chmodSync(lockedDir, 0o700);
        process.stderr.write = originalWrite;
      }
    }
  );
});

describe("resolveSubgraphByName", () => {
  it("returns cache hit without any I/O", () => {
    saveSubgraphToCache({
      workspaceID: "ws-1",
      id: "sg-cache-1",
      name: "Staging",
      steps: [],
    });

    const result = resolveSubgraphByName({
      workspaceID: "ws-1",
      name: "Staging",
    });

    expect(result.id).toBe("sg-cache-1");
    expect(result.source).toBe("cache");
  });

  it("falls back to repo folder when cache misses", () => {
    const repoDir = mkdtempSync(join(tmpdir(), "subgraph-resolve-repo-"));
    try {
      mkdirSync(join(repoDir, "subgraphs"), { recursive: true });
      writeFileSync(
        join(repoDir, "subgraphs", "marts.yml"),
        `id: sg-repo-marts\nname: Marts\nsteps: []\nversion: 1\n`
      );

      const result = resolveSubgraphByName({
        workspaceID: "ws-1",
        name: "Marts",
        repoPath: repoDir,
      });

      expect(result.id).toBe("sg-repo-marts");
      expect(result.source).toBe("repo");
      // Should have also backfilled cache
      expect(
        findSubgraphInCache({ workspaceID: "ws-1", name: "Marts" })?.id
      ).toBe("sg-repo-marts");
    } finally {
      rmSync(repoDir, { recursive: true, force: true });
    }
  });

  it("throws when cache misses and no repoPath is provided (no workspace fallback)", () => {
    expect(() =>
      resolveSubgraphByName({
        workspaceID: "ws-1",
        name: "Production",
      })
    ).toThrow(/Could not find a subgraph named "Production"/);
  });

  it("lists candidate names from the repo when the requested name is missing", () => {
    const repoDir = mkdtempSync(join(tmpdir(), "subgraph-resolve-missing-"));
    try {
      mkdirSync(join(repoDir, "subgraphs"), { recursive: true });
      writeFileSync(
        join(repoDir, "subgraphs", "alpha.yml"),
        `id: sg-a\nname: Alpha\nsteps: []\nversion: 1\n`
      );
      writeFileSync(
        join(repoDir, "subgraphs", "beta.yml"),
        `id: sg-b\nname: Beta\nsteps: []\nversion: 1\n`
      );

      expect(() =>
        resolveSubgraphByName({
          workspaceID: "ws-1",
          name: "NoSuch",
          repoPath: repoDir,
        })
      ).toThrow(/NoSuch.*Alpha.*Beta|Alpha.*Beta/);
    } finally {
      rmSync(repoDir, { recursive: true, force: true });
    }
  });
});

describe("subgraph operations", () => {
  it("createSubgraphWithCache caches the returned UUID", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ id: "sg-new-uuid", name: "Fresh", steps: ["n-1"] });

    const result = await createSubgraphWithCache(client as any, {
      workspaceID: "ws-1",
      name: "Fresh",
      steps: ["n-1"],
    });

    expect(result.subgraphID).toBe("sg-new-uuid");
    expect(result.cached).toBe(true);
    expect(findSubgraphInCache({ workspaceID: "ws-1", name: "Fresh" })?.id).toBe("sg-new-uuid");
  });

  it("createSubgraphWithCache reports when no ID is returned", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ name: "Ghost" });

    const result = await createSubgraphWithCache(client as any, {
      workspaceID: "ws-1",
      name: "Ghost",
      steps: [],
    });

    expect(result.subgraphID).toBeNull();
    expect(result.cached).toBe(false);
    expect(findSubgraphInCache({ workspaceID: "ws-1", name: "Ghost" })).toBeNull();
  });

  it("updateSubgraphResolved uses subgraphID directly when provided", async () => {
    const client = createMockClient();
    client.put.mockResolvedValue({ id: "sg-1", name: "Updated" });

    const result = await updateSubgraphResolved(client as any, {
      workspaceID: "ws-1",
      subgraphID: "sg-1",
      name: "Updated",
      steps: ["n-1"],
    });

    expect(result.subgraphID).toBe("sg-1");
    expect(result.resolvedFrom).toBe("input");
    expect(client.put).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/subgraphs/sg-1",
      { name: "Updated", steps: ["n-1"] }
    );
  });

  it("updateSubgraphResolved resolves from cache when only name given", async () => {
    saveSubgraphToCache({
      workspaceID: "ws-1",
      id: "sg-cached-1",
      name: "Staging",
      steps: [],
    });
    const client = createMockClient();
    client.put.mockResolvedValue({ id: "sg-cached-1", name: "Staging v2" });

    const result = await updateSubgraphResolved(client as any, {
      workspaceID: "ws-1",
      subgraphName: "Staging",
      name: "Staging v2",
      steps: ["n-1", "n-2"],
    });

    expect(result.subgraphID).toBe("sg-cached-1");
    expect(result.resolvedFrom).toBe("cache");
    // Cache refreshed with new steps
    expect(findSubgraphInCache({ workspaceID: "ws-1", name: "Staging v2" })?.steps).toEqual([
      "n-1",
      "n-2",
    ]);
  });

  it("updateSubgraphResolved throws if neither ID nor name given", async () => {
    const client = createMockClient();
    await expect(
      updateSubgraphResolved(client as any, {
        workspaceID: "ws-1",
        name: "X",
        steps: [],
      })
    ).rejects.toThrow(/subgraphID or subgraphName is required/);
  });

  it("deleteSubgraphByID removes cache entry after successful delete", async () => {
    saveSubgraphToCache({
      workspaceID: "ws-1",
      id: "sg-del-1",
      name: "ToDelete",
      steps: [],
    });
    const client = createMockClient();
    client.delete.mockResolvedValue({});

    const result = await deleteSubgraphByID(client as any, {
      workspaceID: "ws-1",
      subgraphID: "sg-del-1",
    });

    expect(result.subgraphID).toBe("sg-del-1");
    expect(client.delete).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/subgraphs/sg-del-1"
    );
    expect(findSubgraphInCache({ workspaceID: "ws-1", name: "ToDelete" })).toBeNull();
  });
});

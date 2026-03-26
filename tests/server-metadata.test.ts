import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  buildServerEnvironmentVariables,
  getRuntimeEnvironmentVariableNames,
  renderReadmeCoreEnvironmentTable,
  renderReadmeSnowflakeEnvironmentTable,
} from "../scripts/env-metadata.mjs";

type ServerManifest = {
  packages?: Array<{
    environmentVariables?: Array<{
      name?: string;
      description?: string;
      isRequired?: boolean;
      isSecret?: boolean;
      format?: string;
    }>;
  }>;
};

function loadServerManifest(): ServerManifest {
  return JSON.parse(readFileSync("server.json", "utf8")) as ServerManifest;
}

function walkFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const fullPath = join(directory, entry.name);
    if (entry.isDirectory()) {
      return walkFiles(fullPath);
    }
    return entry.isFile() ? [fullPath] : [];
  });
}

function getRuntimeEnvNamesFromSource(): string[] {
  const names = new Set<string>();
  for (const filePath of walkFiles("src")) {
    if (!filePath.endsWith(".ts")) {
      continue;
    }
    const content = readFileSync(filePath, "utf8");
    for (const match of content.matchAll(/process\.env\.([A-Z0-9_]+)/g)) {
      names.add(match[1]);
    }
  }
  return [...names].sort();
}

describe("server.json metadata", () => {
  it("matches the generated MCP environment variable manifest", () => {
    const manifest = loadServerManifest();
    expect(manifest.packages?.[0]?.environmentVariables).toEqual(
      buildServerEnvironmentVariables()
    );
  });

  it("keeps the README environment tables in sync with metadata", () => {
    const readme = readFileSync("README.md", "utf8");

    expect(readme).toContain(renderReadmeCoreEnvironmentTable());
    expect(readme).toContain(renderReadmeSnowflakeEnvironmentTable());
  });

  it("covers every runtime process.env variable in the shared metadata", () => {
    expect(getRuntimeEnvNamesFromSource()).toEqual(
      [...getRuntimeEnvironmentVariableNames()].sort()
    );
  });
});

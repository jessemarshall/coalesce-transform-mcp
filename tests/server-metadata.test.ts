import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

type ServerManifest = {
  packages?: Array<{
    environmentVariables?: Array<{
      name?: string;
      description?: string;
      isRequired?: boolean;
      isSecret?: boolean;
    }>;
  }>;
};

function loadServerManifest(): ServerManifest {
  return JSON.parse(readFileSync("server.json", "utf8")) as ServerManifest;
}

describe("server.json metadata", () => {
  it("advertises the supported runtime environment variables", () => {
    const manifest = loadServerManifest();
    const environmentVariables = manifest.packages?.[0]?.environmentVariables ?? [];
    const names = environmentVariables.map((entry) => entry.name);

    expect(names).toEqual([
      "COALESCE_ACCESS_TOKEN",
      "COALESCE_BASE_URL",
      "COALESCE_ORG_ID",
      "COALESCE_REPO_PATH",
      "COALESCE_MCP_AUTO_CACHE_MAX_BYTES",
      "COALESCE_MCP_MAX_REQUEST_BODY_BYTES",
      "SNOWFLAKE_USERNAME",
      "SNOWFLAKE_KEY_PAIR_KEY",
      "SNOWFLAKE_KEY_PAIR_PASS",
      "SNOWFLAKE_WAREHOUSE",
      "SNOWFLAKE_ROLE",
    ]);
  });

  it("marks the documented required and secret variables correctly", () => {
    const manifest = loadServerManifest();
    const environmentVariables = manifest.packages?.[0]?.environmentVariables ?? [];
    const byName = new Map(
      environmentVariables.flatMap((entry) =>
        entry.name ? [[entry.name, entry]] : []
      )
    );

    expect(byName.get("COALESCE_ACCESS_TOKEN")).toMatchObject({
      isRequired: true,
      isSecret: true,
    });
    expect(byName.get("SNOWFLAKE_KEY_PAIR_PASS")).toMatchObject({
      isRequired: false,
      isSecret: true,
    });
    expect(byName.get("COALESCE_REPO_PATH")?.description).toContain("repo-backed tools");
    expect(byName.get("COALESCE_MCP_AUTO_CACHE_MAX_BYTES")?.description).toContain("auto-caching");
    expect(byName.get("COALESCE_MCP_MAX_REQUEST_BODY_BYTES")?.description).toContain("request body size");
  });
});

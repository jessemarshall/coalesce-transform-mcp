import { describe, it, expect, vi, beforeEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { registerResources } from "../src/resources/index.js";

describe("Resources", () => {
  let server: McpServer;
  let resourceSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    server = new McpServer({ name: "test", version: "0.0.1" });
    resourceSpy = vi.spyOn(server, "resource");
  });

  it("registers all fixed Coalesce context resources", () => {
    registerResources(server);

    expect(resourceSpy).toHaveBeenCalledTimes(19);
    const calls = resourceSpy.mock.calls.map((call) => ({
      name: call[0],
      uri: call[1],
    }));
    expect(calls).toEqual([
      {
        name: "Coalesce Overview",
        uri: "coalesce://context/overview",
      },
      {
        name: "SQL Platform Selection",
        uri: "coalesce://context/sql-platform-selection",
      },
      {
        name: "SQL Rules: Snowflake",
        uri: "coalesce://context/sql-snowflake",
      },
      {
        name: "SQL Rules: Databricks",
        uri: "coalesce://context/sql-databricks",
      },
      {
        name: "SQL Rules: BigQuery",
        uri: "coalesce://context/sql-bigquery",
      },
      {
        name: "Data Engineering Principles",
        uri: "coalesce://context/data-engineering-principles",
      },
      {
        name: "Storage Locations and References",
        uri: "coalesce://context/storage-mappings",
      },
      {
        name: "Tool Usage Patterns",
        uri: "coalesce://context/tool-usage",
      },
      {
        name: "ID Discovery",
        uri: "coalesce://context/id-discovery",
      },
      {
        name: "Node Creation Decision Tree",
        uri: "coalesce://context/node-creation-decision-tree",
      },
      {
        name: "Node Payloads",
        uri: "coalesce://context/node-payloads",
      },
      {
        name: "Hydrated Metadata",
        uri: "coalesce://context/hydrated-metadata",
      },
      {
        name: "Run Operations",
        uri: "coalesce://context/run-operations",
      },
      {
        name: "Node Type Corpus",
        uri: "coalesce://context/node-type-corpus",
      },
      {
        name: "Aggregation Patterns",
        uri: "coalesce://context/aggregation-patterns",
      },
      {
        name: "Intelligent Node Configuration",
        uri: "coalesce://context/intelligent-node-configuration",
      },
      {
        name: "Pipeline Workflows",
        uri: "coalesce://context/pipeline-workflows",
      },
      {
        name: "Node Operations",
        uri: "coalesce://context/node-operations",
      },
      {
        name: "Node Type Selection Guide",
        uri: "coalesce://context/node-type-selection-guide",
      },
    ]);
  });

  it("returns markdown content for a registered resource", async () => {
    registerResources(server);

    const overviewCall = resourceSpy.mock.calls.find(
      (call) => call[1] === "coalesce://context/overview"
    );

    expect(overviewCall).toBeDefined();
    const readCallback = overviewCall?.[3];
    expect(typeof readCallback).toBe("function");

    const result = await readCallback?.(
      new URL("coalesce://context/overview"),
      {} as never
    );

    expect(result).toEqual({
      contents: [
        expect.objectContaining({
          uri: "coalesce://context/overview",
          mimeType: "text/markdown",
          text: expect.stringContaining("Coalesce"),
        }),
      ],
    });
  });

  it("can read every registered resource callback", async () => {
    registerResources(server);

    for (const call of resourceSpy.mock.calls) {
      const uri = call[1] as string;
      const readCallback = call[3];

      expect(typeof readCallback).toBe("function");

      const result = await readCallback(new URL(uri), {} as never);
      expect(result).toEqual({
        contents: [
          expect.objectContaining({
            uri,
            mimeType: "text/markdown",
            text: expect.any(String),
          }),
        ],
      });
      expect(result.contents[0]?.text.length).toBeGreaterThan(0);
    }
  });
});

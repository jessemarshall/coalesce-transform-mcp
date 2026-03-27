import { describe, it, expect } from "vitest";
import {
  parseTableReference,
  extractTableReference,
  searchCoalesceForTable,
} from "../../src/services/exploration/search.js";
import { createMockClient } from "../helpers/fixtures.js";

describe("parseTableReference", () => {
  it("parses a single table name", () => {
    expect(parseTableReference("CUSTOMERS")).toEqual({ table: "CUSTOMERS" });
  });

  it("parses 2-part reference with both database and schema set to same value", () => {
    const result = parseTableReference("RAW.CUSTOMERS");
    expect(result).toEqual({
      database: "RAW",
      schema: "RAW",
      table: "CUSTOMERS",
    });
  });

  it("parses DATABASE.SCHEMA.TABLE", () => {
    expect(parseTableReference("RAW.PUBLIC.CUSTOMERS")).toEqual({
      database: "RAW",
      schema: "PUBLIC",
      table: "CUSTOMERS",
    });
  });

  it("uppercases all parts", () => {
    expect(parseTableReference("raw.public.customers")).toEqual({
      database: "RAW",
      schema: "PUBLIC",
      table: "CUSTOMERS",
    });
  });

  it("strips quotes", () => {
    const result = parseTableReference('"RAW"."CUSTOMERS"');
    expect(result).toEqual({
      database: "RAW",
      schema: "RAW",
      table: "CUSTOMERS",
    });
  });

  it("returns null for empty string", () => {
    expect(parseTableReference("")).toBeNull();
  });

  it("returns null for strings with spaces", () => {
    expect(parseTableReference("some table")).toBeNull();
  });

  it("returns null for too many parts", () => {
    expect(parseTableReference("A.B.C.D")).toBeNull();
  });
});

describe("extractTableReference", () => {
  it("extracts dotted reference from a question", () => {
    expect(extractTableReference("what columns does RAW.CUSTOMERS have?")).toBe(
      "RAW.CUSTOMERS"
    );
  });

  it("extracts three-part reference", () => {
    expect(
      extractTableReference("show me data from RAW.PUBLIC.CUSTOMERS")
    ).toBe("RAW.PUBLIC.CUSTOMERS");
  });

  it("extracts uppercase single identifier", () => {
    expect(extractTableReference("describe CUSTOMERS table")).toBe("CUSTOMERS");
  });

  it("returns null for no identifiers", () => {
    expect(extractTableReference("what tables exist?")).toBeNull();
  });
});

describe("searchCoalesceForTable", () => {
  it("finds a matching node in a workspace", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path.includes("/workspaces") && !path.includes("/nodes")) {
        return Promise.resolve({
          data: [{ id: "ws-1" }],
        });
      }
      if (path.includes("/nodes")) {
        return Promise.resolve({
          data: [
            { id: "node-1", name: "CUSTOMERS", database: "RAW", schema: "PUBLIC", nodeType: "Stage" },
            { id: "node-2", name: "ORDERS", database: "RAW", schema: "PUBLIC", nodeType: "Stage" },
          ],
        });
      }
      return Promise.resolve({ data: [] });
    });

    const result = await searchCoalesceForTable(
      client as any,
      { database: "RAW", schema: "RAW", table: "CUSTOMERS" }
    );

    expect(result.found).toBe(true);
    expect(result.matches).toHaveLength(1);
    expect(result.matches[0].name).toBe("CUSTOMERS");
    expect(result.matches[0].nodeID).toBe("node-1");
    expect(result.matches[0].workspaceID).toBe("ws-1");
    expect(result.skippedWorkspaces).toHaveLength(0);
  });

  it("matches 2-part ref against schema field", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path.includes("/workspaces") && !path.includes("/nodes")) {
        return Promise.resolve({ data: [{ id: "ws-1" }] });
      }
      if (path.includes("/nodes")) {
        return Promise.resolve({
          data: [
            { id: "node-1", name: "CUSTOMERS", database: "MY_DB", schema: "PUBLIC", nodeType: "Stage" },
          ],
        });
      }
      return Promise.resolve({ data: [] });
    });

    // 2-part ref "PUBLIC.CUSTOMERS" — qualifier matches schema
    const result = await searchCoalesceForTable(
      client as any,
      { database: "PUBLIC", schema: "PUBLIC", table: "CUSTOMERS" }
    );

    expect(result.found).toBe(true);
    expect(result.matches[0].name).toBe("CUSTOMERS");
  });

  it("returns not found when no match", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path.includes("/workspaces") && !path.includes("/nodes")) {
        return Promise.resolve({ data: [{ id: "ws-1" }] });
      }
      if (path.includes("/nodes")) {
        return Promise.resolve({
          data: [
            { id: "node-1", name: "ORDERS", database: "RAW", nodeType: "Stage" },
          ],
        });
      }
      return Promise.resolve({ data: [] });
    });

    const result = await searchCoalesceForTable(
      client as any,
      { database: "RAW", schema: "RAW", table: "CUSTOMERS" }
    );

    expect(result.found).toBe(false);
    expect(result.matches).toHaveLength(0);
    expect(result.searchedWorkspaces).toEqual(["ws-1"]);
  });

  it("searches only specified workspace when workspaceID provided", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path.includes("/nodes")) {
        return Promise.resolve({
          data: [
            { id: "node-1", name: "CUSTOMERS", database: "RAW", nodeType: "Stage" },
          ],
        });
      }
      return Promise.resolve({ data: [] });
    });

    const result = await searchCoalesceForTable(
      client as any,
      { table: "CUSTOMERS" },
      "ws-specific"
    );

    expect(result.found).toBe(true);
    expect(result.searchedWorkspaces).toEqual(["ws-specific"]);
    // Should NOT call listWorkspaces
    expect(client.get).not.toHaveBeenCalledWith(
      expect.stringMatching(/\/workspaces$/),
      expect.anything()
    );
  });

  it("tracks skipped workspaces on access errors", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path.includes("/workspaces") && !path.includes("/nodes")) {
        return Promise.resolve({ data: [{ id: "ws-1" }, { id: "ws-2" }] });
      }
      if (path.includes("ws-1") && path.includes("/nodes")) {
        const err = Object.assign(new Error("Forbidden"), { status: 403 });
        return Promise.reject(err);
      }
      if (path.includes("ws-2") && path.includes("/nodes")) {
        return Promise.resolve({
          data: [{ id: "n-1", name: "CUSTOMERS", nodeType: "Stage" }],
        });
      }
      return Promise.resolve({ data: [] });
    });

    const result = await searchCoalesceForTable(
      client as any,
      { table: "CUSTOMERS" }
    );

    expect(result.found).toBe(true);
    expect(result.searchedWorkspaces).toEqual(["ws-1", "ws-2"]);
    expect(result.skippedWorkspaces).toEqual([
      { id: "ws-1", reason: "access denied" },
    ]);
  });

  it("throws when all workspaces fail", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path.includes("/workspaces") && !path.includes("/nodes")) {
        return Promise.resolve({ data: [{ id: "ws-1" }, { id: "ws-2" }] });
      }
      return Promise.reject(new Error("Network error"));
    });

    await expect(
      searchCoalesceForTable(client as any, { table: "CUSTOMERS" })
    ).rejects.toThrow("Failed to search any Coalesce workspace");
  });

  it("throws when listWorkspaces fails", async () => {
    const client = createMockClient();
    client.get.mockRejectedValue(new Error("API down"));

    await expect(
      searchCoalesceForTable(client as any, { table: "CUSTOMERS" })
    ).rejects.toThrow("Failed to list Coalesce workspaces");
  });
});

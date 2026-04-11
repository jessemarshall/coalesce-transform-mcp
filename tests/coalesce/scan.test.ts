import { describe, it, expect, vi, beforeEach } from "vitest";
import { scanResourcesByID } from "../../src/coalesce/api/scan.js";
import { CoalesceApiError } from "../../src/client.js";

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

describe("scanResourcesByID", () => {
  let stderrSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    stderrSpy = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
  });

  it("returns resources that exist and skips 404s", async () => {
    const client = createMockClient();
    client.get.mockImplementation(async (path: string) => {
      if (path === "/api/v1/items/3") return { id: 3, name: "C" };
      if (path === "/api/v1/items/7") return { id: 7, name: "G" };
      throw new CoalesceApiError("Not found", 404);
    });

    const result = await scanResourcesByID(client as any, "/api/v1/items");

    expect(result.data).toHaveLength(2);
    expect(result.data[0]).toEqual({ id: 3, name: "C" });
    expect(result.data[1]).toEqual({ id: 7, name: "G" });
  });

  it("returns results sorted by numeric id", async () => {
    const client = createMockClient();
    // Return IDs out of order to verify sort
    client.get.mockImplementation(async (path: string) => {
      const match = path.match(/\/(\d+)$/);
      const id = match ? Number(match[1]) : 0;
      if (id === 10) return { id: 10, name: "Ten" };
      if (id === 2) return { id: 2, name: "Two" };
      if (id === 5) return { id: 5, name: "Five" };
      throw new CoalesceApiError("Not found", 404);
    });

    const result = await scanResourcesByID(client as any, "/api/v1/items");

    expect(result.data).toEqual([
      { id: 2, name: "Two" },
      { id: 5, name: "Five" },
      { id: 10, name: "Ten" },
    ]);
  });

  it("handles resources with non-numeric id fields without NaN sort errors", async () => {
    const client = createMockClient();
    client.get.mockImplementation(async (path: string) => {
      if (path === "/api/v1/items/1") return { id: "abc", name: "Alpha" };
      if (path === "/api/v1/items/2") return { id: 2, name: "Beta" };
      if (path === "/api/v1/items/3") return { id: "xyz", name: "Gamma" };
      throw new CoalesceApiError("Not found", 404);
    });

    const result = await scanResourcesByID(client as any, "/api/v1/items");

    // Non-numeric ids should sort to position 0, so they come before numeric IDs
    expect(result.data).toHaveLength(3);
    // Numeric id=2 should appear after the non-numeric ones (which both sort as 0)
    const numericItem = result.data.find(
      (r: any) => typeof r === "object" && r !== null && r.id === 2
    );
    expect(numericItem).toBeDefined();
  });

  it("returns empty data when no resources exist", async () => {
    const client = createMockClient();
    client.get.mockRejectedValue(new CoalesceApiError("Not found", 404));

    const result = await scanResourcesByID(client as any, "/api/v1/items");

    expect(result.data).toHaveLength(0);
    expect(stderrSpy).toHaveBeenCalledWith(
      expect.stringContaining("found no resources")
    );
  });

  it("propagates non-404 errors immediately", async () => {
    const client = createMockClient();
    client.get.mockRejectedValue(new CoalesceApiError("Forbidden", 403));

    await expect(
      scanResourcesByID(client as any, "/api/v1/items")
    ).rejects.toThrow("Forbidden");
  });

  it("respects the limit parameter and stops early", async () => {
    const client = createMockClient();
    // Return a resource for every ID
    client.get.mockImplementation(async (path: string) => {
      const match = path.match(/\/(\d+)$/);
      const id = match ? Number(match[1]) : 0;
      return { id, name: `Item ${id}` };
    });

    const result = await scanResourcesByID(client as any, "/api/v1/items", 3);

    expect(result.data).toHaveLength(3);
  });

  it("continues scanning when resources exist near the tail of a batch", async () => {
    const client = createMockClient();
    // Resource at ID 48 (near end of first batch of 50), forces a second batch
    // Resource at ID 55 in the second batch
    client.get.mockImplementation(async (path: string) => {
      const match = path.match(/\/(\d+)$/);
      const id = match ? Number(match[1]) : 0;
      if (id === 48) return { id: 48, name: "Near tail" };
      if (id === 55) return { id: 55, name: "Next batch" };
      throw new CoalesceApiError("Not found", 404);
    });

    const result = await scanResourcesByID(client as any, "/api/v1/items");

    expect(result.data).toHaveLength(2);
    expect(result.data[0]).toEqual({ id: 48, name: "Near tail" });
    expect(result.data[1]).toEqual({ id: 55, name: "Next batch" });
  });

  it("stops scanning when no resources exist in the tail of a batch", async () => {
    const client = createMockClient();
    // Only resource at ID 5 (well before the tail threshold)
    client.get.mockImplementation(async (path: string) => {
      if (path === "/api/v1/items/5") return { id: 5, name: "Early" };
      throw new CoalesceApiError("Not found", 404);
    });

    const result = await scanResourcesByID(client as any, "/api/v1/items");

    expect(result.data).toHaveLength(1);
    // Should not scan beyond first batch since highest found (5) < tail start (41)
    const maxIdScanned = Math.max(
      ...client.get.mock.calls.map((call: string[]) => {
        const match = call[0].match(/\/(\d+)$/);
        return match ? Number(match[1]) : 0;
      })
    );
    expect(maxIdScanned).toBeLessThanOrEqual(50);
  });

  // --- Input validation ---

  it("rejects empty basePath", async () => {
    const client = createMockClient();
    await expect(
      scanResourcesByID(client as any, "")
    ).rejects.toThrow("Invalid basePath");
  });

  it("rejects basePath without leading slash", async () => {
    const client = createMockClient();
    await expect(
      scanResourcesByID(client as any, "api/v1/items")
    ).rejects.toThrow("Invalid basePath");
  });

  it("rejects basePath with unsafe characters", async () => {
    const client = createMockClient();
    await expect(
      scanResourcesByID(client as any, "/api/v1/items?q=x")
    ).rejects.toThrow("Invalid basePath");
  });

  it("rejects non-positive limit", async () => {
    const client = createMockClient();
    await expect(
      scanResourcesByID(client as any, "/api/v1/items", 0)
    ).rejects.toThrow("Invalid scan limit");
  });

  it("rejects non-integer limit", async () => {
    const client = createMockClient();
    await expect(
      scanResourcesByID(client as any, "/api/v1/items", 2.5)
    ).rejects.toThrow("Invalid scan limit");
  });

  it("rejects negative limit", async () => {
    const client = createMockClient();
    await expect(
      scanResourcesByID(client as any, "/api/v1/items", -1)
    ).rejects.toThrow("Invalid scan limit");
  });

  it("handles resources without id field in sort", async () => {
    const client = createMockClient();
    client.get.mockImplementation(async (path: string) => {
      if (path === "/api/v1/items/1") return { name: "No ID" };
      if (path === "/api/v1/items/2") return { id: 2, name: "Has ID" };
      throw new CoalesceApiError("Not found", 404);
    });

    const result = await scanResourcesByID(client as any, "/api/v1/items");

    expect(result.data).toHaveLength(2);
    // Resource without id sorts as 0, so it comes first
    expect((result.data[0] as any).name).toBe("No ID");
    expect((result.data[1] as any).name).toBe("Has ID");
  });
});

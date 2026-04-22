import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../src/coalesce/api/nodes.js", () => ({
  listWorkspaceNodes: vi.fn(),
  getWorkspaceNode: vi.fn(),
}));

beforeEach(() => {
  vi.resetAllMocks();
});

import { listWorkspaceNodes, getWorkspaceNode } from "../../src/coalesce/api/nodes.js";
import {
  isUniqueStorageLocationNameError,
  findWorkspaceNodeIndexEntry,
  findExistingNodeForCreation,
  recoverFromUniqueNameError,
} from "../../src/services/workspace/duplicate-detection.js";
import { CoalesceApiError } from "../../src/client.js";
import type { CoalesceClient } from "../../src/client.js";

const mockListWorkspaceNodes = vi.mocked(listWorkspaceNodes);
const mockGetWorkspaceNode = vi.mocked(getWorkspaceNode);

function makeClient(): CoalesceClient {
  return {} as CoalesceClient;
}

describe("isUniqueStorageLocationNameError", () => {
  it("detects the Coalesce unique-name 400 by message", () => {
    const error = new CoalesceApiError(
      "Nodes assigned to the same Storage Location must have unique names.",
      400
    );
    expect(isUniqueStorageLocationNameError(error)).toBe(true);
  });

  it("detects the error when the text is nested in the detail body", () => {
    const error = new CoalesceApiError("Bad request", 400, {
      error: {
        errorString:
          "Nodes assigned to the same Storage Location must have unique names.",
      },
    });
    expect(isUniqueStorageLocationNameError(error)).toBe(true);
  });

  it("ignores unrelated 400s", () => {
    const error = new CoalesceApiError("Some validation error", 400, {
      error: { errorString: "field is required" },
    });
    expect(isUniqueStorageLocationNameError(error)).toBe(false);
  });

  it("ignores non-CoalesceApiError errors", () => {
    expect(isUniqueStorageLocationNameError(new Error("unique names"))).toBe(false);
  });

  it("ignores non-400 statuses", () => {
    const error = new CoalesceApiError("unique names", 500);
    expect(isUniqueStorageLocationNameError(error)).toBe(false);
  });
});

describe("findWorkspaceNodeIndexEntry", () => {
  it("returns the matching entry when name matches", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [
        { id: "n-1", name: "OTHER", locationName: "RAW" },
        { id: "n-2", name: "RAMESH_COCO_TEST", locationName: "EDM" },
      ],
    });

    const found = await findWorkspaceNodeIndexEntry(makeClient(), "ws-1", {
      name: "RAMESH_COCO_TEST",
    });

    expect(found).toMatchObject({ id: "n-2", name: "RAMESH_COCO_TEST" });
  });

  it("filters by locationName when provided", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [
        { id: "n-1", name: "RAMESH_COCO_TEST", locationName: "RAW" },
        { id: "n-2", name: "RAMESH_COCO_TEST", locationName: "EDM" },
      ],
    });

    const found = await findWorkspaceNodeIndexEntry(makeClient(), "ws-1", {
      name: "RAMESH_COCO_TEST",
      locationName: "EDM",
    });

    expect(found).toMatchObject({ id: "n-2", locationName: "EDM" });
  });

  it("prefers a nodeType match when multiple nodes share name+location", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [
        { id: "n-1", name: "CUSTOMER", locationName: "EDM", nodeType: "Stage" },
        {
          id: "n-2",
          name: "CUSTOMER",
          locationName: "EDM",
          nodeType: "Dimension",
        },
      ],
    });

    const found = await findWorkspaceNodeIndexEntry(makeClient(), "ws-1", {
      name: "CUSTOMER",
      locationName: "EDM",
      nodeType: "Dimension",
    });

    expect(found).toMatchObject({ id: "n-2", nodeType: "Dimension" });
  });

  it("matches bare-vs-prefixed nodeType IDs", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [
        {
          id: "n-1",
          name: "ORDERS",
          locationName: "EDM",
          nodeType: "base-nodes:::Stage",
        },
      ],
    });

    const found = await findWorkspaceNodeIndexEntry(makeClient(), "ws-1", {
      name: "ORDERS",
      nodeType: "Stage",
    });

    expect(found).toMatchObject({ id: "n-1" });
  });

  it("returns null when nothing matches", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [{ id: "n-1", name: "OTHER", locationName: "RAW" }],
    });

    const found = await findWorkspaceNodeIndexEntry(makeClient(), "ws-1", {
      name: "NOT_THERE",
    });

    expect(found).toBeNull();
  });
});

describe("findExistingNodeForCreation", () => {
  it("returns preExisting result with the full node body", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [
        { id: "n-42", name: "RAMESH_COCO_TEST", locationName: "EDM", nodeType: "Stage" },
      ],
    });
    mockGetWorkspaceNode.mockResolvedValueOnce({
      id: "n-42",
      name: "RAMESH_COCO_TEST",
      locationName: "EDM",
      nodeType: "Stage",
      metadata: { columns: [{ name: "ID" }] },
    });

    const result = await findExistingNodeForCreation(makeClient(), {
      workspaceID: "ws-1",
      name: "RAMESH_COCO_TEST",
      locationName: "EDM",
      nodeType: "Stage",
    });

    expect(result).not.toBeNull();
    expect(result?.preExisting).toBe(true);
    expect(result?.node).toMatchObject({ id: "n-42", metadata: { columns: [{ name: "ID" }] } });
    expect(result?.warning).toMatch(/already exists/i);
    expect(result?.warning).not.toMatch(/timed out/i);
    expect(result?.nextSteps.length).toBeGreaterThan(0);
  });

  it("returns null when name matches but nodeType does not", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [
        { id: "n-7", name: "CUSTOMER", locationName: "EDM", nodeType: "Stage" },
      ],
    });

    const result = await findExistingNodeForCreation(makeClient(), {
      workspaceID: "ws-1",
      name: "CUSTOMER",
      locationName: "EDM",
      nodeType: "Dimension",
    });

    expect(result).toBeNull();
    // getWorkspaceNode must not be called when there's no confident match.
    expect(mockGetWorkspaceNode).not.toHaveBeenCalled();
  });

  it("rethrows auth errors from the index lookup instead of returning null", async () => {
    mockListWorkspaceNodes.mockRejectedValueOnce(
      new CoalesceApiError("Invalid or expired access token", 401)
    );

    await expect(
      findExistingNodeForCreation(makeClient(), {
        workspaceID: "ws-1",
        name: "X",
      })
    ).rejects.toBeInstanceOf(CoalesceApiError);
  });

  it("returns null for recoverable errors (e.g. 404 on index)", async () => {
    mockListWorkspaceNodes.mockRejectedValueOnce(
      new CoalesceApiError("not found", 404)
    );

    const result = await findExistingNodeForCreation(makeClient(), {
      workspaceID: "ws-1",
      name: "X",
    });
    expect(result).toBeNull();
  });
});

describe("recoverFromUniqueNameError", () => {
  it("produces a warning that mentions the timeout-retry cause", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [
        { id: "n-1", name: "ORDERS", locationName: "EDM", nodeType: "Stage" },
      ],
    });
    mockGetWorkspaceNode.mockResolvedValueOnce({
      id: "n-1",
      name: "ORDERS",
      locationName: "EDM",
    });

    const result = await recoverFromUniqueNameError(makeClient(), {
      workspaceID: "ws-1",
      name: "ORDERS",
      locationName: "EDM",
      nodeType: "Stage",
    });

    expect(result?.preExisting).toBe(true);
    expect(result?.warning).toMatch(/timed out at the client/);
  });
});

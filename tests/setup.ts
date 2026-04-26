import { beforeEach } from "vitest";

beforeEach(async () => {
  const [
    { clearWorkspaceInventoryCache },
    { clearWorkspaceNodeIndexCache },
    { clearWorkspaceNodeDetailIndexCache },
  ] = await Promise.all([
    import("../src/services/cache/workspace-inventory.js"),
    import("../src/services/cache/workspace-node-index.js"),
    import("../src/services/cache/workspace-node-detail-index.js"),
  ]);
  clearWorkspaceInventoryCache();
  clearWorkspaceNodeIndexCache();
  clearWorkspaceNodeDetailIndexCache();
});

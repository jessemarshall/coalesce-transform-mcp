import { beforeEach } from "vitest";

beforeEach(async () => {
  const [{ clearWorkspaceInventoryCache }, { clearWorkspaceNodeIndexCache }] =
    await Promise.all([
      import("../src/services/cache/workspace-inventory.js"),
      import("../src/services/cache/workspace-node-index.js"),
    ]);
  clearWorkspaceInventoryCache();
  clearWorkspaceNodeIndexCache();
});

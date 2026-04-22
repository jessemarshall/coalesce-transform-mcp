import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    setupFiles: ["./tests/setup.ts"],
    exclude: [
      "**/node_modules/**",
      "**/dist/**",
      "**/.worktrees/**",
      "**/.claude/**",
      "tests/integration/live-api.test.ts",
      // NOTE: tests/integration/live-profile.test.ts intentionally runs by default.
      // It self-gates via `describe.skipIf()` on the presence of a `dev_testing_*`
      // profile in ~/.coa/config — absent on CI, so it's a no-op there. Local
      // runs exercise real COALESCE_PROFILE wiring when the profile is present.
    ],
  },
});

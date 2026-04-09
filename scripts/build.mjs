import { spawnSync } from "node:child_process";
import { cpSync, existsSync, mkdirSync, rmSync } from "node:fs";
import { join } from "node:path";

const cwd = process.cwd();
const distDir = join(cwd, "dist");
const resourceSourceDir = join(cwd, "src", "resources", "context");
const resourceDestinationDir = join(distDir, "resources", "context");
const generatedSourceDir = join(cwd, "generated");
const generatedDestinationDir = join(distDir, "generated");
const tscBin = join(
  cwd,
  "node_modules",
  ".bin",
  process.platform === "win32" ? "tsc.cmd" : "tsc"
);

rmSync(distDir, { recursive: true, force: true });

const tscResult = spawnSync(tscBin, [], {
  cwd,
  stdio: "inherit",
  shell: process.platform === "win32",
});

if (tscResult.status !== 0) {
  process.exit(tscResult.status ?? 1);
}

if (!existsSync(resourceSourceDir)) {
  throw new Error(`Resource source directory not found: ${resourceSourceDir}`);
}

mkdirSync(resourceDestinationDir, { recursive: true });
cpSync(resourceSourceDir, resourceDestinationDir, { recursive: true });

if (!existsSync(generatedSourceDir)) {
  throw new Error(`Generated source directory not found: ${generatedSourceDir}`);
}

cpSync(generatedSourceDir, generatedDestinationDir, { recursive: true });

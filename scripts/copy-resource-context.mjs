import { cpSync, existsSync, mkdirSync } from "node:fs";
import { join } from "node:path";

const sourceDir = join(process.cwd(), "src", "resources", "context");
const destinationDir = join(process.cwd(), "dist", "resources", "context");

if (!existsSync(sourceDir)) {
  throw new Error(`Resource source directory not found: ${sourceDir}`);
}

mkdirSync(destinationDir, { recursive: true });
cpSync(sourceDir, destinationDir, { recursive: true });

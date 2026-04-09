import { readFileSync, writeFileSync } from "fs";
import {
  buildServerEnvironmentVariables,
  replaceReadmeEnvironmentTables,
  README_TABLE_MARKERS,
} from "./env-metadata.mjs";

const pkg = JSON.parse(readFileSync("package.json", "utf8"));
const server = JSON.parse(readFileSync("server.json", "utf8"));
const readme = readFileSync("README.md", "utf8");

server.version = pkg.version;
server.packages[0].version = pkg.version;
server.packages[0].environmentVariables = buildServerEnvironmentVariables();

for (const marker of Object.values(README_TABLE_MARKERS)) {
  if (!readme.includes(marker)) {
    throw new Error(`README.md is missing required marker: ${marker}`);
  }
}

const syncedReadme = replaceReadmeEnvironmentTables(readme);

writeFileSync("server.json", JSON.stringify(server, null, 2) + "\n");
writeFileSync("README.md", syncedReadme);

console.log(`server.json and README.md synced to v${pkg.version}`);

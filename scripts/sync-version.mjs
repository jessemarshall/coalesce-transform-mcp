import { readFileSync, writeFileSync } from "fs";

const pkg = JSON.parse(readFileSync("package.json", "utf8"));
const server = JSON.parse(readFileSync("server.json", "utf8"));

server.version = pkg.version;
server.packages[0].version = pkg.version;

writeFileSync("server.json", JSON.stringify(server, null, 2) + "\n");

console.log(`server.json synced to v${pkg.version}`);

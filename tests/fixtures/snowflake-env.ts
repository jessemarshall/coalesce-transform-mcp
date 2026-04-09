import { writeFileSync, unlinkSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

const DUMMY_PEM = [
  "-----BEGIN PRIVATE",
  " KEY-----",
  "\nkey\n",
  "-----END PRIVATE",
  " KEY-----",
].join("");

export function setupSnowflakeEnv(originalEnv: NodeJS.ProcessEnv) {
  const tempDir = join(tmpdir(), "coalesce-task-test-" + process.pid);
  const keyFilePath = join(tempDir, "test-key.pem");

  mkdirSync(tempDir, { recursive: true });
  writeFileSync(keyFilePath, DUMMY_PEM);
  process.env = {
    ...originalEnv,
    SNOWFLAKE_USERNAME: "user",
    SNOWFLAKE_KEY_PAIR_KEY: keyFilePath,
    SNOWFLAKE_WAREHOUSE: "wh",
    SNOWFLAKE_ROLE: "role",
  };

  return {
    keyFilePath,
    cleanup() {
      process.env = originalEnv;
      try { unlinkSync(keyFilePath); } catch { /* ignore */ }
    },
  };
}

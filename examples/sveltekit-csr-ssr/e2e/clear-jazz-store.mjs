import { rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

// The managed dev server persists every row to a SQLite store under
// node_modules/.cache, which is reused across runs. Remove it so each e2e run
// starts from an empty backend, independent of earlier runs or manual use.
const storeDir = join(
  dirname(fileURLToPath(import.meta.url)),
  "..",
  "node_modules",
  ".cache",
  "jazz-dev-server",
);

rmSync(storeDir, { recursive: true, force: true });

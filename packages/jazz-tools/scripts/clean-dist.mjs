import { rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const distDir = fileURLToPath(new URL("../dist", import.meta.url));

export async function cleanDist(path = distDir) {
  if (process.env.JAZZ_TEST_SEALED_TOOLS_DIST === "1") {
    throw new Error(
      "jazz-tools dist is sealed for concurrent tests; rebuild it before launching suites, not from a test child",
    );
  }
  await rm(path, { recursive: true, force: true });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) await cleanDist();

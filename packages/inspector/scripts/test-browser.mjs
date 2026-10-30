import { spawnSync } from "node:child_process";
import { statSync } from "node:fs";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));

function run(args) {
  const result = spawnSync("pnpm", args, { cwd: root, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}

try {
  if (process.env.JAZZ_TEST_SEALED_INSPECTOR_DIST === "1") {
    if (!statSync(new URL("../dist-embedded/embedded.html", import.meta.url)).isFile())
      throw new Error("prepared embedded inspector is missing");
  } else {
    run(["run", "build:embedded"]);
  }
  run(["exec", "playwright", "test", "--config", "playwright.config.ts"]);
} catch (error) {
  console.error(`inspector browser tests: ${error.message}`);
  process.exitCode = 1;
}

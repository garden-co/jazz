import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { runCorrectnessConsumer } from "./run-correctness-consumer.mjs";
const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const options = { cwd: root, rootDir: root };
console.log("Focused diagnostics only: this does not qualify full-suite.");
await runCorrectnessConsumer("pnpm", ["--filter", "jazz-rn", "build"], options);
await runCorrectnessConsumer(
  "pnpm",
  ["exec", "turbo", "run", "build", "--filter=jazz-tools", "--only"],
  options,
);
await runCorrectnessConsumer(
  "pnpm",
  [
    "--filter",
    "jazz-tools",
    "test:node",
    "src/e2ee/first-write-race.test.ts",
    "src/e2ee/first-write-initialisation.test.ts",
  ],
  options,
);

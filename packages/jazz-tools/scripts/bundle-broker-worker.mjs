// Bundle the browser broker SharedWorker into self-contained ESM. The
// SharedWorker constructor is intentionally indirect, so consumer bundlers do
// not reliably discover and bundle its imports themselves.
import { build } from "esbuild";
import { existsSync } from "node:fs";
import { rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const entry = fileURLToPath(new URL("../src/worker/jazz-broker-worker.ts", import.meta.url));
const outfile = fileURLToPath(new URL("../dist/worker/jazz-broker-worker.js", import.meta.url));

await build({
  entryPoints: [entry],
  outfile,
  bundle: true,
  format: "esm",
  platform: "browser",
  target: "es2022",
  legalComments: "none",
});

for (const stale of [`${outfile}.map`, outfile.replace(/\.js$/, ".ts")]) {
  if (existsSync(stale)) await rm(stale);
}

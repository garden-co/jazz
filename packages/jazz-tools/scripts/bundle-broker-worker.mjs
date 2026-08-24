// Bundle the browser broker SharedWorker into self-contained ESM. The
// SharedWorker constructor is intentionally indirect, so consumer bundlers do
// not reliably discover and bundle its imports themselves.
import { build } from "esbuild";
import { existsSync } from "node:fs";
import { copyFile, readFile, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { assertWasmGlueInstantiates } from "../../../dev/artifacts/wasm-glue-abi.mjs";

const entry = fileURLToPath(new URL("../src/worker/jazz-broker-worker.ts", import.meta.url));
const outfile = fileURLToPath(new URL("../dist/worker/jazz-broker-worker.js", import.meta.url));
const wasmSource = fileURLToPath(
  new URL("../../../crates/jazz-wasm/pkg/jazz_wasm_bg.wasm", import.meta.url),
);
const wasmOutfile = fileURLToPath(new URL("../dist/worker/jazz_wasm_bg.wasm", import.meta.url));

await build({
  entryPoints: [entry],
  outfile,
  bundle: true,
  format: "esm",
  platform: "browser",
  target: "es2022",
  legalComments: "none",
});

// `runtimeSources.wasmUrl` can deliberately point a worker at a package-level
// asset. Refuse to publish a worker whose embedded wasm-bindgen glue cannot
// instantiate the binary it is built alongside: the browser otherwise reports
// an opaque missing-import error only after the SharedWorker has started.
await assertWasmGlueInstantiates(await readFile(wasmSource), await readFile(outfile, "utf8"));

// The worker bundles wasm-bindgen's JS glue. Its default initializer resolves
// `jazz_wasm_bg.wasm` relative to that glue, which esbuild places in this
// worker bundle. Keep the binary beside the shipped worker so every consumer
// bundler sees one complete, package-owned worker artifact; applications do
// not need to copy Jazz assets into their own public directory.
await copyFile(wasmSource, wasmOutfile);

for (const stale of [`${outfile}.map`, outfile.replace(/\.js$/, ".ts")]) {
  if (existsSync(stale)) await rm(stale);
}

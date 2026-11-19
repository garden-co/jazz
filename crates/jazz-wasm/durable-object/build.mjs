// `build:durable-object`: the WASM a Cloudflare Durable Object hosts, built
// for size (see ../size-build.mjs). panic = abort there: the host
// reinstantiates after a trap (see reinstantiable.mjs).
// Measured on the 2026-11-18 core: 4.07 MB gzip as the browser build's
// settings make it, 2.46 MB this way.
import { execFileSync } from "node:child_process";
import { join } from "node:path";
import { root, sizeTunedBuild, wasmOptLevelFromArgs } from "../size-build.mjs";

const wasmOptLevel = wasmOptLevelFromArgs();
sizeTunedBuild({
  label: "build:durable-object",
  features: "durable-object",
  outDir: join(root, "durable-object/pkg"),
  outName: "jazz_wasm_do",
  wasmOptLevel,
  patchGlue: (glue) =>
    execFileSync(process.execPath, [join(root, "durable-object/reinstantiable.mjs"), glue], {
      stdio: "inherit",
    }),
});

// Bundle the browser broker SharedWorker into self-contained ESM, replacing the
// unbundled tsc output in dist/.
//
// The broker is loaded as a module SharedWorker whose URL flows through a
// variable and an aliased SharedWorker constructor (browser-broker-client.ts),
// so Turbopack, webpack and Vite never recognise it as a worker entry — they
// copy the file verbatim. The shipped tsc output keeps bare ../runtime/*.js
// imports, which 404 in the worker context and crash the page. Inlining them
// here fixes every consumer (Next, Vite/SvelteKit, plain bundlers, CDN) at once.
// The broker is pure, wasm-free coordination logic, so the bundle is fully
// self-contained.
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

// The bundle carries no source map and no longer needs the copied .ts source;
// drop the stale artifacts an earlier (unbundled) build may have left behind so
// they don't dangle in dist/.
for (const stale of [`${outfile}.map`, outfile.replace(/\.js$/, ".ts")]) {
  if (existsSync(stale)) {
    await rm(stale);
  }
}

// Bundle the browser broker SharedWorker into self-contained ESM, replacing the
// unbundled tsc output in dist/.
//
// The broker is loaded as a module SharedWorker whose URL flows through a
// variable and an aliased SharedWorker constructor (browser-broker-client.ts),
// so Turbopack, webpack and Vite never recognise it as a worker entry — they
// copy the file verbatim. The shipped tsc output keeps bare ../runtime/*.js
// imports, which 404 in the worker context and crash the page. Inlining them
// here fixes every consumer (Next, Vite/SvelteKit, plain bundlers, CDN) at once.
// The broker now uses a tiny wasm state machine, so this bundle embeds the
// wasm bytes as base64 and instantiates them from memory instead of fetching a
// sibling .wasm file that those bundlers would fail to copy.
import { build } from "esbuild";
import { existsSync } from "node:fs";
import { readFile, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const entry = fileURLToPath(new URL("../src/worker/jazz-broker-worker.ts", import.meta.url));
const outfile = fileURLToPath(new URL("../dist/worker/jazz-broker-worker.js", import.meta.url));
const wasmBytesModule = fileURLToPath(
  new URL("../src/worker/jazz-broker-wasm-bytes.ts", import.meta.url),
);
const brokerWasm = fileURLToPath(
  new URL("../../../crates/jazz-broker-wasm/pkg/jazz_broker_wasm_bg.wasm", import.meta.url),
);

await build({
  entryPoints: [entry],
  outfile,
  bundle: true,
  format: "esm",
  platform: "browser",
  target: "es2022",
  legalComments: "none",
  plugins: [
    {
      name: "embed-jazz-broker-wasm",
      setup(build) {
        build.onLoad({ filter: /jazz-broker-wasm-bytes\.ts$/ }, async (args) => {
          if (args.path !== wasmBytesModule) return undefined;
          const wasmBase64 = (await readFile(brokerWasm)).toString("base64");
          return {
            contents: `export const JAZZ_BROKER_WASM_BASE64 = ${JSON.stringify(wasmBase64)};`,
            loader: "ts",
          };
        });
      },
    },
  ],
});

// The bundle carries no source map and no longer needs the copied .ts source;
// drop the stale artifacts an earlier (unbundled) build may have left behind so
// they don't dangle in dist/.
for (const stale of [`${outfile}.map`, outfile.replace(/\.js$/, ".ts")]) {
  if (existsSync(stale)) {
    await rm(stale);
  }
}

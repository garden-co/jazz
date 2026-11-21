#!/usr/bin/env node
// PROTOTYPE (measurement only): "early local-first warm-up".
//
// Today the local-first path starts its two expensive, account-independent
// steps (page WASM compile; SharedWorker spawn + worker WASM compile) only
// after createAccountManagerWithMinter has resolved the account (~55-110 ms
// after main.tsx). This patch starts them at the top of openBrowserSessionCore:
//   1. load the local-first chunk and start the page's loadWasmModule();
//   2. spawn the SharedWorker the last lease used (name remembered in
//      localStorage after each successful lease) and send it a lease *probe*
//      with wasmPrefetch, so it compiles its WASM. A probe does no admission
//      and opens no store; the same-named lease request later attaches to the
//      same realm. The probe port is cancelled after 10 s.
// Gated on localFirst === "now" or this registry having upgraded before.
//
//   node proto-early.mjs <app dir> [--restore]     (apply after marks.mjs)
import { copyFileSync, existsSync, readFileSync, writeFileSync, realpathSync } from "node:fs";
import { join } from "node:path";

const app = process.argv[2];
const restore = process.argv.includes("--restore");
const dist = join(realpathSync(join(app, "node_modules/jazz-tools")), "dist");
const HINT = "jazz-lf-worker-hint-v0";

const edits = [
  ["session/create-jazz-session.js", [
    ["    refuseLocalFirstTiming(localFirst, loadLocalFirst);",
      `$&
    // PROTOTYPE early warm-up (lf-startup).
    if (loadLocalFirst && typeof window !== "undefined") {
        let remembered = false;
        try {
            const key = \`jazz-local-first-v1:\${encodeURIComponent(clientRegistryUrl({ ...config, serverUrl }))}\`;
            remembered = (globalThis.localStorage?.getItem(key) ?? "[]") !== "[]";
        }
        catch { }
        if (localFirst === "now" || remembered) {
            globalThis.performance?.mark?.("lf:warm:start");
            // Run the warm-up before the account manager's synchronous mint
            // (Ed25519 precompute) takes the main thread: wait for the chunk,
            // at most 60 ms, then spawn the worker and start the page compile.
            const warm = Promise.resolve().then(() => loadLocalFirst()).then((m) => m.warmUp?.()).catch(() => { });
            await Promise.race([warm, new Promise((resolve) => setTimeout(resolve, 60))]);
            globalThis.performance?.mark?.("lf:warm:handoff");
        }
    }`],
  ]],
  ["local-first/index.js", [
    ['export { createDb as createClient } from "../runtime/default-create-db.js";',
      `$&\nexport { warmUpLocalFirst as warmUp } from "../runtime/local-first-warm.js";`],
  ]],
  ["runtime/native-runtime/browser-shared-worker-connection.js", [
    ["                if (outcome.type === \"ready\")\n                    return outcome.lease;",
      `                if (outcome.type === "ready") {
                    try {
                        if (!runtimeSources?.wasmModule && !runtimeSources?.wasmSource)
                            localStorage.setItem(${JSON.stringify(HINT)}, JSON.stringify({ workerName, runtimeSources: runtimeSources ?? null }));
                    }
                    catch { }
                    return outcome.lease;
                }`],
    ["function workerWasmPrefetch(runtimeSources) {",
      `/** PROTOTYPE: spawn the remembered worker realm and ask it to prefetch its WASM. */
export function warmBrowserWorkerFromHint() {
    let hint;
    try {
        hint = JSON.parse(localStorage.getItem(${JSON.stringify(HINT)}) ?? "null");
    }
    catch { }
    if (!hint?.workerName)
        return Promise.resolve(false);
    const runtimeSources = hint.runtimeSources ?? undefined;
    const worker = createBrokerSharedWorker(runtimeSources, \`\${hint.workerName}:generation-\${readWorkerGeneration(hint.workerName)}\`);
    globalThis.performance?.mark?.("lf:warm:worker-spawned");
    const port = worker.port;
    port.start();
    port.postMessage({ type: "probe-foreground-node-lease-worker", attemptId: \`warm-\${crypto.randomUUID()}\`, wasmPrefetch: workerWasmPrefetch(runtimeSources) });
    globalThis.__jazzWarmWorker = worker;
    const alive = new Promise((resolve) => port.addEventListener("message", (e) => {
        if (e.data?.type === "foreground-node-lease-worker-alive") {
            globalThis.performance?.mark?.("lf:warm:worker-alive");
            resolve(true);
        }
    }));
    setTimeout(() => {
        try {
            port.postMessage({ type: "cancel-foreground-node-lease" });
            port.close();
        }
        catch { }
    }, 10_000);
    return alive;
}
$&`],
  ]],
];

const NEW_FILE = "runtime/local-first-warm.js";
const NEW_SRC = `// PROTOTYPE (lf-startup): start the account-independent local-first work early.
import { loadWasmModule } from "./wasm-loader.js";
import { warmBrowserWorkerFromHint } from "./native-runtime/browser-shared-worker-connection.js";
/**
 * Spawn the remembered worker and start the page's WASM fetch/compile.
 * Resolves once the worker answered its probe (its realm runs and prefetches
 * its WASM) and the page's WASM request is out; the compile itself continues
 * off the main thread.
 */
export async function warmUpLocalFirst() {
    globalThis.performance?.mark?.("lf:warm:chunk-loaded");
    let alive = Promise.resolve(false);
    try {
        alive = warmBrowserWorkerFromHint();
    }
    catch { }
    void loadWasmModule(undefined).catch(() => { });
    // Same module as loadWasmModule's own import(): its continuation (which
    // issues the WASM fetch) runs before this one.
    const glue = import("jazz-wasm").then(() => globalThis.performance?.mark?.("lf:warm:wasm-requested"));
    await Promise.all([alive, glue]);
}
`;

for (const [rel, list] of edits) {
  const file = join(dist, rel);
  const base = `${file}.pre-early`;
  if (restore) {
    if (existsSync(base)) copyFileSync(base, file);
    continue;
  }
  if (!existsSync(base)) copyFileSync(file, base);
  let src = readFileSync(base, "utf8");
  for (const [anchor, repl] of list) {
    const n = src.split(anchor).length - 1;
    if (n !== 1) throw new Error(`${rel}: anchor found ${n}x: ${anchor.slice(0, 80)}`);
    src = src.replace(anchor, () => repl.replace("$&", anchor));
  }
  writeFileSync(file, src);
  console.error(`patched ${rel} (${list.length})`);
}
if (!restore) writeFileSync(join(dist, NEW_FILE), NEW_SRC);

// Bundles a Worker module the way wrangler does (the `workerd` export
// condition, `.wasm` imports kept as CompiledWasm modules), for Miniflare.
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { build } from "esbuild";

export async function bundleWorker(entry: string): Promise<string> {
  const outdir = await mkdtemp(join(tmpdir(), "jazz-do-worker-"));
  await build({
    entryPoints: [entry],
    outdir,
    bundle: true,
    format: "esm",
    platform: "neutral",
    target: "es2022",
    conditions: ["workerd", "worker", "import"],
    mainFields: ["module", "main"],
    external: ["cloudflare:*", "node:*"],
    loader: { ".wasm": "copy" },
    alias: { "jazz-tools/durable-object": resolve(__dirname, "../../src/durable-object/index.ts") },
    logLevel: "warning",
  });
  return join(outdir, "real-core-worker.js");
}

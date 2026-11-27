// Bundles a Worker module the way wrangler does (the `workerd` export
// condition, `.wasm` imports kept as CompiledWasm modules), for Miniflare.
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { builtinModules } from "node:module";
import { basename, join, resolve } from "node:path";
import { build } from "esbuild";

export async function bundleWorker(entry: string): Promise<string> {
  const src = resolve(__dirname, "../../src");
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
    // workerd provides Node's built-ins under nodejs_compat.
    external: ["cloudflare:*", "node:*", ...builtinModules],
    loader: { ".wasm": "copy" },
    alias: {
      "jazz-tools": join(src, "index.ts"),
      "jazz-tools/package.json": join(src, "../package.json"),
      "jazz-tools/durable-object": join(src, "durable-object/index.ts"),
      // The workerd entry, as the package's `workerd` condition picks it.
      "jazz-tools/backend": join(src, "backend/workerd.ts"),
      "jazz-tools/cloudflare": join(src, "cloudflare/index.ts"),
    },
    logLevel: "warning",
  });
  return join(outdir, basename(entry).replace(/\.(?:ts|mjs)$/, ".js"));
}

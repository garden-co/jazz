import { execFile } from "node:child_process";
import { createReadStream } from "node:fs";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { basename, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import type { Plugin } from "vite";

const runNode = promisify(execFile);
const worktreeRoot = fileURLToPath(new URL("../../../..", import.meta.url));
const bundlerUrl = new URL("../../scripts/bundle-broker-worker.mjs", import.meta.url).href;
const workerEntry = fileURLToPath(new URL("./jazz-broker-worker-test.ts", import.meta.url));

type FixtureAsset = { path: string; contentType: string };
const assetRegistryKey = Symbol.for("jazz-tools.browser-worker-fault-assets.v1");
// Vitest reloads this config for the browser server but runs commands from the
// original config. A process-owned registry bridges those module instances;
// neither a module-local singleton nor a shared build promise has that lifetime.
const registryHost = process as typeof process & {
  [assetRegistryKey]?: Map<string, Map<string, FixtureAsset>>;
};
const assetRegistry = (registryHost[assetRegistryKey] ??= new Map());

/** Keep test-only worker capabilities out of both sealed dist and Vite's module transforms. */
export function createWorkerFaultBundleFixture(wasmPackage: string | undefined): {
  plugin: Plugin;
  url(): Promise<string>;
} {
  // The admitted package path identifies an immutable sealed WASM/glue pair.
  const identity = JSON.stringify([worktreeRoot, wasmPackage && resolve(wasmPackage)]);
  const assets = assetRegistry.get(identity) ?? new Map<string, FixtureAsset>();
  assetRegistry.set(identity, assets);
  // Re-evaluating the config must build fresh source, not reuse an older bundle.
  let bundle: Promise<string> | undefined;

  async function buildBundle(): Promise<string> {
    if (!wasmPackage) {
      throw new Error("Worker fault fixture requires an admitted correctness WASM package");
    }
    const target = resolve(worktreeRoot, "target");
    await mkdir(target, { recursive: true });
    const outputDir = await mkdtemp(resolve(target, "browser-worker-fault-"));
    try {
      // Use a child so the bundler pins the admitted glue/binary pair before its
      // module loads, without changing the concurrent browser runner's environment.
      await runNode(
        process.execPath,
        [
          "--input-type=module",
          "--eval",
          "const { bundleBrokerWorker } = await import(process.argv[1]); " +
            "await bundleBrokerWorker(process.argv[2], process.argv[3]);",
          bundlerUrl,
          outputDir,
          workerEntry,
        ],
        {
          cwd: worktreeRoot,
          env: {
            ...process.env,
            JAZZ_CORRECTNESS_ARTIFACT_RUN: "1",
            JAZZ_CORRECTNESS_WASM_PACKAGE: wasmPackage,
          },
        },
      );
      // Publish routes only after the existing bundler has checked the ABI and
      // published both complete files. Query parameters remain ordinary SDK assets.
      const prefix = `/@jazz-worker-fault/${basename(outputDir)}`;
      assets.set(`${prefix}/jazz-broker-worker.js`, {
        path: resolve(outputDir, "jazz-broker-worker.js"),
        contentType: "text/javascript",
      });
      assets.set(`${prefix}/jazz_wasm_bg.wasm`, {
        path: resolve(outputDir, "jazz_wasm_bg.wasm"),
        contentType: "application/wasm",
      });
      return `${prefix}/jazz-broker-worker.js`;
    } catch (error) {
      await rm(outputDir, { recursive: true, force: true });
      throw error;
    }
  }

  return {
    url: () => (bundle ??= buildBundle()),
    plugin: {
      name: "jazz-worker-fault-bundle",
      configureServer(server) {
        server.middlewares.use((request, response, next) => {
          const asset = assets.get((request.url ?? "").split("?", 1)[0]!);
          if (!asset || (request.method !== "GET" && request.method !== "HEAD")) {
            next();
            return;
          }
          // Serve only these owned files, unchanged: Vitest page instrumentation
          // is not a SharedWorker module loader, even for a prebundled entry.
          response.setHeader("Content-Type", asset.contentType);
          response.setHeader("Cache-Control", "no-store");
          response.setHeader("Cross-Origin-Opener-Policy", "same-origin");
          response.setHeader("Cross-Origin-Embedder-Policy", "require-corp");
          response.setHeader("Cross-Origin-Resource-Policy", "same-origin");
          if (request.method === "HEAD") {
            response.end();
            return;
          }
          createReadStream(asset.path).on("error", next).pipe(response);
        });
      },
    },
  };
}

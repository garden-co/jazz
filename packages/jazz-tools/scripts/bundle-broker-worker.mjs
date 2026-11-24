// Bundle the browser broker SharedWorker into self-contained ESM. The
// SharedWorker constructor is intentionally indirect, so consumer bundlers do
// not reliably discover and bundle its imports themselves.
import { build, transform } from "esbuild";
import { existsSync } from "node:fs";
import { copyFile, mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  assertWasmGlueAbi,
  assertWasmGlueInstantiates,
} from "../../../dev/artifacts/wasm-glue-abi.mjs";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const entry = fileURLToPath(new URL("../src/worker/jazz-broker-worker.ts", import.meta.url));
const canonicalOutputDir = fileURLToPath(new URL("../dist/worker", import.meta.url));
const correctnessArtifactRun = process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1";
const sealedWasmPackage = process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;
if (correctnessArtifactRun && !sealedWasmPackage)
  throw new Error("sealed correctness consumer is missing its admitted WASM package");
const correctnessWasmPackage = correctnessArtifactRun ? sealedWasmPackage : undefined;
const wasmSource = correctnessWasmPackage
  ? resolve(correctnessWasmPackage, "jazz_wasm_bg.wasm")
  : fileURLToPath(new URL("../../../crates/jazz-wasm/pkg/jazz_wasm_bg.wasm", import.meta.url));

export function brokerWorkerOutputDir(args = process.argv.slice(2)) {
  if (args.length === 0) return canonicalOutputDir;
  if (args.length === 2 && args[0] === "--out-dir" && args[1]) return resolve(packageRoot, args[1]);
  throw new Error("Usage: node scripts/bundle-broker-worker.mjs [--out-dir directory]");
}

function assertOutputMayBePublished(outputDir) {
  // The CI parent publishes this directory before it starts concurrent suites.
  // A test that needs to exercise bundling must use its own output directory:
  // overwriting the public one makes a browser fetch observe a missing or
  // partially copied .wasm file even though the preflight was initially valid.
  if (
    process.env.JAZZ_TEST_SEALED_TOOLS_DIST === "1" &&
    resolve(outputDir) === resolve(canonicalOutputDir)
  )
    throw new Error(
      "jazz-tools worker output is sealed for concurrent tests; bundle into a private --out-dir",
    );
}

export async function bundleBrokerWorker(outputDir = canonicalOutputDir, entryPoint = entry) {
  assertOutputMayBePublished(outputDir);
  await mkdir(outputDir, { recursive: true });
  // Prepare the complete pair privately. Publication below only renames fully
  // written files, so even an ordinary (unsealed) rebuild cannot expose a
  // truncated WebAssembly response to a server that opens the final filename.
  const staging = await mkdtemp(resolve(outputDir, ".broker-worker-stage-"));
  const stagedWorker = resolve(staging, "jazz-broker-worker.js");
  const stagedWasm = resolve(staging, "jazz_wasm_bg.wasm");
  const outfile = resolve(outputDir, "jazz-broker-worker.js");
  const wasmOutfile = resolve(outputDir, "jazz_wasm_bg.wasm");
  try {
    const bundle = await build({
      write: false,
      entryPoints: [entryPoint],
      outfile: stagedWorker,
      // Correctness consumers pin both wasm-bindgen glue and the binary.  A
      // binary-only override would still let esbuild follow a mutable package
      // pointer for the JS half of the ABI pair.
      alias: correctnessWasmPackage
        ? { "jazz-wasm": resolve(correctnessWasmPackage, "jazz_wasm.js") }
        : {},
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
    await copyFile(wasmSource, stagedWasm);
    const wasmModule = await WebAssembly.compile(await readFile(stagedWasm));
    const workerSource = bundle.outputFiles[0].text;
    await assertWasmGlueInstantiates(wasmModule, workerSource);

    // Bundlers that start the worker from its URL (Next.js/Turbopack copies
    // it into `media/` byte for byte) ship exactly this file, so it has to be
    // production-ready on its own; Vite and webpack minify it again anyway.
    // Minified after the check above, which evaluates wasm-bindgen's glue by
    // its generated names. No code splitting: the bundlers that copy the file
    // would not emit its chunks.
    const minified = await transform(workerSource, {
      format: "esm",
      target: "es2022",
      legalComments: "none",
      minify: true,
    });
    assertWasmGlueAbi(wasmModule, minified.code);
    await writeFile(stagedWorker, minified.code);

    // The worker bundles wasm-bindgen's JS glue. Its default initializer resolves
    // `jazz_wasm_bg.wasm` relative to that glue, which esbuild places in this
    // worker bundle. Keep the binary beside the shipped worker so every consumer
    // bundler sees one complete, package-owned worker artifact; applications do
    // not need to copy Jazz assets into their own public directory.
    await rename(stagedWorker, outfile);
    await rename(stagedWasm, wasmOutfile);

    for (const stale of [`${outfile}.map`, outfile.replace(/\.js$/, ".ts")]) {
      if (existsSync(stale)) await rm(stale);
    }
  } finally {
    await rm(staging, { recursive: true, force: true });
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    await bundleBrokerWorker(brokerWorkerOutputDir());
  } catch (error) {
    console.error(`Broker worker bundle: ${error.message}`);
    process.exitCode = 1;
  }
}

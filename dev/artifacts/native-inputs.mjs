import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { stageNativeFingerprints } from "./stage-native-fingerprints.mjs";

// The native inputs every jazz-tools runtime build bundles. A fresh checkout
// has none of them; `pnpm --filter jazz-tools build:runtime` produces whatever
// is missing for the host it runs on.
export const repositoryRoot = resolve(fileURLToPath(new URL("../..", import.meta.url)));
export const wasmManifestPath = "crates/jazz-wasm/pkg/.jazz-artifact-manifest.json";
export const wasmEntryPath = "crates/jazz-wasm/pkg/jazz_wasm.js";
export const napiPointerPath = "crates/jazz-napi/native-binding.pointer.cjs";
// CI release assembly ("Assemble npm packages") has no local build pointer: it
// downloads every platform's binding plus their sealed manifests, and
// stage-native-fingerprints.mjs (no flag) stages from this one.
export const napiReleaseManifestPath =
  "crates/jazz-napi/provenance/jazz-napi.linux-x64-gnu.manifest.json";
export const fingerprintPaths = [
  "packages/jazz-tools/src/runtime/native-artifact-fingerprint-napi.ts",
  "packages/jazz-tools/src/runtime/native-artifact-fingerprint-wasm.ts",
];

// Most CI providers set CI=true; treat explicit "false"/"0" as a local run.
export const isCI = (env) => Boolean(env.CI) && !/^(false|0)$/i.test(env.CI);

export function missingNativeInputs(root = repositoryRoot) {
  const has = (path) => existsSync(join(root, path));
  return {
    wasm: !has(wasmManifestPath) || !has(wasmEntryPath),
    napi: !has(napiPointerPath) && !has(napiReleaseManifestPath),
    fingerprints: fingerprintPaths.some((path) => !has(path)),
  };
}

// The staging mode follows the WASM profile that is present, so a
// `jazz-wasm build:fast` package is staged with --local, not rejected.
const stageModeForProfile = { release: "workspace", fast: "local", profiling: "profiling" };

/**
 * Build the host's missing native inputs and stage their fingerprints. Inputs
 * already present are reused as they are. CI never builds them implicitly:
 * its workflows produce or download each artifact explicitly, so a missing one
 * there is a pipeline bug that must stay visible.
 */
export function ensureNativeInputs(
  root = repositoryRoot,
  { env = process.env, run = spawnSync, log = console.error } = {},
) {
  const missing = missingNativeInputs(root);
  if (!missing.wasm && !missing.napi && !missing.fingerprints) return [];
  const names = [
    missing.wasm && "jazz-wasm package",
    missing.napi && "jazz-napi binding",
    missing.fingerprints && "native fingerprints",
  ].filter(Boolean);
  if (isCI(env))
    throw new Error(
      `missing native inputs in CI (${names.join(", ")}); CI must build or download them ` +
        "before building jazz-tools",
    );
  const steps = [];
  const build = (kind) => {
    log(
      `jazz-tools: building missing ${kind === "wasm" ? "jazz-wasm package" : "jazz-napi binding"} for this host`,
    );
    const result = run(process.execPath, [join(root, "dev/artifacts/build.mjs"), kind, "release"], {
      cwd: root,
      env,
      stdio: "inherit",
    });
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`the ${kind} build failed (exit ${result.status})`);
    steps.push(kind);
  };
  if (missing.wasm) build("wasm");
  if (missing.napi) build("napi");
  const profile = JSON.parse(readFileSync(join(root, wasmManifestPath), "utf8")).profile;
  const mode = stageModeForProfile[profile];
  if (!mode) throw new Error(`jazz-wasm package has an unknown profile ${JSON.stringify(profile)}`);
  log(`jazz-tools: staging native fingerprints (--${mode})`);
  stageNativeFingerprints(root, { [mode]: true });
  steps.push(`stage:${mode}`);
  return steps;
}

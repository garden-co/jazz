// build:runtime bundles the jazz-wasm package and the generated native
// fingerprint modules. Neither exists in a fresh checkout, and without this
// check the first sign of that is a wall of esbuild resolve errors.
import { existsSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const repositoryRoot = join(packageRoot, "../..");

export const requiredNativeInputs = [
  "crates/jazz-wasm/pkg/jazz_wasm.js",
  "packages/jazz-tools/src/runtime/native-artifact-fingerprint-napi.ts",
  "packages/jazz-tools/src/runtime/native-artifact-fingerprint-wasm.ts",
];

export function missingNativeInputs(root = repositoryRoot) {
  return requiredNativeInputs.filter((path) => !existsSync(join(root, path)));
}

export function nativeInputsMessage(missing) {
  return [
    "jazz-tools build:runtime needs the native bindings and their generated fingerprints, which are missing:",
    ...missing.map((path) => `  - ${path}`),
    "",
    "Build them first from the repository root. This works on any host (Linux x64/arm64, macOS):",
    "  pnpm build:ci",
    "or step by step:",
    "  pnpm --filter jazz-wasm build",
    "  pnpm --filter jazz-napi build",
    "  node dev/artifacts/stage-native-fingerprints.mjs --workspace",
    "",
    "The WASM build needs wasm-pack; `pnpm run ensure:rust-toolchain` installs it.",
  ].join("\n");
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const missing = missingNativeInputs();
  if (missing.length) {
    console.error(nativeInputsMessage(missing));
    process.exitCode = 1;
  }
}

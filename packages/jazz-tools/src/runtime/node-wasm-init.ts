import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";

export function tryReadPackagedWasmBinary(moduleUrl: string): Uint8Array | null {
  const require = createRequire(moduleUrl);
  const packageJsonPath = require.resolve("jazz-wasm/package.json");
  const packageDir = dirname(packageJsonPath);
  const wasmPath = resolve(packageDir, "pkg/jazz_wasm_bg.wasm");

  if (!existsSync(wasmPath)) {
    return null;
  }

  return readFileSync(wasmPath);
}

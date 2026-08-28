import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath, pathToFileURL } from "node:url";
import type { Runtime } from "../client.js";
import type { WasmSchema } from "../../drivers/types.js";
import { onTestFinished } from "vitest";
import { NativeRuntimeAdapter } from "../native-runtime/native-runtime-adapter.js";
import { assertNativeArtifactCompatibility } from "../native-artifact-compatibility.js";
import { readCorrectnessArtifactSnapshot } from "../../../../../dev/artifacts/test-artifact-store.mjs";

export type TestRuntime = Runtime & {
  free?(): void;
  setLargeValueStagingPolicy?(
    incomingBytesPerWindow: number,
    windowMs: number,
    maxAgeMs?: number | null,
  ): void;
  evictExpiredStagedLargeValues?(): Promise<number>;
};

let wasmModulePromise: Promise<any> | null = null;

async function freeRuntimeSafely(runtime: TestRuntime): Promise<void> {
  if (!runtime.free) return;

  // Allow pending microtasks (scheduled ticks / callbacks) to release borrows
  // before freeing the WASM runtime.
  const maxAttempts = 5;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      runtime.free();
      return;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const borrowConflict =
        message.includes("while it was borrowed") ||
        message.includes("already borrowed") ||
        message.includes("unreachable");
      if (!borrowConflict) {
        throw error;
      }
      if (attempt === maxAttempts - 1) {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
  }
}

type JazzWasmPaths = {
  modulePath: string;
  wasmPath: string;
};

function resolveJazzWasmPaths(): JazzWasmPaths | null {
  const snapshot = readCorrectnessArtifactSnapshot(
    fileURLToPath(new URL("../../../../..", import.meta.url)),
  );
  if (snapshot) {
    const modulePath = resolve(snapshot.wasmPackage, "jazz_wasm.js");
    const wasmPath = resolve(snapshot.wasmPackage, "jazz_wasm_bg.wasm");
    if (existsSync(modulePath) && existsSync(wasmPath)) return { modulePath, wasmPath };
    return null;
  }
  const require = createRequire(import.meta.url);
  let packageJsonPath: string;
  try {
    packageJsonPath = require.resolve("jazz-wasm/package.json");
  } catch {
    return null;
  }

  const packageDir = dirname(packageJsonPath);
  const modulePath = resolve(packageDir, "pkg/jazz_wasm.js");
  const wasmPath = resolve(packageDir, "pkg/jazz_wasm_bg.wasm");

  if (!existsSync(modulePath) || !existsSync(wasmPath)) {
    return null;
  }

  return { modulePath, wasmPath };
}

export function hasJazzWasmBuild(): boolean {
  return resolveJazzWasmPaths() !== null;
}

export function loadWasmModuleForTest(): Promise<any> {
  if (!wasmModulePromise) {
    wasmModulePromise = (async () => {
      const paths = resolveJazzWasmPaths();
      if (!paths) {
        throw new Error(
          "jazz-wasm build artifacts not found. Run `pnpm --filter @jazz/rust build:crates` first.",
        );
      }

      const wasmModule: any = await import(pathToFileURL(paths.modulePath).href);
      wasmModule.initSync({ module: readFileSync(paths.wasmPath) });
      assertNativeArtifactCompatibility(wasmModule, "WASM", ["initSync", "WasmDb"]);
      return wasmModule;
    })();
  }
  return wasmModulePromise;
}

export async function createWasmRuntime(
  schema: WasmSchema,
  opts?: {
    appId?: string;
    env?: string;
    peerId?: string;
  },
): Promise<TestRuntime> {
  const wasmModule = await loadWasmModuleForTest();
  const appId = opts?.appId ?? "test-app";
  const env = opts?.env ?? "test";
  const peerId = opts?.peerId ?? "default";
  const runtime = new NativeRuntimeAdapter(
    wasmModule.WasmDb,
    schema,
    deterministicBytes(`${appId}:${env}:${peerId}:node`),
    testAuthorBytes(`${appId}:${env}:${peerId}:author`),
    1,
    true,
  );
  onTestFinished(async () => {
    await freeRuntimeSafely(runtime);
  });

  return runtime;
}

function testAuthorBytes(seed: string): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(["urn:jazz:test", seed]));
}

function deterministicBytes(seed: string): Uint8Array {
  let hash = 0x811c9dc5;
  const bytes = new Uint8Array(16);
  const view = new DataView(bytes.buffer);
  for (let round = 0; round < 4; round += 1) {
    for (let i = 0; i < seed.length; i += 1) {
      hash ^= seed.charCodeAt(i) + round;
      hash = Math.imul(hash, 0x01000193);
    }
    view.setUint32(round * 4, hash >>> 0, true);
  }
  return bytes;
}

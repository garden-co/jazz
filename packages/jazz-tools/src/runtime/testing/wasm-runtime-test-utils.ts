import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import type { Runtime } from "../client.js";
import type { WasmSchema } from "../../drivers/types.js";
import { serializeRuntimeSchema } from "../../drivers/schema-wire.js";
import { onTestFinished } from "vitest";

export type TestRuntime = Runtime & { free?(): void };

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

function loadWasmModule(): Promise<any> {
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
      return wasmModule;
    })();
  }
  return wasmModulePromise;
}

export async function createWasmRuntime(
  schema: WasmSchema,
  opts?: { appId?: string; env?: string; userBranch?: string; tier?: string },
): Promise<TestRuntime> {
  const wasmModule = await loadWasmModule();
  const runtime = new wasmModule.WasmRuntime(
    serializeRuntimeSchema(schema),
    opts?.appId ?? "test-app",
    opts?.env ?? "test",
    opts?.userBranch ?? "main",
    opts?.tier,
  );

  onTestFinished(async () => {
    await freeRuntimeSafely(runtime);
  });

  return runtime;
}

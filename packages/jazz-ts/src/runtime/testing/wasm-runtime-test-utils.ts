import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import type { Runtime } from "../client.js";
import type { WasmSchema } from "../../drivers/types.js";
import { onTestFinished } from "vitest";

export type TestRuntime = Runtime & { free?(): void };

let wasmModulePromise: Promise<any> | null = null;

type GrooveWasmPaths = {
  modulePath: string;
  wasmPath: string;
};

function resolveGrooveWasmPaths(): GrooveWasmPaths | null {
  const require = createRequire(import.meta.url);
  let packageJsonPath: string;
  try {
    packageJsonPath = require.resolve("groove-wasm/package.json");
  } catch {
    return null;
  }

  const packageDir = dirname(packageJsonPath);
  const modulePath = resolve(packageDir, "pkg/groove_wasm.js");
  const wasmPath = resolve(packageDir, "pkg/groove_wasm_bg.wasm");

  if (!existsSync(modulePath) || !existsSync(wasmPath)) {
    return null;
  }

  return { modulePath, wasmPath };
}

export function hasGrooveWasmBuild(): boolean {
  return resolveGrooveWasmPaths() !== null;
}

function loadWasmModule(): Promise<any> {
  if (!wasmModulePromise) {
    wasmModulePromise = (async () => {
      const paths = resolveGrooveWasmPaths();
      if (!paths) {
        throw new Error(
          "groove-wasm build artifacts not found. Run `pnpm --filter @jazz/rust build:crates` first.",
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
    JSON.stringify(schema),
    opts?.appId ?? "test-app",
    opts?.env ?? "test",
    opts?.userBranch ?? "main",
    opts?.tier,
  );

  onTestFinished(async () => {
    await runtime.free();
  });

  return runtime;
}

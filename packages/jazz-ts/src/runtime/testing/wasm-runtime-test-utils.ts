import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { createRequire } from "node:module";
import type { Runtime } from "../client.js";
import type { WasmSchema } from "../../drivers/types.js";
import { onTestFinished } from "vitest";

export type TestRuntime = Runtime & { free?(): void };

let wasmModulePromise: Promise<any> | null = null;

function loadWasmModule(): Promise<any> {
  if (!wasmModulePromise) {
    wasmModulePromise = (async () => {
      const require = createRequire(import.meta.url);
      const wasmModule: any = await import("groove-wasm");
      const wasmPath = resolve(dirname(require.resolve("groove-wasm")), "groove_wasm_bg.wasm");
      wasmModule.initSync({ module: readFileSync(wasmPath) });
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

import type { WasmSchema } from "./drivers/types.js";

let wasmModulePromise: Promise<any> | null = null;

async function loadSchemaHashWasmModule(): Promise<any> {
  if (!wasmModulePromise) {
    const { loadWasmModule } = await import("./runtime/client.js");
    wasmModulePromise = loadWasmModule();
  }

  return wasmModulePromise;
}

export async function computeSchemaHash(schema: WasmSchema): Promise<string> {
  const wasmModule = await loadSchemaHashWasmModule();
  return wasmModule.WasmRuntime.computeSchemaHash(JSON.stringify(schema));
}

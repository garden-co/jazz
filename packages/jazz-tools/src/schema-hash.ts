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
  const runtime = new wasmModule.WasmRuntime(
    JSON.stringify(schema),
    "jazz-tools-cli",
    "dev",
    "main",
    null,
    null,
  );

  try {
    return runtime.getSchemaHash();
  } finally {
    if (typeof runtime.free === "function") {
      runtime.free();
    }
  }
}

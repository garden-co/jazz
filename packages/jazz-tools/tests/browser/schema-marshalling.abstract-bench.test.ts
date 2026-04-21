import { describe, expect, it } from "vitest";
import { serializeRuntimeSchema } from "../../src/drivers/schema-wire.js";
import { loadWasmModule } from "../../src/runtime/client.js";
import {
  createSyntheticRuntimeSchema,
  runSchemaMarshallingBench,
} from "../../src/runtime/testing/schema-marshalling-bench.js";

declare const __JAZZ_ABSTRACT_BENCH__: string;

describe.skipIf(__JAZZ_ABSTRACT_BENCH__ !== "1")(
  "schema marshalling abstract bench (browser)",
  () => {
    it("measures repeated getSchema overhead for the browser WASM path", async () => {
      const schema = createSyntheticRuntimeSchema();
      const wasmModule = await loadWasmModule();
      const runtime = new wasmModule.WasmRuntime(
        serializeRuntimeSchema(schema),
        "schema-bench-browser-wasm",
        "test",
        "main",
        undefined,
        false,
      );

      try {
        const result = await runSchemaMarshallingBench({
          label: "browser-wasm",
          runtime,
          schema,
        });

        expect(result.dbAll.getSchemaCalls).toBe(0);

        console.info(JSON.stringify(result, null, 2));
      } finally {
        try {
          runtime.free?.();
        } catch {
          // Best effort cleanup for browser bench runtimes.
        }
      }
    }, 120_000);
  },
);

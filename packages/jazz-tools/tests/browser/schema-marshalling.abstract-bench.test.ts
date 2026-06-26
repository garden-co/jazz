import { describe, expect, it } from "vitest";
import { loadWasmModule } from "../../src/runtime/client.js";
import { DirectCoreRuntime } from "../../src/runtime/direct-core/runtime.js";
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
      const runtime = new DirectCoreRuntime(
        wasmModule.WasmDb,
        schema,
        deterministicBytes("schema-bench-browser-wasm:test:main:node"),
        deterministicBytes("schema-bench-browser-wasm:test:main:author"),
        1,
        true,
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

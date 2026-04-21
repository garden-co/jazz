import { describe, expect, it } from "vitest";
import {
  createSyntheticRuntimeSchema,
  runSchemaMarshallingBench,
} from "./testing/schema-marshalling-bench.js";
import { createNapiRuntime, hasJazzNapiBuild } from "./testing/napi-runtime-test-utils.js";
import { createWasmRuntime, hasJazzWasmBuild } from "./testing/wasm-runtime-test-utils.js";

const RUN_ABSTRACT_BENCH = process.env.JAZZ_ABSTRACT_BENCH === "1";

describe.skipIf(!RUN_ABSTRACT_BENCH)("schema marshalling abstract bench (node)", () => {
  it("measures repeated getSchema overhead for NAPI and node WASM", async () => {
    if (!hasJazzNapiBuild()) {
      throw new Error(
        "Node abstract bench requires jazz-napi build artifacts. Run `pnpm --filter jazz-napi build:debug` first.",
      );
    }

    if (!hasJazzWasmBuild()) {
      throw new Error(
        "Node abstract bench requires jazz-wasm build artifacts. Run `pnpm --filter @jazz/rust build:crates` first.",
      );
    }

    const schema = createSyntheticRuntimeSchema();
    const [napiRuntime, wasmRuntime] = await Promise.all([
      createNapiRuntime(schema, { appId: "schema-bench-node-napi" }),
      createWasmRuntime(schema, { appId: "schema-bench-node-wasm" }),
    ]);

    const [napiResult, wasmResult] = await Promise.all([
      runSchemaMarshallingBench({
        label: "node-napi",
        runtime: napiRuntime,
        schema,
      }),
      runSchemaMarshallingBench({
        label: "node-wasm",
        runtime: wasmRuntime,
        schema,
      }),
    ]);

    expect(napiResult.dbAll.getSchemaCalls).toBe(0);
    expect(wasmResult.dbAll.getSchemaCalls).toBe(0);

    console.info(
      JSON.stringify(
        {
          schema: napiResult.schema,
          results: [napiResult, wasmResult],
          ratios: {
            directGetSchemaAvgMs:
              wasmResult.directGetSchema.avgMs > 0
                ? napiResult.directGetSchema.avgMs / wasmResult.directGetSchema.avgMs
                : null,
            dbAllAvgMs:
              wasmResult.dbAll.avgMs > 0 ? napiResult.dbAll.avgMs / wasmResult.dbAll.avgMs : null,
            getSchemaAvgMsPerIteration:
              wasmResult.dbAll.getSchemaAvgMsPerIteration > 0
                ? napiResult.dbAll.getSchemaAvgMsPerIteration /
                  wasmResult.dbAll.getSchemaAvgMsPerIteration
                : null,
          },
        },
        null,
        2,
      ),
    );
  }, 120_000);
});

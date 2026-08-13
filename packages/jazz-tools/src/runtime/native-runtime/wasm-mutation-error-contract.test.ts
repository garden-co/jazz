import { describe, expect, it, vi } from "vitest";
import { createWasmRuntime, hasJazzWasmBuild } from "../testing/wasm-runtime-test-utils.js";

describe("WasmDb mutation error callback contract", () => {
  it.skipIf(!hasJazzWasmBuild())(
    "registers a mutation error callback through the native adapter",
    async () => {
      const runtime = await createWasmRuntime({});

      expect(() => runtime.onMutationError(vi.fn())).not.toThrow();
    },
  );
});

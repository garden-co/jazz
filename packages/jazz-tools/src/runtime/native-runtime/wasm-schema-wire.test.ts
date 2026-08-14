import { describe, expect, it } from "vitest";
import type { WasmSchema } from "../../drivers/types.js";
import { createWasmRuntime, hasJazzWasmBuild } from "../testing/wasm-runtime-test-utils.js";

describe.skipIf(!hasJazzWasmBuild())("WasmDb schema wire format", () => {
  it("opens a versioned content-manifest schema", async () => {
    const schema: WasmSchema = {
      documents: {
        columns: [
          {
            name: "body",
            column_type: { type: "Bytea" },
            nullable: false,
            content_manifest: {
              adapter_kind: "fixture-text-v1",
              max_tail_entries: 8,
              max_tail_bytes: 1024,
            },
          },
        ],
      },
    };

    await expect(createWasmRuntime(schema)).resolves.toBeDefined();
  });

  it("continues to open a legacy schema without manifests", async () => {
    const schema: WasmSchema = {
      documents: {
        columns: [
          {
            name: "title",
            column_type: { type: "Text" },
            nullable: false,
          },
        ],
      },
    };

    await expect(createWasmRuntime(schema)).resolves.toBeDefined();
  });
});

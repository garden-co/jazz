import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { encodeSchema } from "./native-runtime/native-runtime-adapter.js";
import { openConfig } from "./native-runtime/native-codec.js";
import { FEATURE_AUXILIARY_CHUNKS, WIRE_PROTOCOL_VERSION } from "./native-runtime/websocket.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "./testing/wasm-runtime-test-utils.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });

async function fixture(name: string) {
  const { WasmDb } = await loadWasmModuleForTest();
  const db = WasmDb.openMemory(
    encodeSchema(app.wasmSchema),
    openConfig(new Uint8Array(16).fill(57), testAuthorBytes(`wasm-session-${name}`)),
  );
  const opened: Array<{ close(): boolean }> = [];
  const connect = async (version: number, features: number) => {
    const link = await db.connectUpstreamWithSession(
      version,
      features,
      new Uint8Array(16).fill(58),
      1n,
      new Uint8Array(16).fill(57),
      2n,
    );
    opened.push(link);
    return link;
  };
  return {
    features: db.wireFeatures() as number,
    connect,
    async close() {
      for (const link of opened) link.close();
      await db.close();
    },
  };
}

describe.skipIf(!hasJazzWasmBuild())("WASM negotiated session admission", () => {
  it.each([0, WIRE_PROTOCOL_VERSION - 1, WIRE_PROTOCOL_VERSION + 1])(
    "rejects protocol %s before admitting a link and still permits a valid retry",
    async (version) => {
      const h = await fixture(`version-${version}`);
      try {
        await expect(h.connect(version, h.features)).rejects.toMatchObject({
          code: "protocol",
          message: expect.stringContaining(`server negotiated wire protocol ${version}`),
        });
        const link = await h.connect(WIRE_PROTOCOL_VERSION, h.features);
        expect(link.close()).toBe(true);
      } finally {
        await h.close();
      }
    },
  );

  it.each([FEATURE_AUXILIARY_CHUNKS, 1 << 30])(
    "rejects uncompiled feature %s and still permits a valid retry",
    async (unsupported) => {
      const h = await fixture(`feature-${unsupported}`);
      try {
        expect(h.features & unsupported).toBe(0);
        await expect(
          h.connect(WIRE_PROTOCOL_VERSION, h.features | unsupported),
        ).rejects.toMatchObject({
          code: "protocol",
          message: expect.stringContaining(`not compiled with 0x${unsupported.toString(16)}`),
        });
        const link = await h.connect(WIRE_PROTOCOL_VERSION, h.features);
        expect(link.close()).toBe(true);
      } finally {
        await h.close();
      }
    },
  );

  it("accepts both zero optional features and its advertised feature set", async () => {
    const h = await fixture("supported");
    try {
      const none = await h.connect(WIRE_PROTOCOL_VERSION, 0);
      expect(none.close()).toBe(true);
      const advertised = await h.connect(WIRE_PROTOCOL_VERSION, h.features);
      expect(advertised.close()).toBe(true);
    } finally {
      await h.close();
    }
  });
});

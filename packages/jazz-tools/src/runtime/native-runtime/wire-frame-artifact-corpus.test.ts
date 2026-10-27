import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { hasJazzNapiBuild, loadNapiModule } from "../testing/napi-runtime-test-utils.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import {
  FEATURE_PAYLOAD_ZSTD,
  decodeWebSocketFrameBatch,
  decodeWireError,
  encodeWebSocketFrameBatch,
} from "./websocket.js";

type Batch = { name: string; reader: Reader; batch_hex: string };
type Reader = "server-opening" | "server" | "client";
type SyncWireCorpus = {
  format: string;
  messages: Array<{ variant: string; reader: Reader; frame_hex: string }>;
  fates: Array<{ name: string; reason: string | null; reader: Reader; frame_hex: string }>;
  frames: Batch[];
  rejections: Batch[];
};

type ArtifactFrameValidator = {
  __testValidateWireFrameBatchCorpus(
    batch: Uint8Array,
    reader: Reader,
    negotiatedFeatures: string,
  ): void;
  __testWireFrameCorpusFeatures(): string;
};

type WasmArtifactFrameValidator = ArtifactFrameValidator & {
  WasmDb: { prototype: { wireFeatures?: () => number } };
};

// `crates/jazz/fixtures/sqlite_sync_wire_v1.json` freezes one real Rust-encoded
// frame of every SQLite sync message variant (see
// crates/jazz/tests/sync_wire_corpus.rs), rejected Fate frames with and
// without a reason name, Hello and Error frames, and
// batches a reader must reject. Every one runs through both freshly generated
// artifacts, not a JavaScript mirror of the Rust decoders.
describe("wire frame artifact corpus", () => {
  it("decodes the pinned structured error and the frames around it", () => {
    const corpus = syncWireCorpus();
    const error = corpus.frames.find((frame) => frame.name === "structured error");
    expect(error).toBeDefined();
    const [frame] = decodeWebSocketFrameBatch(hexToBytes(error!.batch_hex));
    expect(decodeWireError(frame!)).toEqual({
      code: "malformed_frame",
      retry: "never",
      message: "malformed frame",
    });
  });

  it.skipIf(!hasJazzNapiBuild() || !hasJazzWasmBuild())(
    "executes every pinned sync message and frame and rejects malformed input through NAPI and WASM",
    async () => {
      const corpus = syncWireCorpus();
      expect(corpus.format).toBe("jazz-sqlite-sync-wire-corpus-v1");
      expect(corpus.messages.map((message) => message.variant)).toHaveLength(20);
      expect(corpus.fates.map((fate) => fate.reason)).toEqual(["client_clock_too_far_ahead", null]);
      const accepted = [
        ...corpus.messages.map((message) => ({
          name: message.variant,
          reader: message.reader,
          batch: encodeWebSocketFrameBatch([hexToBytes(message.frame_hex)]),
        })),
        ...corpus.fates.map((fate) => ({
          name: fate.name,
          reader: fate.reader,
          batch: encodeWebSocketFrameBatch([hexToBytes(fate.frame_hex)]),
        })),
        ...corpus.frames.map((frame) => ({
          name: frame.name,
          reader: frame.reader,
          batch: hexToBytes(frame.batch_hex),
        })),
      ];
      const [napi, wasm] = await Promise.all([
        // This is a deliberately test-only export (`skip_typescript` in
        // napi-rs), so production declarations must not advertise it.
        loadNapiModule() as Promise<unknown> as Promise<ArtifactFrameValidator>,
        loadWasmModuleForTest() as Promise<WasmArtifactFrameValidator>,
      ]);

      // This executes the actual sealed artifact which package assembly
      // publishes, rather than inferring transport support from Cargo.toml.
      // A package receiver must advertise and decode the same zstd capability
      // as the packaged jazz-tools server binary.
      expect(BigInt(napi.__testWireFrameCorpusFeatures()) & BigInt(FEATURE_PAYLOAD_ZSTD)).not.toBe(
        0n,
      );

      // A persistent browser worker creates a NativeRuntimeAdapter around this
      // exact WasmDb export. It must use the artifact's own feature mask when
      // sending its WebSocket Hello; otherwise a freshly sealed browser artifact
      // fails before it can connect, even if the NAPI artifact is current.
      expect(typeof wasm.WasmDb.prototype.wireFeatures).toBe("function");

      for (const artifact of [napi, wasm]) {
        const negotiatedFeatures = artifact.__testWireFrameCorpusFeatures();
        for (const frame of accepted) {
          expect(
            () =>
              artifact.__testValidateWireFrameBatchCorpus(
                frame.batch,
                frame.reader,
                negotiatedFeatures,
              ),
            frame.name,
          ).not.toThrow();
        }
        for (const rejection of corpus.rejections) {
          expect(
            () =>
              artifact.__testValidateWireFrameBatchCorpus(
                hexToBytes(rejection.batch_hex),
                rejection.reader,
                negotiatedFeatures,
              ),
            `${rejection.reader}: ${rejection.name}`,
          ).toThrow();
        }
      }
    },
  );
});

function syncWireCorpus(): SyncWireCorpus {
  return JSON.parse(
    readFileSync(
      new URL("../../../../../crates/jazz/fixtures/sqlite_sync_wire_v1.json", import.meta.url),
      "utf8",
    ),
  ) as SyncWireCorpus;
}

function hexToBytes(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

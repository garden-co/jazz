import { createHash } from "node:crypto";
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
  examples: Array<{ name: string; variant: string; reader: Reader; frame_hex: string }>;
  fates?: Array<{ name: string; reason: string | null; reader: Reader; frame_hex: string }>;
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
  __testValidateSqliteSyncFrameCorpus(frame: Uint8Array): void;
};

type WasmArtifactFrameValidator = ArtifactFrameValidator & {
  WasmDb: { prototype: { wireFeatures?: () => number } };
};

// `crates/jazz/fixtures/sqlite_sync_wire_v3.json` pins one independently assembled
// frame of every SQLite sync message variant plus extended forms (see
// crates/jazz/tests/sync_wire_corpus.rs), plus Hello and Error frames and
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
      expect(corpus.format).toBe("jazz-sqlite-sync-wire-corpus-v3");
      expect(corpus.messages.map((message) => message.variant)).toHaveLength(47);
      const accepted = [
        ...[...corpus.messages, ...corpus.examples].map((message) => ({
          name: message.variant,
          reader: message.reader,
          batch: encodeWebSocketFrameBatch([hexToBytes(message.frame_hex)]),
        })),
        ...(corpus.fates ?? []).map((fate) => ({
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
        for (const fixture of [...corpus.messages, ...corpus.examples]) {
          const frame = hexToBytes(fixture.frame_hex);
          expect(
            () => artifact.__testValidateSqliteSyncFrameCorpus(frame),
            fixture.variant,
          ).not.toThrow();
          for (const invalid of [
            new Uint8Array([...frame, 0]),
            frame.slice(0, -1),
            new Uint8Array([...frame.slice(0, 5), frame[5]! | 0x80, 0, ...frame.slice(6)]),
          ]) {
            expect(
              () => artifact.__testValidateSqliteSyncFrameCorpus(invalid),
              fixture.variant,
            ).toThrow();
          }
        }
        const oldUpstream = readJson("sqlite_sync_wire_v1.json") as SyncWireCorpus;
        const oldV2 = readJson("sqlite_sync_wire_v2.json") as SyncWireCorpus;
        for (const retired of [
          ...oldUpstream.messages,
          ...oldV2.messages,
          ...oldV2.examples,
          ...sqliteMessageFixtures(),
        ]) {
          const frame = hexToBytes(retired.frame_hex);
          expect(
            () => artifact.__testValidateSqliteSyncFrameCorpus(frame),
            retired.variant,
          ).toThrow();
          for (const reader of ["client", "server"] as const) {
            expect(
              () =>
                artifact.__testValidateWireFrameBatchCorpus(
                  encodeWebSocketFrameBatch([frame]),
                  reader,
                  negotiatedFeatures,
                ),
              retired.variant,
            ).toThrow();
          }
        }
        const retiredGroove = readJson("wire_message_frames_v6.json") as {
          fixtures: Array<{ name: string; batch_hex: string }>;
        };
        for (const retired of retiredGroove.fixtures) {
          const batch = hexToBytes(retired.batch_hex);
          for (const reader of ["client", "server"] as const) {
            expect(
              () => artifact.__testValidateWireFrameBatchCorpus(batch, reader, negotiatedFeatures),
              retired.name,
            ).toThrow();
          }
          for (const frame of decodeWebSocketFrameBatch(batch)) {
            expect(
              () => artifact.__testValidateSqliteSyncFrameCorpus(frame),
              retired.name,
            ).toThrow();
          }
        }
        const carrier = readJson("wire_frame_artifact_corpus_v6.json") as {
          error_frames: Array<{ name: string; frame_hex: string }>;
          rejections: Array<{ name: string; frame_hex: string; negotiated_features: string }>;
        };
        for (const frame of carrier.error_frames) {
          expect(
            () =>
              artifact.__testValidateWireFrameBatchCorpus(
                encodeWebSocketFrameBatch([hexToBytes(frame.frame_hex)]),
                "client",
                negotiatedFeatures,
              ),
            frame.name,
          ).not.toThrow();
        }
        for (const rejection of carrier.rejections) {
          expect(
            () =>
              artifact.__testValidateWireFrameBatchCorpus(
                encodeWebSocketFrameBatch([hexToBytes(rejection.frame_hex)]),
                "client",
                rejection.negotiated_features,
              ),
            rejection.name,
          ).toThrow();
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
      new URL("../../../../../crates/jazz/fixtures/sqlite_sync_wire_v3.json", import.meta.url),
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

function readJson(name: string): unknown {
  return JSON.parse(
    readFileSync(new URL(`../../../../../crates/jazz/fixtures/${name}`, import.meta.url), "utf8"),
  );
}

function sqliteMessageFixtures(): Array<{ name: string; variant: string; frame_hex: string }> {
  const corpus = readJson("sqlite_sync_artifact_corpus_v1.json") as {
    role: string;
    version: number;
    variants: string[];
    sources: Record<string, string>;
    base_messages: Array<{ name: string; variant: string; frame_hex: string }>;
    existing_messages: Array<{ name: string; variant: string; source: string; pointer: string }>;
  };
  expect(corpus.role).toBe("jazz.sqlite.sync-artifact-corpus.v1");
  expect(corpus.version).toBe(1);
  const sources = new Map<string, unknown>();
  for (const [name, sha256] of Object.entries(corpus.sources)) {
    const bytes = readFileSync(
      new URL(`../../../../../crates/jazz/fixtures/${name}`, import.meta.url),
    );
    expect(createHash("sha256").update(bytes).digest("hex"), name).toBe(sha256);
    sources.set(name, JSON.parse(bytes.toString("utf8")));
  }
  const fixtures = [
    ...corpus.base_messages,
    ...corpus.existing_messages.map((item) => {
      let value: unknown = sources.get(item.source);
      for (const part of item.pointer.slice(1).split("/")) {
        value = (value as Record<string, unknown>)[part];
      }
      expect(typeof value, item.name).toBe("string");
      return { name: item.name, variant: item.variant, frame_hex: value as string };
    }),
  ];
  expect(new Set(fixtures.map((item) => item.name)).size).toBe(fixtures.length);
  expect([...new Set(fixtures.map((item) => item.variant))].sort()).toEqual(
    [...corpus.variants].sort(),
  );
  expect(corpus.variants).toHaveLength(41);
  for (const item of fixtures) {
    expect(item.frame_hex).toMatch(/^(?:[0-9a-f]{2})+$/);
    expect(Number.parseInt(item.frame_hex.slice(0, 2), 16), item.name).toBe(
      corpus.variants.indexOf(item.variant),
    );
  }
  return fixtures;
}

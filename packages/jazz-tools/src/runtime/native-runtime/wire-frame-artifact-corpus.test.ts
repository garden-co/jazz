import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { hasJazzNapiBuild, loadNapiModule } from "../testing/napi-runtime-test-utils.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import {
  FEATURE_PAYLOAD_ZSTD,
  decodeWireError,
  decodeWebSocketFrameBatch,
  encodeWebSocketFrameBatch,
} from "./websocket.js";

type FrameFixture = { name: string; frame_hex: string };
type ArtifactCorpus = {
  format: string;
  error_frames: Array<{
    name: string;
    frame_hex: string;
    code: string;
    retry: string;
    message: string;
  }>;
  rejections: Array<{ name: string; frame_hex: string; negotiated_features: string }>;
};

type ArtifactFrameValidator = {
  __testValidateWireFrameBatchCorpus(frame: Uint8Array, negotiatedFeatures: string): void;
  __testWireFrameCorpusFeatures(): string;
  __testValidateSqliteSyncFrameCorpus(frame: Uint8Array): void;
};

type WasmArtifactFrameValidator = ArtifactFrameValidator & {
  WasmDb: { prototype: { wireFeatures?: () => number } };
};

// Control-frame fixtures still use the shared v6 carrier. SQLite semantics use
// their own exact decoder; retired Groove frames must not become current input.
// Both paths execute Rust in the freshly sealed generated artifacts.
describe("wire frame artifact corpus", () => {
  it("decodes every exact structured v1 error and rejects planted field drift", () => {
    const corpus = artifactCorpus();
    for (const error of corpus.error_frames) {
      expect(decodeWireError(hexToBytes(error.frame_hex)), error.name).toEqual({
        code: error.code,
        retry: error.retry,
        message: error.message,
      });
    }
    const bootstrap = corpus.error_frames.find(
      (error) => error.name === "runtime bootstrap not ready",
    );
    expect(bootstrap).toBeDefined();
    expect(decodeWireError(hexToBytes(bootstrap!.frame_hex))).not.toEqual({
      code: "malformed_frame",
      retry: bootstrap!.retry,
      message: bootstrap!.message,
    });
    expect(decodeWireError(hexToBytes(bootstrap!.frame_hex))).not.toEqual({
      code: bootstrap!.code,
      retry: "never",
      message: bootstrap!.message,
    });
  });

  it.skipIf(!hasJazzNapiBuild() || !hasJazzWasmBuild())(
    "executes current SQLite messages and shared control frames while refusing retired and malformed input through NAPI and WASM",
    async () => {
      const corpus = artifactCorpus();
      expect(corpus.format).toBe("jazz-wire-frame-artifact-corpus-v6");
      const accepted = [...rustHelloFixtures(), ...corpus.error_frames].map((frame) => ({
        name: frame.name,
        batch: encodeWebSocketFrameBatch([hexToBytes(frame.frame_hex)]),
      }));
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
            () => artifact.__testValidateWireFrameBatchCorpus(frame.batch, negotiatedFeatures),
            frame.name,
          ).not.toThrow();
        }
        // Retain every old complete semantic case as an explicit incompatibility
        // check, at both its retired decoder and the actual SQLite boundary.
        for (const retired of rustMessageFixtures()) {
          const batch = hexToBytes(retired.batch_hex);
          expect(
            () => artifact.__testValidateWireFrameBatchCorpus(batch, negotiatedFeatures),
            retired.name,
          ).toThrow();
          for (const frame of decodeWebSocketFrameBatch(batch)) {
            expect(
              () => artifact.__testValidateSqliteSyncFrameCorpus(frame),
              retired.name,
            ).toThrow();
          }
        }
        for (const fixture of sqliteMessageFixtures()) {
          const frame = hexToBytes(fixture.frame_hex);
          expect(
            () => artifact.__testValidateSqliteSyncFrameCorpus(frame),
            fixture.name,
          ).not.toThrow();
          for (const invalid of [
            new Uint8Array([...frame, 0]),
            frame.slice(0, -1),
            new Uint8Array([frame[0]! | 0x80, 0, ...frame.slice(1)]),
          ]) {
            expect(
              () => artifact.__testValidateSqliteSyncFrameCorpus(invalid),
              fixture.name,
            ).toThrow();
          }
        }
        for (const rejection of corpus.rejections) {
          expect(
            () =>
              artifact.__testValidateWireFrameBatchCorpus(
                encodeWebSocketFrameBatch([hexToBytes(rejection.frame_hex)]),
                rejection.negotiated_features,
              ),
            rejection.name,
          ).toThrow();
        }
      }
    },
  );
});

function artifactCorpus(): ArtifactCorpus {
  return readJson("wire_frame_artifact_corpus_v6.json") as ArtifactCorpus;
}

function rustHelloFixtures(): FrameFixture[] {
  return (readJson("wire_hello_frames_v6.json") as { fixtures: FrameFixture[] }).fixtures;
}

function rustMessageFixtures(): Array<{ name: string; batch_hex: string }> {
  return (
    readJson("wire_message_frames_v6.json") as {
      fixtures: Array<{ name: string; batch_hex: string }>;
    }
  ).fixtures;
}

function readJson(name: string): unknown {
  return JSON.parse(
    readFileSync(new URL(`../../../../../crates/jazz/fixtures/${name}`, import.meta.url), "utf8"),
  );
}

function hexToBytes(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
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

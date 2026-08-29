import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { hasJazzNapiBuild, loadNapiModule } from "../testing/napi-runtime-test-utils.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";

type FrameFixture = { name: string; frame_hex: string };
type ArtifactCorpus = {
  format: string;
  error_frame_hex: string;
  rejections: Array<{ name: string; frame_hex: string; negotiated_features: string }>;
};

type ArtifactFrameValidator = {
  __testValidateWireFrameCorpus(frame: Uint8Array, negotiatedFeatures: string): void;
  __testWireFrameCorpusFeatures(): string;
};

// The source fixtures freeze the exact Rust-produced v1 bytes. This test then
// sends every complete Hello/message frame through both freshly generated artifacts,
// not a JavaScript mirror of the Rust frame/payload/compression decoder.
describe("wire frame artifact corpus", () => {
  it.skipIf(!hasJazzNapiBuild() || !hasJazzWasmBuild())(
    "executes every complete v1 frame and rejects malformed input through NAPI and WASM",
    async () => {
      const corpus = artifactCorpus();
      expect(corpus.format).toBe("jazz-wire-frame-artifact-corpus-v1");
      const accepted = [
        ...rustHelloFixtures(),
        ...rustMessageFixtures(),
        { name: "structured error", frame_hex: corpus.error_frame_hex },
      ];
      const [napi, wasm] = await Promise.all([
        // This is a deliberately test-only export (`skip_typescript` in
        // napi-rs), so production declarations must not advertise it.
        loadNapiModule() as Promise<unknown> as Promise<ArtifactFrameValidator>,
        loadWasmModuleForTest() as Promise<ArtifactFrameValidator>,
      ]);

      for (const artifact of [napi, wasm]) {
        const negotiatedFeatures = artifact.__testWireFrameCorpusFeatures();
        for (const frame of accepted) {
          expect(
            () =>
              artifact.__testValidateWireFrameCorpus(
                hexToBytes(frame.frame_hex),
                negotiatedFeatures,
              ),
            frame.name,
          ).not.toThrow();
        }
        for (const rejection of corpus.rejections) {
          expect(
            () =>
              artifact.__testValidateWireFrameCorpus(
                hexToBytes(rejection.frame_hex),
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
  return readJson("wire_frame_artifact_corpus.json") as ArtifactCorpus;
}

function rustHelloFixtures(): FrameFixture[] {
  return (readJson("wire_hello_frames.json") as { fixtures: FrameFixture[] }).fixtures;
}

function rustMessageFixtures(): FrameFixture[] {
  return (readJson("wire_message_frames.json") as { fixtures: FrameFixture[] }).fixtures;
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

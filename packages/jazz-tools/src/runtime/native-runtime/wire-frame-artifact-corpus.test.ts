import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { hasJazzNapiBuild, loadNapiModule } from "../testing/napi-runtime-test-utils.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";

type FrameFixture = { name: string; frame_hex: string };
type ArtifactCorpus = {
  format: string;
  hello_cases: string[];
  message_cases: string[];
  error_frame_hex: string;
  rejections: Array<{ name: string; frame_hex: string }>;
};

type ArtifactFrameValidator = {
  __testValidateWireFrameCorpus(frame: Uint8Array): void;
};

// The source fixtures freeze the exact Rust-produced v1 bytes. This test then
// sends the selected complete frames through both freshly generated artifacts,
// not a JavaScript mirror of the Rust frame/payload/compression decoder.
describe("wire frame artifact corpus", () => {
  it.skipIf(!hasJazzNapiBuild() || !hasJazzWasmBuild())(
    "accepts complete v1 frames and rejects malformed frame input through NAPI and WASM",
    async () => {
      const corpus = artifactCorpus();
      expect(corpus.format).toBe("jazz-wire-frame-artifact-corpus-v1");
      const hello = namedFrames(rustHelloFixtures(), corpus.hello_cases);
      const messages = namedFrames(rustMessageFixtures(), corpus.message_cases);
      const accepted = [
        ...hello,
        ...messages,
        { name: "structured error", frame_hex: corpus.error_frame_hex },
      ];
      const [napi, wasm] = await Promise.all([
        loadNapiModule() as Promise<ArtifactFrameValidator>,
        loadWasmModuleForTest() as Promise<ArtifactFrameValidator>,
      ]);

      for (const artifact of [napi, wasm]) {
        for (const frame of accepted) {
          expect(
            () => artifact.__testValidateWireFrameCorpus(hexToBytes(frame.frame_hex)),
            frame.name,
          ).not.toThrow();
        }
        for (const rejection of corpus.rejections) {
          expect(
            () => artifact.__testValidateWireFrameCorpus(hexToBytes(rejection.frame_hex)),
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

function namedFrames(fixtures: FrameFixture[], names: string[]): FrameFixture[] {
  return names.map((name) => {
    const fixture = fixtures.find((candidate) => candidate.name === name);
    if (!fixture) throw new Error(`wire artifact corpus names missing fixture ${name}`);
    return fixture;
  });
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

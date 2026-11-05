import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { hasJazzNapiBuild, loadNapiModule } from "../testing/napi-runtime-test-utils.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import { encodeWebSocketFrameBatch } from "./websocket.js";

type Reader = "server-opening" | "server" | "client";
type Frame = { frame_hex: string; reader: Reader };
type Batch = { batch_hex: string; reader: Reader };
type Corpus = {
  messages: Frame[];
  examples: Frame[];
  fates?: Frame[];
  frames: Batch[];
  rejections: Batch[];
  cases?: Frame[];
};
type Artifact = {
  __testValidateWireFrameBatchCorpus(batch: Uint8Array, reader: Reader, features: string): void;
  __testWireFrameCorpusFeatures(): string;
  __testValidateSqliteSyncFrameCorpus(frame: Uint8Array): void;
};

const read = (name: string): Corpus =>
  JSON.parse(
    readFileSync(new URL(`../../../../../crates/jazz/fixtures/${name}`, import.meta.url), "utf8"),
  );
const bytes = (hex: string) => Uint8Array.from(Buffer.from(hex, "hex"));

describe("version-3 wire artifacts", () => {
  for (const kind of ["NAPI", "WASM"] as const) {
    it.skipIf(kind === "NAPI" ? !hasJazzNapiBuild() : !hasJazzWasmBuild())(
      `${kind} accepts every current frame and refuses historical and malformed frames`,
      async () => {
        // Exercise the actual native decoder; release qualification must run
        // both cases against freshly rebuilt artifacts without skips.
        const artifact = (await (kind === "NAPI"
          ? loadNapiModule()
          : loadWasmModuleForTest())) as unknown as Artifact;
        const current = read("sqlite_sync_wire_v3.json");
        expect(current.messages).toHaveLength(47);
        const errors = read("sqlite-sync-v3/error-cases.json").cases!;
        expect(errors).toHaveLength(7);
        const features = artifact.__testWireFrameCorpusFeatures();
        for (const frame of [
          ...current.messages,
          ...current.examples,
          ...current.fates!,
          ...errors,
        ]) {
          artifact.__testValidateSqliteSyncFrameCorpus(bytes(frame.frame_hex));
          artifact.__testValidateWireFrameBatchCorpus(
            encodeWebSocketFrameBatch([bytes(frame.frame_hex)]),
            frame.reader,
            features,
          );
        }
        for (const batch of current.frames) {
          artifact.__testValidateWireFrameBatchCorpus(
            bytes(batch.batch_hex),
            batch.reader,
            features,
          );
        }
        for (const batch of current.rejections) {
          expect(() =>
            artifact.__testValidateWireFrameBatchCorpus(
              bytes(batch.batch_hex),
              batch.reader,
              features,
            ),
          ).toThrow();
        }
        for (const name of ["sqlite_sync_wire_v1.json", "sqlite_sync_wire_v2.json"]) {
          const old = read(name);
          for (const frame of [...old.messages, ...(old.examples ?? [])]) {
            expect(() =>
              artifact.__testValidateSqliteSyncFrameCorpus(bytes(frame.frame_hex)),
            ).toThrow();
            for (const reader of ["client", "server"] as const) {
              expect(() =>
                artifact.__testValidateWireFrameBatchCorpus(
                  encodeWebSocketFrameBatch([bytes(frame.frame_hex)]),
                  reader,
                  features,
                ),
              ).toThrow();
            }
          }
        }
      },
    );
  }
});

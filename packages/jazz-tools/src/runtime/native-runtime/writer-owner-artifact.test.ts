import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { hasJazzNapiBuild, loadNapiModule } from "../testing/napi-runtime-test-utils.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import { encodeWebSocketFrameBatch } from "./websocket.js";

type Artifact = {
  __testValidateWireFrameBatchCorpus(batch: Uint8Array, reader: "server", features: string): void;
  __testWireFrameCorpusFeatures(): string;
  __testValidateSqliteSyncFrameCorpus(frame: Uint8Array): void;
};
type Case = { name: string; bytes_hex: string };
const cases: Case[] = JSON.parse(
  readFileSync(
    new URL("../../../../../crates/jazz/fixtures/sqlite_writer_owner_v1.json", import.meta.url),
    "utf8",
  ),
).cases;

// This boundary test uses independent bytes because application writes cannot
// distinguish a newly built codec from an older artifact with the same JS API.
describe("original writer ownership artifacts", () => {
  for (const kind of ["NAPI", "WASM"] as const) {
    it(`${kind} admits the new message and rejects malformed or older encodings`, async () => {
      expect(cases.map(({ name }) => name)).toEqual([
        "accountless",
        "account",
        "system",
        "system-origin",
      ]);
      // Qualification requires both real artifacts, with no skip fallback.
      expect(kind === "NAPI" ? hasJazzNapiBuild() : hasJazzWasmBuild()).toBe(true);
      if (kind === "NAPI") {
        expect(createRequire(import.meta.url).resolve("jazz-napi")).toBe(
          fileURLToPath(new URL("../../../../../crates/jazz-napi/index.cjs", import.meta.url)),
        );
      }
      const artifact = (await (kind === "NAPI"
        ? loadNapiModule()
        : loadWasmModuleForTest())) as unknown as Artifact;
      const features = artifact.__testWireFrameCorpusFeatures();
      const admit = (frame: Uint8Array) => {
        artifact.__testValidateSqliteSyncFrameCorpus(frame);
        artifact.__testValidateWireFrameBatchCorpus(
          encodeWebSocketFrameBatch([frame]),
          "server",
          features,
        );
      };
      const refuse = (frame: Uint8Array) => {
        expect(() => artifact.__testValidateSqliteSyncFrameCorpus(frame)).toThrow();
        expect(() =>
          artifact.__testValidateWireFrameBatchCorpus(
            encodeWebSocketFrameBatch([frame]),
            "server",
            features,
          ),
        ).toThrow();
      };
      for (const { bytes_hex } of cases) {
        const frame = Uint8Array.from(Buffer.from(bytes_hex, "hex"));
        admit(frame);
        for (let end = 0; end < frame.length; end++) refuse(frame.subarray(0, end));
        refuse(Uint8Array.from([...frame, 0]));
        refuse(Uint8Array.from([...frame.subarray(0, 5), 0xaf, 0, ...frame.subarray(6)]));
        const unknownTag = frame.slice();
        unknownTag[5] = 48;
        refuse(unknownTag);
        for (const oldVersion of [1, 2]) {
          const old = frame.slice();
          old[4] = oldVersion;
          refuse(old);
        }
      }
    });
  }
});

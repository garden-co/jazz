import { readFileSync } from "node:fs";

import { expect, it } from "vitest";

import { hasJazzNapiBuild, loadNapiModule } from "../testing/napi-runtime-test-utils.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import { encodeWebSocketFrameBatch } from "./websocket.js";

type Validator = {
  __testValidateWireFrameBatchCorpus(batch: Uint8Array, reader: "server", features: string): void;
  __testWireFrameCorpusFeatures(): string;
};

// These are the actual sealed artifacts admitted by run-correctness-consumer.
// The independent fixture pins the Rust enum's new nested content vector;
// JavaScript does not mirror its decoder or assemble expected serializer bytes.
it.skipIf(!hasJazzNapiBuild() || !hasJazzWasmBuild())(
  "NAPI and WASM decode the content-bearing commit and reject malformed frames",
  async () => {
    const fixture = JSON.parse(
      readFileSync(
        new URL(
          "../../../../../crates/jazz/fixtures/sqlite_serializable_content_v1.json",
          import.meta.url,
        ),
        "utf8",
      ),
    ) as { wireRole: string; commitHex: string };
    expect(fixture.wireRole).toBe("jazz.sqlite.serializable-content.v1");
    const frame = Uint8Array.from(Buffer.from(fixture.commitHex, "hex"));
    const [napi, wasm] = await Promise.all([
      loadNapiModule() as Promise<unknown> as Promise<Validator>,
      loadWasmModuleForTest() as Promise<Validator>,
    ]);
    for (const artifact of [napi, wasm]) {
      const features = artifact.__testWireFrameCorpusFeatures();
      const validate = (frame: Uint8Array) =>
        artifact.__testValidateWireFrameBatchCorpus(
          encodeWebSocketFrameBatch([frame]),
          "server",
          features,
        );
      expect(() => validate(frame)).not.toThrow();
      for (let end = 0; end < frame.length; end += 1) {
        expect(() => validate(frame.slice(0, end)), `truncated at ${end}`).toThrow();
      }
      const trailing = new Uint8Array(frame.length + 1);
      trailing.set(frame);
      expect(() => validate(trailing)).toThrow();
      const unknown = frame.slice();
      expect(unknown[24]).toBe(2); // envelope, Commit tag, node UUID, transaction 128
      unknown[24] = 127;
      expect(() => validate(unknown)).toThrow();
    }
  },
);

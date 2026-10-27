import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { encodeSchema } from "./native-runtime/native-runtime-adapter.js";
import {
  openConfig,
  PostcardReader,
  queryFromTable,
  readNativeRowBatch,
} from "./native-runtime/native-codec.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";
import { hasJazzNapiBuild, loadNapiModule } from "./testing/napi-runtime-test-utils.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "./testing/wasm-runtime-test-utils.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });
// Existing SQLite Unsubscribe { id: 128 }, assembled from independent fields.
// Byte-level ingress checks are required here: row equality cannot detect an
// unread suffix or alternate varint spelling at the generated artifact boundary.
const valid = new Uint8Array([1, 128, 1]);
const invalid = [
  new Uint8Array([...valid, 0]),
  new Uint8Array([129, 0, 128, 1]),
  new Uint8Array([1, 128, 129, 0]),
  new Uint8Array(),
  new Uint8Array([255]),
  valid.slice(0, -1),
];

for (const artifact of ["napi", "wasm"] as const) {
  describe.skipIf(artifact === "napi" ? !hasJazzNapiBuild() : !hasJazzWasmBuild())(
    `${artifact} SQLite sync ingress`,
    () => {
      it.each(["single", "batch"] as const)(
        "%s refuses malformed bytes synchronously before the next database tick",
        async (mode) => {
          const Runtime =
            artifact === "napi"
              ? (await loadNapiModule()).NapiDb
              : (await loadWasmModuleForTest()).WasmDb;
          const db = Runtime.openMemory(
            encodeSchema(app.wasmSchema),
            openConfig(
              new Uint8Array(16).fill(43),
              testAuthorBytes(`sync-ingress-${artifact}-${mode}`),
            ),
          );
          const link = await db.connectUpstream();
          try {
            for (const frame of invalid) {
              expect(
                () =>
                  mode === "single"
                    ? link.sendWireFrame(frame)
                    : link.sendWireFrames([valid, frame]),
                Buffer.from(frame).toString("hex"),
              ).toThrow(/invalid SQLite sync frame/);
            }
            // Refusal is synchronous; this handle can still accept a complete,
            // canonical frame. Applying its direction-specific semantics is a
            // separate sync contract exercised by the binding/server suites.
            expect(() => link.sendWireFrame(valid)).not.toThrow();
          } finally {
            link.close();
            await db.close();
          }
        },
      );

      it("keeps a read pending when its valid answer precedes a malformed frame in a batch", async () => {
        const Runtime =
          artifact === "napi"
            ? (await loadNapiModule()).NapiDb
            : (await loadWasmModuleForTest()).WasmDb;
        // Alice's backend can read the empty table; only a remote answer may
        // finish this read. Bob's transport submits [answer, malformed].
        const db = Runtime.openMemoryAsBackend(
          encodeSchema(app.wasmSchema),
          openConfig(new Uint8Array(16).fill(44), testAuthorBytes(`batch-read-${artifact}`)),
        );
        const link = await db.connectUpstream();
        try {
          const read = db.all(queryFromTable("notes"), { tier: "remote" });
          if (read instanceof Uint8Array)
            throw new Error("Remote read completed without an answer");
          expect(read.poll()).toBeNull();
          await db.tick();
          const fetches = (link.recvWireFrames() as Uint8Array[]).filter((frame) => frame[0] === 3);
          expect(fetches).toHaveLength(1);
          // Fetch's first field is its canonical request ID. Answer uses the
          // same ID, followed by through=0 and an empty row-ID vector. Copy
          // only the ID bytes, not a serializer's complete expected output.
          const fetch = fetches[0];
          let idEnd = 1;
          while (fetch[idEnd++] & 0x80) {}
          const answer = new Uint8Array([11, ...fetch.slice(1, idEnd), 0, 0]);

          expect(() => link.sendWireFrames([answer, invalid[0]])).toThrow(
            /invalid SQLite sync frame/,
          );
          await db.tick();
          expect(read.poll(), "A rejected batch must not deliver its valid prefix").toBeNull();

          link.sendWireFrame(answer);
          await db.tick();
          const result = read.poll();
          expect(result).toBeInstanceOf(Uint8Array);
          expect(new PostcardReader(result!).readVec(readNativeRowBatch)).toEqual([]);
        } finally {
          link.close();
          await db.close();
        }
      });
    },
  );
}

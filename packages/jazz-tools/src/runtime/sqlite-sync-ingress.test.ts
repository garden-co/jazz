import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { encodeSchema } from "./native-runtime/native-runtime-adapter.js";
import { openConfig } from "./native-runtime/native-codec.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";
import { hasJazzNapiBuild, loadNapiModule } from "./testing/napi-runtime-test-utils.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "./testing/wasm-runtime-test-utils.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });
// jazz.sqlite.sync.v3: the explicit JSQL/3 header and Unsubscribe { id: 128 }.
// Byte-level ingress checks are required here: row equality cannot detect an
// unread suffix or alternate varint spelling at the generated artifact boundary.
const prefix = [0x4a, 0x53, 0x51, 0x4c, 3];
const valid = new Uint8Array([...prefix, 1, 128, 1]);
const invalid = [
  new Uint8Array([...valid, 0]),
  new Uint8Array([...prefix, 129, 0, 128, 1]),
  new Uint8Array([...prefix, 1, 128, 129, 0]),
  new Uint8Array(),
  new Uint8Array([255]),
  valid.slice(0, -1),
  new Uint8Array([1, 128, 1]), // Historical unversioned SQLite frame.
  new Uint8Array([0x4a, 0x53, 0x51, 0x4c, 1, 1, 128, 1]), // Rejected prior version.
  new Uint8Array([0x4a, 0x53, 0x51, 0x4c, 2, 1, 128, 1]), // Rejected prior version.
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
    },
  );
}

import { randomUUID } from "node:crypto";
import { WebSocket } from "undici";
import { expect, it, onTestFinished } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import { startLocalJazzServer } from "../testing/index.js";
import { deterministicBytes } from "./deterministic-bytes.js";
import { openConfig } from "./native-runtime/native-codec.js";
import { nativeCoreErrorCode } from "./native-runtime/native-error-code.js";
import { NativeRuntimeAdapter } from "./native-runtime/native-runtime-adapter.js";
import { encodeSchema } from "./native-runtime/schema-codec.js";
import { webSocketUrl } from "./native-runtime/websocket.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";
import { allowAll } from "./testing/allow-all.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";
import { loadWasmModuleForTest } from "./testing/wasm-runtime-test-utils.js";

const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = allowAll(app);
const schema = mergePermissionsIntoWasmSchema(app.wasmSchema, permissions);

async function open(host: "NAPI" | "WASM", label: string) {
  const Runtime =
    host === "NAPI" ? (await loadNapiModule()).NapiDb : (await loadWasmModuleForTest()).WasmDb;
  const node = deterministicBytes(`${label}:node`);
  const author = testAuthorBytes(label);
  const db = Runtime.openMemory(encodeSchema(schema), openConfig(node, author, 1, false));
  return { db, node, author };
}

it.each(["NAPI", "WASM"] as const)(
  "%s keeps NotObserved distinct from an unknown catalogue name, and refuses closed reads",
  async (host) => {
    const { db } = await open(host, `${host}:alice`);
    try {
      for (const read of [
        () => db.tableIdentity("notes"),
        () => db.tableIdentity("missing"),
        () => db.columnIdentity("notes", "title"),
        () => db.columnIdentity("notes", "missing"),
      ]) {
        let error: unknown;
        try {
          read();
        } catch (thrown) {
          error = thrown;
        }
        expect(nativeCoreErrorCode(error)).toBe("not_observed");
      }
    } finally {
      await db.close();
    }
    expect(() => db.tableIdentity("notes")).toThrow(/closed/);
    expect(() => db.columnIdentity("notes", "title")).toThrow(/closed/);
  },
);

it("Alice's NAPI and WASM hosts expose the same authority UUID bytes after the real handshake", async () => {
  globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;
  const server = await startLocalJazzServer({
    appId: randomUUID(),
    inMemory: true,
    schema,
    permissions,
  });
  onTestFinished(() => server.stop());
  const identities: Array<{ table: number[]; column: number[] }> = [];
  for (const host of ["NAPI", "WASM"] as const) {
    const { db, node, author } = await open(host, `${host}:alice:connected`);
    const runtime = NativeRuntimeAdapter.fromDb(db as never, schema, node, author, 1, false);
    onTestFinished(() => runtime.close());
    runtime.connect(
      webSocketUrl(server.url, server.appId),
      JSON.stringify({ backend_secret: server.backendSecret }),
    );
    await expect
      .poll(
        () => {
          try {
            return db.tableIdentity("notes").length;
          } catch (error) {
            if (nativeCoreErrorCode(error) !== "not_observed") throw error;
            return 0;
          }
        },
        { timeout: 5_000 },
      )
      .toBe(16);
    const table = db.tableIdentity("notes");
    const column = db.columnIdentity("notes", "title");
    expect(table).toBeInstanceOf(Uint8Array);
    expect(column).toBeInstanceOf(Uint8Array);
    expect(column.length).toBe(16);
    expect([...table]).not.toEqual([...column]);
    expect([...db.tableIdentity("missing")]).toEqual([]);
    expect([...db.columnIdentity("notes", "missing")]).toEqual([]);
    const view = db.registerSchema(encodeSchema(schema));
    expect([...view.tableIdentity("notes")]).toEqual([...table]);
    expect([...view.columnIdentity("notes", "title")]).toEqual([...column]);
    identities.push({ table: [...table], column: [...column] });
  }
  expect(identities[0]).toEqual(identities[1]);
}, 15_000);

import { expect, it, onTestFinished } from "vitest";
import { schema as s } from "../../schema-namespace.js";
import { mergePermissionsIntoWasmSchema } from "../../schema-permissions.js";
import { createOpenTransactionId } from "../client.js";
import { allowAll } from "../testing/allow-all.js";
import { testAuthorBytes } from "../testing/account-fixtures.js";
import { loadNapiModule } from "../testing/napi-runtime-test-utils.js";
import { loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import { openConfig } from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";
import {
  encodeCellsForStreamingRow,
  NativeRuntimeAdapter,
  type RowState,
} from "./native-runtime-adapter.js";

type Staged = { attach(id: string): void; abort(): boolean | Promise<boolean> };
type Upload = {
  push(chunk: Uint8Array): void | Promise<void>;
  stage(): Staged | Promise<Staged>;
  finishInTransaction(): void;
};
type Raw = {
  registerSchema(schema: Uint8Array): Raw;
  close(): unknown;
  beginStreamingMutation(
    table: string,
    row: Uint8Array,
    cells: Uint8Array,
    column: string,
    mutation?: string,
    author?: Uint8Array,
    attribution?: Uint8Array,
    time?: number,
    head?: unknown,
    base?: unknown,
    transaction?: string,
  ): Upload;
  beginTransaction(id: string, kind: string): void;
  commitTransaction(id: string, kind: string): { close(): unknown };
};
const app = s.defineApp({ files: s.table({ payload: s.bytes() }, {}) }, { migrations: "none" });
const schema = mergePermissionsIntoWasmSchema(app.wasmSchema, allowAll(app));

async function open(host: "NAPI" | "WASM") {
  const ctor =
    host === "NAPI" ? (await loadNapiModule()).NapiDb : (await loadWasmModuleForTest()).WasmDb;
  const node = new Uint8Array(16).fill(host === "NAPI" ? 41 : 42);
  const author = testAuthorBytes(`staged-stream:${host}:alice`);
  const native = ctor.openMemory(encodeSchema(schema), openConfig(node, author, 1, false));
  const runtime = NativeRuntimeAdapter.fromDb(native as never, schema, node, author, 1, false);
  onTestFinished(() => runtime.close());
  const raw = native as unknown as Raw;
  const rows = async () =>
    (await runtime.query(
      JSON.stringify({ table: "files" }),
      undefined,
      "local-only",
    )) as RowState[];
  const begin = (id: number, tx?: string, target: Raw = raw) =>
    target.beginStreamingMutation(
      "files",
      new Uint8Array(16).fill(id),
      encodeCellsForStreamingRow(schema.files!, {}, "payload"),
      "payload",
      "insert",
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      tx,
    );
  return { raw, runtime, rows, begin };
}

for (const host of ["NAPI", "WASM"] as const) {
  it(`${host} publishes a completed chunked stream only through its later batch`, async () => {
    const { raw, rows, begin } = await open(host);
    const bytes = Uint8Array.from({ length: 196_633 }, (_, i) => i % 251);
    const upload = begin(1);
    for (let offset = 0; offset < bytes.length; offset += 4096)
      await upload.push(bytes.subarray(offset, offset + 4096));
    const staged = await upload.stage();
    expect(await rows()).toEqual([]);
    const tx = createOpenTransactionId();
    raw.beginTransaction(tx, "serializable");
    staged.attach(tx);
    expect(await rows()).toEqual([]);
    const receipt = raw.commitTransaction(tx, "serializable");
    receipt.close();
    const published = await rows();
    expect(published).toHaveLength(1);
    expect(published[0]?.valuesByColumn?.get("payload")).toEqual({ type: "Bytea", value: bytes });
    expect(() => staged.attach(tx)).toThrow(/closed/);
    expect(await staged.abort()).toBe(false);
  });

  it(`${host} keeps a stream admitted inside its original transaction`, async () => {
    const { raw, rows, begin } = await open(host);
    const tx = createOpenTransactionId();
    raw.beginTransaction(tx, "serializable");
    const upload = begin(2, tx);
    await upload.push(new Uint8Array([9, 8, 7]));
    await expect(Promise.resolve().then(() => upload.stage())).rejects.toThrow(/transaction/);
    upload.finishInTransaction();
    expect(await rows()).toEqual([]);
    const receipt = raw.commitTransaction(tx, "serializable");
    receipt.close();
    expect((await rows())[0]?.valuesByColumn?.get("payload")).toEqual({
      type: "Bytea",
      value: new Uint8Array([9, 8, 7]),
    });
  });

  it(`${host} consumes invalid attachment and refuses a retired owner`, async () => {
    const { raw, runtime, rows, begin } = await open(host);
    const upload = begin(3);
    await upload.push(new Uint8Array([1]));
    const invalid = await upload.stage();
    expect(() => invalid.attach("not a transaction")).toThrow();
    expect(await invalid.abort()).toBe(false);
    expect(await rows()).toEqual([]);
    const retained = begin(4);
    await retained.push(new Uint8Array([2]));
    const staged = await retained.stage();
    const tx = createOpenTransactionId();
    raw.beginTransaction(tx, "serializable");
    await runtime.close();
    expect(() => staged.attach(tx)).toThrow();
    expect(await staged.abort()).toBe(false);
  });

  it(`${host} keeps a nonowning view close separate from the stream owner`, async () => {
    const { raw, rows, begin } = await open(host);
    const view = raw.registerSchema(encodeSchema(schema));
    const upload = begin(5, undefined, view);
    await upload.push(new Uint8Array([4, 5, 6]));
    const staged = await upload.stage();
    await view.close();
    const tx = createOpenTransactionId();
    raw.beginTransaction(tx, "serializable");
    staged.attach(tx);
    raw.commitTransaction(tx, "serializable").close();
    expect((await rows())[0]?.valuesByColumn?.get("payload")).toEqual({
      type: "Bytea",
      value: new Uint8Array([4, 5, 6]),
    });
  });

  it(`${host} retires view-held capabilities and unfinished streams with the physical owner`, async () => {
    const { raw, runtime, begin } = await open(host);
    const view = raw.registerSchema(encodeSchema(schema));
    const upload = begin(6, undefined, view);
    await upload.push(new Uint8Array([1]));
    const staged = await upload.stage();
    const unfinished = begin(7, undefined, view);
    await unfinished.push(new Uint8Array([2]));
    const tx = createOpenTransactionId();
    raw.beginTransaction(tx, "serializable");
    await runtime.close();
    expect(() => staged.attach(tx)).toThrow();
    expect(await staged.abort()).toBe(false);
    await expect(
      Promise.resolve().then(() => unfinished.push(new Uint8Array([3]))),
    ).rejects.toThrow();
    await expect(Promise.resolve().then(() => unfinished.stage())).rejects.toThrow();
  });
}

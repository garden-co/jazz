import assert from "node:assert/strict";
import test from "node:test";
import {
  createFileBlobHelpers,
  createFileFromBlob,
  fileBlobTable,
  loadFileAsBlob,
  readFileBytes,
  readFiles,
  type AlphaFileBlobHelpers,
} from "./file-blob.js";
import {
  schema,
  type AuthState,
  type Db,
  type InsertOptions,
  type QueryBuilder,
  type Subscription,
  type Table,
  type Transaction,
  type UpsertOptions,
  type WriteResult,
  type WriteTimestampOptions,
} from "./jazz-tools.js";

type StoredFile = {
  id: string;
  name?: string;
  mime_type?: string;
  data: Uint8Array;
};

const fileId = "11111111-1111-1111-1111-111111111111";
const secondFileId = "22222222-2222-2222-2222-222222222222";

test("file/blob helpers store one files row with mime_type and data", async () => {
  const db = new MemoryDb({ files: [] });
  const helpers: AlphaFileBlobHelpers<StoredFile> = createFileBlobHelpers(db);
  const bytes = new TextEncoder().encode("alpha-shaped blob in one row");

  const created = await helpers.createFileFromBlob({
    fileId,
    name: "alpha-note.txt",
    mimeType: "text/plain",
    blob: new Blob([bytes], { type: "text/plain" }),
  });

  assert.equal(created.id, fileId);
  assert.equal(created.name, "alpha-note.txt");
  assert.equal(created.mime_type, "text/plain");
  assert.deepEqual(created.data, bytes);
  assert.deepEqual(helpers.readFiles(), [created]);
  assert.deepEqual(helpers.readFileBytes(fileId), bytes);

  const loaded = await helpers.loadFileAsBlob(fileId);
  assert.equal(loaded.type, "text/plain");
  assert.deepEqual(new Uint8Array(await loaded.arrayBuffer()), bytes);

  assert.deepEqual(Object.keys(db.rows.files[0]).sort(), ["data", "id", "mime_type", "name"]);
  assert.equal(db.rows.file_parts, undefined);

  helpers.deleteFile(fileId);
  assert.deepEqual(helpers.readFiles(), []);
});

test("top-level alpha-ish file helpers default to the files table", async () => {
  const db = new MemoryDb({ files: [] });
  const bytes = Uint8Array.from([1, 2, 3, 5, 8]);

  await createFileFromBlob<StoredFile>(db, {
    rowId: secondFileId,
    mimeType: "application/octet-stream",
    blob: new Blob([bytes]),
  });

  assert.deepEqual(
    readFiles<StoredFile>(db).map((file) => file.id),
    [secondFileId],
  );
  assert.deepEqual(readFileBytes<StoredFile>(db, secondFileId), bytes);
  assert.deepEqual(
    new Uint8Array(await (await loadFileAsBlob<StoredFile>(db, secondFileId)).arrayBuffer()),
    bytes,
  );
});

test("binaryLargeValueTable documents no file_parts convention", () => {
  const files = schema.binaryLargeValueTable();

  assert.deepEqual(files.columns, [
    { name: "name", column_type: "Text" },
    { name: "mime_type", column_type: "Text" },
    { name: "data", column_type: "Bytea", large: true },
  ]);
  assert.equal(fileBlobTable(new MemoryDb({ files: [] }))._table, "files");
});

class MemoryDb implements Db {
  constructor(readonly rows: Record<string, Array<Record<string, unknown>>>) {}

  beginTransaction(): Transaction {
    throw new Error("transactions are not implemented by this test fixture");
  }

  transaction<Value>(): Value {
    throw new Error("transactions are not implemented by this test fixture");
  }

  table<Row extends { id: string | Uint8Array }, Init = Omit<Row, "id">>(
    name: string,
  ): Table<Row, Init> {
    this.rows[name] ??= [];
    return {
      _table: name,
      _schema: name === "files" ? { files: schema.binaryLargeValueTable() } : {},
      _rowType: undefined as unknown as Row,
      _initType: undefined as Init,
      where: unsupportedQueryMethod,
      select: unsupportedQueryMethod,
      orderBy: unsupportedQueryMethod,
      limit: unsupportedQueryMethod,
      offset: unsupportedQueryMethod,
      include: unsupportedQueryMethod,
      requireIncludes: unsupportedQueryMethod,
      hop: unsupportedQueryMethod,
      gather: unsupportedQueryMethod,
    } as unknown as Table<Row, Init>;
  }

  insert<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    row: Init & Partial<Pick<Row, "id">>,
    options: InsertOptions<Row> = {},
  ): WriteResult<Row> & Row {
    const next = {
      id: options.id ?? row.id ?? `row-${this.rows[table._table].length + 1}`,
      ...row,
    };
    this.rows[table._table].push(next as Record<string, unknown>);
    return makeMemoryWriteResult(next as unknown as Row);
  }

  update<Row extends { id: string | Uint8Array }>(
    table: Table<Row, unknown>,
    id: Row["id"],
    patch: Partial<Omit<Row, "id">>,
    _options: WriteTimestampOptions = {},
  ): WriteResult<Row> & Row {
    const rows = this.rows[table._table];
    const index = rows.findIndex((row) => row.id === id);
    if (index < 0) throw new Error(`missing row ${String(id)}`);
    rows[index] = { ...rows[index], ...patch };
    return makeMemoryWriteResult(rows[index] as Row);
  }

  upsert<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    row: Init & Partial<Pick<Row, "id">>,
    options: UpsertOptions<Row>,
  ): WriteResult<Row> & Row {
    const id = options.id ?? row.id;
    if (id == null) throw new Error("upsert requires id");
    const rows = this.rows[table._table];
    const index = rows.findIndex((existing) => existing.id === id);
    if (index < 0) {
      const next = { id, ...row };
      rows.push(next as Record<string, unknown>);
      return makeMemoryWriteResult(next as unknown as Row);
    }
    rows[index] = { ...rows[index], ...row, id };
    return makeMemoryWriteResult(rows[index] as Row);
  }

  delete<Row extends { id: string | Uint8Array }>(
    table: Table<Row, unknown>,
    id: Row["id"],
    _options: WriteTimestampOptions = {},
  ): WriteResult<void> {
    const rows = this.rows[table._table];
    const index = rows.findIndex((row) => row.id === id);
    if (index >= 0) rows.splice(index, 1);
    return makeMemoryWriteResult(undefined);
  }

  restore<Row extends { id: string | Uint8Array }, Init>(
    table: Table<Row, Init>,
    id: Row["id"],
    row: Init,
    _options: WriteTimestampOptions = {},
  ): WriteResult<Row> & Row {
    const rows = this.rows[table._table];
    if (rows.some((existing) => existing.id === id))
      throw new Error(`Restore failed: row not deleted: ${String(id)}`);
    const restored = { id, ...row };
    rows.push(restored as Record<string, unknown>);
    return makeMemoryWriteResult(restored as unknown as Row);
  }

  all<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | { readonly _table: string },
  ): Row[] {
    return [...this.rows[tableOrQuery._table]] as Row[];
  }

  one<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | { readonly _table: string },
  ): Row | null {
    return this.all(tableOrQuery)[0] ?? null;
  }

  allForIdentity<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | { readonly _table: string },
  ): Row[] {
    return this.all(tableOrQuery);
  }

  subscribe<Row>(
    tableOrQuery: Table<Row & { id: string | Uint8Array }, unknown> | { readonly _table: string },
    callback: (rows: Row[]) => void,
  ): Subscription<Row> {
    callback(this.all(tableOrQuery));
    return {
      unsubscribe: () => undefined,
    };
  }

  getAuthState(): AuthState {
    return { authMode: "local-first", session: null };
  }

  onAuthChanged(listener: (state: AuthState) => void): () => void {
    listener(this.getAuthState());
    return () => undefined;
  }

  updateAuthToken(): void {}
}

function unsupportedQueryMethod<Row>(): QueryBuilder<Row> {
  throw new Error("query builder methods are not used by file/blob tests");
}

function makeMemoryWriteResult<Value extends object>(value: Value): WriteResult<Value> & Value;
function makeMemoryWriteResult(value: void): WriteResult<void>;
function makeMemoryWriteResult<Value>(
  value: Value,
): WriteResult<Value> | (WriteResult<Value> & object) {
  const target = value && typeof value === "object" ? (value as object) : {};
  Object.defineProperties(target, {
    value: { value, enumerable: false },
    handle: { value: null, enumerable: false },
    wait: { value: async () => value, enumerable: false },
  });
  return target as WriteResult<Value> & object;
}

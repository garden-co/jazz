import { describe, expect, it } from "vitest";
import {
  createBinaryLargeValueFileStorage,
  FileNotFoundError,
  type FileStorageDb,
} from "./file-storage.js";
import { QueryBuilder, QueryOptions, TableProxy } from "./db.js";
import { WriteResult, JazzClient } from "./client.js";

interface StoredFile {
  id: string;
  name?: string;
  mime_type: string;
  data: Uint8Array;
}

type StoredFileInit = Omit<StoredFile, "id">;

type BuiltQuery = {
  table: string;
  conditions: Array<{ column: string; op: string; value: unknown }>;
};

type QueryableTable<Row, Init> = TableProxy<Row, Init> & {
  where(conditions: Record<string, unknown>): QueryBuilder<Row>;
};

function makeTable<Row, Init>(table: string): QueryableTable<Row, Init> {
  return {
    _table: table,
    _schema: {
      [table]: { columns: [] },
    },
    _rowType: {} as Row,
    _initType: {} as Init,
    where(conditions: Record<string, unknown>) {
      const built: BuiltQuery = {
        table,
        conditions: Object.entries(conditions)
          .filter(([, value]) => value !== undefined)
          .map(([column, value]) => ({
            column,
            op: "eq",
            value,
          })),
      };

      return {
        _table: table,
        _schema: this._schema,
        _rowType: {} as Row,
        _build() {
          return JSON.stringify(built);
        },
      };
    },
  };
}

class FakeDb implements FileStorageDb {
  private nextSyntheticId = 1;
  private nextTransactionId = 1;
  readonly inserts: Array<{
    table: string;
    data: Record<string, unknown>;
    durable: boolean;
    tier?: string;
  }> = [];
  readonly queries: BuiltQuery[] = [];
  readonly queryOptions: Array<QueryOptions | undefined> = [];
  readonly files = new Map<string, StoredFile>();
  readonly #insertsByTransactionId = new Map<string, number>();

  insert<T, Init>(table: TableProxy<T, Init>, data: Init): WriteResult<T> {
    const transactionId = `transaction-${this.nextTransactionId++}`;
    const row = this.store(table, data);
    this.#insertsByTransactionId.set(transactionId, this.inserts.length - 1);
    const client = {
      waitForTransaction: async (persistedTransactionId: string, tier: string) => {
        const insertIndex = this.#insertsByTransactionId.get(persistedTransactionId);
        if (insertIndex === undefined) {
          throw new Error(`unknown transaction ${persistedTransactionId}`);
        }
        const insert = this.inserts[insertIndex];
        if (!insert) {
          throw new Error(`missing insert for transaction ${persistedTransactionId}`);
        }
        insert.durable = true;
        insert.tier = tier;
      },
    } as unknown as JazzClient;

    return new WriteResult(row as T, transactionId, client);
  }

  async one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    const built = JSON.parse(query._build()) as BuiltQuery;
    this.queries.push(built);
    this.queryOptions.push(options);

    const id = built.conditions.find(
      (condition) => condition.column === "id" && condition.op === "eq",
    )?.value;

    if (typeof id !== "string" || built.table !== "files") {
      return null;
    }

    return (this.files.get(id) as T | undefined) ?? null;
  }

  private store<T, Init>(table: TableProxy<T, Init>, data: Init): T {
    const id = `${table._table}-${this.nextSyntheticId++}`;
    const row = {
      id,
      ...(data as Record<string, unknown>),
    };

    this.inserts.push({
      table: table._table,
      data: row,
      durable: false,
    });

    if (table._table === "files") {
      this.files.set(id, row as unknown as StoredFile);
    }

    return row as T;
  }
}

function streamFromChunks(chunks: number[][]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(Uint8Array.from(chunk));
      }
      controller.close();
    },
  });
}

async function blobToNumbers(blob: Blob): Promise<number[]> {
  return Array.from(new Uint8Array(await blob.arrayBuffer()));
}

describe("createFileStorage", () => {
  const filesTable = makeTable<StoredFile, StoredFileInit>("files");
  const app = { files: filesTable };

  it("stores stream input as one files row with binary large value data", async () => {
    const db = new FakeDb();
    const storage = createBinaryLargeValueFileStorage(db, app);

    const file = await storage.fromStream(streamFromChunks([[1, 2], [3, 4, 5], [6]]), {
      name: "demo.bin",
      mimeType: "application/test",
    });

    expect(db.inserts.map((insert) => insert.table)).toEqual(["files"]);
    expect(Array.from((db.inserts[0]?.data.data as Uint8Array) ?? [])).toEqual([1, 2, 3, 4, 5, 6]);
    expect(file.name).toBe("demo.bin");
    expect(file.mime_type).toBe("application/test");
  });

  it("supports durable blob writes", async () => {
    const db = new FakeDb();
    const storage = createBinaryLargeValueFileStorage(db, app);
    const file = await storage.fromBlob(
      new File([Uint8Array.from([9, 8, 7])], "image.png", {
        type: "image/png",
      }),
      {
        tier: "edge",
      },
    );

    expect(file.mime_type).toBe("image/png");
    expect(file.name).toBe("image.png");
    expect(db.inserts).toHaveLength(1);
    expect(db.inserts[0]?.durable).toBe(true);
    expect(db.inserts[0]?.tier).toBe("edge");
  });

  it("omits the optional file name for unnamed blobs", async () => {
    const db = new FakeDb();
    const storage = createBinaryLargeValueFileStorage(db, app);

    const file = await storage.fromBlob(new Blob([Uint8Array.from([1, 2, 3])]));

    expect(file.name).toBeUndefined();
    expect(db.inserts[0]?.data.name).toBeUndefined();
  });

  it("loads file metadata and data from the files row only", async () => {
    const db = new FakeDb();
    db.files.set("file-1", {
      id: "file-1",
      name: "demo.bin",
      mime_type: "application/test",
      data: Uint8Array.from([1, 2, 3, 4, 5]),
    });

    const storage = createBinaryLargeValueFileStorage(db, app);
    const stream = await storage.toStream("file-1", {
      tier: "edge",
      propagation: "local-only",
    });
    expect(db.queries.map((query) => query.table)).toEqual(["files"]);

    const reader = stream.getReader();
    const first = await reader.read();
    expect(first.done).toBe(false);
    expect(Array.from(first.value ?? [])).toEqual([1, 2, 3, 4, 5]);

    const done = await reader.read();
    expect(done.done).toBe(true);
    expect(db.queryOptions).toEqual([
      { tier: "edge", propagation: "local-only", visibility: undefined },
    ]);
  });

  it("reassembles a Blob from the binary large value", async () => {
    const db = new FakeDb();
    db.files.set("file-1", {
      id: "file-1",
      name: "demo.bin",
      mime_type: "application/test",
      data: Uint8Array.from([1, 2, 3, 4, 5]),
    });

    const storage = createBinaryLargeValueFileStorage(db, app);
    const blob = await storage.toBlob("file-1");

    expect(blob.type).toBe("application/test");
    expect(await blobToNumbers(blob)).toEqual([1, 2, 3, 4, 5]);
  });

  it("fails with an incomplete-file error when the file row has no data", async () => {
    const db = new FakeDb();
    db.files.set("file-1", {
      id: "file-1",
      name: "demo.bin",
      mime_type: "application/test",
      data: undefined as unknown as Uint8Array,
    });

    const storage = createBinaryLargeValueFileStorage(db, app);
    await expect(storage.toBlob("file-1")).rejects.toMatchObject({
      name: "IncompleteFileDataError",
    });
  });

  it("fails with a not-found error when the file row itself is missing", async () => {
    const db = new FakeDb();
    const storage = createBinaryLargeValueFileStorage(db, app);

    await expect(storage.toBlob("missing-file")).rejects.toBeInstanceOf(FileNotFoundError);
  });
});

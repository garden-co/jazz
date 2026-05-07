import { describe, expect, it } from "vitest";
import {
  createConventionalFileStorage,
  FileNotFoundError,
  MAX_FILE_PART_BYTES,
  type FileStorageDb,
} from "./file-storage.js";
import { QueryBuilder, QueryOptions, TableProxy } from "./db.js";
import { WriteResult, JazzClient } from "./client.js";

interface StoredFile {
  id: string;
  name?: string;
  mimeType: string;
  partIds: string[];
  partSizes: number[];
}

interface StoredFilePart {
  id: string;
  data: Uint8Array;
}

type StoredFileInit = Omit<StoredFile, "id">;
type StoredFilePartInit = Omit<StoredFilePart, "id">;

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
  private nextBatchId = 1;
  readonly inserts: Array<{
    table: string;
    data: Record<string, unknown>;
    durable: boolean;
    tier?: string;
  }> = [];
  readonly queries: BuiltQuery[] = [];
  readonly queryOptions: Array<QueryOptions | undefined> = [];
  readonly files = new Map<string, StoredFile>();
  readonly fileParts = new Map<string, StoredFilePart>();
  readonly #insertsByBatchId = new Map<string, number>();

  insert<T, Init>(table: TableProxy<T, Init>, data: Init): WriteResult<T> {
    const batchId = `batch-${this.nextBatchId++}`;
    const row = this.store(table, data, false);
    this.#insertsByBatchId.set(batchId, this.inserts.length - 1);
    const client = {
      waitForBatch: async (persistedBatchId: string, tier: string) => {
        const insertIndex = this.#insertsByBatchId.get(persistedBatchId);
        if (insertIndex === undefined) {
          throw new Error(`unknown batch ${persistedBatchId}`);
        }
        const insert = this.inserts[insertIndex];
        if (!insert) {
          throw new Error(`missing insert for batch ${persistedBatchId}`);
        }
        insert.durable = true;
        insert.tier = tier;
      },
    } as JazzClient;

    return new WriteResult(row as T, batchId, client);
  }

  async one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    const built = JSON.parse(query._build()) as BuiltQuery;
    this.queries.push(built);
    this.queryOptions.push(options);

    const id = built.conditions.find(
      (condition) => condition.column === "id" && condition.op === "eq",
    )?.value;

    if (typeof id !== "string") {
      return null;
    }

    if (built.table === "files") {
      return (this.files.get(id) as T | undefined) ?? null;
    }

    if (built.table === "file_parts") {
      return (this.fileParts.get(id) as T | undefined) ?? null;
    }

    return null;
  }

  private store<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    durable: boolean,
    tier?: string,
  ): T {
    const id = `${table._table}-${this.nextSyntheticId++}`;
    const row = {
      id,
      ...(data as Record<string, unknown>),
    };

    this.inserts.push({
      table: table._table,
      data: row,
      durable,
      tier,
    });

    if (table._table === "files") {
      this.files.set(id, row as unknown as StoredFile);
    } else if (table._table === "file_parts") {
      this.fileParts.set(id, row as unknown as StoredFilePart);
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
  const filePartsTable = makeTable<StoredFilePart, StoredFilePartInit>("file_parts");
  const app = {
    files: filesTable,
    file_parts: filePartsTable,
  };

  it("chunks stream input into file parts before creating the file row", async () => {
    const db = new FakeDb();
    const storage = createConventionalFileStorage(db, app);

    const file = await storage.fromStream(streamFromChunks([[1, 2], [3, 4, 5], [6]]), {
      name: "demo.bin",
      mimeType: "application/test",
      chunkSizeBytes: 4,
    });

    expect(db.inserts.map((insert) => insert.table)).toEqual(["file_parts", "file_parts", "files"]);
    expect(Array.from((db.inserts[0]?.data.data as Uint8Array) ?? [])).toEqual([1, 2, 3, 4]);
    expect(Array.from((db.inserts[1]?.data.data as Uint8Array) ?? [])).toEqual([5, 6]);
    expect(file.partIds).toEqual(["file_parts-1", "file_parts-2"]);
    expect(file.partSizes).toEqual([4, 2]);
    expect(file.name).toBe("demo.bin");
    expect(file.mimeType).toBe("application/test");
  });

  it("supports durable blob writes", async () => {
    const db = new FakeDb();
    const storage = createConventionalFileStorage(db, app);
    const file = await storage.fromBlob(
      new File([Uint8Array.from([9, 8, 7])], "image.png", {
        type: "image/png",
      }),
      {
        tier: "edge",
        chunkSizeBytes: 8,
      },
    );

    expect(file.mimeType).toBe("image/png");
    expect(file.name).toBe("image.png");
    expect(db.inserts.every((insert) => insert.durable)).toBe(true);
    expect(db.inserts.map((insert) => insert.tier)).toEqual(["edge", "edge"]);
  });

  it("omits the optional file name for unnamed blobs", async () => {
    const db = new FakeDb();
    const storage = createConventionalFileStorage(db, app);

    const file = await storage.fromBlob(new Blob([Uint8Array.from([1, 2, 3])]), {
      chunkSizeBytes: 8,
    });

    expect(file.name).toBeUndefined();
    expect(db.inserts[1]?.data.name).toBeUndefined();
  });

  it("loads file metadata first and then fetches file parts sequentially", async () => {
    const db = new FakeDb();
    db.files.set("file-1", {
      id: "file-1",
      name: "demo.bin",
      mimeType: "application/test",
      partIds: ["part-1", "part-2"],
      partSizes: [2, 3],
    });
    db.fileParts.set("part-1", { id: "part-1", data: Uint8Array.from([1, 2]) });
    db.fileParts.set("part-2", { id: "part-2", data: Uint8Array.from([3, 4, 5]) });

    const storage = createConventionalFileStorage(db, app);
    const stream = await storage.toStream("file-1");
    expect(db.queries.map((query) => query.table)).toEqual(["files", "file_parts"]);
    expect(db.queries[1]?.conditions[0]?.value).toBe("part-1");

    const reader = stream.getReader();
    const first = await reader.read();
    expect(first.done).toBe(false);
    expect(Array.from(first.value ?? [])).toEqual([1, 2]);
    expect(db.queries.map((query) => query.table)).toEqual(["files", "file_parts"]);

    const second = await reader.read();
    expect(second.done).toBe(false);
    expect(Array.from(second.value ?? [])).toEqual([3, 4, 5]);
    expect(db.queries.map((query) => query.table)).toEqual(["files", "file_parts", "file_parts"]);
    expect(db.queries[2]?.conditions[0]?.value).toBe("part-2");

    const done = await reader.read();
    expect(done.done).toBe(true);
  });

  it("reassembles a Blob from sequentially loaded parts", async () => {
    const db = new FakeDb();
    db.files.set("file-1", {
      id: "file-1",
      name: "demo.bin",
      mimeType: "application/test",
      partIds: ["part-1", "part-2"],
      partSizes: [2, 3],
    });
    db.fileParts.set("part-1", { id: "part-1", data: Uint8Array.from([1, 2]) });
    db.fileParts.set("part-2", { id: "part-2", data: Uint8Array.from([3, 4, 5]) });

    const storage = createConventionalFileStorage(db, app);
    const blob = await storage.toBlob("file-1", {
      tier: "edge",
      propagation: "local-only",
    });

    expect(blob.type).toBe("application/test");
    expect(await blobToNumbers(blob)).toEqual([1, 2, 3, 4, 5]);
    expect(db.queryOptions).toEqual([
      { tier: "edge", propagation: "local-only", visibility: undefined },
      { tier: "edge", propagation: "local-only", visibility: undefined },
      { tier: "edge", propagation: "local-only", visibility: undefined },
    ]);
  });

  it("fails with an incomplete-file error when a referenced part is missing", async () => {
    const db = new FakeDb();
    db.files.set("file-1", {
      id: "file-1",
      name: "demo.bin",
      mimeType: "application/test",
      partIds: ["part-1", "part-2"],
      partSizes: [2, 3],
    });
    db.fileParts.set("part-1", { id: "part-1", data: Uint8Array.from([1, 2]) });

    const storage = createConventionalFileStorage(db, app);
    await expect(
      storage.toBlob("file-1", {
        tier: "local",
        propagation: "local-only",
      }),
    ).rejects.toMatchObject({
      name: "IncompleteFileDataError",
      fileId: "file-1",
      reason: "missing-part",
      partId: "part-2",
      partIndex: 1,
    });
  });

  it("fails with a not-found error when the file row itself is missing", async () => {
    const db = new FakeDb();
    const storage = createConventionalFileStorage(db, app);

    await expect(storage.toBlob("missing-file")).rejects.toBeInstanceOf(FileNotFoundError);
  });

  it("rejects chunk sizes above the BYTEA limit", async () => {
    const storage = createConventionalFileStorage(new FakeDb(), app);
    await expect(
      storage.fromStream(streamFromChunks([[1]]), {
        chunkSizeBytes: MAX_FILE_PART_BYTES + 1,
      }),
    ).rejects.toThrow(`chunkSizeBytes must be <= ${MAX_FILE_PART_BYTES} bytes`);
  });
});

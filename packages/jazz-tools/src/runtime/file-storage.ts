import type { DurabilityTier } from "./client.js";
import type { QueryBuilder, QueryOptions, TableProxy } from "./db.js";

export const DEFAULT_FILE_CHUNK_SIZE_BYTES = 256 * 1024;
export const MAX_FILE_PART_BYTES = 1_048_576;

const DEFAULT_MIME_TYPE = "application/octet-stream";

type WhereQueryTable<Row, Init> = TableProxy<Row, Init> & {
  where(conditions: Record<string, unknown>): QueryBuilder<unknown>;
};

export class FileNotFoundError extends Error {
  readonly fileId: string;

  constructor(fileId: string) {
    super(`File "${fileId}" was not found.`);
    this.name = "FileNotFoundError";
    this.fileId = fileId;
  }
}

export type IncompleteFileDataReason =
  | "invalid-file-record"
  | "missing-part"
  | "part-size-mismatch";

export class IncompleteFileDataError extends Error {
  readonly fileId: string;
  readonly reason: IncompleteFileDataReason;
  readonly partId?: string;
  readonly partIndex?: number;

  constructor(
    fileId: string,
    reason: IncompleteFileDataReason,
    message: string,
    options: { partId?: string; partIndex?: number } = {},
  ) {
    super(message);
    this.name = "IncompleteFileDataError";
    this.fileId = fileId;
    this.reason = reason;
    this.partId = options.partId;
    this.partIndex = options.partIndex;
  }
}

export interface FileStorageDb {
  insert<T, Init>(table: TableProxy<T, Init>, data: Init): T;
  insertDurable<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    options: { tier: DurabilityTier },
  ): Promise<T>;
  one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null>;
}

export interface FileStorageColumnNames {
  name: string;
  mimeType: string;
  parts: string;
  partSizes: string;
  data: string;
}

export interface CreateFileStorageOptions<FileRow, FileInit, FilePartRow, FilePartInit> {
  files: WhereQueryTable<FileRow, FileInit>;
  fileParts: WhereQueryTable<FilePartRow, FilePartInit>;
  columns?: Partial<FileStorageColumnNames>;
  defaultChunkSizeBytes?: number;
}

export interface FileWriteOptions {
  name?: string;
  mimeType?: string;
  chunkSizeBytes?: number;
  tier?: DurabilityTier;
}

export interface FileReadOptions extends QueryOptions {}

export interface FileStorage<FileRow> {
  fromBlob(blob: Blob, options?: FileWriteOptions): Promise<FileRow>;
  fromStream(stream: ReadableStream<unknown>, options?: FileWriteOptions): Promise<FileRow>;
  toStream(
    fileOrId: string | FileRow,
    options?: FileReadOptions,
  ): Promise<ReadableStream<Uint8Array>>;
  toBlob(fileOrId: string | FileRow, options?: FileReadOptions): Promise<Blob>;
}

export interface ConventionalFileApp<FileRow, FileInit, FilePartRow, FilePartInit> {
  files: WhereQueryTable<FileRow, FileInit>;
  file_parts: WhereQueryTable<FilePartRow, FilePartInit>;
}

export type ConventionalFileRow<TApp> =
  TApp extends ConventionalFileApp<infer FileRow, any, any, any> ? FileRow : never;

type LoadedFileRecord = {
  id: string;
  name?: string;
  mimeType: string;
  parts: string[];
  partSizes: number[];
};

const DEFAULT_COLUMNS: FileStorageColumnNames = {
  name: "name",
  mimeType: "mimeType",
  parts: "parts",
  partSizes: "partSizes",
  data: "data",
};

export function createFileStorage<
  FileRow extends { id: string },
  FileInit,
  FilePartRow,
  FilePartInit,
>(
  db: FileStorageDb,
  options: CreateFileStorageOptions<FileRow, FileInit, FilePartRow, FilePartInit>,
): FileStorage<FileRow> {
  const columns: FileStorageColumnNames = {
    ...DEFAULT_COLUMNS,
    ...options.columns,
  };
  const defaultChunkSizeBytes = options.defaultChunkSizeBytes ?? DEFAULT_FILE_CHUNK_SIZE_BYTES;

  validateChunkSize(defaultChunkSizeBytes);

  const insertRow = async <T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    writeOptions?: FileWriteOptions,
  ): Promise<T> => {
    if (writeOptions?.tier) {
      return db.insertDurable(table, data, { tier: writeOptions.tier });
    }

    return db.insert(table, data);
  };

  const loadFileRecord = async (
    fileOrId: string | FileRow,
    readOptions?: FileReadOptions,
  ): Promise<LoadedFileRecord> => {
    const queryOptions = toQueryOptions(readOptions);
    if (typeof fileOrId === "string") {
      const file = await db.one(
        options.files.where({ id: fileOrId }) as QueryBuilder<Record<string, unknown>>,
        queryOptions,
      );

      if (!file) {
        throw new FileNotFoundError(fileOrId);
      }

      return normalizeFileRecord(file as Record<string, unknown>, columns);
    }

    return normalizeFileRecord(fileOrId as Record<string, unknown>, columns);
  };

  const loadPartBytes = async (
    file: LoadedFileRecord,
    partIndex: number,
    readOptions?: FileReadOptions,
  ): Promise<Uint8Array> => {
    const partId = file.parts[partIndex]!;
    const expectedSize = file.partSizes[partIndex]!;
    const queryOptions = toQueryOptions(readOptions);
    const part = await db.one(
      options.fileParts.where({ id: partId }) as QueryBuilder<Record<string, unknown>>,
      queryOptions,
    );

    if (!part) {
      throw new IncompleteFileDataError(
        file.id,
        "missing-part",
        `File "${file.id}" is incomplete: missing part ${partIndex} (${partId}) at the requested query tier.`,
        { partId, partIndex },
      );
    }

    const raw = (part as Record<string, unknown>)[columns.data];
    const bytes = asUint8Array(raw, `File part "${partId}" has invalid "${columns.data}" data.`);

    if (bytes.length !== expectedSize) {
      throw new IncompleteFileDataError(
        file.id,
        "part-size-mismatch",
        `File "${file.id}" is incomplete: part ${partIndex} (${partId}) expected ${expectedSize} bytes, got ${bytes.length}.`,
        { partId, partIndex },
      );
    }

    return bytes;
  };

  const createReadStream = (
    file: LoadedFileRecord,
    readOptions: FileReadOptions,
  ): ReadableStream<Uint8Array> => {
    let nextIndex = 0;
    let canceled = false;

    return new ReadableStream<Uint8Array>({
      async pull(controller) {
        if (canceled) {
          controller.close();
          return;
        }

        if (nextIndex >= file.parts.length) {
          controller.close();
          return;
        }

        const currentIndex = nextIndex;
        nextIndex += 1;

        try {
          const bytes = await loadPartBytes(file, currentIndex, readOptions);

          if (canceled) {
            controller.close();
            return;
          }

          controller.enqueue(bytes);

          if (nextIndex >= file.parts.length) {
            controller.close();
          }
        } catch (error) {
          controller.error(error);
        }
      },
      cancel() {
        canceled = true;
      },
    });
  };

  return {
    async fromBlob(blob: Blob, writeOptions: FileWriteOptions = {}): Promise<FileRow> {
      const name = writeOptions.name ?? getFileName(blob);
      const mimeType = writeOptions.mimeType ?? (blob.type || DEFAULT_MIME_TYPE);

      return this.fromStream(blob.stream(), {
        ...writeOptions,
        mimeType,
        ...(name !== undefined ? { name } : {}),
      });
    },

    async fromStream(
      stream: ReadableStream<unknown>,
      writeOptions: FileWriteOptions = {},
    ): Promise<FileRow> {
      const chunkSizeBytes = writeOptions.chunkSizeBytes ?? defaultChunkSizeBytes;
      validateChunkSize(chunkSizeBytes);

      const filePartIds: string[] = [];
      const partSizes: number[] = [];

      for await (const chunk of chunkReadableStream(stream, chunkSizeBytes)) {
        if (chunk.length > MAX_FILE_PART_BYTES) {
          throw new Error(
            `File chunk exceeded the ${MAX_FILE_PART_BYTES}-byte BYTEA limit: ${chunk.length} bytes.`,
          );
        }

        const part = await insertRow(
          options.fileParts,
          { [columns.data]: chunk } as FilePartInit,
          writeOptions,
        );

        if (typeof (part as { id?: unknown }).id !== "string") {
          throw new Error(`Inserted file part row is missing a string "id".`);
        }

        filePartIds.push((part as { id: string }).id);
        partSizes.push(chunk.length);
      }

      return insertRow(
        options.files,
        {
          [columns.mimeType]: writeOptions.mimeType ?? DEFAULT_MIME_TYPE,
          [columns.parts]: filePartIds,
          [columns.partSizes]: partSizes,
          ...(writeOptions.name !== undefined ? { [columns.name]: writeOptions.name } : {}),
        } as FileInit,
        writeOptions,
      );
    },

    async toStream(
      fileOrId: string | FileRow,
      readOptions: FileReadOptions = {},
    ): Promise<ReadableStream<Uint8Array>> {
      const file = await loadFileRecord(fileOrId, readOptions);
      return createReadStream(file, readOptions);
    },

    async toBlob(fileOrId: string | FileRow, readOptions: FileReadOptions = {}): Promise<Blob> {
      const file = await loadFileRecord(fileOrId, readOptions);
      const stream = createReadStream(file, readOptions);
      const reader = stream.getReader();
      const chunks: Uint8Array[] = [];

      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }
        chunks.push(value);
      }

      return new Blob(
        chunks.map((chunk) => toBlobPart(chunk)),
        { type: file.mimeType },
      );
    },
  };

  async function* chunkReadableStream(
    stream: ReadableStream<unknown>,
    chunkSizeBytes: number,
  ): AsyncGenerator<Uint8Array> {
    const reader = stream.getReader();
    const pending: Uint8Array[] = [];
    let pendingBytes = 0;

    try {
      while (true) {
        const { value, done } = await reader.read();

        if (done) {
          break;
        }

        const bytes = asUint8Array(value, "ReadableStream chunk must be binary data.");
        if (bytes.length === 0) {
          continue;
        }

        pending.push(bytes);
        pendingBytes += bytes.length;

        while (pendingBytes >= chunkSizeBytes) {
          yield takePendingBytes(pending, chunkSizeBytes);
          pendingBytes -= chunkSizeBytes;
        }
      }

      if (pendingBytes > 0) {
        yield takePendingBytes(pending, pendingBytes);
      }
    } finally {
      try {
        reader.releaseLock();
      } catch {
        // Ignore release errors for already-closed or canceled streams.
      }
    }
  }

  function takePendingBytes(pending: Uint8Array[], targetLength: number): Uint8Array {
    const out = new Uint8Array(targetLength);
    let offset = 0;

    while (offset < targetLength) {
      const current = pending[0];
      if (!current) {
        throw new Error("Chunking logic ran out of pending bytes.");
      }

      const remaining = targetLength - offset;
      const consume = Math.min(remaining, current.length);
      out.set(current.subarray(0, consume), offset);
      offset += consume;

      if (consume === current.length) {
        pending.shift();
      } else {
        pending[0] = current.subarray(consume);
      }
    }

    return out;
  }

  function normalizeFileRecord(
    file: Record<string, unknown>,
    names: FileStorageColumnNames,
  ): LoadedFileRecord {
    const id = file.id;
    if (typeof id !== "string") {
      throw new Error(`File row is missing a string "id".`);
    }

    const parts = readStringArray(
      file[names.parts],
      new IncompleteFileDataError(
        id,
        "invalid-file-record",
        `File "${id}" is incomplete: invalid "${names.parts}" metadata.`,
      ),
    );
    const partSizes = readIntegerArray(
      file[names.partSizes],
      new IncompleteFileDataError(
        id,
        "invalid-file-record",
        `File "${id}" is incomplete: invalid "${names.partSizes}" metadata.`,
      ),
    );

    if (parts.length !== partSizes.length) {
      throw new IncompleteFileDataError(
        id,
        "invalid-file-record",
        `File "${id}" is incomplete: "${names.parts}" and "${names.partSizes}" lengths do not match.`,
      );
    }

    return {
      id,
      name: typeof file[names.name] === "string" ? (file[names.name] as string) : undefined,
      mimeType:
        typeof file[names.mimeType] === "string" && (file[names.mimeType] as string).length > 0
          ? (file[names.mimeType] as string)
          : DEFAULT_MIME_TYPE,
      parts,
      partSizes,
    };
  }

  function readStringArray(value: unknown, error: Error): string[] {
    if (!Array.isArray(value) || value.some((entry) => typeof entry !== "string")) {
      throw error;
    }

    return [...value];
  }

  function readIntegerArray(value: unknown, error: Error): number[] {
    if (
      !Array.isArray(value) ||
      value.some((entry) => !Number.isInteger(entry) || (entry as number) < 0)
    ) {
      throw error;
    }

    return value.map((entry) => Number(entry));
  }

  function asUint8Array(value: unknown, message: string): Uint8Array {
    if (value instanceof Uint8Array) {
      return value;
    }

    if (value instanceof ArrayBuffer) {
      return new Uint8Array(value);
    }

    if (ArrayBuffer.isView(value)) {
      return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    }

    if (Array.isArray(value)) {
      const numbers = value.map((entry) => {
        const n = Number(entry);
        if (!Number.isInteger(n) || n < 0 || n > 255) {
          throw new Error(message);
        }
        return n;
      });
      return Uint8Array.from(numbers);
    }

    throw new Error(message);
  }

  function toBlobPart(bytes: Uint8Array): ArrayBuffer {
    const copy = new Uint8Array(bytes.byteLength);
    copy.set(bytes);
    return copy.buffer;
  }

  function getFileName(blob: Blob): string | undefined {
    if (typeof File !== "undefined" && blob instanceof File) {
      return blob.name;
    }

    return undefined;
  }
}

export function createConventionalFileStorage<
  FileRow extends { id: string },
  FileInit,
  FilePartRow,
  FilePartInit,
>(
  db: FileStorageDb,
  app: ConventionalFileApp<FileRow, FileInit, FilePartRow, FilePartInit>,
): FileStorage<FileRow> {
  return createFileStorage(db, {
    files: app.files,
    fileParts: app.file_parts,
  });
}

function validateChunkSize(chunkSizeBytes: number): void {
  if (!Number.isInteger(chunkSizeBytes) || chunkSizeBytes <= 0) {
    throw new Error("chunkSizeBytes must be a positive integer.");
  }

  if (chunkSizeBytes > MAX_FILE_PART_BYTES) {
    throw new Error(
      `chunkSizeBytes must be <= ${MAX_FILE_PART_BYTES} bytes to fit inside a BYTEA file part.`,
    );
  }
}

function toQueryOptions(readOptions?: FileReadOptions): QueryOptions | undefined {
  if (!readOptions) {
    return undefined;
  }

  const { propagation, tier, visibility } = readOptions;
  if (propagation === undefined && tier === undefined && visibility === undefined) {
    return undefined;
  }

  return { propagation, tier, visibility };
}

import type { DurabilityTier, WriteResult } from "./client.js";
import type { QueryBuilder, QueryOptions, TableProxy } from "./db.js";

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

export class IncompleteFileDataError extends Error {
  readonly fileId: string;

  constructor(fileId: string, message: string) {
    super(message);
    this.name = "IncompleteFileDataError";
    this.fileId = fileId;
  }
}

export interface FileStorageDb {
  insert<T, Init>(table: TableProxy<T, Init>, data: Init): WriteResult<T>;
  one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null>;
}

export interface FileStorageColumnNames {
  name: string;
  mimeType: string;
  data: string;
}

export interface CreateFileStorageOptions<FileRow, FileInit> {
  files: WhereQueryTable<FileRow, FileInit>;
  columns?: Partial<FileStorageColumnNames>;
}

export interface FileWriteOptions {
  name?: string;
  mimeType?: string;
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

export interface BinaryLargeValueFileApp<FileRow, FileInit> {
  files: WhereQueryTable<FileRow, FileInit>;
}

export type BinaryLargeValueFileRow<TApp> =
  TApp extends BinaryLargeValueFileApp<infer FileRow, any> ? FileRow : never;

type LoadedFileRecord = {
  id: string;
  name?: string;
  mimeType: string;
  data: Uint8Array;
};

const DEFAULT_COLUMNS: FileStorageColumnNames = {
  name: "name",
  mimeType: "mime_type",
  data: "data",
};

export function createFileStorage<FileRow extends { id: string }, FileInit>(
  db: FileStorageDb,
  options: CreateFileStorageOptions<FileRow, FileInit>,
): FileStorage<FileRow> {
  const columns: FileStorageColumnNames = {
    ...DEFAULT_COLUMNS,
    ...options.columns,
  };

  const insertRow = async (data: FileInit, writeOptions?: FileWriteOptions): Promise<FileRow> => {
    const result = db.insert(options.files, data);
    if (writeOptions?.tier) {
      return result.wait({ tier: writeOptions.tier });
    }
    return result.value;
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

  return {
    async fromBlob(blob: Blob, writeOptions: FileWriteOptions = {}): Promise<FileRow> {
      const name = writeOptions.name ?? getFileName(blob);
      const mimeType = writeOptions.mimeType ?? (blob.type || DEFAULT_MIME_TYPE);
      const bytes = new Uint8Array(await blob.arrayBuffer());

      return insertRow(
        {
          [columns.mimeType]: mimeType,
          [columns.data]: bytes,
          ...(name !== undefined ? { [columns.name]: name } : {}),
        } as FileInit,
        writeOptions,
      );
    },

    async fromStream(
      stream: ReadableStream<unknown>,
      writeOptions: FileWriteOptions = {},
    ): Promise<FileRow> {
      const bytes = await readStreamBytes(stream);

      return insertRow(
        {
          [columns.mimeType]: writeOptions.mimeType ?? DEFAULT_MIME_TYPE,
          [columns.data]: bytes,
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
      return new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(file.data);
          controller.close();
        },
      });
    },

    async toBlob(fileOrId: string | FileRow, readOptions: FileReadOptions = {}): Promise<Blob> {
      const file = await loadFileRecord(fileOrId, readOptions);
      return new Blob([toBlobPart(file.data)], { type: file.mimeType });
    },
  };
}

export function createBinaryLargeValueFileStorage<FileRow extends { id: string }, FileInit>(
  db: FileStorageDb,
  app: BinaryLargeValueFileApp<FileRow, FileInit>,
): FileStorage<FileRow> {
  return createFileStorage(db, { files: app.files });
}

async function readStreamBytes(stream: ReadableStream<unknown>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }

      const bytes = asUint8Array(value, "ReadableStream chunk must be binary data.");
      if (bytes.length === 0) {
        continue;
      }

      chunks.push(bytes);
      total += bytes.length;
    }
  } finally {
    try {
      reader.releaseLock();
    } catch {
      // Ignore release errors for already-closed or canceled streams.
    }
  }

  if (chunks.length === 1) {
    return copyBytes(chunks[0]!);
  }

  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
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

  return {
    id,
    name: typeof file[names.name] === "string" ? (file[names.name] as string) : undefined,
    mimeType:
      typeof file[names.mimeType] === "string" && (file[names.mimeType] as string).length > 0
        ? (file[names.mimeType] as string)
        : DEFAULT_MIME_TYPE,
    data: asUint8Array(
      file[names.data],
      `File "${id}" is incomplete: invalid "${names.data}" binary large value.`,
    ),
  };
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

  throw new IncompleteFileDataError("unknown", message);
}

function copyBytes(bytes: Uint8Array): Uint8Array {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy;
}

function toBlobPart(bytes: Uint8Array): ArrayBuffer {
  return copyBytes(bytes).buffer as ArrayBuffer;
}

function getFileName(blob: Blob): string | undefined {
  if (typeof File !== "undefined" && blob instanceof File) {
    return blob.name;
  }

  return undefined;
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

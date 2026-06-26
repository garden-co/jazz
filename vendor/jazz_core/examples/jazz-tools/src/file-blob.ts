import {
  createFileFromBlob as createFileFromBlobRow,
  deleteFile as deleteFileRow,
  loadFileAsBlob as loadFileRowAsBlob,
  readFileBytes as readFileRowBytes,
  readFiles as readFileRows,
  type BinaryLargeValueInput,
  type BinaryLargeValueRow,
  type Db,
  type Table,
} from "./jazz-tools.js";

export type FileBlobTable<Row extends BinaryLargeValueRow = BinaryLargeValueRow> = Table<Row, unknown>;

export type AlphaFileBlobHelpers<Row extends BinaryLargeValueRow = BinaryLargeValueRow> = {
  readonly table: FileBlobTable<Row>;
  createFileFromBlob(options: BinaryLargeValueInput): Promise<Row>;
  loadFileAsBlob(fileId: Row["id"]): Promise<Blob>;
  readFileBytes(fileId: Row["id"]): Uint8Array;
  readFiles(): Row[];
  deleteFile(fileId: Row["id"]): void;
};

export function fileBlobTable<Row extends BinaryLargeValueRow = BinaryLargeValueRow>(
  db: Db,
  tableName = "files",
): FileBlobTable<Row> {
  return db.table<Row, Omit<Row, "id">>(tableName);
}

export function createFileBlobHelpers<Row extends BinaryLargeValueRow = BinaryLargeValueRow>(
  db: Db,
  table: FileBlobTable<Row> = fileBlobTable<Row>(db),
): AlphaFileBlobHelpers<Row> {
  return {
    table,
    createFileFromBlob(options) {
      return createFileFromBlobRow(db, table, options);
    },
    loadFileAsBlob(fileId) {
      return loadFileRowAsBlob(db, table, fileId);
    },
    readFileBytes(fileId) {
      return readFileRowBytes(db, table, fileId);
    },
    readFiles() {
      return readFileRows(db, table);
    },
    deleteFile(fileId) {
      deleteFileRow(db, table, fileId);
    },
  };
}

export async function createFileFromBlob<Row extends BinaryLargeValueRow = BinaryLargeValueRow>(
  db: Db,
  options: BinaryLargeValueInput,
  table: FileBlobTable<Row> = fileBlobTable<Row>(db),
): Promise<Row> {
  return createFileFromBlobRow(db, table, options);
}

export function readFiles<Row extends BinaryLargeValueRow = BinaryLargeValueRow>(
  db: Db,
  table: FileBlobTable<Row> = fileBlobTable<Row>(db),
): Row[] {
  return readFileRows(db, table);
}

export function readFileBytes<Row extends BinaryLargeValueRow = BinaryLargeValueRow>(
  db: Db,
  fileId: Row["id"],
  table: FileBlobTable<Row> = fileBlobTable<Row>(db),
): Uint8Array {
  return readFileRowBytes(db, table, fileId);
}

export function loadFileAsBlob<Row extends BinaryLargeValueRow = BinaryLargeValueRow>(
  db: Db,
  fileId: Row["id"],
  table: FileBlobTable<Row> = fileBlobTable<Row>(db),
): Promise<Blob> {
  return loadFileRowAsBlob(db, table, fileId);
}

export function deleteFile<Row extends BinaryLargeValueRow = BinaryLargeValueRow>(
  db: Db,
  fileId: Row["id"],
  table: FileBlobTable<Row> = fileBlobTable<Row>(db),
): void {
  deleteFileRow(db, table, fileId);
}

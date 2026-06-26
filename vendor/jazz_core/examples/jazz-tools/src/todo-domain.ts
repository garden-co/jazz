import {
  type AbiRowBatch,
  type DescriptorField,
  PostcardWriter,
  decodeRecordBool,
  decodeRecordBytes,
  decodeRecordString,
  encodedCells,
  fieldIndex,
  writeValueType,
  utf8,
} from "./direct-codec.js";

export type TodoInput = {
  title: string;
  done: boolean;
  owner: Uint8Array;
};

export type TodoView = TodoInput & {
  rowId: Uint8Array;
};

export type FileInput = {
  name: string;
  mimeType: string;
  data: Uint8Array;
  size: number;
  owner: Uint8Array;
};

export type FileView = FileInput & {
  rowId: Uint8Array;
};

export function todosSchema(): Uint8Array {
  const writer = new PostcardWriter();
  writer.vec((table, index) => {
    if (index === 0)
      writeTable(
        table,
        "todos",
        todoWriteDescriptor(),
        [],
        writeTodosOwnerOnlyPolicy,
        writeTodosOwnerOnlyPolicy,
      );
    if (index === 1)
      writeTable(
        table,
        "files",
        fileWriteDescriptor(),
        [],
        writeFilesOwnerOnlyPolicy,
        writeFilesOwnerOnlyPolicy,
      );
  }, 2);
  writer.none();
  writer.none();
  return writer.finish();
}

export function encodedTodoCells(todo: TodoInput): Uint8Array {
  const descriptor = todoWriteDescriptor();
  return encodedCells(descriptor, [
    utf8(todo.title),
    new Uint8Array([todo.done ? 1 : 0]),
    todo.owner,
  ]);
}

export function encodedTodoPatch(patch: Partial<Omit<TodoInput, "owner">>): Uint8Array {
  const fields = todoWriteDescriptor().filter((field) => field.name && field.name in patch);
  const values = fields.map((field) => {
    const value = patch[field.name as keyof Omit<TodoInput, "owner">];
    if (typeof value === "boolean") return new Uint8Array([value ? 1 : 0]);
    if (typeof value === "string") return utf8(value);
    throw new Error(`missing patch value for ${field.name}`);
  });
  return encodedCells(fields, values);
}

export function encodedFileCells(file: FileInput): Uint8Array {
  return encodedCells(fileWriteDescriptor(), [
    utf8(file.name),
    utf8(file.mimeType),
    file.data,
    u64Le(file.size),
    file.owner,
  ]);
}

export function todoViews(batches: AbiRowBatch[]): TodoView[] {
  return batches.flatMap((batch) =>
    batch.rows.map((row) => ({
      rowId: row.rowId,
      title: decodeRecordString(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "title")),
      done: decodeRecordBool(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "done")),
      owner: decodeRecordBytes(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "owner")),
    })),
  );
}

export function fileViews(batches: AbiRowBatch[]): FileView[] {
  return batches.flatMap((batch) =>
    batch.rows.map((row) => ({
      rowId: row.rowId,
      name: decodeRecordString(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "name")),
      mimeType: decodeRecordString(
        batch.descriptor,
        row.raw,
        fieldIndex(batch.descriptor, "mime_type"),
      ),
      data: decodeRecordBytes(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "data")),
      size: decodeRecordU64(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "size")),
      owner: decodeRecordBytes(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "owner")),
    })),
  );
}

export function formatTodos(todos: TodoView[]): string {
  return todos.map((todo) => `${todo.title}:${todo.done ? "done" : "open"}`).join(", ") || "none";
}

export function formatRowId(rowId: Uint8Array): string {
  return Array.from(rowId, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function parseRowIdHex(hex: string): Uint8Array | undefined {
  if (hex.length !== 32 || !/^[0-9a-f]+$/i.test(hex)) return undefined;
  const rowId = new Uint8Array(16);
  for (let index = 0; index < rowId.length; index++) {
    rowId[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return rowId;
}

export function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.length === right.length && left.every((byte, index) => byte === right[index]);
}

function todoWriteDescriptor(): DescriptorField[] {
  return [
    { name: "title", valueType: { tag: 6 } },
    { name: "done", valueType: { tag: 5 } },
    { name: "owner", valueType: { tag: 8 } },
  ];
}

function fileWriteDescriptor(): DescriptorField[] {
  return [
    { name: "name", valueType: { tag: 6 } },
    { name: "mime_type", valueType: { tag: 6 } },
    { name: "data", valueType: { tag: 7 } },
    { name: "size", valueType: { tag: 3 } },
    { name: "owner", valueType: { tag: 8 } },
  ];
}

function writeTable(
  writer: PostcardWriter,
  tableName: string,
  descriptor: DescriptorField[],
  references: [string, string][],
  writeReadPolicy: ((writer: PostcardWriter) => void) | undefined,
  writeUpdatePolicy: ((writer: PostcardWriter) => void) | undefined,
): void {
  writer.string(tableName);
  writer.vec((column, index) => {
    const columnSpec = descriptor[index];
    column.string(columnSpec.name ?? "");
    writeValueType(column, columnSpec.valueType);
    if (tableName === "files" && columnSpec.name === "data") {
      column.some((largeValue) => largeValue.enumUnit(1));
    } else {
      column.none();
    }
  }, descriptor.length);
  writer.map(references.length);
  for (const [column, target] of references) {
    writer.string(column);
    writer.string(target);
  }
  if (writeReadPolicy) writer.some(writeReadPolicy);
  else writer.none();
  if (writeUpdatePolicy) writer.some(writeUpdatePolicy);
  else writer.none();
  writer.set(0);
  writer.map(0);
}

function writeTodosOwnerOnlyPolicy(writer: PostcardWriter): void {
  writeOwnerOnlyPolicy(writer, "todos");
}

function writeFilesOwnerOnlyPolicy(writer: PostcardWriter): void {
  writeOwnerOnlyPolicy(writer, "files");
}

function writeOwnerOnlyPolicy(writer: PostcardWriter, tableName: string): void {
  writer.string(tableName);
  writer.vec((filter) => {
    filter.enumUnit(3);
    filter.enumUnit(0);
    filter.string("owner");
    filter.enumUnit(2);
    filter.string("sub");
  }, 1);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.none();
  writer.vec(() => undefined, 0);
  writer.none();
  writer.none();
  writer.u64(0);
}

function u64Le(value: number): Uint8Array {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`invalid u64 value ${value}`);
  const bytes = new Uint8Array(8);
  const view = new DataView(bytes.buffer);
  view.setBigUint64(0, BigInt(value), true);
  return bytes;
}

function decodeRecordU64(
  descriptor: DescriptorField[],
  raw: Uint8Array,
  logicalIndex: number,
): number {
  const bytes = decodeRecordBytes(descriptor, raw, logicalIndex);
  if (bytes.length !== 8) throw new Error(`invalid u64 size ${bytes.length}`);
  const value = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getBigUint64(
    0,
    true,
  );
  return Number(value);
}

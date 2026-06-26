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

export type SharedTodoInput = {
  title: string;
  done: boolean;
  owner: Uint8Array;
};

export type SharedTodoView = SharedTodoInput & {
  rowId: Uint8Array;
};

export type TodoShareInput = {
  todo: Uint8Array;
  user: Uint8Array;
  role: "owner" | "reader" | "editor";
  canEdit: boolean;
};

export type TodoShareView = TodoShareInput & {
  rowId: Uint8Array;
};

export type SharedTodoShareWithTodoView = {
  share: TodoShareView;
  todo: SharedTodoView;
};

export function sharedTodosSchema(): Uint8Array {
  const writer = new PostcardWriter();
  writer.vec((table, index) => {
    if (index === 0) writeTable(table, "todos", todoDescriptor(), [], writeTodoReadPolicy, writeTodoUpdatePolicy);
    if (index === 1) writeTable(table, "todo_shares", todoShareDescriptor(), [["todo", "todos"]], undefined, undefined);
  }, 2);
  writer.none();
  writer.none();
  return writer.finish();
}

export function encodedSharedTodoCells(todo: SharedTodoInput): Uint8Array {
  return encodedCells(todoDescriptor(), [
    utf8(todo.title),
    new Uint8Array([todo.done ? 1 : 0]),
    todo.owner,
  ]);
}

export function encodedSharedTodoPatch(patch: Partial<Omit<SharedTodoInput, "owner">>): Uint8Array {
  const fields = todoDescriptor().filter((field) => field.name && field.name in patch);
  const values = fields.map((field) => {
    const value = patch[field.name as keyof Omit<SharedTodoInput, "owner">];
    if (typeof value === "boolean") return new Uint8Array([value ? 1 : 0]);
    if (typeof value === "string") return utf8(value);
    throw new Error(`missing patch value for ${field.name}`);
  });
  return encodedCells(fields, values);
}

export function encodedTodoShareCells(share: TodoShareInput): Uint8Array {
  return encodedCells(todoShareDescriptor(), [
    share.todo,
    share.user,
    utf8(share.role),
    new Uint8Array([share.canEdit ? 1 : 0]),
  ]);
}

export function encodedTodoSharePatch(patch: Partial<Pick<TodoShareInput, "role" | "canEdit">>): Uint8Array {
  const entries = todoShareDescriptor()
    .map((field) => ({ field, key: sharePatchKey(field.name) }))
    .filter((entry): entry is { field: DescriptorField; key: keyof Pick<TodoShareInput, "role" | "canEdit"> } =>
      entry.key !== undefined && entry.key in patch
    );
  const values = entries.map(({ field, key }) => {
    const value = patch[key];
    if (typeof value === "boolean") return new Uint8Array([value ? 1 : 0]);
    if (typeof value === "string") return utf8(value);
    throw new Error(`missing share patch value for ${field.name}`);
  });
  return encodedCells(entries.map((entry) => entry.field), values);
}

export function sharedTodoViews(batches: AbiRowBatch[]): SharedTodoView[] {
  return batches.flatMap((batch) => batch.rows.map((row) => ({
    rowId: row.rowId,
    title: decodeRecordString(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "title")),
    done: decodeRecordBool(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "done")),
    owner: decodeRecordBytes(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "owner")),
  })));
}

export function todoShareViews(batches: AbiRowBatch[]): TodoShareView[] {
  return batches.flatMap((batch) => batch.rows.map((row) => ({
    rowId: row.rowId,
    todo: decodeRecordBytes(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "todo")),
    user: decodeRecordBytes(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "user")),
    role: decodeRecordString(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "role")) as TodoShareView["role"],
    canEdit: decodeRecordBool(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "can_edit")),
  })));
}

export function formatSharedTodos(todos: SharedTodoView[]): string {
  return todos.map((todo) => `${todo.title}:${todo.done ? "done" : "open"}`).join(", ") || "none";
}

function todoDescriptor(): DescriptorField[] {
  return [
    { name: "title", valueType: { tag: 6 } },
    { name: "done", valueType: { tag: 5 } },
    { name: "owner", valueType: { tag: 8 } },
  ];
}

function todoShareDescriptor(): DescriptorField[] {
  return [
    { name: "todo", valueType: { tag: 8 } },
    { name: "user", valueType: { tag: 8 } },
    { name: "role", valueType: { tag: 6 } },
    { name: "can_edit", valueType: { tag: 5 } },
  ];
}

function sharePatchKey(fieldName: string | undefined): keyof Pick<TodoShareInput, "role" | "canEdit"> | undefined {
  if (fieldName === "role") return "role";
  if (fieldName === "can_edit") return "canEdit";
  return undefined;
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
    column.none();
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

function writeTodoReadPolicy(writer: PostcardWriter): void {
  writeTodoSharePolicy(writer, false);
}

function writeTodoUpdatePolicy(writer: PostcardWriter): void {
  writeTodoSharePolicy(writer, true);
}

function writeTodoSharePolicy(writer: PostcardWriter, requireCanEdit: boolean): void {
  writer.string("todos");
  writer.vec(() => undefined, 0);
  writer.vec((join) => writeTodoShareJoin(join, requireCanEdit), 1);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.none();
  writer.vec(() => undefined, 0);
  writer.none();
  writer.none();
  writer.u64(0);
}

function writeTodoShareJoin(writer: PostcardWriter, requireCanEdit: boolean): void {
  writer.string("todo_shares");
  writer.string("todo");
  writer.none();
  writer.vec((filter, index) => {
    if (index === 0) writeShareUserClaimFilter(filter);
    if (index === 1) writeShareCanEditFilter(filter);
  }, requireCanEdit ? 2 : 1);
}

function writeShareUserClaimFilter(writer: PostcardWriter): void {
  writeClaimFilter(writer, "user");
}

function writeClaimFilter(writer: PostcardWriter, column: string): void {
  writer.enumUnit(3);
  writer.enumUnit(0);
  writer.string(column);
  writer.enumUnit(2);
  writer.string("sub");
}

function writeShareCanEditFilter(writer: PostcardWriter): void {
  writer.u64(3);
  writer.u64(0);
  writer.string("can_edit");
  writer.u64(3);
  writer.u64(5);
  writer.bool(true);
}

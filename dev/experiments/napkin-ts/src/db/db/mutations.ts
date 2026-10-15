// Row insertion, update, deletion, restoration, large-value edits and authorization.
import * as X from "../../_extern/db-core.js";
import { concatBytes, fromUtf8, utf8 } from "../../support/bytes.js";
import type { AuthorSubject, CurrentRow, SyncMessage, TxId, Value } from "../../_extern/db-core.js";
import type {
  Db,
  DeleteOptions,
  ExactWriteTarget,
  InsertOptions,
  ResolvedWriteIdentity,
  RestoreOptions,
  RowCells,
  UpdateOptions,
  UpsertOptions,
  WriteIdentity,
  WriteTarget,
} from "../db.js";
import {
  WriteHandle,
  dbError,
  exactTargetBranch,
  pendingOpenSchemaError,
  readForWriteDenied,
  rowAlreadyDeleted,
  rowAlreadyExists,
  updateTargetNotLoaded,
  type DbError,
  type QueuedMutationStatus,
} from "../db.js";

export type LargeValueUpdatePage =
  | { kind: "bytes"; from: number; to: number }
  | { kind: "text_utf16"; from: number; to: number }
  | { kind: "text_utf8"; from: number; to: number };

export interface LargeValueUpdateSplice {
  at: number;
  delete: number;
  insert: Uint8Array;
}

export interface JsonSetEdit {
  at: string;
  value: unknown;
}

export type LargeValueUpdate =
  | {
      kind: "splice";
      column: string;
      within: LargeValueUpdatePage;
      splices: LargeValueUpdateSplice[];
    }
  | { kind: "json_set"; column: string; edits: JsonSetEdit[] };

export type StreamingMutationKind = "Insert" | "Update" | "Upsert";

const MAX_PHYSICAL_TIMESTAMP_MS = X.HLC_MAX_PHYSICAL_MS;

export function validateUpdatedAtMs(updatedAtMs: number | undefined): void {
  if (updatedAtMs !== undefined && updatedAtMs > MAX_PHYSICAL_TIMESTAMP_MS)
    throw dbError(
      "WriteRejected",
      `updated_at_ms ${updatedAtMs} exceeds the packed-HLC physical millisecond range`,
    );
}

export class StreamingValueUpload {
  constructor(
    readonly id: X.StagedLargeValueId,
    readonly kind: X.LargeValueKind,
    public initialized: boolean,
    public preparation: X.PushStreamingPreparation | undefined,
    readonly emitted: X.StagedChunk[],
  ) {}
  cleanupId(): X.StagedLargeValueId {
    this.preparation = undefined;
    return this.id;
  }
}

const largeValueCellTypeError = (table: string, column: string) =>
  dbError("Schema", `${table}.${column} is not a bytes or string cell`);

function checkedUsize(value: number, what: string): number {
  if (!Number.isSafeInteger(value)) throw dbError("Query", `${what} exceeds addressable memory`);
  return value;
}

function ownedByteRange(bytes: Uint8Array, start: number, end: number): Uint8Array {
  start = checkedUsize(start, "range start");
  end = checkedUsize(end, "range end");
  if (start > end || end > bytes.length) throw dbError("Query", "value range is out of bounds");
  return bytes.slice(start, end);
}

const isLowSurrogate = (c: number) => c >= 0xdc00 && c <= 0xdfff;

function ownedUtf16Range(text: string, start: number, end: number): string {
  if (start > end || end > text.length) throw dbError("Query", "UTF-16 range is out of bounds");
  if (start < text.length && isLowSurrogate(text.charCodeAt(start)))
    throw dbError("Query", "UTF-16 range starts inside a surrogate pair");
  if (end < text.length && isLowSurrogate(text.charCodeAt(end)))
    throw dbError("Query", "UTF-16 range ends inside a surrogate pair");
  return text.slice(start, end);
}

type CellEdit =
  | { kind: "Append"; suffix: Uint8Array }
  | { kind: "Splice"; offset: number; deleteLength: number; insert: Uint8Array };

function applyCellEditInline(edit: CellEdit, bytes: Uint8Array): Uint8Array {
  return edit.kind === "Append"
    ? concatBytes(bytes, edit.suffix)
    : spliceOwnedBytes(bytes, edit.offset, edit.deleteLength, edit.insert);
}

type LargeValueEdit = [Value, X.StagedLargeValue | undefined, X.StagedLargeValueId[]];

function utf16SpliceByte(f: () => number): number {
  try {
    return f();
  } catch (error: any) {
    if (isUtf16CoordinateLoweringError(error))
      throw dbError("Query", "UTF-16 splice splits a surrogate pair");
    throw X.toApiError(error);
  }
}

function spliceOwnedBytes(
  bytes: Uint8Array,
  offset: number,
  deleteLength: number,
  insert: Uint8Array,
): Uint8Array {
  const start = checkedUsize(offset, "splice offset");
  const endRaw = offset + deleteLength;
  if (!Number.isSafeInteger(endRaw)) throw dbError("Query", "splice range overflows");
  const end = checkedUsize(endRaw, "splice end");
  if (start > end || end > bytes.length) throw dbError("Query", "splice range is out of bounds");
  return concatBytes(bytes.subarray(0, start), insert, bytes.subarray(end));
}

function checkedRange(from: number, to: number, length: number, coordinate: string): void {
  if (from > to || to > length) throw dbError("Query", `${coordinate} page is out of bounds`);
}

export function applyJsonSet(holder: { value: any }, pointer: string, replacement: unknown): void {
  if (pointer === "") {
    holder.value = replacement;
    return;
  }
  if (!pointer.startsWith("/"))
    throw dbError("Query", "JSON pointer must be empty or begin with '/'");
  const tokens = pointer
    .slice(1)
    .split("/")
    .map((token) => {
      if (/~[^01]|~$/.test(token)) throw dbError("Query", "JSON pointer has an invalid escape");
      return token.replace(/~1/g, "/").replace(/~0/g, "~");
    });
  const last = tokens.pop()!;
  const step = (current: any, token: string): [any, string | number] => {
    if (Array.isArray(current)) {
      const index = jsonPointerArrayIndex(token);
      if (index >= current.length) throw dbError("Query", "JSON set path does not exist");
      return [current, index];
    }
    if (current !== null && typeof current === "object") {
      if (!Object.prototype.hasOwnProperty.call(current, token))
        throw dbError("Query", "JSON set path does not exist");
      return [current, token];
    }
    throw dbError("Query", "JSON set path has a scalar parent");
  };
  let current = holder.value;
  for (const token of tokens) {
    const [c, k] = step(current, token);
    current = c[k];
  }
  const [target, key] = step(current, last);
  target[key] = replacement;
}

function jsonPointerArrayIndex(token: string): number {
  if (token === "0") return 0;
  if (!/^[1-9][0-9]*$/.test(token))
    throw dbError("Query", "JSON array pointer token is not an index");
  const n = Number(token);
  if (!Number.isSafeInteger(n)) throw dbError("Query", "JSON array pointer token is not an index");
  return n;
}

function decodeText(bytes: Uint8Array, message: string, code = "Schema"): string {
  try {
    return fromUtf8(bytes);
  } catch {
    throw dbError(code, message);
  }
}

function applyInlineSplices(
  value: Value,
  expectedKind: X.LargeValueKind,
  page: LargeValueUpdatePage,
  splices: LargeValueUpdateSplice[],
): Value {
  const [inner, nullable] = unwrapPresentNullable(value);
  let bytes: Uint8Array;
  let isText: boolean;
  if (inner.kind === "Bytes") {
    bytes = inner.value;
    isText = false;
  } else if (inner.kind === "String") {
    bytes = utf8(inner.value);
    isText = true;
  } else throw dbError("Schema", "partial splice requires a bytes or string cell");
  if ((isText ? "String" : "Bytes") !== expectedKind)
    throw dbError("Schema", "stored scalar kind does not match its column schema");
  let pageStart: number;
  let pageLength: number;
  if (page.kind === "bytes" && !isText) {
    checkedRange(page.from, page.to, bytes.length, "byte");
  } else if (page.kind === "text_utf8" && isText) {
    checkedRange(page.from, page.to, bytes.length, "UTF-8");
    decodeText(bytes, "stored text is not valid UTF-8");
    const boundary = (i: number) => i === bytes.length || (bytes[i] & 0xc0) !== 0x80;
    if (
      !boundary(checkedUsize(page.from, "UTF-8 page start")) ||
      !boundary(checkedUsize(page.to, "UTF-8 page end"))
    )
      throw dbError("Query", "UTF-8 page splits a code point");
  } else if (page.kind === "text_utf16" && isText) {
    const text = decodeText(bytes, "stored text is not valid UTF-8");
    checkedRange(page.from, page.to, text.length, "UTF-16");
    ownedUtf16Range(text, 0, page.from);
    ownedUtf16Range(text, 0, page.to);
  } else if (page.kind === "bytes") throw largeValueCellTypeError("row", "column");
  else throw dbError("Schema", "text splice requires a string cell");
  pageStart = page.from;
  pageLength = page.to - page.from;
  for (const splice of splices) {
    const end = splice.at + splice.delete;
    if (!Number.isSafeInteger(end)) throw dbError("Query", "splice range overflows");
    if (splice.at > pageLength || end > pageLength)
      throw dbError("Query", "splice deletion leaves the selected page");
    let absolute: number;
    let deleteBytes: number;
    let insertedUnits: number;
    if (page.kind !== "text_utf16") {
      absolute = pageStart + splice.at;
      if (!Number.isSafeInteger(absolute)) throw dbError("Query", "splice offset overflows");
      deleteBytes = splice.delete;
      insertedUnits = splice.insert.length;
    } else {
      const text = decodeText(bytes, "stored text is not valid UTF-8");
      const startUtf16 = pageStart + splice.at;
      if (!Number.isSafeInteger(startUtf16))
        throw dbError("Query", "UTF-16 splice offset overflows");
      const endUtf16 = startUtf16 + splice.delete;
      if (!Number.isSafeInteger(endUtf16)) throw dbError("Query", "UTF-16 splice range overflows");
      absolute = utf8(ownedUtf16Range(text, 0, startUtf16)).length;
      const e = utf8(ownedUtf16Range(text, 0, endUtf16)).length;
      deleteBytes = e - absolute;
      insertedUnits = decodeText(
        splice.insert,
        "text splice insert is not valid UTF-8",
        "Query",
      ).length;
    }
    bytes = spliceOwnedBytes(bytes, absolute, deleteBytes, splice.insert);
    if (isText) decodeText(bytes, "text splice is not valid UTF-8", "Query");
    pageLength = pageLength - splice.delete + insertedUnits;
    if (pageLength < 0) throw dbError("Query", "splice page length overflows");
  }
  const result: Value = isText
    ? { kind: "String", value: decodeText(bytes, "text splice is not valid UTF-8", "Query") }
    : { kind: "Bytes", value: bytes };
  return preserveNullable(result, nullable);
}

function isUtf16CoordinateLoweringError(error: unknown): boolean {
  const message = String(error);
  return (
    message.includes("MalformedScalar") ||
    message.includes("InvalidUtf8") ||
    message.includes("malformed physical scalar encoding")
  );
}

function unwrapPresentNullable(value: Value): [Value, boolean] {
  return value.kind === "Nullable" && value.value !== undefined
    ? [value.value, true]
    : [value, false];
}

const preserveNullable = (value: Value, nullable: boolean): Value =>
  nullable ? { kind: "Nullable", value } : value;

const largeValueUpdateColumn = (u: LargeValueUpdate) => u.column;

async function lockNode<T>(db: Db, f: (node: X.NodeState) => Promise<T> | T): Promise<T> {
  const mutex = db.node.node;
  const node = await mutex.lock();
  try {
    return await f(node);
  } finally {
    mutex.unlock();
  }
}

async function withStaging<T>(db: Db, f: (node: X.NodeState) => Promise<T>): Promise<T> {
  const node = await db.node.lockForLargeValueStaging();
  try {
    return await f(node);
  } finally {
    db.node.node.unlock();
  }
}

async function evictPendingQuietly(db: Db, id: X.StagedLargeValueId): Promise<void> {
  try {
    await withStaging(db, (n) => n.evictPendingLargeValueUpload(id));
  } catch {}
}

async function evictStagedQuietly(db: Db, id: X.StagedLargeValueId): Promise<void> {
  try {
    await lockNode(db, (n) => n.evictStagedLargeValue(id));
  } catch {}
}

function extend(target: RowCells, extra: RowCells): RowCells {
  for (const [k, v] of extra) target.set(k, v);
  return target;
}

const defaultIdentity: WriteIdentity = { kind: "Database" };

function borrowNode(db: Db): X.NodeState {
  const n = db.node.node.tryLock();
  if (!n) throw new Error("synchronous node operation reentered a suspended operation");
  return n;
}

export const mutationMethods = {
  queuedWriteHandle(
    this: Db,
    row: X.RowUuid,
    txId: TxId,
    status: { v: QueuedMutationStatus },
    alias?: { v: TxId | undefined },
  ): WriteHandle {
    const node = this.node.node;
    return new WriteHandle({ deref: () => node }, row, txId, "None", status, alias);
  },

  enqueueInsert(this: Db, table: string, cells: RowCells, options: InsertOptions): WriteHandle {
    validateUpdatedAtMs(options.updatedAtMs);
    const row = options.rowId ?? this.rowIdSource.nextRowId();
    const nowMs = this.queuedNowMs(options);
    options.rowId = row;
    return this.enqueueWrite(row, nowMs, (db: Db) => db.insert(table, cells, options));
  },

  enqueueUpdate(
    this: Db,
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    options: UpdateOptions,
  ): WriteHandle {
    const nowMs = this.queuedNowMs(options);
    const txId = this.reserveTransactionIdAtMs(nowMs);
    const db = this.cloneForReservedTransaction(txId);
    const emptyPatch = patch.size === 0;
    const alias: { v: TxId | undefined } = { v: undefined };
    const status = this.node.enqueueMutation(txId, async () => {
      const write: WriteHandle = await db.update(table, row, patch, options);
      const actual = write.mergeableTxId();
      if (emptyPatch && X.cmpTxId(actual, txId) !== 0) alias.v = actual;
    });
    return this.queuedWriteHandle(row, txId, status, alias);
  },

  enqueueUpsert(
    this: Db,
    table: string,
    row: X.RowUuid,
    cells: RowCells,
    options: UpsertOptions,
  ): WriteHandle {
    const nowMs = this.queuedNowMs(options);
    return this.enqueueWrite(row, nowMs, (db: Db) =>
      db.upsertWithInsertValidation(table, row, cells, options, true),
    );
  },

  enqueueDelete(this: Db, table: string, row: X.RowUuid, options: DeleteOptions): WriteHandle {
    const nowMs = this.queuedNowMs(options);
    return this.enqueueWrite(row, nowMs, (db: Db) => db.delete(table, row, options));
  },

  enqueueRestore(
    this: Db,
    table: string,
    row: X.RowUuid,
    cells: RowCells | undefined,
    options: RestoreOptions,
  ): WriteHandle {
    const nowMs = this.queuedNowMs(options);
    return this.enqueueWrite(row, nowMs, (db: Db) => db.restore(table, row, cells, options));
  },

  enqueueLargeValueUpdate(
    this: Db,
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    mutations: LargeValueUpdate[],
    updatedAtMs: number | undefined,
  ): WriteHandle {
    const holder = { updatedAtMs };
    const nowMs = this.queuedNowMs(holder);
    return this.enqueueWrite(row, nowMs, (db: Db) =>
      db.updateWithLargeValueMutationsAtMs(table, row, patch, mutations, nowMs),
    );
  },

  queuedNowMs(this: Db, holder: { updatedAtMs?: number }): number {
    validateUpdatedAtMs(holder.updatedAtMs);
    const nowMs = holder.updatedAtMs ?? this.nextNowMsValue();
    holder.updatedAtMs = nowMs;
    return nowMs;
  },

  enqueueWrite(
    this: Db,
    row: X.RowUuid,
    nowMs: number,
    write: (db: Db) => Promise<WriteHandle>,
  ): WriteHandle {
    const txId = this.reserveTransactionIdAtMs(nowMs);
    const db = this.cloneForReservedTransaction(txId);
    const status = this.node.enqueueMutation(txId, async () => {
      await write(db);
    });
    return this.queuedWriteHandle(row, txId, status, undefined);
  },

  async readValueRange(
    this: Db,
    table: string,
    row: X.RowUuid,
    column: string,
    from: number,
    to: number,
  ): Promise<Uint8Array> {
    const [value] = unwrapPresentNullable(await this.authorizedPhysicalCell(table, row, column));
    switch (value.kind) {
      case "Bytes":
        return ownedByteRange(value.value, from, to);
      case "String":
        return ownedByteRange(utf8(value.value), from, to);
      case "Large":
        return lockNode(this, (n) => n.readLargeValueRange(value.value, from, to));
      default:
        throw largeValueCellTypeError(table, column);
    }
  },

  async readTextUtf16Range(
    this: Db,
    table: string,
    row: X.RowUuid,
    column: string,
    from: number,
    to: number,
  ): Promise<string> {
    const [value] = unwrapPresentNullable(await this.authorizedPhysicalCell(table, row, column));
    switch (value.kind) {
      case "String":
        return ownedUtf16Range(value.value, from, to);
      case "Large":
        return lockNode(this, (n) => n.readLargeTextUtf16Range(value.value, from, to));
      default:
        throw largeValueCellTypeError(table, column);
    }
  },

  async readJsonPointer(
    this: Db,
    table: string,
    row: X.RowUuid,
    column: string,
    pointer: string,
  ): Promise<unknown> {
    const [value] = unwrapPresentNullable(await this.authorizedPhysicalCell(table, row, column));
    const [kind] = this.largeValueColumnKind(table, column);
    if (kind !== "Json") throw dbError("Schema", "JSON pointer selection requires a JSON column");
    if (value.kind === "String") {
      let parsed: unknown;
      try {
        parsed = JSON.parse(value.value);
      } catch (error) {
        throw dbError("Query", `invalid stored JSON: ${error}`);
      }
      return X.jsonPointer(parsed, pointer);
    }
    if (value.kind === "Large" && value.value.kind === "Json")
      return lockNode(this, (n) => n.readLargeJsonPointer(value.value, pointer));
    throw largeValueCellTypeError(table, column);
  },

  appendValue(
    this: Db,
    table: string,
    row: X.RowUuid,
    column: string,
    bytes: Uint8Array,
  ): Promise<WriteHandle> {
    return this.editValueCell(table, row, column, { kind: "Append", suffix: bytes });
  },

  spliceValue(
    this: Db,
    table: string,
    row: X.RowUuid,
    column: string,
    offset: number,
    deleteLength: number,
    insert: Uint8Array,
  ): Promise<WriteHandle> {
    return this.editValueCell(table, row, column, { kind: "Splice", offset, deleteLength, insert });
  },

  async editValueCell(
    this: Db,
    table: string,
    row: X.RowUuid,
    column: string,
    edit: CellEdit,
  ): Promise<WriteHandle> {
    this.ensureMutationOperationAdmitted();
    const [value, nullable] = unwrapPresentNullable(
      await this.authorizedPhysicalCell(table, row, column),
    );
    let next: Value;
    if (value.kind === "Bytes")
      next = { kind: "Bytes", value: applyCellEditInline(edit, value.value) };
    else if (value.kind === "String") {
      const invalid =
        edit.kind === "Append"
          ? "string append is not valid UTF-8"
          : "string splice is not valid UTF-8";
      next = {
        kind: "String",
        value: decodeText(applyCellEditInline(edit, utf8(value.value)), invalid),
      };
    } else if (value.kind === "Large") {
      const node = await this.node.lockForLargeValueStaging();
      let staged: X.StagedLargeValue;
      try {
        staged =
          edit.kind === "Append"
            ? await node.appendAndStageLargeValue(value.value, edit.suffix)
            : await node.editAndStageLargeValue(
                value.value,
                edit.offset,
                edit.deleteLength,
                edit.insert,
              );
      } finally {
        this.node.node.unlock();
      }
      return this.writeStagedLargeValueUpdate(table, row, column, staged, nullable);
    } else throw largeValueCellTypeError(table, column);
    return this.update(table, row, new Map([[column, preserveNullable(next, nullable)]]), {});
  },

  updateWithLargeValueMutations(
    this: Db,
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    mutations: LargeValueUpdate[],
  ): Promise<WriteHandle> {
    this.ensureMutationOperationAdmitted();
    return this.updateWithLargeValueMutationsAtMsOption(table, row, patch, mutations, undefined);
  },

  updateWithLargeValueMutationsAtMs(
    this: Db,
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    mutations: LargeValueUpdate[],
    nowMs: number,
  ): Promise<WriteHandle> {
    this.ensureMutationOperationAdmitted();
    return this.updateWithLargeValueMutationsAtMsOption(table, row, patch, mutations, nowMs);
  },

  async updateWithLargeValueMutationsAtMsOption(
    this: Db,
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    mutations: LargeValueUpdate[],
    nowMs: number | undefined,
  ): Promise<WriteHandle> {
    if (!mutations.length) return this.update(table, row, patch, { updatedAtMs: nowMs });
    const [cells, , authoredColumns] = await this.mergeExistingCells(table, row, patch);
    const staged: [string, X.StagedLargeValue][] = [];
    let cleanup: X.StagedLargeValueId[] = [];
    let published: any;
    let failure: unknown;
    try {
      const touched = new Set<string>();
      for (const mutation of mutations) {
        const [column, [next, columnStaged, obsolete]] = await this.applyLargeValueUpdate(
          table,
          mutation,
          cells,
          touched,
          (c: string) => authoredColumns.has(c),
        );
        cleanup.push(...obsolete);
        if (columnStaged) {
          cleanup.push(columnStaged.id);
          staged.push([column, columnStaged]);
        }
        cells.set(column, next);
        authoredColumns.add(column);
      }
      const commit = X.MergeableCommit.new(table, row, nowMs ?? this.nextNowMsValue())
        .madeBy(this.identity.author)
        .cells(cells)
        .authoredColumns(authoredColumns);
      const node = await this.node.lockForLargeValueStaging();
      try {
        const sealed = staged.length
          ? await node.sealLargeValueUpdates(commit, staged, this.schemaVersionId)
          : await node.sealInheritedLargeValues(commit, this.schemaVersionId, true);
        published = await this.commitMergeableBatch(node, [sealed]);
      } finally {
        this.node.node.unlock();
      }
    } catch (error) {
      failure = error;
    }
    if (failure === undefined) {
      const consumed = new Set(staged.map(([, v]) => X.stagedIdKey(v.id)));
      cleanup = cleanup.filter((id) => !consumed.has(X.stagedIdKey(id)));
    }
    for (const id of cleanup) {
      try {
        await lockNode(this, (n) => n.evictStagedLargeValue(id));
      } catch {}
    }
    if (failure !== undefined) throw failure;
    return this.finishPublishedWrite(row, published);
  },

  async stageTransactionLargeValueUpdate(
    this: Db,
    txId: X.OpenTransactionId,
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    mutations: LargeValueUpdate[],
    updatedAtMs: number | undefined,
  ): Promise<void> {
    const nowMs = updatedAtMs ?? this.nextNowMsValue();
    const exclusive = await this.transactionIsExclusive(txId);
    const target: RowCells = await this.transactionTargetForWrite(
      txId,
      table,
      row,
      "UPDATE",
      false,
    );
    if (!target) throw new Error("a required transaction write target is present");
    const authored = patch;
    const staged = new Map<string, X.StagedLargeValue>();
    try {
      const touched = new Set<string>();
      for (const mutation of mutations) {
        const [column, [nextValue, columnStagedRaw, obsolete]] = await this.applyLargeValueUpdate(
          table,
          mutation,
          target,
          touched,
          (c: string) => authored.has(c),
        );
        let next = nextValue;
        await this.evictStagedLargeValues(obsolete);
        let columnStaged = columnStagedRaw;
        if (!columnStaged) {
          const semanticKind =
            this.tableSchema(table).columns.find((c: any) => c.name === column)?.largeValueKind ??
            "NotLarge";
          const node = await this.node.lockForLargeValueStaging();
          try {
            const holder = { value: next };
            columnStaged = await node.prepareAndStageLargeScalar(holder, semanticKind);
            next = holder.value;
          } finally {
            this.node.node.unlock();
          }
        }
        if (columnStaged) staged.set(column, columnStaged);
        else if (target.has(column) && X.deepEq(target.get(column), next)) continue;
        authored.set(column, next);
      }
      if (!authored.size) return;
      const node = await this.lockForTransactionOperation(txId);
      try {
        if (exclusive) {
          const cells = new Map([...target, ...authored]);
          await node.txWriteInSchemaWithStagedLargeValues(
            txId,
            this.schemaVersionId,
            table,
            row,
            cells,
            undefined,
            nowMs,
            new Map(staged),
          );
        } else {
          await node.txPatchMergeableWithStagedLargeValues(
            txId,
            this.schemaVersionId,
            table,
            row,
            authored,
            nowMs,
            X.BranchSelector.default(),
            false,
            new Map(staged),
          );
        }
      } finally {
        this.node.node.unlock();
      }
    } catch (error) {
      await this.evictStagedLargeValues([...staged.values()].map((v) => v.id));
      throw error;
    }
  },

  async evictStagedLargeValues(this: Db, ids: X.StagedLargeValueId[]): Promise<void> {
    let node: X.NodeState;
    try {
      node = await this.node.lockForLargeValueStaging();
    } catch {
      return;
    }
    try {
      for (const id of ids) {
        try {
          await node.evictStagedLargeValue(id);
        } catch {}
      }
    } finally {
      this.node.node.unlock();
    }
  },

  async applyLargeValueUpdate(
    this: Db,
    table: string,
    mutation: LargeValueUpdate,
    current: RowCells,
    touched: Set<string>,
    isPatched: (c: string) => boolean,
  ): Promise<[string, LargeValueEdit]> {
    const column = largeValueUpdateColumn(mutation);
    if (touched.has(column))
      throw dbError("Query", "a large-value field may have one descriptor per update");
    touched.add(column);
    if (isPatched(column))
      throw dbError("Query", "a large-value field cannot be both patched and partially updated");
    const value = current.get(column);
    if (value === undefined) throw dbError("Schema", `unknown column ${table}.${column}`);
    const [kind] = this.largeValueColumnKind(table, column);
    const edit =
      mutation.kind === "splice"
        ? await this.applyLargeValueSplices(value, kind, mutation.within, mutation.splices)
        : await this.applyLargeValueJsonSet(value, kind, mutation.edits);
    return [column, edit];
  },

  async applyLargeValueSplices(
    this: Db,
    raw: Value,
    expectedKind: X.LargeValueKind,
    page: LargeValueUpdatePage,
    splices: LargeValueUpdateSplice[],
  ): Promise<LargeValueEdit> {
    const [value, nullable] = unwrapPresentNullable(raw);
    if (expectedKind !== "Bytes" && expectedKind !== "String")
      throw dbError("Schema", "partial splice requires a bytes or string column");
    if (value.kind !== "Large")
      return [
        applyInlineSplices(preserveNullable(value, nullable), expectedKind, page, splices),
        undefined,
        [],
      ];
    let current: X.LargeValueRef = value.value;
    if (current.kind !== expectedKind)
      throw dbError("Schema", "stored large-value kind does not match its column schema");
    const text = expectedKind === "String";
    if (page.kind === "bytes" && !text)
      checkedRange(page.from, page.to, current.byteLength, "byte");
    else if (page.kind === "text_utf8" && text) {
      checkedRange(page.from, page.to, current.byteLength, "UTF-8");
      for (const boundary of [page.from, page.to]) {
        if (boundary >= current.byteLength) continue;
        const byte = await lockNode(this, (n) =>
          n.readLargeValueRange(current, boundary, boundary + 1),
        );
        if (byte.length && (byte[0] & 0xc0) === 0x80)
          throw dbError("Query", "UTF-8 page splits a code point");
      }
    } else if (page.kind === "text_utf16" && text) {
      if (current.utf16Length === undefined)
        throw dbError("Schema", "text descriptor lacks UTF-16 metrics");
      checkedRange(page.from, page.to, current.utf16Length, "UTF-16");
    } else if (page.kind === "bytes") throw dbError("Schema", "byte splice requires a bytes cell");
    else throw dbError("Schema", "text splice requires a string cell");
    const pageStart = page.from;
    let pageLength = page.to - page.from;
    const priorClaims: X.StagedLargeValueId[] = [];
    let finalStaged: X.StagedLargeValue | undefined;
    for (const splice of splices) {
      const end = splice.at + splice.delete;
      if (!Number.isSafeInteger(end)) throw dbError("Query", "splice range overflows");
      if (splice.at > pageLength || end > pageLength)
        throw dbError("Query", "splice deletion leaves the selected page");
      let offset: number;
      let deleteLength: number;
      let insertedUnits: number;
      if (page.kind !== "text_utf16") {
        offset = pageStart + splice.at;
        if (!Number.isSafeInteger(offset)) throw dbError("Query", "splice offset overflows");
        deleteLength = splice.delete;
        insertedUnits = splice.insert.length;
      } else {
        const offsetUtf16 = pageStart + splice.at;
        if (!Number.isSafeInteger(offsetUtf16))
          throw dbError("Query", "UTF-16 splice offset overflows");
        const endUtf16 = offsetUtf16 + splice.delete;
        if (!Number.isSafeInteger(endUtf16))
          throw dbError("Query", "UTF-16 splice range overflows");
        const cur = current;
        const [o, e] = await lockNode(this, async (n) => [
          await n.largeTextUtf16OffsetToByte(cur, offsetUtf16).then(
            (v: number) => v,
            (err: unknown) =>
              utf16SpliceByte(() => {
                throw err;
              }),
          ),
          await n.largeTextUtf16OffsetToByte(cur, endUtf16).then(
            (v: number) => v,
            (err: unknown) =>
              utf16SpliceByte(() => {
                throw err;
              }),
          ),
        ]);
        offset = o;
        deleteLength = e - o;
        insertedUnits = decodeText(
          splice.insert,
          "text splice insert is not valid UTF-8",
          "Query",
        ).length;
      }
      const node = await this.node.lockForLargeValueStaging();
      let staged: X.StagedLargeValue;
      try {
        staged = await node.editAndStageLargeValue(current, offset, deleteLength, splice.insert);
      } finally {
        this.node.node.unlock();
      }
      if (finalStaged) priorClaims.push(finalStaged.id);
      finalStaged = staged;
      current = staged.valueRef;
      pageLength = pageLength - splice.delete + insertedUnits;
      if (pageLength < 0) throw dbError("Query", "splice page length overflows");
    }
    return [
      preserveNullable({ kind: "Large", value: current }, nullable),
      finalStaged,
      priorClaims,
    ];
  },

  async applyLargeValueJsonSet(
    this: Db,
    raw: Value,
    expectedKind: X.LargeValueKind,
    edits: JsonSetEdit[],
  ): Promise<LargeValueEdit> {
    if (expectedKind !== "Json") throw dbError("Schema", "JSON set requires a JSON column");
    const [value, nullable] = unwrapPresentNullable(raw);
    let source: Uint8Array;
    let large: X.LargeValueRef | undefined;
    if (value.kind === "String") source = utf8(value.value);
    else if (value.kind === "Large" && value.value.kind === "Json") {
      large = value.value;
      source = await lockNode(this, (n) => n.readLargeValueRange(large, 0, large!.byteLength));
    } else throw dbError("Schema", "JSON set requires a JSON cell");
    const holder: { value: any } = { value: undefined };
    try {
      holder.value = JSON.parse(fromUtf8(source));
    } catch {
      throw dbError("Query", "stored JSON is invalid");
    }
    for (const edit of edits) applyJsonSet(holder, edit.at, edit.value);
    const replacement = JSON.stringify(holder.value);
    if (!large)
      return [preserveNullable({ kind: "String", value: replacement }, nullable), undefined, []];
    const node = await this.node.lockForLargeValueStaging();
    let staged: X.StagedLargeValue;
    try {
      staged = await node.editAndStageLargeValue(large, 0, large.byteLength, utf8(replacement));
    } finally {
      this.node.node.unlock();
    }
    return [preserveNullable({ kind: "Large", value: staged.valueRef }, nullable), staged, []];
  },

  async authorizedPhysicalCell(
    this: Db,
    table: string,
    row: X.RowUuid,
    column: string,
  ): Promise<Value> {
    const tableSchema = this.tableSchema(table);
    if (!tableSchema.columns.some((c: any) => c.name === column))
      throw dbError("Schema", `unknown column ${table}.${column}`);
    if (this.authorizeReadForIdentity(table, row, this.identity.author).kind !== "Allowed")
      throw dbError("NotObserved", "large-value cell is not observed");
    const cell = await lockNode(this, (n) =>
      n.currentPhysicalCellInSchema(this.schemaVersionId, table, row, column),
    );
    if (cell === undefined) throw dbError("NotObserved", "large-value cell is not observed");
    return cell;
  },

  async writeStagedLargeValueUpdate(
    this: Db,
    table: string,
    row: X.RowUuid,
    column: string,
    staged: X.StagedLargeValue,
    nullable: boolean,
  ): Promise<WriteHandle> {
    const node = await this.node.lockForLargeValueStaging();
    let published: any;
    try {
      const cells: RowCells | undefined = await node.currentPhysicalCellsInSchema(
        this.schemaVersionId,
        table,
        row,
      );
      if (!cells) throw dbError("NotObserved", "large-value row is not observed");
      cells.set(column, preserveNullable({ kind: "Large", value: staged.valueRef }, nullable));
      const commit = X.MergeableCommit.new(table, row, this.nextNowMsValue())
        .madeBy(this.identity.author)
        .cells(cells)
        .authoredColumns(new Set([column]));
      const sealed = await node.sealLargeValueUpdates(
        commit,
        [[column, staged]],
        this.schemaVersionId,
      );
      published = await this.commitMergeableBatch(node, [sealed]);
    } finally {
      this.node.node.unlock();
    }
    return this.finishPublishedWrite(row, published);
  },

  async insert(
    this: Db,
    table: string,
    cells: RowCells,
    options: InsertOptions = {},
  ): Promise<WriteHandle> {
    this.ensureMutationOperationAdmitted();
    const identity = options.identity ?? { kind: "Database" };
    const target: ExactWriteTarget = options.target ?? { kind: "Root" };
    this.rejectAttributedBranchTarget(identity, target.kind === "Branch");
    validateUpdatedAtMs(options.updatedAtMs);
    const supplied = options.rowId !== undefined;
    const row = options.rowId ?? this.rowIdSource.nextRowId();
    const { madeBy, permissionSubject } = this.resolveWriteIdentity(
      identity,
    ) as ResolvedWriteIdentity;
    const branch = exactTargetBranch(target);
    if (supplied) {
      if (target.kind === "Root") await this.ensureRowAbsent(table, row);
      else await this.ensureExactBranchRowAbsent(table, branch, row);
    }
    return this.writeMergeableAtMsWithAuthorshipInBranch(
      madeBy,
      permissionSubject,
      table,
      row,
      cells,
      [],
      undefined,
      undefined,
      options.updatedAtMs ?? this.nextNowMsValue(),
      branch,
      undefined,
    );
  },

  beginStreamingValueUpload(
    this: Db,
    table: string,
    cells: RowCells,
    column: string,
  ): StreamingValueUpload {
    this.ensureMutationOperationAdmitted();
    const [kind] = this.validateStreamingColumn(table, cells, column);
    const emitted: X.StagedChunk[] = [];
    const preparation = X.PushStreamingPreparation.new(kind, (chunk: X.StagedChunk) => {
      emitted.push(chunk);
    });
    return new StreamingValueUpload(
      X.StagedLargeValueId.random(),
      kind,
      false,
      preparation,
      emitted,
    );
  },

  async pushStreamingValueUpload(
    this: Db,
    upload: StreamingValueUpload,
    bytes: Uint8Array,
  ): Promise<void> {
    this.ensureMutationOperationAdmitted();
    const initializedNow = !upload.initialized;
    if (!upload.initialized) {
      await withStaging(this, (n) => n.beginStreamingLargeValueUpload(upload.id, upload.kind));
      upload.initialized = true;
    }
    if (!upload.preparation) throw dbError("Schema", "streaming upload is closed");
    try {
      upload.preparation.push(bytes);
    } catch (error) {
      upload.preparation = undefined;
      await withStaging(this, (n) => n.evictPendingLargeValueUpload(upload.id));
      throw X.toApiError(error);
    }
    const chunks = upload.emitted.splice(0);
    if (initializedNow && !chunks.length) return;
    try {
      await withStaging(this, (n) => n.stageLargeValueChunkBatch(upload.id, upload.kind, chunks));
    } catch (error) {
      upload.preparation = undefined;
      await evictPendingQuietly(this, upload.id);
      throw X.toApiError(error);
    }
  },

  async abortStreamingValueUpload(this: Db, upload: StreamingValueUpload): Promise<void> {
    this.ensureMutationOperationAdmitted();
    upload.preparation = undefined;
    await withStaging(this, (n) => n.evictPendingLargeValueUpload(upload.id));
  },

  async finishStreamingValueUpload(
    this: Db,
    upload: StreamingValueUpload,
    mutation: StreamingMutationKind,
    table: string,
    row: X.RowUuid,
    cells: RowCells,
    column: string,
    identity: WriteIdentity,
    nowMs: number | undefined,
    head: X.BranchSelector | undefined,
    base: X.BranchViewBase | undefined,
  ): Promise<WriteHandle> {
    this.ensureMutationOperationAdmitted();
    let resolved: ResolvedWriteIdentity;
    try {
      this.rejectAttributedBranchTarget(identity, head !== undefined || base !== undefined);
      resolved = this.resolveWriteIdentity(identity);
    } catch (error) {
      await this.abortStreamingValueUpload(upload);
      throw error;
    }
    if (!upload.initialized) {
      await withStaging(this, (n) => n.beginStreamingLargeValueUpload(upload.id, upload.kind));
      upload.initialized = true;
    }
    const preparation = upload.preparation;
    upload.preparation = undefined;
    if (!preparation) throw dbError("Schema", "streaming upload is closed");
    let valueRef: X.LargeValueRef;
    try {
      [valueRef] = preparation.finish();
    } catch (error) {
      await evictPendingQuietly(this, upload.id);
      throw X.toApiError(error);
    }
    const chunks = upload.emitted.splice(0);
    let staged: X.StagedLargeValue;
    try {
      staged = await withStaging(this, async (n) => {
        await n.stageLargeValueChunkBatch(upload.id, upload.kind, chunks);
        return n.finalizeLargeValueUpload(upload.id, valueRef);
      });
    } catch (error) {
      await evictPendingQuietly(this, upload.id);
      throw X.toApiError(error);
    }
    let nullable: boolean;
    try {
      const [expected, n] = this.validateStreamingColumn(table, cells, column);
      if (expected !== staged.valueRef.kind) throw largeValueCellTypeError(table, column);
      nullable = n;
    } catch (error) {
      await evictStagedQuietly(this, staged.id);
      throw error;
    }
    const at = nowMs ?? this.nextNowMsValue();
    try {
      return await this.publishInWriterOrder(at, (db: Db) =>
        db.publishStreamingValueWithId(
          mutation,
          table,
          row,
          cells,
          column,
          staged,
          nullable,
          resolved.madeBy,
          resolved.permissionSubject,
          at,
          head,
          base,
        ),
      );
    } catch (error) {
      await evictStagedQuietly(this, staged.id);
      throw error;
    }
  },

  async publishInWriterOrder(
    this: Db,
    nowMs: number,
    publish: (db: Db) => Promise<WriteHandle>,
  ): Promise<WriteHandle> {
    const txId = this.reserveTransactionIdAtMs(nowMs);
    const db = this.cloneForReservedTransaction(txId);
    let settled = false;
    let result: { ok: true; v: WriteHandle } | { ok: false; e: unknown } | undefined;
    const wake = { fn: undefined as undefined | (() => void) };
    this.node.enqueueReservedWrite(txId, async () => {
      try {
        result = { ok: true, v: await publish(db) };
      } catch (e) {
        result = { ok: false, e };
        throw e;
      } finally {
        settled = true;
        wake.fn?.();
      }
    });
    for (;;) {
      if (settled) {
        if (!result)
          throw dbError("WriteRejected", "database closed before the write was published");
        if (result.ok) return result.v;
        throw result.e;
      }
      const queued = this.node.queuedMutationCount();
      this.node.pollQueuedMutationOnce();
      if (this.node.queuedMutationCount() >= queued && !settled)
        await new Promise<void>((resolve) => (wake.fn = resolve));
    }
  },

  validateStreamingColumn(
    this: Db,
    table: string,
    cells: RowCells,
    column: string,
  ): [X.LargeValueKind, boolean] {
    if (cells.has(column))
      throw dbError("Schema", `streamed column ${table}.${column} was also supplied in row cells`);
    return this.largeValueColumnKind(table, column);
  },

  largeValueColumnKind(this: Db, table: string, column: string): [X.LargeValueKind, boolean] {
    const schema = this.tableSchema(table).columns.find((c: any) => c.name === column);
    if (!schema) throw dbError("Schema", `unknown streamed column ${table}.${column}`);
    const nullable = schema.columnType.kind === "Nullable";
    const columnType = nullable ? schema.columnType.inner : schema.columnType;
    const t = columnType.kind;
    switch (schema.largeValueKind) {
      case "Bytes":
        if (t === "Bytes") return ["Bytes", nullable];
        break;
      case "String":
        if (t === "String") return ["String", nullable];
        break;
      case "Json":
        if (t === "String") return ["Json", nullable];
        break;
    }
    throw largeValueCellTypeError(table, schema.name);
  },

  async publishStreamingValueWithId(
    this: Db,
    mutation: StreamingMutationKind,
    table: string,
    row: X.RowUuid,
    rawCells: RowCells,
    column: string,
    staged: X.StagedLargeValue,
    nullable: boolean,
    madeBy: AuthorSubject,
    permissionSubject: AuthorSubject | undefined,
    nowMs: number | undefined,
    head: X.BranchSelector | undefined,
    base: X.BranchViewBase | undefined,
  ): Promise<WriteHandle> {
    const branch = head ?? X.BranchSelector.default();
    let cells = rawCells;
    let authored: Set<string> | undefined;
    let inserting = false;
    const authoredWith = () => new Set([...rawCells.keys(), column]);
    const branchViewCells = async (
      operation: string,
      requireVisible: boolean,
    ): Promise<[RowCells, boolean] | undefined> => {
      const visible =
        permissionSubject !== undefined
          ? await this.visibleBranchViewCellsForIdentity(table, head!, base, row, permissionSubject)
          : undefined;
      if (requireVisible && permissionSubject !== undefined && !visible)
        throw readForWriteDenied(operation, table);
      return lockNode(this, async (node) => {
        const current: RowCells | undefined = await node.visibleCurrentCellsInBranch(
          table,
          head!,
          row,
        );
        if (current) {
          await node.localContentWinnerTxIdInBranch(table, head!, row);
          if (permissionSubject !== undefined && !visible)
            throw readForWriteDenied(operation, table);
          return [extend(visible ?? current, rawCells), false];
        }
        const inherited: RowCells | undefined = await node.visibleCurrentCellsInBranchView(
          table,
          head!,
          base,
          row,
        );
        if (!inherited) return undefined;
        if (permissionSubject !== undefined && !visible) throw readForWriteDenied(operation, table);
        return [extend(visible ?? inherited, rawCells), true];
      });
    };
    const mergeRoot = async () =>
      permissionSubject !== undefined
        ? (await this.mergeExistingCellsForIdentity(table, row, rawCells, permissionSubject))[0]
        : (await this.mergeExistingCells(table, row, rawCells))[0];
    switch (mutation) {
      case "Insert":
        if (head) await this.ensureExactBranchRowAbsent(table, branch, row);
        else await this.ensureRowAbsent(table, row);
        inserting = true;
        break;
      case "Update":
        authored = authoredWith();
        if (head) {
          const r = await branchViewCells("UPDATE", true);
          if (!r)
            throw dbError(
              "NotObserved",
              `row is not visible in branch view: ${X.rowUuidString(row)}`,
            );
          [cells, inserting] = r;
        } else cells = await mergeRoot();
        break;
      case "Upsert":
        if (head) {
          const r = await branchViewCells("UPSERT", false);
          if (r) {
            [cells, inserting] = r;
            authored = authoredWith();
          } else inserting = true;
        } else {
          await this.ensureRowNotDeleted(table, row);
          const exists =
            permissionSubject !== undefined
              ? (await this.upsertTargetForTrustedIdentity(table, row, permissionSubject)) !==
                undefined
              : (await this.upsertTargetForClientIdentity(table, row, this.identity.author)) !==
                undefined;
          if (exists) {
            cells = await mergeRoot();
            authored = authoredWith();
          } else inserting = true;
        }
        break;
    }
    if (inserting) cells = this.applyInsertDefaults(table, cells);
    let commit = X.MergeableCommit.new(table, row, nowMs ?? this.nextNowMsValue())
      .branch(branch)
      .madeBy(madeBy)
      .cells(cells);
    if (authored) commit = commit.authoredColumns(authored);
    if (permissionSubject !== undefined) commit = commit.permissionSubject(permissionSubject);
    const node = commit.needsLargeValueStaging()
      ? await this.node.lockForLargeValueStaging()
      : await this.node.node.lock();
    let published: any;
    try {
      const sealed = (
        await node.sealInheritedLargeValues(commit, this.schemaVersionId, true)
      ).stagedLargeCell(column, staged, nullable);
      published = await this.commitMergeableBatch(node, [sealed]);
    } finally {
      this.node.node.unlock();
    }
    return this.finishPublishedWrite(row, published);
  },

  canInsert(this: Db, _table: string, _cells: RowCells): X.PermissionAdvice {
    return { kind: "Unknown" } as X.PermissionAdvice;
  },

  async update(
    this: Db,
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    options: UpdateOptions = {},
  ): Promise<WriteHandle> {
    this.ensureMutationOperationAdmitted();
    const identity = options.identity ?? defaultIdentity;
    const target: WriteTarget = options.target ?? { kind: "Root" };
    this.rejectAttributedBranchTarget(identity, target.kind === "BranchView");
    validateUpdatedAtMs(options.updatedAtMs);
    const { madeBy, permissionSubject } = this.resolveWriteIdentity(
      identity,
    ) as ResolvedWriteIdentity;
    const nowMs = options.updatedAtMs ?? this.nextNowMsValue();
    if (target.kind === "Root") {
      if (!patch.size) {
        await this.ensureRowNotDeleted(table, row);
        const existing =
          identity.kind === "Session"
            ? await this.localRowForTrustedIdentity(table, row, identity.author)
            : await this.localRowForClientIdentity(table, row, permissionSubject ?? madeBy);
        return this.noOpUpdateHandle(table, row, existing);
      }
      const [cells, parent, authoredColumns] =
        identity.kind === "Session"
          ? await this.mergeExistingCellsForIdentity(table, row, patch, identity.author)
          : await this.mergeExistingCells(table, row, patch);
      return this.writeMergeableAtMsWithAuthorship(
        madeBy,
        permissionSubject,
        table,
        row,
        cells,
        parent ? [parent] : [],
        undefined,
        authoredColumns,
        nowMs,
      );
    }
    const { head, base } = target;
    if (!patch.size) throw dbError("Schema", "branch update requires at least one authored column");
    let visible: RowCells | undefined;
    if (identity.kind === "Session") {
      visible = await this.visibleBranchViewCellsForIdentity(
        table,
        head,
        base,
        row,
        identity.author,
      );
      if (!visible) throw readForWriteDenied("UPDATE", table);
    }
    const local: RowCells | undefined = await lockNode(this, (n) =>
      n.visibleCurrentCellsInBranch(table, head, row),
    );
    let cells: RowCells;
    let parents: TxId[] = [];
    let authoredColumns: Set<string> | undefined;
    let verifiedInherited: RowCells | undefined;
    if (local) {
      const parent = await lockNode(this, (n) =>
        n.localContentWinnerTxIdInBranch(table, head, row),
      );
      cells = visible ?? local;
      parents = parent ? [parent] : [];
      authoredColumns = new Set(patch.keys());
    } else {
      const inherited: RowCells | undefined = await lockNode(this, (n) =>
        n.visibleCurrentCellsInBranchView(table, head, base, row),
      );
      if (!inherited)
        throw dbError("NotObserved", `row is not visible in branch view: ${X.rowUuidString(row)}`);
      cells = visible ?? inherited;
      verifiedInherited = new Map(cells);
    }
    extend(cells, patch);
    return this.writeMergeableAtMsWithAuthorshipInBranch(
      madeBy,
      permissionSubject,
      table,
      row,
      cells,
      parents,
      undefined,
      authoredColumns,
      nowMs,
      head,
      verifiedInherited,
    );
  },

  upsert(
    this: Db,
    table: string,
    row: X.RowUuid,
    cells: RowCells,
    options: UpsertOptions = {},
  ): Promise<WriteHandle> {
    return this.upsertWithInsertValidation(table, row, cells, options, false);
  },

  async upsertWithInsertValidation(
    this: Db,
    table: string,
    row: X.RowUuid,
    rawCells: RowCells,
    options: UpsertOptions,
    requireCompleteInsert: boolean,
  ): Promise<WriteHandle> {
    this.ensureMutationOperationAdmitted();
    const identity = options.identity ?? defaultIdentity;
    const target: WriteTarget = options.target ?? { kind: "Root" };
    this.rejectAttributedBranchTarget(identity, target.kind === "BranchView");
    validateUpdatedAtMs(options.updatedAtMs);
    const { madeBy, permissionSubject } = this.resolveWriteIdentity(
      identity,
    ) as ResolvedWriteIdentity;
    const nowMs = options.updatedAtMs ?? this.nextNowMsValue();
    let branch: X.BranchSelector;
    let cells = rawCells;
    let parents: TxId[] = [];
    let authoredColumns: Set<string> | undefined;
    let verifiedInherited: RowCells | undefined;
    if (target.kind === "Root") {
      await this.ensureRowNotDeleted(table, row);
      const exists =
        identity.kind === "Session"
          ? (await this.upsertTargetForTrustedIdentity(table, row, identity.author)) !== undefined
          : (await this.upsertTargetForClientIdentity(table, row, permissionSubject ?? madeBy)) !==
            undefined;
      if (exists) {
        const [merged, parent, authored] =
          identity.kind === "Session"
            ? await this.mergeExistingCellsForIdentity(table, row, rawCells, identity.author)
            : await this.mergeExistingCells(table, row, rawCells);
        cells = merged;
        parents = parent ? [parent] : [];
        authoredColumns = authored;
      }
      branch = X.BranchSelector.default();
    } else {
      const { head, base } = target;
      branch = head;
      await this.ensureBranchViewRowNotDeleted(table, head, base, row);
      const visible =
        identity.kind === "Session"
          ? await this.visibleBranchViewCellsForIdentity(table, head, base, row, identity.author)
          : undefined;
      const authored = new Set(rawCells.keys());
      await lockNode(this, async (node) => {
        const current: RowCells | undefined = await node.visibleCurrentCellsInBranch(
          table,
          head,
          row,
        );
        if (current) {
          if (!rawCells.size)
            throw dbError("Schema", "branch upsert update requires at least one authored column");
          if (identity.kind === "Session" && !visible) throw readForWriteDenied("UPSERT", table);
          const parent = await node.localContentWinnerTxIdInBranch(table, head, row);
          cells = extend(visible ?? current, rawCells);
          parents = parent ? [parent] : [];
          authoredColumns = authored;
          return;
        }
        const inherited: RowCells | undefined = await node.visibleCurrentCellsInBranchView(
          table,
          head,
          base,
          row,
        );
        if (inherited) {
          if (identity.kind === "Session" && !visible) throw readForWriteDenied("UPSERT", table);
          verifiedInherited = new Map(inherited);
          cells = extend(visible ?? inherited, rawCells);
          authoredColumns = authored;
        }
      });
    }
    if (requireCompleteInsert && !parents.length) this.validateCompleteInsert(table, cells);
    return this.writeMergeableAtMsWithAuthorshipInBranch(
      madeBy,
      permissionSubject,
      table,
      row,
      cells,
      parents,
      undefined,
      authoredColumns,
      nowMs,
      branch,
      verifiedInherited,
    );
  },

  async delete(
    this: Db,
    table: string,
    row: X.RowUuid,
    options: DeleteOptions = {},
  ): Promise<WriteHandle> {
    this.ensureMutationOperationAdmitted();
    const identity = options.identity ?? defaultIdentity;
    const target: WriteTarget = options.target ?? { kind: "Root" };
    this.rejectAttributedBranchTarget(identity, target.kind === "BranchView");
    validateUpdatedAtMs(options.updatedAtMs);
    const { madeBy, permissionSubject } = this.resolveWriteIdentity(
      identity,
    ) as ResolvedWriteIdentity;
    const nowMs = options.updatedAtMs ?? this.nextNowMsValue();
    let branch: X.BranchSelector;
    let parents: TxId[];
    if (target.kind === "Root") {
      await this.ensureRowNotDeleted(table, row);
      parents = (await this.rowLayerParents(table, row))[1];
      branch = X.BranchSelector.default();
    } else {
      const { head, base } = target;
      branch = head;
      parents = await lockNode(this, async (node) => {
        const local = await node.visibleCurrentCellsInBranch(table, head, row);
        if (!local && !(await node.visibleCurrentCellsInBranchView(table, head, base, row)))
          throw dbError(
            "NotObserved",
            `row is not visible in branch view: ${X.rowUuidString(row)}`,
          );
        const deletion = await node.localDeletionWinnerTxIdInBranch(table, head, row);
        return deletion ? [deletion] : [];
      });
    }
    return this.writeMergeableAtMsWithAuthorshipInBranch(
      madeBy,
      permissionSubject,
      table,
      row,
      new Map(),
      parents,
      "Deleted",
      undefined,
      nowMs,
      branch,
      undefined,
    );
  },

  canRead(this: Db, _table: string, _row: X.RowUuid): X.PermissionAdvice {
    return { kind: "Unknown" } as X.PermissionAdvice;
  },

  authorizeReadForIdentity(
    this: Db,
    table: string,
    row: X.RowUuid,
    author: AuthorSubject,
  ): X.PermissionAdvice {
    this.tableSchema(table);
    const allowed = X.blockOn(borrowNode(this).dryRunReadCurrentAllows(table, row, author));
    return { kind: allowed ? "Allowed" : "Denied" } as X.PermissionAdvice;
  },

  canUpdate(this: Db, _table: string, _row: X.RowUuid): X.PermissionAdvice {
    return { kind: "Unknown" } as X.PermissionAdvice;
  },

  canDelete(this: Db, _table: string, _row: X.RowUuid): X.PermissionAdvice {
    return { kind: "Unknown" } as X.PermissionAdvice;
  },

  setIdentityClaims(this: Db, identity: AuthorSubject, claims: Map<string, Value>): void {
    const apply = (node: X.NodeState) => {
      const previous = node.sessionClaimRevision(identity);
      node.setSessionClaims(identity, claims);
      return node.sessionClaimRevision(identity) !== previous;
    };
    if (this.node.ownerQueueIsQuiescent()) {
      const node = this.node.node.tryLock();
      if (node) {
        const changed = apply(node);
        this.node.node.unlock();
        if (changed) this.node.scheduleTick("Deferred");
        return;
      }
    }
    const runtime = this.node;
    runtime.enqueueOwnerOperation(async () => {
      const node = await runtime.node.lock();
      let changed: boolean;
      try {
        changed = apply(node);
      } finally {
        runtime.node.unlock();
      }
      if (changed) runtime.scheduleTick("Deferred");
    });
  },

  async setIdentityClaimsAsync(
    this: Db,
    identity: AuthorSubject,
    claims: Map<string, Value>,
  ): Promise<void> {
    const changed = await lockNode(this, (node) => {
      const previous = node.sessionClaimRevision(identity);
      node.setSessionClaims(identity, claims);
      return node.sessionClaimRevision(identity) !== previous;
    });
    if (changed) this.node.scheduleTick("Deferred");
  },

  async restore(
    this: Db,
    table: string,
    row: X.RowUuid,
    rawCells: RowCells | undefined,
    options: RestoreOptions = {},
  ): Promise<WriteHandle> {
    this.ensureMutationOperationAdmitted();
    const identity = options.identity ?? defaultIdentity;
    const target: ExactWriteTarget = options.target ?? { kind: "Root" };
    this.rejectAttributedBranchTarget(identity, target.kind === "Branch");
    validateUpdatedAtMs(options.updatedAtMs);
    const { madeBy, permissionSubject } = this.resolveWriteIdentity(
      identity,
    ) as ResolvedWriteIdentity;
    const branch = exactTargetBranch(target);
    const nowMs = options.updatedAtMs ?? this.nextNowMsValue();
    const cells = rawCells && this.applyInsertDefaults(table, rawCells);
    if (target.kind === "Root") {
      await this.ensureRowDeleted(table, row);
      await this.rowLayerParents(table, row);
    } else {
      await lockNode(this, async (node) => {
        const deletion = await node.localDeletionWinnerTxIdInBranch(table, branch, row);
        if (!deletion)
          throw dbError("NotObserved", `branch deletion not observed: ${X.rowUuidString(row)}`);
        await node.localContentWinnerTxIdInBranch(table, branch, row);
      });
    }
    const withSubject = (c: X.MergeableCommit) =>
      permissionSubject !== undefined ? c.permissionSubject(permissionSubject) : c;
    const commits: X.MergeableCommit[] = [];
    if (cells)
      commits.push(
        withSubject(
          X.MergeableCommit.new(table, row, nowMs).branch(branch).madeBy(madeBy).cells(cells),
        ),
      );
    commits.push(
      withSubject(
        X.MergeableCommit.new(table, row, nowMs)
          .branch(branch)
          .madeBy(madeBy)
          .cells(new Map())
          .deletion("Restored"),
      ),
    );
    const node = commits.some((c) => c.needsLargeValueStaging())
      ? await this.node.lockForLargeValueStaging()
      : await this.node.node.lock();
    let published: any;
    try {
      published = await this.commitMergeableBatch(node, commits);
    } finally {
      this.node.node.unlock();
    }
    return this.finishPublishedWrite(row, published);
  },

  writeMergeableAtMsWithAuthorship(
    this: Db,
    madeBy: AuthorSubject,
    permissionSubject: AuthorSubject | undefined,
    table: string,
    row: X.RowUuid,
    cells: RowCells,
    parents: TxId[],
    deletion: X.DeletionEvent | undefined,
    authoredColumns: Set<string> | undefined,
    nowMs: number,
  ): Promise<WriteHandle> {
    return this.writeMergeableAtMsWithAuthorshipInBranch(
      madeBy,
      permissionSubject,
      table,
      row,
      cells,
      parents,
      deletion,
      authoredColumns,
      nowMs,
      X.BranchSelector.default(),
      undefined,
    );
  },

  async writeMergeableAtMsWithAuthorshipInBranch(
    this: Db,
    madeBy: AuthorSubject,
    permissionSubject: AuthorSubject | undefined,
    table: string,
    row: X.RowUuid,
    rawCells: RowCells,
    parents: TxId[],
    deletion: X.DeletionEvent | undefined,
    authoredColumns: Set<string> | undefined,
    nowMs: number,
    branch: X.BranchSelector,
    verifiedInherited: RowCells | undefined,
  ): Promise<WriteHandle> {
    const operation = deletion === "Deleted" ? "DELETE" : parents.length ? "UPDATE" : "INSERT";
    const cells = operation === "INSERT" ? this.applyInsertDefaults(table, rawCells) : rawCells;
    let allowInherited = true;
    if (authoredColumns) {
      try {
        allowInherited = !this.tableSchema(table).columns.every((c: any) =>
          authoredColumns.has(c.name),
        );
      } catch {}
    }
    let commit = X.MergeableCommit.new(table, row, nowMs)
      .branch(branch)
      .madeBy(madeBy)
      .cells(cells);
    if (authoredColumns) commit = commit.authoredColumns(authoredColumns);
    if (verifiedInherited) commit = commit.verifiedInheritedLargeCells(verifiedInherited);
    if (permissionSubject !== undefined) commit = commit.permissionSubject(permissionSubject);
    if (deletion) commit = commit.deletion(deletion);
    const node = commit.needsLargeValueStaging()
      ? await this.node.lockForLargeValueStaging()
      : await this.node.node.lock();
    let published: any;
    try {
      const sealed = await node.sealInheritedLargeValues(
        commit,
        this.schemaVersionId,
        allowInherited,
      );
      published = await this.commitMergeableBatch(node, [sealed]);
    } finally {
      this.node.node.unlock();
    }
    return this.finishPublishedWrite(row, published);
  },

  commitMergeableBatch(
    this: Db,
    node: X.NodeState,
    commits: X.MergeableCommit[],
  ): Promise<X.PublishedTransaction> {
    return this.reservedTxId !== undefined
      ? node.commitMergeableManyInSchemaAt(this.schemaVersionId, commits, this.reservedTxId)
      : node.commitMergeableManyInSchema(this.schemaVersionId, commits);
  },

  async finishPublishedWrite(
    this: Db,
    row: X.RowUuid,
    published: X.PublishedTransaction,
  ): Promise<WriteHandle> {
    const txId = published.txId;
    const localTier = await this.settleLocalPublication(published, undefined);
    const node = this.node.node;
    return new WriteHandle({ deref: () => node }, row, txId, localTier, undefined, undefined);
  },

  async settleLocalPublication(
    this: Db,
    published: X.PublishedTransaction,
    unit: SyncMessage | undefined,
  ): Promise<X.DurabilityTier> {
    if (this.node.deferLocalPersistence.v) {
      await this.finishDeferredLocalPublication(published, unit);
      return "None";
    }
    const txId = published.txId;
    await this.finishPublicationOutcome(X.PublicationOutcome.published(undefined, published));
    if (!unit) return this.finalizeLocalCommit(txId);
    this.node.queuePendingUpload(txId, unit);
    return "None";
  },

  async finishDeferredLocalPublication(
    this: Db,
    published: X.PublishedTransaction,
    uploadUnit: SyncMessage | undefined,
  ): Promise<void> {
    const changedTables = new Set<string>(published.changedTables());
    this.node.queueLocalPublication(published, uploadUnit);
    try {
      await this.refreshSubscriptionsForTables(changedTables);
    } catch (error) {
      X.routeSubscriptionRefreshFailure(this.node.subscriptions, error);
    }
  },

  async finishPublicationOutcome<T>(this: Db, outcome: X.PublicationOutcome<T>): Promise<T> {
    let publications = outcome.publications;
    const postSettlementWork: SyncMessage[] = [...outcome.postSettlementWork];
    for (;;) {
      if (publications.length) {
        const changedTables = new Set<string>(
          publications.flatMap((p: any) => [...p.changedTables()]),
        );
        const persisted: [TxId, unknown][] = [];
        for (const p of publications) persisted.push([p.txId(), await p.persist()]);
        await lockNode(this, (node) => {
          for (const [txId, persistence] of persisted)
            node.settlePublishedTransaction(txId, persistence);
        });
        try {
          await this.refreshSubscriptionsForTables(changedTables);
        } catch (error) {
          X.routeSubscriptionRefreshFailure(this.node.subscriptions, error);
        }
      }
      const message = postSettlementWork.shift();
      if (!message) break;
      const next = await lockNode(this, (node) =>
        node.applySyncMessageWithIngestContext(message, {
          identity: X.AuthorSubjectT.SYSTEM,
          trust: "TrustedBackend",
          admittedWriteAuthorization: false,
          versionReceiptsValidated: false,
        }),
      );
      publications = next.publications;
      postSettlementWork.push(...next.postSettlementWork);
    }
    return outcome.value;
  },

  resolveWriteIdentity(this: Db, identity: WriteIdentity): ResolvedWriteIdentity {
    switch (identity.kind) {
      case "Database":
        return { madeBy: this.identity.author, permissionSubject: undefined };
      case "Session":
        return { madeBy: identity.author, permissionSubject: identity.author };
      case "Attribution":
        if (X.authorSubjectEq(identity.author, this.identity.author) || this.backendAttribution)
          return { madeBy: identity.author, permissionSubject: this.identity.author };
        throw dbError("WriteRejected", "attribution requires a trusted serving node");
    }
  },

  rejectAttributedBranchTarget(this: Db, identity: WriteIdentity, targetsBranch: boolean): void {
    if (identity.kind === "Attribution" && targetsBranch)
      throw dbError("WriteRejected", "backend-attributed writes do not support branch targets");
  },

  checkCatalogueAdmin(this: Db): void {
    if (!X.authorSubjectEq(this.identity.author, X.AuthorSubjectT.SYSTEM))
      throw dbError("Protocol", "catalogue updates require a serving Node");
  },

  finalizeLocalCommit(this: Db, txId: TxId): X.DurabilityTier {
    this.node.queuePendingUpload(txId, undefined);
    return borrowNode(this).authoredCommitDurability();
  },

  nextNowMsValue(this: Db): number {
    return this.nextNowMs.nextNowMs();
  },

  ensureOpenSchemaAdmitted(this: Db): void {
    if (!this.requiresOpenSchemaAdmission) return;
    const result = this.node.openSchemaAdmission.v?.result;
    if (!result) throw pendingOpenSchemaError();
    if (!result.ok) throw result.error;
  },

  async awaitOpenSchemaForCoverage(this: Db, coverageTier: X.DurabilityTier): Promise<void> {
    if (!this.requiresOpenSchemaAdmission) return;
    if (X.durabilityRank(coverageTier) < X.durabilityRank("Global"))
      return this.ensureOpenSchemaAdmitted();
    const pending = this.node.openSchemaAdmission.v;
    if (!pending) throw new Error("pending owner has admission state");
    if (pending.result) {
      if (!pending.result.ok) throw pending.result.error;
      return;
    }
    await pending.wait;
    this.ensureOpenSchemaAdmitted();
  },

  currentWriteSchemaForQuery(this: Db): [X.JazzSchema, X.SchemaVersionId] {
    this.ensureOpenSchemaAdmitted();
    if (this.schemaViewIsFixed) return [this.schema, this.schemaVersionId];
    const node = borrowNode(this);
    const current = node.currentWriteSchema();
    if (X.deepEq(current.schema, this.schemaVersionId)) return [this.schema, this.schemaVersionId];
    const schema = node.catalogueSchemas().get(current.schema);
    if (!schema)
      throw dbError(
        "Schema",
        `current write schema ${X.debugFmt(current.schema)} is missing from catalogue`,
      );
    return [schema.schema, current.schema];
  },

  tableSchema(this: Db, table: string): X.TableSchema {
    const schema = this.schema.tables.find((t: any) => t.name === table);
    if (!schema) throw dbError("Schema", `unknown table ${table}`);
    return schema;
  },

  validateCompleteInsert(this: Db, table: string, cells: RowCells): void {
    const schema = this.tableSchema(table);
    for (const column of schema.columns) {
      if (
        !schema.branchBy.includes(column.name) &&
        !cells.has(column.name) &&
        column.default === undefined &&
        column.columnType.kind !== "Nullable"
      )
        throw dbError(
          "WriteRejected",
          `missing required field \`${column.name}\` on table \`${table}\``,
        );
    }
  },

  applyInsertDefaults(this: Db, table: string, cells: RowCells): RowCells {
    const schema = this.tableSchema(table);
    for (const column of schema.columns)
      if (!cells.has(column.name) && column.default !== undefined)
        cells.set(column.name, X.defaultCellForColumnType(column.columnType, column.default));
    if (!cells.size) {
      for (const column of schema.columns)
        if (X.storageColumnType(column).kind === "Nullable")
          cells.set(column.name, { kind: "Nullable", value: undefined });
      if (
        !cells.size &&
        schema.columns.length &&
        schema.columns.every(
          (c: any) => c.largeValueKind === "Json" && c.columnType.kind === "Nullable",
        )
      )
        throw dbError(
          "Schema",
          `inserting a row with every column omitted is not supported yet for table \`${table}\`: its optional columns are all JSON, which cannot store null until #3007 lands; set at least one column`,
        );
    }
    return cells;
  },

  async upsertTargetForClientIdentity(
    this: Db,
    table: string,
    row: X.RowUuid,
    identity: AuthorSubject,
  ): Promise<CurrentRow | undefined> {
    const target = await this.localRowForClientIdentity(table, row, identity);
    if (target) return target;
    if (!(await this.localCurrentRow(table, row))) return undefined;
    if (
      X.authorSubjectEq(identity, X.AuthorSubjectT.SYSTEM) ||
      this.tableSchema(table).readPolicy === undefined
    )
      return undefined;
    throw readForWriteDenied("UPSERT", table);
  },

  async upsertTargetForTrustedIdentity(
    this: Db,
    table: string,
    row: X.RowUuid,
    identity: AuthorSubject,
  ): Promise<CurrentRow | undefined> {
    const target = await this.localRowForTrustedIdentity(table, row, identity);
    if (target) return target;
    if (!(await this.localCurrentRow(table, row))) return undefined;
    if (X.authorSubjectEq(identity, X.AuthorSubjectT.SYSTEM)) return undefined;
    throw readForWriteDenied("UPSERT", table);
  },

  async localCurrentRow(this: Db, table: string, row: X.RowUuid): Promise<CurrentRow | undefined> {
    this.ensureOpenSchemaAdmitted();
    this.tableSchema(table);
    return lockNode(this, (n) => n.localCurrentRow(table, row));
  },

  async ensureRowAbsent(this: Db, table: string, row: X.RowUuid): Promise<void> {
    this.tableSchema(table);
    const [content, deletion] = await lockNode(this, async (n) => [
      await n.localContentWinnerTxIdInSchema(this.schemaVersionId, table, row),
      await n.localDeletionWinnerTxIdInSchema(this.schemaVersionId, table, row),
    ]);
    if (deletion) throw rowAlreadyDeleted(row);
    if (content)
      throw dbError(
        "WriteRejected",
        `encoding error: object already exists: ${X.rowUuidString(row)}`,
      );
  },

  async ensureExactBranchRowAbsent(
    this: Db,
    table: string,
    branch: X.BranchSelector,
    row: X.RowUuid,
  ): Promise<void> {
    this.tableSchema(table);
    const [content, deletion] = await lockNode(this, async (n) => [
      await n.localContentWinnerTxIdInBranch(table, branch, row),
      await n.localDeletionWinnerTxIdInBranch(table, branch, row),
    ]);
    if (deletion) throw rowAlreadyDeleted(row);
    if (content)
      throw dbError(
        "WriteRejected",
        `encoding error: branch-local row already exists: ${X.rowUuidString(row)}`,
      );
  },

  async ensureRowDeleted(this: Db, table: string, row: X.RowUuid): Promise<void> {
    this.tableSchema(table);
    const deletion = await lockNode(this, (n) =>
      n.localDeletionWinnerTxIdInSchema(this.schemaVersionId, table, row),
    );
    if (!deletion) throw dbError("WriteRejected", `row not deleted: ${X.rowUuidString(row)}`);
  },

  async ensureRowNotDeleted(this: Db, table: string, row: X.RowUuid): Promise<void> {
    this.tableSchema(table);
    const deleted = await lockNode(
      this,
      async (n) =>
        !!(await n.localDeletionWinnerTxIdInSchema(this.schemaVersionId, table, row)) &&
        !(await n.localCurrentRowInSchema(table, row, this.schemaVersionId)),
    );
    if (deleted) throw rowAlreadyDeleted(row);
  },

  precheckMutationAdmission(this: Db, table: string): void {
    this.ensureMutationOperationAdmitted();
    this.tableSchema(table);
  },

  async ensureBranchViewRowNotDeleted(
    this: Db,
    table: string,
    head: X.BranchSelector,
    base: X.BranchViewBase | undefined,
    row: X.RowUuid,
  ): Promise<void> {
    this.tableSchema(table);
    const hidden = await lockNode(
      this,
      async (n) =>
        !!(await n.localDeletionWinnerTxIdInBranch(table, head, row)) &&
        !(await n.visibleCurrentCellsInBranchView(table, head, base, row)),
    );
    if (hidden) throw rowAlreadyDeleted(row);
  },

  rowLayerParents(this: Db, table: string, row: X.RowUuid): Promise<[TxId[], TxId[]]> {
    return lockNode(this, async (n) => {
      const c = await n.localContentWinnerTxIdInSchema(this.schemaVersionId, table, row);
      const d = await n.localDeletionWinnerTxIdInSchema(this.schemaVersionId, table, row);
      return [c ? [c] : [], d ? [d] : []] as [TxId[], TxId[]];
    });
  },

  async updateTargetMissing(this: Db, table: string, row: X.RowUuid): Promise<DbError> {
    return (await this.localCurrentRow(table, row))
      ? readForWriteDenied("UPDATE", table)
      : updateTargetNotLoaded("UPDATE", table, row);
  },

  async localRowForClientIdentity(
    this: Db,
    table: string,
    row: X.RowUuid,
    identity: AuthorSubject,
  ): Promise<CurrentRow | undefined> {
    if (this.tableSchema(table).readPolicy === undefined) return this.localCurrentRow(table, row);
    const query = this.prepareQuery(X.QueryT.from(table));
    const rows = await lockNode(this, (n) =>
      n.queryRowsForClientPhysicalRow(query.shapeValue, query.bindingValue, "Local", identity, row),
    );
    return rows[0];
  },

  async localRowForTrustedIdentity(
    this: Db,
    table: string,
    row: X.RowUuid,
    identity: AuthorSubject,
  ): Promise<CurrentRow | undefined> {
    const query = this.prepareQuery(X.QueryT.from(table));
    const rows = await lockNode(this, (n) =>
      n.queryRowsForLinkPhysicalRow(query.shapeValue, query.bindingValue, "Local", identity, row),
    );
    return rows[0];
  },

  async visibleBranchViewCellsForIdentity(
    this: Db,
    table: string,
    head: X.BranchSelector,
    base: X.BranchViewBase | undefined,
    row: X.RowUuid,
    identity: AuthorSubject,
  ): Promise<RowCells | undefined> {
    const tableSchema = this.tableSchema(table);
    const query = this.prepareQuery(X.QueryT.from(table));
    const opts = {
      tier: "LocalOnly",
      includeDeleted: false,
      readView: X.ReadViewSpec.branchView(head, base),
      firstLoad: "Deliver",
    };
    const rows: CurrentRow[] = await this.allForIdentity(query, opts, identity);
    const candidate = rows.find((r) => X.deepEq(r.rowUuid(), row));
    if (!candidate) return undefined;
    const cells: RowCells = new Map();
    for (const column of tableSchema.columns) {
      const v = candidate.cell(tableSchema, column.name);
      if (v !== undefined) cells.set(column.name, v);
    }
    return cells;
  },

  async noOpUpdateHandle(
    this: Db,
    table: string,
    row: X.RowUuid,
    existing: CurrentRow | undefined,
  ): Promise<WriteHandle> {
    if (!existing) throw await this.updateTargetMissing(table, row);
    const txId = await lockNode(this, (n) => n.currentRowTxId(existing));
    if (!txId) throw dbError("NotObserved", "current row has no transaction");
    const localTier = this.writeState(txId).durability;
    const node = this.node.node;
    return new WriteHandle({ deref: () => node }, row, txId, localTier, undefined, undefined);
  },

  mergeExistingCells(
    this: Db,
    table: string,
    row: X.RowUuid,
    patch: RowCells,
  ): Promise<[RowCells, TxId | undefined, Set<string>]> {
    return this.mergeExistingCellsForClientIdentity(table, row, patch, this.identity.author);
  },

  async mergeExistingCellsForClientIdentity(
    this: Db,
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    identity: AuthorSubject,
  ): Promise<[RowCells, TxId | undefined, Set<string>]> {
    const schema = this.tableSchema(table);
    await this.ensureRowNotDeleted(table, row);
    const isPartial = schema.columns.some((c: any) => !patch.has(c.name));
    const authored = new Set(patch.keys());
    if (
      isPartial &&
      (X.authorSubjectEq(identity, X.AuthorSubjectT.SYSTEM) || schema.readPolicy === undefined)
    ) {
      const found = await lockNode(this, (n) =>
        n.currentPhysicalCellsAndWinnerInSchema(this.schemaVersionId, table, row),
      );
      if (!found) throw updateTargetNotLoaded("partial UPDATE", table, row);
      return [extend(found[0], patch), found[1], authored];
    }
    const existing = await this.localRowForClientIdentity(table, row, identity);
    if (!existing) throw await this.updateTargetMissing(table, row);
    const [cells, parent] = await lockNode(this, async (n) => {
      const cells: RowCells | undefined = await n.currentPhysicalCellsInSchema(
        this.schemaVersionId,
        table,
        row,
      );
      if (!cells) throw readForWriteDenied("UPDATE", table);
      return [cells, await n.currentRowTxId(existing)] as [RowCells, TxId | undefined];
    });
    return [extend(cells, patch), parent, authored];
  },

  async mergeExistingCellsForIdentity(
    this: Db,
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    identity: AuthorSubject,
  ): Promise<[RowCells, TxId | undefined, Set<string>]> {
    await this.ensureRowNotDeleted(table, row);
    if (this.authorizeReadForIdentity(table, row, identity).kind !== "Allowed")
      throw await this.updateTargetMissing(table, row);
    const authored = new Set(patch.keys());
    if (this.tableSchema(table).columns.every((c: any) => patch.has(c.name))) {
      const existing = await this.localCurrentRow(table, row);
      const parent = existing ? await lockNode(this, (n) => n.currentRowTxId(existing)) : undefined;
      return [patch, parent, authored];
    }
    const found = await lockNode(this, (n) =>
      n.currentPhysicalCellsAndWinnerInSchema(this.schemaVersionId, table, row),
    );
    if (!found) throw updateTargetNotLoaded("partial UPDATE", table, row);
    return [extend(found[0], patch), found[1], authored];
  },
};

/**
 * The thin client's guard over end-to-end encrypted columns. The thin
 * client holds no keys, so it must never send the plaintext of an encrypted
 * column, nor hand the app a stored (sealed) cell as if it were plaintext.
 *
 * Every thin request is described here first: its table, values, partial-edit
 * descriptors, streamed column, query and transaction. {@link
 * EncryptedGuard.admit} decides whether it touches encryption and only then
 * encodes it into the {@link ThinRequest} the wire takes. A request that
 * touches encryption throws {@link EncryptionNeedsLocalFirstError} before
 * anything is encoded. A shape the guard cannot prove plain counts as
 * touching.
 *
 * It reads only the app's encryption declarations, never keys or
 * membership, and loads nothing of the local-first client.
 */
import type { InsertValues, Value, WasmSchema } from "../drivers/types.js";
import { declaredEntries } from "../encryption/plain-schema.js";
import {
  EncryptionNeedsLocalFirstError,
  type EncryptionNeedsLocalFirstReason,
} from "../runtime/encryption-needs-local-first.js";
import { historySourceTable } from "../runtime/history-relation.js";
import {
  encodeCellsForPatch,
  encodeCellsForRow,
  encodeCellsForStreamingPatch,
  encodeCellsForStreamingRow,
  encodeQueryJson,
} from "../runtime/native-runtime/native-runtime-adapter.js";
import { stripColumnQualifier } from "../runtime/query-column-name.js";
import type { ThinOp, ThinRequest } from "./wire.js";

/** Why a refused operation can't wait here: decided by who opened the thin `Db`. */
export type ThinRefusalReason = Extract<
  EncryptionNeedsLocalFirstReason,
  "no-loader" | "direct-thin"
>;

/** A stream whose begin the guard admitted: its chunks and finish need it. */
export interface AdmittedStream {
  readonly id: number;
}

/** A thin request as the runtime asks for it, before it is encoded. */
export type ThinRequestDescription =
  | { type: "schema"; schema: Uint8Array }
  | { type: "subscribe"; queryJson: string; options: object }
  | { type: "unsubscribe"; subscription: number }
  | { type: "query"; queryJson: string; options: object; transaction?: string }
  | { type: "insert"; table: string; values: InsertValues; options: object; transaction?: string }
  | {
      type: "update" | "upsert";
      table: string;
      row: Uint8Array;
      values: Record<string, Value>;
      options: object;
      transaction?: string;
    }
  | { type: "delete"; table: string; row: Uint8Array; options: object; transaction?: string }
  | {
      type: "restore";
      table: string;
      row: Uint8Array;
      values: InsertValues;
      options: object;
      transaction?: string;
    }
  | {
      type: "updateLargeValues";
      table: string;
      row: Uint8Array;
      values: Record<string, Value>;
      descriptors: readonly unknown[];
      updatedAtMs?: number;
      transaction?: string;
    }
  | { type: "begin"; transaction: string; kind: string }
  | { type: "commit" | "rollback"; transaction: string }
  | { type: "captureSnapshot" }
  | { type: "streamChunk"; stream: AdmittedStream; chunk: Uint8Array }
  | { type: "finishStream" | "abortStream"; stream: AdmittedStream };

/** A streaming write's begin. `stream` is the request id its chunks name. */
export type BeginStreamDescription = {
  type: "beginStream";
  stream: number;
  table: string;
  row: Uint8Array;
  values: InsertValues;
  column: string;
  mutation: "insert" | "update" | "upsert";
  options: object;
  transaction?: string;
};

type Entry = {
  table: string;
  /** Encrypted columns, and the equality token columns of those declared for equality. */
  sealed: Set<string>;
  space: string | undefined;
  /** Every stored column, for columns the guard does not know. */
  stored: Set<string>;
};

const EQUALITY_PREFIX = "__e2ee_eq_";
const SYSTEM_PREFIX = "__e2ee_";

/** Keys of a flat runtime query whose shape {@link EncryptedGuard} reads. */
const FLAT_QUERY_KEYS = new Set([
  "table",
  "conditions",
  "array_subqueries",
  "include_deleted",
  "select_columns",
  "order_by",
  "limit",
  "offset",
]);

const SELECT_KINDS = new Set(["full", "bytes", "text_utf16", "text_utf8", "json_pointer"]);
const DESCRIPTOR_KINDS = new Set(["splice", "json_set", "text_edit"]);

/** A query that touches `table` (and `column`, when one column decided it). */
type Touch = { table: string; column?: string };

export class EncryptedGuard {
  private readonly entries = new Map<string, Entry>();
  private readonly streams = new WeakSet<AdmittedStream>();

  /**
   * @throws when `schema` has encryption tables but no declarations to
   * classify requests by: the app's bindings did not come with it.
   */
  constructor(
    private readonly schema: WasmSchema,
    private readonly reason: () => ThinRefusalReason = () => "no-loader",
  ) {
    const declared = declaredEntries(schema);
    if (!declared && Object.keys(schema).some((table) => table.startsWith(SYSTEM_PREFIX))) {
      throw new Error(
        "This schema has end-to-end encryption tables but no encryption declarations, so the thin client can't tell which columns are encrypted. Open it with the app's generated bindings (`app` from jazz.generated.ts).",
      );
    }
    for (const [table, entry] of Object.entries(declared ?? {})) {
      const columns = Object.keys(entry.columns);
      if (columns.length === 0) continue;
      this.entries.set(table, {
        table,
        sealed: new Set([
          ...columns,
          ...(entry.equality ?? []).map((column) => `${EQUALITY_PREFIX}${column}`),
        ]),
        space: (entry as { space?: unknown }).space as string | undefined,
        stored: new Set(schema[table]?.columns.map((column) => column.name) ?? []),
      });
    }
  }

  /**
   * Classify `request` and encode it for the wire.
   *
   * @throws EncryptionNeedsLocalFirstError when it touches an encrypted
   * column, before anything is encoded.
   * @throws Error for a partial edit of an encrypted column, which the
   * local-first client refuses too.
   */
  admit(request: BeginStreamDescription): { request: ThinRequest; stream: AdmittedStream };
  admit(request: ThinRequestDescription): ThinRequest;
  admit(
    request: ThinRequestDescription | BeginStreamDescription,
  ): ThinRequest | { request: ThinRequest; stream: AdmittedStream } {
    this.check(request);
    if (request.type === "beginStream") {
      const stream: AdmittedStream = Object.freeze({ id: request.stream });
      this.streams.add(stream);
      return { request: this.encode(request), stream };
    }
    return this.encode(request);
  }

  /** `request` with a write key in its options; nothing else changes. */
  keyed(request: ThinRequest, writeKey: object): ThinRequest {
    const options = (request as { options?: object }).options;
    if (options === undefined) return request;
    return { ...request, options: { ...options, writeKey } } as ThinRequest;
  }

  // ---- classification ---------------------------------------------------

  private check(request: ThinRequestDescription | BeginStreamDescription): void {
    switch (request.type) {
      case "schema":
      case "unsubscribe":
      case "begin":
      case "commit":
      case "rollback":
      case "captureSnapshot":
      case "delete":
        return;
      case "streamChunk":
      case "finishStream":
      case "abortStream":
        if (!this.streams.has(request.stream)) {
          throw new Error(`${request.type} names a stream the thin client did not begin`);
        }
        if (request.type !== "streamChunk") this.streams.delete(request.stream);
        return;
      case "query":
      case "subscribe": {
        const touch = this.queryTouches(request.queryJson);
        if (touch) this.refuse(request.type, touch.table, touch.column);
        return;
      }
      case "insert":
      case "upsert":
      case "restore": {
        // Every encrypted cell of a row is stored, null included: a whole
        // row always touches.
        const entry = this.entries.get(request.table);
        if (entry) this.refuse(request.type, entry.table, firstSealed(entry, request.values));
        return;
      }
      case "update":
        this.checkPatch("update", request.table, request.values);
        return;
      case "updateLargeValues":
        this.checkDescriptors(request.table, request.descriptors);
        this.checkPatch("update", request.table, request.values);
        return;
      case "beginStream": {
        const entry = this.entries.get(request.table);
        if (!entry) return;
        const operation = `${request.mutation} streaming`;
        if (request.mutation !== "update") {
          this.refuse(operation, entry.table, firstSealed(entry, request.values));
        }
        if (this.columnTouches(entry, request.column)) {
          this.refuse(operation, entry.table, request.column);
        }
        this.checkPatch(operation, request.table, request.values);
        return;
      }
      default:
        // A request this guard does not know: refused, not sent.
        throw new Error(
          `the thin client cannot send a ${(request as { type: string }).type} request`,
        );
    }
  }

  private refuse(operation: string, table: string, column?: string): never {
    throw new EncryptionNeedsLocalFirstError({
      reason: this.reason(),
      operation,
      table,
      ...(column === undefined ? {} : { column }),
    });
  }

  /** A patch touches when it names an encrypted, equality, space or unknown column. */
  private checkPatch(operation: string, table: string, values: Record<string, unknown>): void {
    const entry = this.entries.get(table);
    if (!entry) return;
    for (const column of Object.keys(values)) {
      if (this.columnTouches(entry, column)) this.refuse(operation, table, column);
    }
  }

  /** Whether writing `column` of `entry`'s table touches encryption. */
  private columnTouches(entry: Entry, column: string): boolean {
    // Only the local-first client knows whether a space value changes.
    return entry.sealed.has(column) || column === entry.space || !entry.stored.has(column);
  }

  /** A partial edit of an encrypted column is refused, as local-first refuses it. */
  private checkDescriptors(table: string, descriptors: readonly unknown[]): void {
    const entry = this.entries.get(table);
    if (!entry) return;
    for (const descriptor of descriptors) {
      const { kind, column } = (descriptor ?? {}) as { kind?: unknown; column?: unknown };
      if (typeof kind !== "string" || !DESCRIPTOR_KINDS.has(kind) || typeof column !== "string") {
        this.refuse("update", table);
      }
      if (entry.sealed.has(column)) {
        throw new Error(
          `reading or writing part of encrypted column \`${table}.${column}\` is not supported yet`,
        );
      }
      if (!entry.stored.has(column)) this.refuse("update", table, column);
    }
  }

  /**
   * The encrypted table (and column) a runtime query touches, or undefined
   * when it is proven plain: it names no encrypted table anywhere, or it
   * reads one encrypted table at its root through an explicit column list,
   * and the list, its filters and its ordering name plain columns only.
   */
  queryTouches(queryJson: string): Touch | undefined {
    if (this.entries.size === 0) return undefined;
    const query = JSON.parse(queryJson) as unknown;
    const named = this.namedEncryptedTable(query);
    if (named === undefined) return undefined;
    if (!isRecord(query) || query.table !== named) return { table: named };
    const entry = this.entries.get(named)!;
    for (const key of Object.keys(query)) {
      if (!FLAT_QUERY_KEYS.has(key)) return { table: named };
    }
    const subqueries = query.array_subqueries;
    if (subqueries !== undefined && !(Array.isArray(subqueries) && subqueries.length === 0)) {
      return { table: named };
    }
    const plain = (column: unknown): Touch | undefined => {
      if (typeof column !== "string") return { table: named };
      const name = stripColumnQualifier(column);
      const base = name.startsWith("$") ? name.split(".")[0]! : name;
      if (entry.sealed.has(base)) return { table: named, column: base };
      if (base === "id" || base.startsWith("$") || entry.stored.has(base)) return undefined;
      return { table: named, column: base };
    };
    // An explicit column list: without one, every column is read.
    const select = query.select_columns;
    if (!Array.isArray(select) || select.length === 0) return { table: named };
    for (const projection of select) {
      if (typeof projection === "string") {
        const touch = plain(projection);
        if (touch) return touch;
        continue;
      }
      if (!isRecord(projection) || typeof projection.kind !== "string") return { table: named };
      if (!SELECT_KINDS.has(projection.kind)) return { table: named };
      const touch = plain(projection.column);
      if (touch) return touch;
    }
    const conditions = query.conditions;
    if (conditions !== undefined) {
      if (!Array.isArray(conditions)) return { table: named };
      for (const condition of conditions) {
        const touch = predicateTouches(condition, plain, named);
        if (touch) return touch;
      }
    }
    const orderBy = query.order_by;
    if (orderBy !== undefined) {
      if (!Array.isArray(orderBy)) return { table: named };
      for (const term of orderBy) {
        const column = Array.isArray(term) ? term[0] : isRecord(term) ? term.column : undefined;
        const touch = plain(column);
        if (touch) return touch;
      }
    }
    return undefined;
  }

  /**
   * An encrypted table `value` names anywhere, as a table, a history
   * relation or a qualifier, or undefined. Any string counts, so a literal
   * that happens to spell one also does: that refuses a plain query, never
   * lets a touching one through.
   */
  private namedEncryptedTable(value: unknown): string | undefined {
    if (typeof value === "string") {
      const source = historySourceTable(value) ?? value;
      if (this.entries.has(source)) return source;
      const qualifier = source.indexOf(".");
      if (qualifier > 0 && this.entries.has(source.slice(0, qualifier))) {
        return source.slice(0, qualifier);
      }
      return undefined;
    }
    if (Array.isArray(value)) {
      for (const inner of value) {
        const named = this.namedEncryptedTable(inner);
        if (named !== undefined) return named;
      }
      return undefined;
    }
    if (isRecord(value)) {
      // The root table first, so it names the error.
      const root = this.namedEncryptedTable(value.table);
      if (root !== undefined) return root;
      for (const inner of Object.values(value)) {
        const named = this.namedEncryptedTable(inner);
        if (named !== undefined) return named;
      }
    }
    return undefined;
  }

  // ---- encoding ---------------------------------------------------------

  private encode(request: ThinRequestDescription | BeginStreamDescription): ThinRequest {
    let op: ThinOp;
    switch (request.type) {
      case "schema":
      case "unsubscribe":
      case "begin":
      case "commit":
      case "rollback":
      case "captureSnapshot":
      case "delete":
        op = request;
        break;
      case "streamChunk":
        op = { type: "streamChunk", stream: request.stream.id, chunk: request.chunk };
        break;
      case "finishStream":
      case "abortStream":
        op = { type: request.type, stream: request.stream.id };
        break;
      case "query":
        op = {
          type: "query",
          query: encodeQueryJson(request.queryJson, this.schema),
          options: request.options,
          transaction: request.transaction,
        };
        break;
      case "subscribe":
        op = {
          type: "subscribe",
          query: encodeQueryJson(request.queryJson, this.schema),
          options: request.options,
        };
        break;
      case "insert":
        op = {
          type: "insert",
          table: request.table,
          cells: encodeCellsForRow(this.definition(request.table), request.values, request.table),
          options: request.options,
          transaction: request.transaction,
        };
        break;
      case "update":
        op = {
          type: "update",
          table: request.table,
          row: request.row,
          patch: encodeCellsForPatch(this.definition(request.table), request.values),
          options: request.options,
          transaction: request.transaction,
        };
        break;
      case "upsert":
        op = {
          type: "upsert",
          table: request.table,
          row: request.row,
          cells: encodeCellsForPatch(this.definition(request.table), request.values),
          options: request.options,
          transaction: request.transaction,
        };
        break;
      case "restore":
        op = {
          type: "restore",
          table: request.table,
          row: request.row,
          cells: encodeCellsForRow(this.definition(request.table), request.values, request.table),
          options: request.options,
          transaction: request.transaction,
        };
        break;
      case "updateLargeValues":
        op = {
          type: "updateLargeValues",
          table: request.table,
          row: request.row,
          patch: encodeCellsForPatch(this.definition(request.table), request.values),
          mutations: request.descriptors,
          updatedAtMs: request.updatedAtMs,
          transaction: request.transaction,
        };
        break;
      case "beginStream": {
        const definition = this.definition(request.table);
        op = {
          type: "beginStream",
          table: request.table,
          row: request.row,
          cells:
            request.mutation === "insert"
              ? encodeCellsForStreamingRow(
                  definition,
                  request.values,
                  request.column,
                  request.table,
                )
              : encodeCellsForStreamingPatch(definition, request.values, request.column),
          column: request.column,
          mutation: request.mutation,
          options: request.options,
          transaction: request.transaction,
        };
        break;
      }
    }
    return op as ThinRequest;
  }

  private definition(table: string): WasmSchema[string] {
    const definition = this.schema[table];
    if (!definition) throw new Error(`unknown table ${table}`);
    return definition;
  }
}

/** The first encrypted column `values` names, for the error. */
function firstSealed(entry: Entry, values: Record<string, unknown>): string | undefined {
  const column = Object.keys(values).find((name) => entry.sealed.has(name));
  return column?.startsWith(EQUALITY_PREFIX) ? column.slice(EQUALITY_PREFIX.length) : column;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * What a filter of the runtime query IR touches: a column `plain` rejects,
 * or the table for a shape this does not read.
 */
function predicateTouches(
  predicate: unknown,
  plain: (column: unknown) => Touch | undefined,
  table: string,
): Touch | undefined {
  if (predicate === "True" || predicate === "False") return undefined;
  if (!isRecord(predicate)) return { table };
  const keys = Object.keys(predicate);
  if (keys.length !== 1) return { table };
  const key = keys[0]!;
  const body = predicate[key];
  const ref = (value: unknown): Touch | undefined => {
    if (!isRecord(value)) return { table };
    for (const inner of Object.keys(value)) {
      if (inner !== "column" && inner !== "scope") return { table };
    }
    return plain(value.column);
  };
  const operand = (value: unknown): Touch | undefined => {
    if (!isRecord(value)) return { table };
    const inner = Object.keys(value);
    return inner.length === 1 && (inner[0] === "Literal" || inner[0] === "RowId")
      ? undefined
      : { table };
  };
  const only = (record: Record<string, unknown>, allowed: string[]) =>
    Object.keys(record).every((inner) => allowed.includes(inner));
  switch (key) {
    case "And":
    case "Or": {
      if (!Array.isArray(body)) return { table };
      for (const child of body) {
        const touch = predicateTouches(child, plain, table);
        if (touch) return touch;
      }
      return undefined;
    }
    case "Not":
      return predicateTouches(body, plain, table);
    case "IsNull":
    case "IsNotNull":
      return isRecord(body) && Object.keys(body).length === 1 ? ref(body.column) : { table };
    case "Cmp":
    case "Contains":
      if (!isRecord(body) || !only(body, ["left", "op", "right"])) return { table };
      return ref(body.left) ?? operand(body.right);
    case "In": {
      if (!isRecord(body) || !only(body, ["left", "values"]) || !Array.isArray(body.values))
        return { table };
      const touch = ref(body.left);
      if (touch) return touch;
      for (const value of body.values) {
        const inner = operand(value);
        if (inner) return inner;
      }
      return undefined;
    }
    default:
      return { table };
  }
}

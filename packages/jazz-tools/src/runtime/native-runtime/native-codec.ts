import {
  type NativeRowBatch,
  type NativeRelationSubscriptionSnapshot,
  type NativeRemovedRow,
  type NativeSubscriptionDelta,
  type DescriptorField,
  createRecord,
  readNativeRowBatch,
  readNativeRelationSubscriptionSnapshot,
  readNativeRemovedRow,
  readNativeSubscriptionDelta,
  writeDescriptor,
  writeValueType,
} from "./native-row-codec.js";
import { exactSignedI64 } from "./exact-integer.js";

const fatalUtf8Decoder = new TextDecoder("utf-8", { fatal: true });

export {
  createRecord,
  decodeRecordBool,
  decodeRecordBytes,
  decodeRecordString,
  fieldIndex,
  nativeRowDescriptorPublicName,
  readNativeRowBatch,
  readNativeRelationSubscriptionSnapshot,
  readNativeRemovedRow,
  readNativeSubscriptionDelta,
  readDescriptor,
  readNativeRowDescriptor,
  readValueType,
  writeDescriptor,
  writeNativeRowDescriptor,
  writeValueType,
} from "./native-row-codec.js";
export type {
  NativeRelationSubscriptionSnapshot,
  NativeRemovedRow,
  NativeRow,
  NativeRowBatch,
  NativeRowDescriptorField,
  NativeSubscriptionDelta,
  DescriptorField,
  ValueType,
} from "./native-row-codec.js";

export type SubscriptionSnapshotChunk = {
  type: "snapshot";
  rows: NativeRowBatch[];
  settled?: boolean;
  tier?: string;
};
export type SubscriptionDeltaChunk = {
  type: "delta";
  delta: NativeSubscriptionDelta;
  settled?: boolean;
  tier?: string;
};
export type SubscriptionRejectedChunk = {
  type: "rejected";
  reason:
    | {
        type: "UnsupportedShapeCapability";
        detail: string;
      }
    | {
        type: "ServerFailure";
        code: string;
      }
    | {
        type: "InvalidAuthoritySourceClosure";
        transition: string;
      };
};
export type SubscriptionStreamChunk =
  | SubscriptionSnapshotChunk
  | SubscriptionDeltaChunk
  | SubscriptionRejectedChunk;

/** A self-signed proof may derive a Jazz-owned client author only at DB open. */
export type NativeSelfSignedClientProof = {
  token: string;
  appId: string;
  claimedAuthor: string;
};

// The proof-bearing native open ABI is the only authority that may derive a
// first-party reserved author. Its ordinary config field is still decoded by
// the untrusted ingress first, so it must carry a harmless external placeholder
// rather than the verified local-first/anonymous identity supplied by the
// proof. The runtime keeps the real author separately for peer attribution.
const SELF_SIGNED_OPEN_CONFIG_AUTHOR = new TextEncoder().encode(
  '["https://jazz.invalid","self-signed-open"]',
);

/** Choose the untrusted config author for a native database open. */
export function authorForNativeOpenConfig(
  author: Uint8Array,
  selfSignedClientProof?: NativeSelfSignedClientProof,
): Uint8Array {
  return selfSignedClientProof ? SELF_SIGNED_OPEN_CONFIG_AUTHOR : author;
}

export async function readSubscriptionSnapshot(
  reader: ReadableStreamDefaultReader<SubscriptionStreamChunk>,
): Promise<SubscriptionSnapshotChunk> {
  const next = await reader.read();
  if (next.done || next.value.type !== "snapshot") {
    throw new Error("expected subscription snapshot chunk");
  }
  return next.value;
}

export async function readSubscriptionDelta(
  reader: ReadableStreamDefaultReader<SubscriptionStreamChunk>,
): Promise<SubscriptionDeltaChunk> {
  const next = await reader.read();
  if (next.done || next.value.type !== "delta") {
    throw new Error("expected subscription delta chunk");
  }
  return next.value;
}

export function tableSchema(tableName: string, descriptor: DescriptorField[]): Uint8Array {
  const writer = new PostcardWriter();
  writer.vec((table) => {
    table.string(tableName);
    table.vec((column, index) => {
      const columnSpec = descriptor[index];
      column.string(columnSpec.name ?? "");
      writeValueType(column, columnSpec.valueType);
      column.none();
      column.none();
    }, descriptor.length);
    table.map(0);
    table.none();
    table.none();
    table.none();
    table.none();
    table.none();
    table.set(0);
    table.map(0);
  }, 1);
  writer.none();
  writer.none();
  return writer.finish();
}

export function openConfig(
  node: Uint8Array,
  author: Uint8Array,
  sourceId?: number,
  historyComplete = false,
  initialSyncFlushEvery?: number,
  selfSignedClientProof?: NativeSelfSignedClientProof,
): Uint8Array {
  const writer = new PostcardWriter();
  writer.bytes(node);
  writer.string(canonicalOpenAuthor(authorForNativeOpenConfig(author, selfSignedClientProof)));
  if (sourceId == null) {
    writer.none();
  } else {
    writer.some((value) => value.u64(sourceId));
  }
  writer.bool(historyComplete);
  if (initialSyncFlushEvery == null) {
    writer.none();
  } else {
    writer.some((value) => value.u64(initialSyncFlushEvery));
  }
  // The native binding keeps a trailing `backend_credential` slot solely to
  // reject legacy raw ingress. New configs must never serialize one.
  writer.none();
  return writer.finish();
}

/**
 * Core open configuration carries a portable canonical `[issuer, subject]`
 * JSON author. There is deliberately no UUID fallback.
 */
function canonicalOpenAuthor(author: Uint8Array): string {
  try {
    const canonical = new TextDecoder("utf-8", { fatal: true }).decode(author);
    const parsed = JSON.parse(canonical);
    if (
      Array.isArray(parsed) &&
      parsed.length === 2 &&
      typeof parsed[0] === "string" &&
      typeof parsed[1] === "string" &&
      JSON.stringify(parsed) === canonical
    ) {
      return canonical;
    }
  } catch {
    // Fall through to the consistent public-boundary diagnostic.
  }
  throw new Error("native open config author must be canonical UTF-8 JSON [issuer, subject]");
}

export function queryFromTable(table: string): Uint8Array {
  return queryWithPredicates(table, []);
}

export function queryWhereBool(table: string, column: string, value: boolean): Uint8Array {
  return queryWithEqFilters(table, [{ column, value: { type: "Boolean", value } }]);
}

export function queryWhereStringContains(
  table: string,
  column: string,
  value: string,
  limit?: number,
): Uint8Array {
  return queryWithPredicates(
    table,
    [{ column, op: "Contains", value: { type: "Text", value } }],
    limit,
  );
}

export type QueryLiteral =
  | { type: "Boolean"; value: boolean }
  | { type: "Integer"; value: number }
  | { type: "BigInt"; value: bigint }
  | { type: "Double"; value: number }
  | { type: "Timestamp"; value: number }
  | { type: "Text"; value: string }
  | { type: "Uuid"; value: string }
  | { type: "Bytea"; value: Uint8Array }
  | { type: "Array"; value: QueryLiteral[] }
  | { type: "Nullable"; value: QueryLiteral | null };

export type QueryPredicate =
  | {
      op: "All" | "Any";
      predicates: QueryPredicate[];
    }
  | {
      op: "Not";
      predicate: QueryPredicate;
    }
  | {
      column: string;
      op: QueryPredicateOp;
      value: QueryLiteral;
    }
  | {
      column: string;
      op: "In";
      values: QueryLiteral[];
    }
  | {
      column: string;
      op: "Contains";
      value: QueryLiteral;
    }
  | {
      column: string;
      op: "EnumMatch";
      case: string;
      payload: QueryPredicate;
    }
  | {
      column: string;
      op: "IsNull";
    }
  | {
      column: string;
      op: "IsNotNull";
    };

export type QueryPredicateOp = "Eq" | "Ne" | "Gt" | "Gte" | "Lt" | "Lte";
export type QueryOrder = {
  column: string;
  direction: "Asc" | "Desc";
};
export type QueryArraySubqueryRequirement =
  | "Optional"
  | "AtLeastOne"
  | "MatchCorrelationCardinality";
export type QueryArraySubquery = {
  columnName: string;
  table: string;
  innerColumn: string;
  outerColumn: string;
  filters?: QueryPredicate[];
  select?: string[];
  orderBy?: QueryOrder[];
  limit?: number | null;
  offset?: number;
  requirement?: QueryArraySubqueryRequirement;
  nestedArrays?: QueryArraySubquery[];
};

export type QueryOptions = {
  limit?: number;
  offset?: number;
  orderBy?: QueryOrder[];
  select?: string[];
  arraySubqueries?: QueryArraySubquery[];
  /** Relation expression carried in Query.relation by the public IR adapter. */
  relation?: unknown;
};

export function queryWithEqFilters(
  table: string,
  filters: Array<{ column: string; value: QueryLiteral }>,
  limit?: number,
): Uint8Array {
  return queryWithPredicates(
    table,
    filters.map((filter) => ({ ...filter, op: "Eq" })),
    limit,
  );
}

export function queryWithPredicates(
  table: string,
  predicates: QueryPredicate[],
  options: number | QueryOptions = {},
): Uint8Array {
  const queryOptions = typeof options === "number" ? { limit: options } : options;
  const { limit, offset = 0, orderBy = [], select, arraySubqueries = [], relation } = queryOptions;
  if (limit != null) validateQueryBound("query limit", limit);
  validateQueryBound("query offset", offset);
  const writer = new PostcardWriter();
  writer.string(table);
  writer.vec((filter, index) => {
    const predicate = predicates[index]!;
    writePredicate(filter, predicate);
  }, predicates.length);
  writer.vec(() => undefined, 0);
  // Query.flat_join follows joins in the postcard struct layout. The public
  // runtime does not lower flat joins yet, so encode the absent option.
  writer.none();
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec((subquery, index) => {
    writeArraySubquery(subquery, arraySubqueries[index]!);
  }, arraySubqueries.length);
  if (select == null) {
    writer.none();
  } else {
    writer.some((selectWriter) => {
      selectWriter.vec((columnWriter, index) => {
        columnWriter.string(select[index]!);
      }, select.length);
    });
  }
  writer.vec((order, index) => {
    const term = orderBy[index]!;
    order.string(term.column);
    order.u64(term.direction === "Asc" ? 0 : 1);
  }, orderBy.length);
  writer.none();
  if (limit == null) {
    writer.none();
  } else {
    writer.some((valueWriter) => valueWriter.u64(limit));
  }
  writer.u64(offset);
  // `relation` is skip-serialized when absent by the Rust Query envelope.
  // A retained relation writes its Option::Some discriminant and tree.
  if (relation != null)
    writer.some((relationWriter) => writeRelationExpr(relationWriter, relation));
  return writer.finish();
}

function relationRecord(value: unknown, context: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`Native runtime cannot encode invalid relation ${context}.`);
  }
  return value as Record<string, unknown>;
}

function relationString(value: unknown, context: string): string {
  if (typeof value !== "string")
    throw new Error(`Native runtime cannot encode relation ${context}.`);
  return value;
}

function relationNumber(value: unknown, context: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new Error(`Native runtime cannot encode relation ${context}.`);
  }
  return value as number;
}

function relationArray(value: unknown, context: string): unknown[] {
  if (!Array.isArray(value)) throw new Error(`Native runtime cannot encode relation ${context}.`);
  return value;
}

/** Encode the exact serde/postcard RelationQuery.rel tree emitted by the public adapter. */
function writeRelationExpr(writer: PostcardWriter, value: unknown): void {
  const relation = relationRecord(value, "expression");
  const entries = Object.entries(relation);
  if (entries.length !== 1)
    throw new Error("Native runtime cannot encode ambiguous relation expression.");
  const [kind, payload] = entries[0]!;
  const node = relationRecord(payload, `${kind} payload`);
  switch (kind) {
    case "TableScan":
      writer.u64(0);
      writer.string(relationString(node.table, "TableScan.table"));
      if (node.alias == null) writer.none();
      else writer.some((out) => out.string(relationString(node.alias, "TableScan.alias")));
      return;
    case "Filter":
      writer.u64(1);
      writeRelationExpr(writer, node.input);
      writeRelationPredicate(writer, node.predicate);
      return;
    case "Union": {
      writer.u64(2);
      const inputs = relationArray(node.inputs, "Union.inputs");
      writer.vec((out, index) => {
        const arm = relationRecord(inputs[index], "Union arm");
        out.string(relationString(arm.label, "Union arm label"));
        writeRelationExpr(out, arm.input);
      }, inputs.length);
      return;
    }
    case "Join": {
      writer.u64(3);
      writeRelationExpr(writer, node.left);
      writeRelationExpr(writer, node.right);
      const conditions = relationArray(node.on, "Join.on");
      writer.vec((out, index) => {
        const condition = relationRecord(conditions[index], "Join condition");
        writeRelationColumn(out, condition.left);
        writeRelationColumn(out, condition.right);
      }, conditions.length);
      const joinKind = relationString(node.join_kind, "Join.join_kind");
      if (joinKind === "Inner") writer.u64(0);
      else if (joinKind === "Left") writer.u64(1);
      else throw new Error("Native runtime cannot encode relation Join.join_kind.");
      return;
    }
    case "Project": {
      writer.u64(4);
      writeRelationExpr(writer, node.input);
      const columns = relationArray(node.columns, "Project.columns");
      writer.vec((out, index) => {
        const column = relationRecord(columns[index], "Project column");
        out.string(relationString(column.alias, "Project column alias"));
        writeRelationProjectExpr(out, column.expr);
      }, columns.length);
      return;
    }
    case "Gather": {
      writer.u64(5);
      writeRelationExpr(writer, node.seed);
      writeRelationExpr(writer, node.step);
      writeRelationKey(writer, node.frontier_key);
      writeRelationBound(writer, node.bound);
      const keys = relationArray(node.dedupe_key, "Gather.dedupe_key");
      writer.vec((out, index) => writeRelationKey(out, keys[index]), keys.length);
      return;
    }
    case "Distinct": {
      writer.u64(6);
      writeRelationExpr(writer, node.input);
      const keys = relationArray(node.key, "Distinct.key");
      writer.vec((out, index) => writeRelationKey(out, keys[index]), keys.length);
      return;
    }
    case "OrderBy": {
      writer.u64(7);
      writeRelationExpr(writer, node.input);
      const terms = relationArray(node.terms, "OrderBy.terms");
      writer.vec((out, index) => {
        const term = relationRecord(terms[index], "OrderBy term");
        writeRelationColumn(out, term.column);
        const direction = relationString(term.direction, "OrderBy direction");
        if (direction === "Asc") out.u64(0);
        else if (direction === "Desc") out.u64(1);
        else throw new Error("Native runtime cannot encode relation OrderBy.direction.");
      }, terms.length);
      return;
    }
    case "Offset":
      writer.u64(8);
      writeRelationExpr(writer, node.input);
      writer.u64(relationNumber(node.offset, "Offset.offset"));
      return;
    case "Limit":
      writer.u64(9);
      writeRelationExpr(writer, node.input);
      writer.u64(relationNumber(node.limit, "Limit.limit"));
      return;
    default:
      throw new Error(`Native runtime cannot encode relation operator ${kind}.`);
  }
}

function writeRelationColumn(writer: PostcardWriter, value: unknown): void {
  const column = relationRecord(value, "column reference");
  if (column.scope == null) writer.none();
  else writer.some((out) => out.string(relationString(column.scope, "column scope")));
  writer.string(relationString(column.column, "column name"));
}

function writeRelationKey(writer: PostcardWriter, value: unknown): void {
  const key = relationRecord(value, "key reference");
  if ("Column" in key) {
    writer.u64(0);
    writeRelationColumn(writer, key.Column);
    return;
  }
  if ("RowId" in key) {
    writer.u64(1);
    writeRelationRowId(writer, key.RowId);
    return;
  }
  throw new Error("Native runtime cannot encode relation key reference.");
}

function writeRelationProjectExpr(writer: PostcardWriter, value: unknown): void {
  const expr = relationRecord(value, "project expression");
  if ("Column" in expr) {
    writer.u64(0);
    writeRelationColumn(writer, expr.Column);
    return;
  }
  if ("RowId" in expr) {
    writer.u64(1);
    writeRelationRowId(writer, expr.RowId);
    return;
  }
  throw new Error("Native runtime cannot encode relation project expression.");
}

function writeRelationRowId(writer: PostcardWriter, value: unknown): void {
  const tag = relationString(value, "row-id reference");
  if (tag === "Current") writer.u64(0);
  else if (tag === "Outer") writer.u64(1);
  else if (tag === "Frontier") writer.u64(2);
  else throw new Error("Native runtime cannot encode relation row-id reference.");
}

function writeRelationBound(writer: PostcardWriter, value: unknown): void {
  if (value === "Fixpoint") {
    writer.u64(0);
    return;
  }
  const bound = relationRecord(value, "recursion bound");
  if ("MaxDepth" in bound) {
    writer.u64(1);
    writer.u64(relationNumber(bound.MaxDepth, "MaxDepth"));
    return;
  }
  throw new Error("Native runtime cannot encode relation recursion bound.");
}

function writeRelationPredicate(writer: PostcardWriter, value: unknown): void {
  if (value === "True") {
    writer.u64(9);
    return;
  }
  if (value === "False") {
    writer.u64(10);
    return;
  }
  const predicate = relationRecord(value, "predicate");
  if ("Cmp" in predicate) {
    const cmp = relationRecord(predicate.Cmp, "Cmp");
    writer.u64(0);
    writeRelationColumn(writer, cmp.left);
    const op = relationString(cmp.op, "Cmp.op");
    const tag = ["Eq", "Ne", "Lt", "Le", "Gt", "Ge"].indexOf(op);
    if (tag < 0) throw new Error("Native runtime cannot encode relation comparison operator.");
    writer.u64(tag);
    writeRelationValue(writer, cmp.right);
    return;
  }
  if ("IsNull" in predicate) {
    writer.u64(1);
    writeRelationColumn(writer, relationRecord(predicate.IsNull, "IsNull").column);
    return;
  }
  if ("IsNotNull" in predicate) {
    writer.u64(2);
    writeRelationColumn(writer, relationRecord(predicate.IsNotNull, "IsNotNull").column);
    return;
  }
  if ("In" in predicate) {
    const input = relationRecord(predicate.In, "In");
    writer.u64(3);
    writeRelationColumn(writer, input.left);
    const values = relationArray(input.values, "In.values");
    writer.vec((out, i) => writeRelationValue(out, values[i]), values.length);
    return;
  }
  if ("Contains" in predicate) {
    const input = relationRecord(predicate.Contains, "Contains");
    writer.u64(4);
    writeRelationColumn(writer, input.left);
    writeRelationValue(writer, input.right);
    return;
  }
  if ("EnumMatch" in predicate) {
    const input = relationRecord(predicate.EnumMatch, "EnumMatch");
    writer.u64(5);
    writeRelationColumn(writer, input.column);
    writer.string(relationString(input.case, "EnumMatch.case"));
    writeRelationPredicate(writer, input.payload);
    return;
  }
  if ("And" in predicate || "Or" in predicate) {
    const values = relationArray(predicate.And ?? predicate.Or, "logical predicate");
    writer.u64("And" in predicate ? 6 : 7);
    writer.vec((out, i) => writeRelationPredicate(out, values[i]), values.length);
    return;
  }
  if ("Not" in predicate) {
    writer.u64(8);
    writeRelationPredicate(writer, predicate.Not);
    return;
  }
  throw new Error("Native runtime cannot encode relation predicate.");
}

function writeRelationValue(writer: PostcardWriter, value: unknown): void {
  const reference = relationRecord(value, "value reference");
  if ("Literal" in reference) {
    writer.u64(0);
    writeJsonValue(writer, reference.Literal);
    return;
  }
  if ("Param" in reference) {
    writer.u64(1);
    writer.string(relationString(reference.Param, "Param"));
    return;
  }
  if ("SessionRef" in reference) {
    const path = relationArray(reference.SessionRef, "SessionRef");
    writer.u64(2);
    writer.vec((out, i) => out.string(relationString(path[i], "SessionRef entry")), path.length);
    return;
  }
  if ("OuterColumn" in reference) {
    writer.u64(3);
    writeRelationColumn(writer, reference.OuterColumn);
    return;
  }
  if ("FrontierColumn" in reference) {
    writer.u64(4);
    writeRelationColumn(writer, reference.FrontierColumn);
    return;
  }
  if ("RowId" in reference) {
    writer.u64(5);
    writeRelationRowId(writer, reference.RowId);
    return;
  }
  throw new Error("Native runtime cannot encode relation value reference.");
}

/** serde_json::Value's postcard representation is its ordinary serde value tree. */
function writeJsonValue(writer: PostcardWriter, value: unknown): void {
  if (value == null) {
    writer.none();
    return;
  }
  if (typeof value === "boolean") {
    writer.bool(value);
    return;
  }
  if (typeof value === "string") {
    writer.string(value);
    return;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value))
      throw new Error("Native runtime cannot encode non-finite relation JSON literal.");
    if (Number.isSafeInteger(value) && value >= 0) {
      writer.u64(value);
      return;
    }
    if (Number.isSafeInteger(value)) {
      writer.i64(BigInt(value));
      return;
    }
    writer.f64Le(value);
    return;
  }
  if (Array.isArray(value)) {
    writer.vec((out, i) => writeJsonValue(out, value[i]), value.length);
    return;
  }
  const object = relationRecord(value, "JSON literal");
  const entries = Object.entries(object);
  writer.map(entries.length);
  for (const [key, entry] of entries) {
    writer.string(key);
    writeJsonValue(writer, entry);
  }
}

function writeArraySubquery(writer: PostcardWriter, subquery: QueryArraySubquery): void {
  const {
    filters = [],
    select,
    orderBy = [],
    limit = null,
    offset = 0,
    requirement = "Optional",
    nestedArrays = [],
  } = subquery;
  if (limit != null) validateQueryBound(`array subquery ${subquery.columnName} limit`, limit);
  validateQueryBound(`array subquery ${subquery.columnName} offset`, offset);
  writer.string(subquery.columnName);
  writer.string(subquery.table);
  writer.string(subquery.innerColumn);
  writer.string(subquery.outerColumn);
  writer.vec((filter, index) => {
    writePredicate(filter, filters[index]!);
  }, filters.length);
  if (select == null) {
    writer.none();
  } else {
    writer.some((selectWriter) => {
      selectWriter.vec((columnWriter, index) => {
        columnWriter.string(select[index]!);
      }, select.length);
    });
  }
  writer.vec((order, index) => {
    const term = orderBy[index]!;
    order.string(term.column);
    order.u64(term.direction === "Asc" ? 0 : 1);
  }, orderBy.length);
  if (limit == null) {
    writer.none();
  } else {
    writer.some((valueWriter) => valueWriter.u64(limit));
  }
  writer.u64(offset);
  writer.u64(arraySubqueryRequirementTag(requirement));
  writer.vec((nested, index) => {
    writeArraySubquery(nested, nestedArrays[index]!);
  }, nestedArrays.length);
}

function validateQueryBound(label: string, value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${label} must be a non-negative safe integer`);
  }
}

function arraySubqueryRequirementTag(requirement: QueryArraySubqueryRequirement): number {
  switch (requirement) {
    case "Optional":
      return 0;
    case "AtLeastOne":
      return 1;
    case "MatchCorrelationCardinality":
      return 2;
  }
}

function writePredicate(writer: PostcardWriter, predicate: QueryPredicate): void {
  if (predicate.op === "All" || predicate.op === "Any") {
    writer.u64(predicate.op === "All" ? 0 : 1);
    writer.vec(
      (predicateWriter, index) => writePredicate(predicateWriter, predicate.predicates[index]!),
      predicate.predicates.length,
    );
    return;
  }
  if (predicate.op === "Not") {
    writer.u64(2); // Predicate::Not
    writePredicate(writer, predicate.predicate);
    return;
  }
  if (predicate.op === "In") {
    writer.u64(5); // Predicate::In
    writeColumnOperand(writer, predicate.column);
    writer.vec((valueWriter, index) => {
      valueWriter.u64(3); // Operand::Literal
      writeGrooveValue(valueWriter, predicate.values[index]!);
    }, predicate.values.length);
    return;
  }
  if (predicate.op === "Contains") {
    writer.u64(10); // Predicate::Contains
    writeColumnOperand(writer, predicate.column);
    writeLiteralOperand(writer, predicate.value);
    return;
  }
  if (predicate.op === "EnumMatch") {
    writer.u64(11); // Predicate::EnumMatch
    writeColumnOperand(writer, predicate.column);
    writer.string(predicate.case);
    writePredicate(writer, predicate.payload);
    return;
  }
  if (predicate.op === "IsNull" || predicate.op === "IsNotNull") {
    if (predicate.op === "IsNotNull") {
      writer.u64(2); // Predicate::Not
    }
    writer.u64(12); // Predicate::IsNull
    writeColumnOperand(writer, predicate.column);
    return;
  }
  if (isQueryPredicateCmp(predicate)) {
    writePredicateCmpLiteral(writer, predicate.column, predicate.op, predicate.value);
    return;
  }
  throw new Error(`unsupported query predicate ${JSON.stringify(predicate)}`);
}

function isQueryPredicateCmp(
  predicate: QueryPredicate,
): predicate is Extract<QueryPredicate, { op: QueryPredicateOp }> {
  return (
    predicate.op === "Eq" ||
    predicate.op === "Ne" ||
    predicate.op === "Gt" ||
    predicate.op === "Gte" ||
    predicate.op === "Lt" ||
    predicate.op === "Lte"
  );
}

function writePredicateCmpLiteral(
  writer: PostcardWriter,
  column: string,
  op: QueryPredicateOp,
  value: QueryLiteral,
): void {
  writer.u64(predicateOpTag(op));
  writeColumnOperand(writer, column);
  writeLiteralOperand(writer, value);
}

function writeColumnOperand(writer: PostcardWriter, column: string): void {
  writer.u64(0); // Operand::Column
  writer.string(column);
}

function writeLiteralOperand(writer: PostcardWriter, value: QueryLiteral): void {
  writer.u64(3); // Operand::Literal
  writeGrooveValue(writer, value);
}

function predicateOpTag(op: QueryPredicateOp): number {
  switch (op) {
    case "Eq":
      return 3; // Predicate::Eq
    case "Ne":
      return 4; // Predicate::Ne
    case "Gt":
      return 6; // Predicate::Gt
    case "Gte":
      return 7; // Predicate::Gte
    case "Lt":
      return 8; // Predicate::Lt
    case "Lte":
      return 9; // Predicate::Lte
  }
}

function writeGrooveValue(writer: PostcardWriter, value: QueryLiteral): void {
  if (value.type === "Nullable") {
    writer.u64(13); // groove::records::Value::Nullable
    if (value.value == null) {
      writer.none();
    } else {
      writer.some((inner) => writeGrooveValue(inner, value.value!));
    }
    return;
  }
  if (value.type === "Boolean") {
    writer.u64(5); // groove::records::Value::Bool
    writer.bool(value.value);
    return;
  }
  if (value.type === "Integer") {
    if (
      !Number.isSafeInteger(value.value) ||
      value.value < -0x80000000 ||
      value.value > 0x7fffffff
    ) {
      throw new Error("Integer value must be a signed 32-bit integer");
    }
    writer.u64(15); // groove::records::Value::I32
    writer.i64(value.value);
    return;
  }
  if (value.type === "BigInt") {
    if (value.value < -(1n << 63n) || value.value > (1n << 63n) - 1n) {
      throw new Error("BigInt value must be a signed 64-bit integer");
    }
    writer.u64(14); // groove::records::Value::I64
    writer.i64(value.value);
    return;
  }
  if (value.type === "Timestamp") {
    if (!Number.isSafeInteger(value.value) || value.value < 0) {
      throw new Error(`${value.type} value must be a non-negative safe integer`);
    }
    writer.u64(3); // groove::records::Value::U64
    writer.u64(value.value);
    return;
  }
  if (value.type === "Double") {
    if (!Number.isFinite(value.value)) {
      throw new Error("Double value must be finite");
    }
    writer.u64(4); // groove::records::Value::F64
    writer.f64Le(value.value);
    return;
  }
  if (value.type === "Uuid") {
    writer.u64(9); // groove::records::Value::Uuid
    writer.bytes(parseUuidBytes(value.value));
    return;
  }
  if (value.type === "Bytea") {
    writer.u64(7); // groove::records::Value::Bytes
    writer.bytes(value.value);
    return;
  }
  if (value.type === "Array") {
    writer.u64(12); // groove::records::Value::Array
    writer.vec((item, index) => writeGrooveValue(item, value.value[index]!), value.value.length);
    return;
  }
  writer.u64(6); // groove::records::Value::String
  writer.string(value.value);
}

function parseUuidBytes(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error(`invalid uuid ${value}`);
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i += 1) {
    bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

export function encodedCells(descriptor: DescriptorField[], values: Uint8Array[]): Uint8Array {
  const writer = new PostcardWriter();
  writeDescriptor(writer, descriptor);
  writer.bytes(createRecord(descriptor, values));
  return writer.finish();
}

export function rowCount(batches: NativeRowBatch[]): number {
  return batches.reduce((sum, batch) => sum + batch.rows.length, 0);
}

export class PostcardWriter {
  private chunks: number[] = [];

  finish(): Uint8Array {
    return Uint8Array.from(this.chunks);
  }

  u64(value: number | bigint): void {
    if (typeof value === "bigint") {
      if (value < 0n || value > 0xffff_ffff_ffff_ffffn) {
        throw new Error(`u64 out of range: ${value}`);
      }
      this.u64Big(value);
      return;
    }
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error(`u64 must be a non-negative safe integer, got ${value}`);
    }
    let remaining = value;
    do {
      let byte = remaining & 0x7f;
      remaining = Math.floor(remaining / 128);
      if (remaining !== 0) byte |= 0x80;
      this.chunks.push(byte);
    } while (remaining !== 0);
  }

  i64(value: bigint | number): void {
    const bigintValue = exactSignedI64(value, "i64");
    const encoded = bigintValue < 0n ? (-bigintValue << 1n) - 1n : bigintValue << 1n;
    this.u64Big(encoded);
  }

  private u64Big(value: bigint): void {
    let remaining = value;
    do {
      let byte = Number(remaining & 0x7fn);
      remaining >>= 7n;
      if (remaining !== 0n) byte |= 0x80;
      this.chunks.push(byte);
    } while (remaining !== 0n);
  }

  u32Le(value: number): void {
    if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) {
      throw new Error(`u32Le must be an unsigned 32-bit integer, got ${value}`);
    }
    this.chunks.push(
      value & 0xff,
      (value >>> 8) & 0xff,
      (value >>> 16) & 0xff,
      (value >>> 24) & 0xff,
    );
  }

  f64Le(value: number): void {
    const bytes = new Uint8Array(8);
    new DataView(bytes.buffer).setFloat64(0, value, true);
    this.bytes(bytes, false);
  }

  bool(value: boolean): void {
    this.chunks.push(value ? 1 : 0);
  }

  string(value: string): void {
    this.bytes(utf8(value));
  }

  bytes(value: Uint8Array, withLength = true): void {
    if (withLength) this.u64(value.length);
    for (let offset = 0; offset < value.length; offset += 16_384) {
      this.chunks.push(...value.subarray(offset, offset + 16_384));
    }
  }

  vec(writeItem: (writer: PostcardWriter, index: number) => void, length: number): void {
    this.u64(length);
    for (let index = 0; index < length; index += 1) {
      writeItem(this, index);
    }
  }

  map(length: number): void {
    this.u64(length);
  }

  set(length: number): void {
    this.u64(length);
  }

  none(): void {
    this.chunks.push(0);
  }

  some(writeValue: (writer: PostcardWriter) => void): void {
    this.chunks.push(1);
    writeValue(this);
  }

  enumUnit(index: number): void {
    this.u64(index);
  }
}

export class PostcardReader {
  private offset = 0;

  constructor(private readonly bytesValue: Uint8Array) {}

  u64(): number {
    const value = this.readCanonicalU64();
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new Error(`postcard u64 exceeds Number.MAX_SAFE_INTEGER: ${value}`);
    }
    return Number(value);
  }

  u64BigInt(): bigint {
    return this.readCanonicalU64();
  }

  i64(): bigint {
    const result = this.readCanonicalU64();
    return (result & 1n) === 0n ? result >> 1n : -((result + 1n) >> 1n);
  }

  private readCanonicalU64(): bigint {
    let result = 0n;
    for (let byteIndex = 0; byteIndex < 10; byteIndex += 1) {
      const byte = this.readByte();
      const payload = BigInt(byte & 0x7f);
      if (byteIndex === 9 && payload > 1n) throw new Error("postcard u64 overflow");

      result |= payload << BigInt(byteIndex * 7);
      if ((byte & 0x80) === 0) {
        if (byteIndex !== 0 && result < 1n << BigInt(byteIndex * 7)) {
          throw new Error("postcard u64 is not minimally encoded");
        }
        return result;
      }
    }
    throw new Error("postcard u64 overflow");
  }

  string(): string {
    return fatalUtf8Decoder.decode(this.bytes());
  }

  bool(): boolean {
    const tag = this.readByte();
    if (tag === 0) return false;
    if (tag === 1) return true;
    throw new Error(`invalid bool tag ${tag}`);
  }

  f64Le(): number {
    const bytes = this.bytesOfLength(8);
    return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getFloat64(0, true);
  }

  bytes(withLength = true): Uint8Array {
    const length = withLength ? this.u64() : 16;
    return this.bytesOfLength(length);
  }

  /**
   * Read a postcard byte string only after admitting its declared length.
   *
   * Protocol adapters use this before retaining attacker-controlled byte
   * strings. `bytesOfLength` also verifies that the admitted length remains
   * within the physical carrier.
   */
  bytesAtMost(maxLength: number, label: string): Uint8Array {
    const length = this.u64();
    if (length > maxLength) {
      throw new Error(`${label} exceeds maximum length of ${maxLength} bytes`);
    }
    return this.bytesOfLength(length);
  }

  private bytesOfLength(length: number): Uint8Array {
    const end = this.offset + length;
    if (end > this.bytesValue.length) throw new Error("postcard bytes overflow");
    const value = this.bytesValue.subarray(this.offset, end);
    this.offset = end;
    return value;
  }

  option<T>(readValue: (reader: PostcardReader) => T): T | undefined {
    const tag = this.readByte();
    if (tag === 0) return undefined;
    if (tag !== 1) throw new Error(`invalid option tag ${tag}`);
    return readValue(this);
  }

  readVec<T>(readItem: (reader: PostcardReader) => T): T[] {
    const length = this.u64();
    return Array.from({ length }, () => readItem(this));
  }

  done(): boolean {
    return this.offset === this.bytesValue.length;
  }

  private readByte(): number {
    if (this.offset >= this.bytesValue.length) throw new Error("postcard eof");
    return this.bytesValue[this.offset++];
  }
}

export function assertBytes(value: unknown, label: string): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(
      value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength),
    );
  }
  if (
    Array.isArray(value) &&
    value.every((byte) => Number.isInteger(byte) && byte >= 0 && byte <= 255)
  ) {
    return Uint8Array.from(value);
  }
  throw new Error(`expected ${label} to be bytes`);
}

export function normalizePayloadBytes(value: unknown, label = "payload"): Uint8Array {
  return assertBytes(value, label);
}

export function optionalNumber(value: unknown): number | undefined {
  return typeof value === "number" ? value : undefined;
}

export function utf8(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

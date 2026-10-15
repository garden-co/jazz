// Aggregate planning and evaluation over reconstructed input multisets.
import { type Bytes, compareBytes } from "../../../support/bytes.js";
import {
  BorrowedRecord,
  IvmRuntimeError,
  encodeKeyPart,
  remapEnum,
  remapEnumTag,
  resolveFieldName,
  type AggregateExpr,
  type AggregateOp,
  type PlanExpr,
  type RecordDelta,
  type RecordDescriptor,
  type Value,
} from "../../../_extern/groove-ivm-ops.js";
import { planExprField } from "./evaluator.js";
import { recordsBeforeDeltas } from "./windows.js";

type Weighted = [Bytes, number];

export function resolveAggregateExpr(
  input: RecordDescriptor,
  aggregate: AggregateExpr,
): AggregateExpr {
  // Only validate: evaluation resolves this same PlanExpr name again.
  const field = aggregate.expression && planExprField(aggregate.expression);
  if (field !== undefined && resolveFieldName(input, field) === undefined) {
    throw new IvmRuntimeError("GraphFieldNotFound", field);
  }
  return aggregate;
}

/** Reconstructs the positive pre-tick input multiset from its post-tick state. */
export function recordsBeforeFromDeltas(after: Weighted[], deltas: RecordDelta[]): Weighted[] {
  return recordsBeforeDeltas(after, deltas).filter(([, w]) => w > 0);
}

export function aggregateRowFromRecords(
  inputDesc: RecordDescriptor,
  outputDesc: RecordDescriptor,
  aggregate: AggregateOp,
  records: Weighted[],
): Bytes | undefined {
  const positive: Bytes[] = [];
  let totalWeight = 0;
  for (const [record, weight] of records) {
    if (weight < 0) throw new IvmRuntimeError("UnsupportedOperator");
    if (weight > 0) {
      totalWeight += weight;
      positive.push(record);
    }
  }
  if (totalWeight === 0 && aggregate.groupKey.length > 0) return undefined;
  const values: Value[] = [];
  if (positive.length) {
    const first = new BorrowedRecord(positive[0], inputDesc);
    for (const expr of aggregate.groupKey) values.push(evaluateAggregateExpr(first, expr));
  }
  for (const expr of aggregate.aggregates) values.push(evaluateAggregate(records, inputDesc, expr));
  try {
    return outputDesc.create(values);
  } catch (error) {
    throw new IvmRuntimeError("RecordEncoding", error);
  }
}

function evaluateAggregate(
  records: Weighted[],
  inputDesc: RecordDescriptor,
  aggregate: AggregateExpr,
): Value {
  if (aggregate.distinct) throw new IvmRuntimeError("UnsupportedOperator");
  switch (aggregate.function) {
    case "Count": {
      let count = 0n;
      for (const [record, weight] of records) {
        if (weight <= 0) continue;
        if (aggregate.expression) {
          const value = evaluateAggregateExpr(
            new BorrowedRecord(record, inputDesc),
            aggregate.expression,
          );
          if (isNullValue(value)) continue;
        }
        count += BigInt(weight);
        if (count > 0xffffffffffffffffn) throw new IvmRuntimeError("UnsupportedOperator");
      }
      return { kind: "U64", value: count };
    }
    case "Sum":
      return nullable(aggregateSum(records, inputDesc, aggregate));
    case "Avg":
      return nullable(aggregateAvg(records, inputDesc, aggregate));
    default:
      return nullable(aggregateExtremum(records, inputDesc, aggregate));
  }
}

/** The non-null expression values of the live (positive-weight) records. */
function* liveValues(
  records: Weighted[],
  inputDesc: RecordDescriptor,
  expr: PlanExpr,
): Generator<[Bytes, number, Value]> {
  for (const [record, weight] of records) {
    if (weight <= 0) continue;
    const value = unwrapNullable(
      evaluateAggregateExpr(new BorrowedRecord(record, inputDesc), expr),
    );
    if (value !== undefined) yield [record, weight, value];
  }
}

const RANGES: Record<string, [bigint, bigint]> = {
  U8: [0n, 0xffn],
  U16: [0n, 0xffffn],
  U32: [0n, 0xffffffffn],
  U64: [0n, 0xffffffffffffffffn],
  I32: [-(2n ** 31n), 2n ** 31n - 1n],
  I64: [-(2n ** 63n), 2n ** 63n - 1n],
};

function checked(kind: string, v: bigint): bigint {
  const [lo, hi] = RANGES[kind];
  if (v < lo || v > hi) throw new IvmRuntimeError("AggregateOverflow");
  return v;
}

function aggregateSum(
  records: Weighted[],
  inputDesc: RecordDescriptor,
  aggregate: AggregateExpr,
): Value | undefined {
  const expr = aggregate.expression;
  if (!expr) throw new IvmRuntimeError("UnsupportedOperator");
  let kind: string | undefined;
  let intSum = 0n;
  let floatSum = 0;
  for (const [, weight, value] of liveValues(records, inputDesc, expr)) {
    switch (value.kind) {
      case "U8":
      case "U16":
      case "U32":
      case "U64":
      case "I32":
      case "I64": {
        kind ??= value.kind;
        // Accumulate in the wide integer (u64 or i64), narrowed at the end.
        const wide = value.kind.startsWith("U") ? "U64" : "I64";
        intSum = checked(wide, intSum + checked(wide, BigInt(value.value) * BigInt(weight)));
        break;
      }
      case "F64":
        kind ??= "F64";
        floatSum += value.value * weight;
        break;
      default:
        throw new IvmRuntimeError("UnsupportedOperator");
    }
  }
  if (kind === undefined) return undefined;
  if (kind === "F64") return { kind, value: floatSum };
  return { kind, value: kind === "U64" || kind === "I64" ? intSum : Number(checked(kind, intSum)) };
}

function aggregateAvg(
  records: Weighted[],
  inputDesc: RecordDescriptor,
  aggregate: AggregateExpr,
): Value | undefined {
  const expr = aggregate.expression;
  if (!expr) throw new IvmRuntimeError("UnsupportedOperator");
  let sum = 0;
  let scale = 0;
  let count = 0;
  let hasF64 = false;
  let hasNonFinite = false;
  for (const [, weight, value] of liveValues(records, inputDesc, expr)) {
    const numeric = numericValueAsF64(value);
    sum += numeric * weight;
    count += weight;
    hasF64 ||= value.kind === "F64";
    if (Number.isFinite(numeric)) scale = Math.max(scale, Math.abs(numeric));
    else hasNonFinite = true;
  }
  if (count <= 0) return undefined;
  // Scaling is only needed for finite F64 values whose plain sum overflowed.
  if (!hasF64 || hasNonFinite || Number.isFinite(sum)) return { kind: "F64", value: sum / count };
  if (scale === 0) return { kind: "F64", value: 0 };
  let scaledSum = 0;
  for (const [, weight, value] of liveValues(records, inputDesc, expr)) {
    scaledSum += (numericValueAsF64(value) / scale) * weight;
  }
  const normalized = Math.min(1, Math.max(-1, scaledSum / count));
  return { kind: "F64", value: scale * normalized };
}

function aggregateExtremum(
  records: Weighted[],
  inputDesc: RecordDescriptor,
  aggregate: AggregateExpr,
): Value | undefined {
  const expr = aggregate.expression;
  if (!expr) throw new IvmRuntimeError("UnsupportedOperator");
  let best: [Bytes, Bytes, Value] | undefined;
  for (const [record, , value] of liveValues(records, inputDesc, expr)) {
    const keyParts: number[] = [];
    encodeKeyPart(keyParts, value);
    const valueKey = Uint8Array.from(keyParts);
    let replaces = best === undefined;
    if (best) {
      const c = compareBytes(valueKey, best[0]);
      const tie = c === 0 && compareBytes(record, best[1]) < 0;
      if (aggregate.function === "Min") replaces = c < 0 || tie;
      else if (aggregate.function === "Max") replaces = c > 0 || tie;
    }
    if (replaces) best = [valueKey, record, value];
  }
  return best?.[2];
}

function numericValueAsF64(value: Value): number {
  switch (value.kind) {
    case "U8":
    case "U16":
    case "U32":
    case "U64":
    case "U48":
    case "I32":
    case "I64":
    case "F64":
      return Number(value.value);
    default:
      throw new IvmRuntimeError("UnsupportedOperator");
  }
}

const nullable = (value: Value | undefined): Value => ({ kind: "Nullable", value });

function unwrapNullable(value: Value): Value | undefined {
  return value.kind === "Nullable" ? value.value : value;
}

const isNullValue = (value: Value) => value.kind === "Nullable" && value.value === undefined;

function evaluateAggregateExpr(record: BorrowedRecord, expr: PlanExpr): Value {
  const fieldValue = (field: string) => {
    const index = resolveFieldName(record.descriptor(), field);
    if (index === undefined) throw new IvmRuntimeError("GraphFieldNotFound", field);
    try {
      return record.getIdx(index);
    } catch (error) {
      throw new IvmRuntimeError("RecordEncoding", error);
    }
  };
  switch (expr.kind) {
    case "Field":
    case "Nullable":
    case "NullableFlat":
      return fieldValue(expr.value);
    case "EnumTagRemap":
      return remapEnumTag(fieldValue(expr.field), expr.tags);
    case "EnumRemap":
      return remapEnum(fieldValue(expr.field), expr.tags);
    case "RecursiveEnumRemap":
      // Aggregates carry no output descriptor for a recursive re-encoding.
      throw new IvmRuntimeError("UnsupportedOperator");
    case "Literal":
      return expr.value.toValue();
    default:
      return { kind: "Nullable", value: undefined };
  }
}

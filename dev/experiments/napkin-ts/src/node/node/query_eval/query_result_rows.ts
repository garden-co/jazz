import { compareBytes } from "../../../support/index.js";
import type {
  CurrentRow,
  Query,
  TableSchema,
  Value,
  AppRowSchema,
  BorrowedRecord,
  DescriptorField,
  Aggregate,
  ColumnType,
  RowUuid,
} from "../../../_extern/node-qe-b.js";
import {
  Err,
  ColumnSchema,
  TableSchemaNew,
  aggregateOutputColumn,
  runtimeResultIdentityBytes,
  SyntheticReplacementToken,
} from "../../../_extern/node-qe-b.js";
import { aggregateResultMemberRowUuid } from "../query_eval.js";

export function compareOptionalValues(left: Value | undefined, right: Value | undefined): number {
  if (left !== undefined && right !== undefined) return compareOrderValues(left, right);
  if (left === undefined && right !== undefined) return -1;
  if (left !== undefined && right === undefined) return 1;
  return 0;
}

function aggregateRowCell(row: CurrentRow, column: string): Value | undefined {
  const idx = row.applicationColumnIndexByName(column);
  if (idx === undefined)
    throw Err.InvalidStoredValue("aggregate ordering field has no publication binding");
  const value = row.record.borrowed().getIdx(idx);
  return value.kind === "Nullable" ? (value.value ?? undefined) : value;
}

export function sortAggregateRows<T>(
  query: Query,
  rows: T[],
  row: (t: T) => CurrentRow,
  tieBreak: (a: T, b: T) => number,
): void {
  const keys = rows.map((item, index) => ({
    index,
    item,
    values: query.orderBy.map((order: any) => aggregateRowCell(row(item), order.column)),
  }));
  keys.sort((a, b) => {
    for (let i = 0; i < query.orderBy.length; i++) {
      let ordering = compareOptionalValues(a.values[i], b.values[i]);
      if (query.orderBy[i].direction === "Desc") ordering = -ordering;
      if (ordering !== 0) return ordering;
    }
    return tieBreak(a.item, b.item);
  });
  keys.forEach((k, i) => (rows[i] = k.item));
}

export function aggregateResultTable(query: Query, sourceTable: TableSchema): TableSchema {
  const aggregate = query.aggregate;
  if (!aggregate) throw Err.InvalidStoredValue("aggregate query missing aggregate");
  const columns: any[] = [];
  if (aggregate.groupBy !== undefined) {
    const column = sourceTable.columns.find((c: any) => c.name === aggregate.groupBy);
    if (!column) throw Err.InvalidStoredValue("aggregate group column missing");
    columns.push(ColumnSchema.new(column.name, column.columnType));
  }
  for (const a of aggregate.aggregates)
    columns.push(
      ColumnSchema.new(aggregateOutputColumn(a.alias), aggregateResultColumnType(a, sourceTable)),
    );
  return TableSchemaNew(query.table, columns);
}

function aggregateResultColumnType(aggregate: Aggregate, sourceTable: TableSchema): ColumnType {
  switch (aggregate.function) {
    case "Count":
      return { kind: "U64" };
    case "Avg":
      return { kind: "F64" };
    default: {
      const column = aggregate.column;
      if (column === undefined) throw Err.InvalidStoredValue("aggregate input column missing");
      const found = sourceTable.columns.find((c: any) => c.name === column);
      if (!found) throw Err.InvalidStoredValue("aggregate input column missing");
      const ty = found.columnType;
      return ty.kind === "Nullable" ? ty.inner : ty;
    }
  }
}

export function aggregateOutputSchema(output: AppRowSchema): any {
  if (output.terminal.kind === "Aggregate") return output.terminal.schema;
  throw Err.InvalidStoredValue("aggregate materialization has no lowered aggregate schema");
}

export function aggregateRecordFieldIndex(record: BorrowedRecord, field: DescriptorField): number {
  const identity = field.identity;
  if (identity === undefined)
    throw Err.InvalidStoredValue("lowered aggregate field has no identity");
  const index = record.descriptor().fieldIndexByIdentity(identity);
  if (index === undefined)
    throw Err.InvalidStoredValue("aggregate output is missing its lowered field identity");
  return index;
}

export function aggregateQueryRowUuid(output: AppRowSchema, record: BorrowedRecord): RowUuid {
  const aggregate = aggregateOutputSchema(output);
  if (aggregate.groupKeyFields.length > 1)
    throw Err.InvalidStoredValue("aggregate row identity requires one lowered group field");
  let rowValue: Value, rowType: any;
  const group = aggregate.groupKeyFields[0];
  if (group) {
    const index = aggregateRecordFieldIndex(record, group);
    rowValue = record.getIdx(index);
    rowType = record.descriptor().fields()[index].valueType;
  } else {
    rowValue = { kind: "String", value: "global" };
    rowType = { kind: "String" };
  }
  const row = runtimeResultIdentityBytes(rowValue, rowType);
  return aggregateResultMemberRowUuid({
    kind: "Synthetic",
    table: "aggregate_result",
    row,
    replacement: SyntheticReplacementToken.fromEncodedRecord(
      runtimeResultIdentityBytes({ kind: "String", value: "identity-only" }, { kind: "String" }),
    ),
  });
}

const cmp = (a: any, b: any) => (a < b ? -1 : a > b ? 1 : 0);

export function compareOrderValues(left: Value, right: Value): number {
  if (left.kind !== right.kind) return 0;
  switch (left.kind) {
    case "U8":
    case "U16":
    case "U32":
    case "U64":
    case "I32":
    case "I64":
    case "Bool":
    case "String":
    case "EnumTag":
      return cmp(left.value, right.value);
    case "F64": {
      const a = left.value,
        b = right.value;
      if (Number.isNaN(a) || Number.isNaN(b))
        return Number.isNaN(a) ? (Number.isNaN(b) ? 0 : 1) : -1;
      return (
        cmp(a, b) || (Object.is(a, -0) ? (Object.is(b, -0) ? 0 : -1) : Object.is(b, -0) ? 1 : 0)
      );
    }
    case "Bytes":
    case "Uuid":
      return compareBytes(left.value, right.value);
    case "Tuple":
    case "Array":
      return compareOrderValueSlices(left.value, right.value);
    case "Nullable": {
      const a = left.value,
        b = right.value;
      if (a != null && b != null) return compareOrderValues(a, b);
      return a == null ? (b == null ? 0 : -1) : 1;
    }
  }
  return 0;
}

function compareOrderValueSlices(left: Value[], right: Value[]): number {
  for (let i = 0; i < Math.min(left.length, right.length); i++) {
    const o = compareOrderValues(left[i], right[i]);
    if (o !== 0) return o;
  }
  return left.length - right.length;
}

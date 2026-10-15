import { SortedMap, cmpStr } from "../../../../support/collections.js";
import { type Value, valueEquals } from "./value.js";
import { type TablePolicies, defaultTablePolicies, tablePoliciesEqual } from "./policy.js";
import { SchemaHash } from "./branch.js";

/** Interned table/column names are plain strings in TS. */
export type TableName = string;
export type ColumnName = string;

export type EnumCaseDescriptor = { name: string; fields: ColumnDescriptor[] };

export type ColumnType =
  | { type: "Integer" }
  | { type: "BigInt" }
  | { type: "Boolean" }
  | { type: "Text" }
  | { type: "Enum"; variants: string[] }
  | { type: "ScalarEnum"; name: string; variants: string[] }
  | { type: "CatalogueEnumPayload"; name: string; cases: EnumCaseDescriptor[] }
  | { type: "EnumPayload"; cases: EnumCaseDescriptor[] }
  | { type: "Timestamp" }
  | { type: "Double" }
  | { type: "Uuid" }
  | { type: "TransactionId" }
  | { type: "Bytea" }
  | { type: "Json"; schema?: unknown }
  | { type: "Array"; element: ColumnType }
  | { type: "Row"; columns: RowDescriptor };

export const elementType = (t: ColumnType) => (t.type === "Array" ? t.element : undefined);
export const rowDescriptorOf = (t: ColumnType) => (t.type === "Row" ? t.columns : undefined);

export function columnTypeEquals(a: ColumnType, b: ColumnType): boolean {
  if (a.type !== b.type) return false;
  switch (a.type) {
    case "Array":
      return columnTypeEquals(a.element, (b as typeof a).element);
    case "Row":
      return rowDescriptorEquals(a.columns, (b as typeof a).columns);
    case "Enum":
      return a.variants.join("\0") === (b as typeof a).variants.join("\0");
    case "ScalarEnum": {
      const o = b as typeof a;
      return a.name === o.name && a.variants.join("\0") === o.variants.join("\0");
    }
    case "EnumPayload":
    case "CatalogueEnumPayload": {
      const o = b as typeof a;
      if ((a as any).name !== (o as any).name || a.cases.length !== o.cases.length) return false;
      return a.cases.every(
        (c, i) =>
          c.name === o.cases[i].name &&
          c.fields.length === o.cases[i].fields.length &&
          c.fields.every((f, j) => columnDescriptorEquals(f, o.cases[i].fields[j])),
      );
    }
    case "Json":
      return JSON.stringify(a.schema) === JSON.stringify((b as typeof a).schema);
    default:
      return true;
  }
}

export type ColumnMergeStrategy = "Counter" | "GSet";

export type ColumnDescriptor = {
  name: ColumnName;
  column_type: ColumnType;
  nullable: boolean;
  references?: TableName;
  default?: Value;
  merge_strategy?: ColumnMergeStrategy;
};

export function columnDescriptor(
  name: string,
  column_type: ColumnType,
  opts: Partial<Omit<ColumnDescriptor, "name" | "column_type">> = {},
): ColumnDescriptor {
  return { name, column_type, nullable: false, ...opts };
}

export function columnDescriptorEquals(a: ColumnDescriptor, b: ColumnDescriptor): boolean {
  return (
    a.name === b.name &&
    a.nullable === b.nullable &&
    a.references === b.references &&
    a.merge_strategy === b.merge_strategy &&
    columnTypeEquals(a.column_type, b.column_type) &&
    (a.default === undefined
      ? b.default === undefined
      : b.default !== undefined && valueEquals(a.default, b.default))
  );
}

export function validateMergeStrategy(c: ColumnDescriptor): void {
  switch (c.merge_strategy) {
    case undefined:
      return;
    case "Counter":
      if (c.nullable || !(c.column_type.type === "Integer" || c.column_type.type === "BigInt"))
        throw new Error(
          "Counter merge strategy is only supported on non-nullable INTEGER or BIGINT columns",
        );
      return;
    case "GSet":
      if (c.nullable || c.column_type.type !== "Array")
        throw new Error(
          `g-set merge strategy is only supported on non-nullable ARRAY columns, got ${c.name} (${JSON.stringify(c.column_type)}, nullable=${c.nullable})`,
        );
  }
}

/** Descriptor for a row's schema, defining column order and types. */
export type RowDescriptor = { columns: ColumnDescriptor[] };

export const rowDescriptor = (columns: ColumnDescriptor[]): RowDescriptor => ({ columns });
export const columnIndex = (d: RowDescriptor, name: string) => {
  const i = d.columns.findIndex((c) => c.name === name);
  return i < 0 ? undefined : i;
};
export const columnByName = (d: RowDescriptor, name: string) =>
  d.columns.find((c) => c.name === name);
export const combineDescriptors = (ds: RowDescriptor[]): RowDescriptor => ({
  columns: ds.flatMap((d) => d.columns),
});
export const rowDescriptorEquals = (a: RowDescriptor, b: RowDescriptor) =>
  a.columns.length === b.columns.length &&
  a.columns.every((c, i) => columnDescriptorEquals(c, b.columns[i]));

export type TableSchema = {
  columns: RowDescriptor;
  indexed_columns?: ColumnName[];
  composite_indexes: ColumnName[][];
  policies: TablePolicies;
  branch_by: ColumnName[];
};

export const tableSchema = (
  columns: RowDescriptor,
  policies: TablePolicies = defaultTablePolicies(),
): TableSchema => ({
  columns,
  composite_indexes: [],
  policies,
  branch_by: [],
});

/** Lexicographic over each index's column names (UTF-8 byte order). */
export function canonicalCompositeIndexOrder(indexes: ColumnName[][]): ColumnName[][] {
  return indexes.map((i) => i.slice()).sort(cmpStrArrays);
}
function cmpStrArrays(a: string[], b: string[]): number {
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    const c = cmpStr(a[i], b[i]);
    if (c) return c;
  }
  return a.length - b.length;
}

export function tableSchemaEquals(a: TableSchema, b: TableSchema): boolean {
  return (
    rowDescriptorEquals(a.columns, b.columns) &&
    JSON.stringify(a.indexed_columns) === JSON.stringify(b.indexed_columns) &&
    JSON.stringify(canonicalCompositeIndexOrder(a.composite_indexes)) ===
      JSON.stringify(canonicalCompositeIndexOrder(b.composite_indexes)) &&
    tablePoliciesEqual(a.policies, b.policies) &&
    a.branch_by.join("\0") === b.branch_by.join("\0")
  );
}

/** Serde-equivalent JSON decode: canonicalizes composite indexes, applies defaults. */
export function tableSchemaFromJson(json: any): TableSchema {
  return {
    columns: { columns: json.columns },
    indexed_columns: json.indexed_columns ?? undefined,
    composite_indexes: canonicalCompositeIndexOrder(json.composite_indexes ?? []),
    policies: { ...defaultTablePolicies(), ...json.policies },
    branch_by: json.branchBy ?? [],
  };
}

export function isIndexedColumn(t: TableSchema, column: string): boolean {
  if (column === "_id" || column === "_id_deleted") return true;
  if (columnByName(t.columns, column)?.column_type.type === "Bytea") return false;
  return t.indexed_columns === undefined || t.indexed_columns.includes(column);
}

/** Fluent builder for a TableSchema. */
export class TableSchemaBuilder {
  private columns: ColumnDescriptor[] = [];
  private indexed?: ColumnName[];
  private composite: ColumnName[][] = [];
  private _policies = defaultTablePolicies();
  private _branchBy: ColumnName[] = [];
  constructor(readonly name: string) {}
  column(name: string, t: ColumnType): this {
    this.columns.push(columnDescriptor(name, t));
    return this;
  }
  columnWithDefault(name: string, t: ColumnType, d: Value): this {
    this.columns.push(columnDescriptor(name, t, { default: d }));
    return this;
  }
  nullableColumn(name: string, t: ColumnType): this {
    this.columns.push(columnDescriptor(name, t, { nullable: true }));
    return this;
  }
  fkColumn(name: string, references: string): this {
    this.columns.push(columnDescriptor(name, { type: "Uuid" }, { references }));
    return this;
  }
  nullableFkColumn(name: string, references: string): this {
    this.columns.push(columnDescriptor(name, { type: "Uuid" }, { nullable: true, references }));
    return this;
  }
  arrayFkColumn(name: string, references: string): this {
    this.columns.push(
      columnDescriptor(name, { type: "Array", element: { type: "Uuid" } }, { references }),
    );
    return this;
  }
  policies(p: TablePolicies): this {
    this._policies = p;
    return this;
  }
  branchBy(column: string): this {
    this._branchBy.push(column);
    return this;
  }
  indexOnly(columns: Iterable<string>): this {
    this.indexed = [...columns];
    return this;
  }
  compositeIndex(columns: Iterable<string>): this {
    this.composite.push([...columns]);
    return this;
  }
  build(): TableSchema {
    return {
      columns: { columns: this.columns },
      indexed_columns: this.indexed,
      composite_indexes: canonicalCompositeIndexOrder(this.composite),
      policies: this._policies,
      branch_by: this._branchBy,
    };
  }
  buildNamed(): [TableName, TableSchema] {
    return [this.name, this.build()];
  }
}

/** Developer-authored schema persisted in the catalogue, in table-name order. */
export type Schema = SortedMap<TableName, TableSchema>;
export const newSchema = (entries?: Iterable<[TableName, TableSchema]>): Schema =>
  new SortedMap(cmpStr, entries);

export function schemaEquals(a: Schema, b: Schema): boolean {
  if (a.size !== b.size) return false;
  for (const [k, v] of a) {
    const o = b.get(k);
    if (!o || !tableSchemaEquals(v, o)) return false;
  }
  return true;
}

export class SchemaBuilder {
  private tables: TableSchemaBuilder[] = [];
  table(b: TableSchemaBuilder): this {
    this.tables.push(b);
    return this;
  }
  build(): Schema {
    return newSchema(this.tables.map((t) => t.buildNamed()));
  }
  hash(): SchemaHash {
    return SchemaHash.compute(this.build());
  }
}

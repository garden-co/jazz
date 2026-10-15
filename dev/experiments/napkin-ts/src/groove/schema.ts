// SQL DDL-ish schema metadata for record layout and durable indices.
import {
  RecordDescriptor,
  type ValueType,
  type VariantRegistry,
  collectVariantRegistries,
  rebindVariantRegistries,
  stampVariantRegistries,
  variantRegistryEq,
  variantRegistryIdForPath,
  variantRegistryOccurrenceCount,
} from "./records/index.js";
import { LARGE_VALUE_METADATA_CF } from "../_extern/groove-records.js";

export type ColumnType = ValueType;

export class DatabaseSchema {
  directRecordStores: DirectRecordStoreSchema[] = [];
  constructor(public tables: TableSchema[]) {}
  withDirectRecordStore(store: DirectRecordStoreSchema): this {
    this.directRecordStores.push(store);
    return this;
  }
  table(name: string): TableSchema | undefined {
    return this.tables.find((t) => t.name === name);
  }
  directRecordStore(name: string): DirectRecordStoreSchema | undefined {
    return this.directRecordStores.find((s) => s.name === name);
  }
  columnFamilies(): string[] {
    const hasIndices = this.tables.some((t) => t.indices.length > 0);
    return [
      ...this.tables.map((t) => t.name),
      ...this.directRecordStores.map((s) => s.name),
      ...(hasIndices ? ["indices"] : []),
      LARGE_VALUE_METADATA_CF,
    ];
  }
}

const descriptorFields = (d: RecordDescriptor): [string, ValueType][] =>
  d.fields.map((f) => {
    if (f.name === null) throw new Error("direct store fields are named");
    return [f.name, f.valueType];
  });

export class DirectRecordStoreSchema {
  readonly key: [string, ValueType][];
  readonly value: [string, ValueType][];
  constructor(
    readonly name: string,
    key: RecordDescriptor,
    value: RecordDescriptor,
  ) {
    this.key = descriptorFields(key);
    this.value = descriptorFields(value);
  }
  keyDescriptor(): RecordDescriptor {
    return RecordDescriptor.new(this.key);
  }
  valueDescriptor(): RecordDescriptor {
    return RecordDescriptor.new(this.value);
  }
}

export type ColumnSchema = { name: string; columnType: ColumnType };
export const columnSchema = (name: string, columnType: ColumnType): ColumnSchema => ({
  name,
  columnType,
});

function mergeRegistries(
  target: Map<bigint, VariantRegistry>,
  vt: ValueType,
  name: string,
  what: string,
  allowEqual: boolean,
) {
  const registries = new Map<bigint, VariantRegistry>();
  collectVariantRegistries(vt, registries);
  if (registries.size !== variantRegistryOccurrenceCount(vt))
    throw new Error(`variant registry identity collision within ${what} ${name}`);
  for (const [id, registry] of registries) {
    const existing = target.get(id);
    if (existing === undefined) target.set(id, registry);
    else if (!allowEqual) throw new Error("variant registry identity collision across columns");
    else if (!variantRegistryEq(existing, registry))
      throw new Error("variant registry definition collision");
  }
}

export class TableSchema {
  variantRegistryId: bigint;
  valueVariantRegistries = new Map<bigint, VariantRegistry>();
  columns: ColumnSchema[];
  primaryKey: PrimaryKey | null = null;
  indices: IndexSchema[] = [];
  foreignKeys: ForeignKey[] = [];
  variants: TableVariant[] = [];

  constructor(
    public name: string,
    columns: Iterable<ColumnSchema>,
    preserveBoundRegistries = false,
  ) {
    this.columns = [...columns].map((c) => {
      const path = `table/${name}/column/${c.name}`;
      return {
        name: c.name,
        columnType: preserveBoundRegistries
          ? stampVariantRegistries(c.columnType, path)
          : rebindVariantRegistries(c.columnType, path),
      };
    });
    for (const c of this.columns)
      mergeRegistries(this.valueVariantRegistries, c.columnType, c.name, "column", false);
    this.variantRegistryId = variantRegistryIdForPath(`table/${name}/row`);
  }
  static newWithBoundRegistries(name: string, columns: Iterable<ColumnSchema>): TableSchema {
    return new TableSchema(name, columns, true);
  }
  withPrimaryKey(pk: PrimaryKey): this {
    this.primaryKey = pk;
    return this;
  }
  withIndex(index: IndexSchema): this {
    this.indices.push(index);
    return this;
  }
  withForeignKey(fk: ForeignKey): this {
    this.foreignKeys.push(fk);
    return this;
  }
  withVariant(tag: number, fields: Iterable<string>): this {
    this.variants.push({ tag, fields: [...fields], payloadFields: [] });
    return this;
  }
  withVariantPayload(tag: number, fields: Iterable<TableVariantField>): this {
    const payload = [...fields].map((f) => {
      const path = `${this.variantRegistryId}/case/${tag}/field/${f.name}`;
      return {
        ...f,
        valueType:
          f.sharedColumn !== null
            ? stampVariantRegistries(f.valueType, path)
            : rebindVariantRegistries(f.valueType, path),
      };
    });
    for (const f of payload)
      mergeRegistries(this.valueVariantRegistries, f.valueType, f.name, "payload field", true);
    this.variants.push({ tag, fields: [], payloadFields: payload });
    return this;
  }
  hasVariants(): boolean {
    return this.variants.length > 0;
  }
  variant(tag: number): TableVariant | undefined {
    return this.variants.find((v) => v.tag === tag);
  }
  recordSchemaForVariant(tag: number): RecordDescriptor | undefined {
    if (!this.variants.length) return this.recordSchema();
    const variant = this.variant(tag);
    if (!variant) return undefined;
    if (variant.payloadFields.length)
      return RecordDescriptor.new(variant.payloadFields.map((f) => [f.name, f.valueType]));
    const fields: [string, ValueType][] = [];
    for (const name of variant.fields) {
      const column = this.columns.find((c) => c.name === name);
      if (!column) return undefined;
      fields.push([column.name, column.columnType]);
    }
    return RecordDescriptor.new(fields);
  }
  recordSchema(): RecordDescriptor {
    return RecordDescriptor.new(this.columns.map((c) => [c.name, c.columnType]));
  }
}

export type TableVariant = { tag: number; fields: string[]; payloadFields: TableVariantField[] };

export function payloadNameForShared(v: TableVariant, shared: string): string | undefined {
  if (!v.payloadFields.length) return v.fields.find((f) => f === shared);
  return v.payloadFields.find((f) => f.sharedColumn === shared)?.name;
}

export type TableVariantField = { name: string; valueType: ValueType; sharedColumn: string | null };
export const localVariantField = (name: string, valueType: ValueType): TableVariantField => ({
  name,
  valueType,
  sharedColumn: null,
});
export const sharedVariantField = (
  name: string,
  valueType: ValueType,
  sharedColumn: string,
): TableVariantField => ({ name, valueType, sharedColumn });

export type IntegerKeyType = "U8" | "U16" | "U32" | "U64";
export type PrimaryKeyType =
  | { kind: "Integer"; integer: IntegerKeyType }
  | { kind: "Bool" | "String" | "Bytes" | "Uuid" };
export const primaryKeyColumnType = (t: PrimaryKeyType): ColumnType =>
  t.kind === "Integer" ? t.integer : t.kind;

export type PrimaryKeyColumn = { column: string; keyType: PrimaryKeyType };
export const PrimaryKeyColumn = {
  integer: (column: string, integer: IntegerKeyType): PrimaryKeyColumn => ({
    column,
    keyType: { kind: "Integer", integer },
  }),
  bytes: (column: string): PrimaryKeyColumn => ({ column, keyType: { kind: "Bytes" } }),
  uuid: (column: string): PrimaryKeyColumn => ({ column, keyType: { kind: "Uuid" } }),
};

export class PrimaryKey {
  constructor(
    public columns: PrimaryKeyColumn[],
    public generated: boolean,
  ) {}
  static new(column: string, integer: IntegerKeyType): PrimaryKey {
    return new PrimaryKey([PrimaryKeyColumn.integer(column, integer)], true);
  }
  static composite(columns: Iterable<PrimaryKeyColumn>): PrimaryKey {
    return new PrimaryKey([...columns], false);
  }
  userSupplied(): this {
    this.generated = false;
    return this;
  }
}

export type IndexSchema = { name: string; columns: string[]; unique: boolean };
export const indexSchema = (
  name: string,
  columns: Iterable<string>,
  unique = false,
): IndexSchema => ({
  name,
  columns: [...columns],
  unique,
});

export type ForeignKey = {
  name: string;
  columns: string[];
  referencedTable: string;
  referencedColumns: string[];
};

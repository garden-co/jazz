/**
 * The schema's tables in `jazz.app.json`'s readable form (SPEC §10.10): the
 * input of the Rust writer, which checks and prints the file. Nothing here
 * hashes or lowers a schema; Rust does.
 */
import type { AnyColumnDefinition } from "../dsl.js";
import {
  hasEncryptionDeclaration,
  hasEncryptionDeclarationInJsonSchema,
  isComputedColumn,
} from "../dsl.js";
import type { Column, SqlType } from "../schema.js";
import type { DefinedTable } from "./table.js";

export type ReadableJson =
  | string
  | number
  | boolean
  | null
  | ReadableJson[]
  | { [key: string]: ReadableJson };

/** A readable Table: `{columns, relations?, indexes?, branchBy?, history?, computed?}`. */
export type ReadableTable = { [key: string]: ReadableJson };

/** What a schema contributes to `jazz.app.json`: its `tables` and `transforms`. */
export interface ReadableApp {
  tables: { [table: string]: ReadableTable };
  transforms?: { [name: string]: { stored: ReadableJson; view: ReadableJson } };
}

/** The column holding an equality column's tokens (protocol §1.5). */
const EQUALITY_PREFIX = "__e2ee_eq_";

const SCALAR_NAMES: Record<string, string> = {
  TEXT: "string",
  BOOLEAN: "boolean",
  INTEGER: "int",
  BIGINT: "bigint",
  REAL: "float",
  TIMESTAMP: "timestamp",
  UUID: "uuid",
  BYTEA: "bytes",
};

/** A column type without options: a type string, or `{enum}` / `{json}` / `{array}`. */
export function readableType(sqlType: SqlType): ReadableJson {
  if (typeof sqlType === "string") {
    const name = SCALAR_NAMES[sqlType];
    if (!name) throw new Error(`unknown column type ${sqlType}`);
    return name;
  }
  switch (sqlType.kind) {
    case "ENUM":
      if (!sqlType.variants) throw new Error("enum cases with fields are not supported");
      return { enum: [...sqlType.variants] };
    case "JSON":
      return { json: sqlType.schema ? (sqlType.schema as ReadableJson) : true };
    case "ARRAY":
      return { array: readableType(sqlType.element) };
  }
}

function base64(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

/** A value in a column's readable form: ms for timestamps, decimal strings for bigints, base64 for bytes. */
export function readableValue(value: unknown, sqlType: SqlType): ReadableJson {
  if (value === null) return null;
  if (value instanceof Date) return value.getTime();
  if (typeof value === "bigint") return value.toString();
  if (value instanceof Uint8Array) return base64(value);
  if (sqlType === "BIGINT" && typeof value === "number") return String(value);
  if (Array.isArray(value)) {
    const element =
      typeof sqlType === "object" && sqlType.kind === "ARRAY" ? sqlType.element : sqlType;
    return value.map((item) => readableValue(item, element));
  }
  return value as ReadableJson;
}

/** One column in readable form: a type string when it has no options, an object otherwise. */
export function readableColumn(
  column: Column,
  transform?: string,
  forwardReference = false,
): ReadableJson {
  const type = readableType(column.sqlType);
  const options: { [key: string]: ReadableJson } = {};
  if (column.default !== undefined) options.default = readableValue(column.default, column.sqlType);
  if (column.mergeStrategy) options.merge = column.mergeStrategy;
  if (column.references && !forwardReference) options.references = column.references;
  if (transform) options.transform = transform;
  if (typeof type === "string" && Object.keys(options).length === 0)
    return column.nullable ? `${type}?` : type;
  const head: { [key: string]: ReadableJson } =
    typeof type === "string" ? { type } : { ...(type as { [key: string]: ReadableJson }) };
  return { ...head, ...(column.nullable ? { optional: true } : {}), ...options };
}

/**
 * The `space` and `scope` of a table's `encryption` entry: `"id"` and the
 * table itself, or a forward relation's `uuid` column and its target. What
 * deploy would refuse is refused here, naming the table.
 */
function readableSpace(tableName: string, table: DefinedTable): { space: string; scope: string } {
  const space = table.space!;
  if (space === "id") return { space: "id", scope: tableName };
  const relation = table.relations[space];
  const how = 'name "id" or a forward relation over a uuid column (s.rel("<table>", "<column>"))';
  if (!relation || !Object.hasOwn(table.relations, space))
    throw new Error(`${tableName}: space "${space}" is not a relation of the table; ${how}.`);
  if (relation.kind !== "forward")
    throw new Error(`${tableName}: space "${space}" is a reverse relation; ${how}.`);
  const builder = (table.columns as Record<string, AnyColumnDefinition>)[relation.column];
  const sqlType = builder && !isComputedColumn(builder) ? builder._sqlType : undefined;
  if (sqlType !== "UUID")
    throw new Error(
      `${tableName}: space "${space}" goes through ${relation.column}, which is not a uuid column ` +
        `(a row is in exactly one space); ${how}.`,
    );
  return { space: relation.column, scope: relation.table };
}

/** The readable form of every table, by name, plus the transforms they use. */
export function readableApp(tables: Record<string, DefinedTable>): ReadableApp {
  const out: ReadableApp["tables"] = {};
  const transforms: NonNullable<ReadableApp["transforms"]> = {};
  for (const [tableName, table] of Object.entries(tables)) {
    const columns: { [key: string]: ReadableJson } = {};
    const computed: { [key: string]: ReadableJson } = {};
    const encrypted: { [key: string]: ReadableJson } = {};
    const equality: string[] = [];
    for (const [name, definition] of Object.entries(
      table.columns as Record<string, AnyColumnDefinition>,
    )) {
      const refuse = (reason: string) => {
        throw new Error(`${tableName}.${name}: ${reason}`);
      };
      if (isComputedColumn(definition)) {
        const builder = definition._computed.type;
        if (hasEncryptionDeclaration(builder))
          refuse("computed fields cannot be encrypted; encrypt a stored column.");
        const column = builder._build(name);
        const type = readableType(column.sqlType);
        computed[name] = {
          type:
            typeof type === "string"
              ? column.nullable
                ? `${type}?`
                : type
              : {
                  ...(type as { [key: string]: ReadableJson }),
                  ...(column.nullable ? { optional: true } : {}),
                },
          from: [...definition._computed.from],
        };
        continue;
      }
      const builder = definition;
      if (hasEncryptionDeclaration(builder._element))
        refuse("array elements cannot be encrypted individually; encrypt the whole array column.");
      if (hasEncryptionDeclaration(builder._transform?.view))
        refuse("transform views cannot be encrypted; use a stored column without a transform.");
      const column = builder._build(name);
      if (
        typeof column.sqlType === "object" &&
        column.sqlType.kind === "JSON" &&
        hasEncryptionDeclarationInJsonSchema(column.sqlType.schema)
      )
        refuse(
          "JSON schema properties cannot be encrypted individually; encrypt the whole JSON column.",
        );
      if (builder._encryption) {
        if (column.mergeStrategy)
          refuse(
            "an encrypted column takes no merge strategy: the server can't merge ciphertext, " +
              "so its writes are last-writer-wins.",
          );
        if (builder._transform) refuse("an encrypted column takes no transform.");
        if (
          column.references ||
          Object.values(table.relations).some(
            (relation) => relation.kind === "forward" && relation.column === name,
          )
        )
          refuse("an encrypted column can't reference a table or carry a relation.");
        // The device sees the declared type and default; the server stores
        // required `bytes` (the ciphertext) and, for equality, its tokens.
        encrypted[name] = readableColumn(column);
        columns[name] = "bytes";
        if (builder._encryption.equality) {
          equality.push(name);
          columns[`${EQUALITY_PREFIX}${name}`] = "bytes";
        }
        continue;
      }
      const transform = builder._transform;
      if (transform) {
        const stored = readableType(column.sqlType);
        const view = readableType(transform.view._sqlType);
        const previous = transforms[transform.name];
        if (
          previous &&
          (JSON.stringify(previous.stored) !== JSON.stringify(stored) ||
            JSON.stringify(previous.view) !== JSON.stringify(view))
        )
          throw new Error(
            `${tableName}.${name}: transform "${transform.name}" is used with different types ` +
              `(${JSON.stringify(previous)} and ${JSON.stringify({ stored, view })}).`,
          );
        transforms[transform.name] = { stored, view };
      }
      const forward = Object.values(table.relations).some(
        (relation) => relation.kind === "forward" && relation.column === name,
      );
      columns[name] = readableColumn(column, transform?.name, forward);
    }
    const entry: ReadableTable = { columns };
    const relations = Object.entries(table.relations);
    if (relations.length > 0)
      entry.relations = Object.fromEntries(
        relations.map(([name, relation]): [string, ReadableJson] => [
          name,
          relation.kind === "forward"
            ? { rel: relation.table, column: relation.column }
            : { reverse: relation.table, relation: relation.relation },
        ]),
      );
    const indexes: ReadableJson[] = [
      ...(table.indexedColumns ?? []),
      ...(table.compositeIndexes ?? []).map((index) => [...index]),
    ];
    if (indexes.length > 0) entry.indexes = indexes;
    if (table.branchColumns?.length) entry.branchBy = [...table.branchColumns];
    if (table.historyRetention) entry.history = table.historyRetention;
    if (Object.keys(computed).length > 0) entry.computed = computed;
    if (table.space !== undefined) {
      entry.encryption = {
        ...readableSpace(tableName, table),
        columns: encrypted,
        ...(equality.length > 0 ? { equality } : {}),
      };
    } else if (Object.keys(encrypted).length > 0) {
      throw new Error(
        `${tableName}: encrypted columns (${Object.keys(encrypted).join(", ")}) need the table's space: ` +
          `s.table(columns, relations, { space: "id" | "<relation>" }).`,
      );
    }
    out[tableName] = entry;
  }
  return Object.keys(transforms).length > 0 ? { tables: out, transforms } : { tables: out };
}

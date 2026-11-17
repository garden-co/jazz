import { initGetFieldName, initGetModelName } from "better-auth/adapters";
import type { DBAdapterSchemaCreation } from "better-auth";
import type { BetterAuthDBSchema, DBFieldAttribute } from "better-auth/db";
import type { ColumnType, WasmSchema } from "../drivers/types.js";
import { assertUserColumnNameAllowed } from "../magic-columns.js";
import { assertSchemaNameAllowed } from "../schema-name.js";

interface SchemaNameResolvers {
  getModelName: (model: string) => string;
  getFieldName: ({ model, field }: { model: string; field: string }) => string;
}

function toJazzColumnType(field: DBFieldAttribute): ColumnType {
  if (Array.isArray(field.type)) {
    return { type: "Enum", variants: [...field.type] };
  }

  switch (field.type) {
    case "string":
      return { type: "Text" };
    case "number":
      return field.bigint ? { type: "BigInt" } : { type: "Integer" };
    case "boolean":
      return { type: "Boolean" };
    case "date":
      return { type: "Timestamp" };
    case "json":
      return { type: "Json" };
    case "string[]":
      return { type: "Array", element: { type: "Text" } };
    case "number[]":
      return {
        type: "Array",
        element: field.bigint ? { type: "BigInt" } : { type: "Integer" },
      };
    default:
      throw new Error(`Unsupported Better Auth field type: ${String(field.type)}`);
  }
}

function toJazzReferenceColumn(args: {
  modelName: string;
  fieldName: string;
  storedFieldName: string;
  field: DBFieldAttribute;
  getModelName: (model: string) => string;
}): { columnType: ColumnType; references: string } {
  const { modelName, fieldName, field, getModelName } = args;
  const reference = field.references;

  if (!reference) {
    throw new Error(`Field "${modelName}.${fieldName}" is missing Better Auth reference metadata.`);
  }

  if (reference.field !== "id") {
    throw new Error(
      `Field "${modelName}.${fieldName}" references "${reference.model}.${reference.field}", but Jazz schema generation only supports references to "id".`,
    );
  }

  const references = getModelName(reference.model);

  switch (field.type) {
    case "string":
      return {
        columnType: { type: "Uuid" },
        references,
      };
    case "string[]":
      return {
        columnType: {
          type: "Array",
          element: { type: "Uuid" },
        },
        references,
      };
    case "number":
    case "number[]":
      throw new Error(
        `Field "${modelName}.${fieldName}" cannot be emitted as a Jazz reference because Better Auth references must use string or string[] fields.`,
      );
    default:
      throw new Error(
        `Field "${modelName}.${fieldName}" uses unsupported Better Auth reference type: ${String(field.type)}.`,
      );
  }
}

function assertStoredFieldNameAllowed(args: {
  modelName: string;
  fieldName: string;
  storedFieldName: string;
}): void {
  const { modelName, fieldName, storedFieldName } = args;

  if (storedFieldName === "id") {
    throw new Error(`Field "${modelName}.${fieldName}" conflicts with reserved Jazz row id.`);
  }

  assertUserColumnNameAllowed(storedFieldName);
}

export function createSchemaNameResolvers(args: {
  tables: BetterAuthDBSchema;
  usePlural?: boolean;
}): SchemaNameResolvers {
  const { tables, usePlural } = args;

  return {
    getModelName: initGetModelName({
      schema: tables,
      usePlural,
    }),
    getFieldName: initGetFieldName({
      schema: tables,
      usePlural,
    }),
  };
}

export function buildJazzSchema(args: {
  tables: BetterAuthDBSchema;
  getModelName: (model: string) => string;
  getFieldName: ({ model, field }: { model: string; field: string }) => string;
}): WasmSchema {
  const { tables, getModelName, getFieldName } = args;
  const wasmSchema: WasmSchema = {};

  for (const [modelName, model] of Object.entries(tables)) {
    const tableName = getModelName(modelName);
    assertSchemaNameAllowed(tableName);
    const columns: WasmSchema[string]["columns"] = [];
    const relations: NonNullable<WasmSchema[string]["relations"]> = {};

    for (const [fieldName, field] of Object.entries(model.fields)) {
      if (fieldName === "id") {
        continue;
      }

      const storedFieldName = getFieldName({ model: modelName, field: fieldName });
      assertStoredFieldNameAllowed({ modelName, fieldName, storedFieldName });

      if (field.references) {
        const referenceColumn = toJazzReferenceColumn({
          modelName,
          fieldName,
          storedFieldName,
          field,
          getModelName,
        });

        relations[`${storedFieldName}Relation`] = {
          kind: "forward",
          table: referenceColumn.references,
          column: storedFieldName,
        };
        columns.push({
          name: storedFieldName,
          column_type: referenceColumn.columnType,
          nullable: field.required === false,
          references: referenceColumn.references,
        });
        continue;
      }

      columns.push({
        name: storedFieldName,
        column_type: toJazzColumnType(field),
        nullable: field.required === false,
      });
    }

    for (const name of Object.keys(relations)) {
      if (columns.some((c) => c.name === name))
        throw new Error(`Generated relationship "${tableName}.${name}" collides with a column.`);
    }
    wasmSchema[tableName] = { columns, relations };
  }

  return wasmSchema;
}

export function buildJazzSchemaFromTables(args: {
  tables: BetterAuthDBSchema;
  usePlural?: boolean;
}): WasmSchema {
  const { tables, usePlural } = args;
  const resolvers = createSchemaNameResolvers({ tables, usePlural });

  return buildJazzSchema({
    tables,
    getModelName: resolvers.getModelName,
    getFieldName: resolvers.getFieldName,
  });
}

// ---------------------------------------------------------------------------
// schema-better-auth/schema.ts generation, for Better Auth's `generate` command.
// ---------------------------------------------------------------------------

const DEFAULT_SCHEMA_FILE_PATH = "./schema-better-auth/schema.ts";
const APP_FILE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** A column of `jazz.app.json`, in its readable form (SPEC §10.10). */
export type AppFileColumn = string | Record<string, unknown>;

/** A table of `jazz.app.json`. */
export interface AppFileTable {
  columns: Record<string, AppFileColumn>;
  relations?: Record<
    string,
    { rel: string; column: string } | { reverse: string; relation: string }
  >;
  [key: string]: unknown;
}

function assertAppFileIdentifier(kind: "table" | "column", name: string, where: string): void {
  if (!APP_FILE_IDENTIFIER.test(name)) {
    throw new Error(
      `${where}: the ${kind} name "${name}" can't be written to jazz.app.json, which only allows identifiers ([A-Za-z_][A-Za-z0-9_]*). ` +
        `Rename it with Better Auth's ${kind === "table" ? "modelName" : "fieldName"} option.`,
    );
  }
}

function optionalColumn(type: string | Record<string, unknown>, field: DBFieldAttribute) {
  const optional = field.required === false;
  if (typeof type === "string") return optional ? `${type}?` : type;
  return optional ? { ...type, optional: true } : type;
}

function toAppFileColumn(modelName: string, fieldName: string, field: DBFieldAttribute) {
  const where = `Field "${modelName}.${fieldName}"`;
  const reference = field.references;
  if (reference) {
    if (reference.field !== "id") {
      throw new Error(
        `${where} references "${reference.model}.${reference.field}", but Jazz app files only support references to "id".`,
      );
    }
    switch (field.type) {
      case "string":
        return optionalColumn("uuid", field);
      case "string[]":
        return optionalColumn({ array: "uuid" }, field);
      case "number":
      case "number[]":
        throw new Error(
          `${where} cannot be emitted as a Jazz reference because Better Auth references must use string or string[] fields.`,
        );
      default:
        throw new Error(
          `${where} uses unsupported Better Auth reference type: ${String(field.type)}.`,
        );
    }
  }
  if (Array.isArray(field.type)) return optionalColumn({ enum: [...field.type] }, field);
  switch (field.type) {
    case "string":
      return optionalColumn("string", field);
    case "number":
      if (field.bigint) {
        throw new Error(
          `${where} uses Better Auth bigint numbers, which Jazz app files cannot represent.`,
        );
      }
      return optionalColumn("int", field);
    case "boolean":
      return optionalColumn("boolean", field);
    case "date":
      return optionalColumn("timestamp", field);
    case "json":
      return optionalColumn({ json: true }, field);
    case "string[]":
      return optionalColumn({ array: "string" }, field);
    case "number[]":
      if (field.bigint) {
        throw new Error(
          `${where} uses Better Auth bigint arrays, which Jazz app files cannot represent.`,
        );
      }
      return optionalColumn({ array: "int" }, field);
    default:
      throw new Error(`Unsupported Better Auth field type: ${String(field.type)}`);
  }
}

/**
 * The Better Auth tables as `jazz.app.json` tables, by table name. Each
 * reference column gets a forward relation `<column>Relation`.
 */
export function buildBetterAuthAppTables(args: {
  tables: BetterAuthDBSchema;
  getModelName: (model: string) => string;
  getFieldName: ({ model, field }: { model: string; field: string }) => string;
}): Record<string, AppFileTable> {
  const { tables, getModelName, getFieldName } = args;
  const result: Record<string, AppFileTable> = {};

  for (const [modelName, model] of Object.entries(tables)) {
    const tableName = getModelName(modelName);
    assertSchemaNameAllowed(tableName);
    assertAppFileIdentifier("table", tableName, `Model "${modelName}"`);
    const columns: Record<string, AppFileColumn> = {};
    const relations: NonNullable<AppFileTable["relations"]> = {};

    for (const [fieldName, field] of Object.entries(model.fields)) {
      if (fieldName === "id") continue;
      const storedFieldName = getFieldName({ model: modelName, field: fieldName });
      assertStoredFieldNameAllowed({ modelName, fieldName, storedFieldName });
      assertAppFileIdentifier("column", storedFieldName, `Field "${modelName}.${fieldName}"`);
      columns[storedFieldName] = toAppFileColumn(modelName, fieldName, field);
      if (field.references) {
        relations[`${storedFieldName}Relation`] = {
          rel: getModelName(field.references.model),
          column: storedFieldName,
        };
      }
    }

    for (const name of Object.keys(relations)) {
      if (name in columns) {
        throw new Error(`Generated relationship "${tableName}.${name}" collides with a column.`);
      }
    }
    result[tableName] = Object.keys(relations).length > 0 ? { columns, relations } : { columns };
  }

  return result;
}

/** A key of an object literal: bare when it can be. */
function objectKey(name: string): string {
  return APP_FILE_IDENTIFIER.test(name) ? name : JSON.stringify(name);
}

/** A readable column type as the `s.*` builder expression that declares it. */
function columnExpression(column: AppFileColumn): string {
  if (typeof column === "string") {
    const optional = column.endsWith("?");
    const base = optional ? column.slice(0, -1) : column;
    return `${scalarExpression(base)}${optional ? ".optional()" : ""}`;
  }
  const { optional, ...type } = column as Record<string, unknown> & { optional?: boolean };
  return `${typeExpression(type)}${optional ? ".optional()" : ""}`;
}

function scalarExpression(type: string): string {
  if (!["string", "uuid", "int", "float", "bigint", "boolean", "timestamp", "bytes"].includes(type))
    throw new Error(`unsupported column type ${type}`);
  return `s.${type}()`;
}

function typeExpression(type: unknown): string {
  if (typeof type === "string") return scalarExpression(type);
  const object = type as Record<string, unknown>;
  if (Array.isArray(object.enum))
    return `s.enum(${(object.enum as string[]).map((variant) => JSON.stringify(variant)).join(", ")})`;
  if (object.json !== undefined)
    return object.json === true ? "s.json()" : `s.json(${JSON.stringify(object.json)})`;
  if (object.array !== undefined) return `s.array(${typeExpression(object.array)})`;
  if (typeof object.type === "string") return scalarExpression(object.type);
  throw new Error(`unsupported column type ${JSON.stringify(type)}`);
}

/**
 * The Better Auth tables as `schema-better-auth/schema.ts`: `tables` (to
 * spread into the app's `s.defineSchema({ ...betterAuth.tables, … })`) and
 * `permissions` denying every client read and write (to spread into
 * `definePermissions({ ...betterAuth.permissions, … })`): only the backend's
 * adapter touches these tables. Each operation is `false` rather than left
 * out, as the files have always held them.
 */
export function buildBetterAuthSchemaSource(args: {
  tables: BetterAuthDBSchema;
  getModelName: (model: string) => string;
  getFieldName: ({ model, field }: { model: string; field: string }) => string;
}): string {
  const tables = buildBetterAuthAppTables(args);
  const names = Object.keys(tables).sort();
  const blocks = names.map((name) => {
    const table = tables[name]!;
    const columns = Object.entries(table.columns).map(
      ([column, type]) => `      ${objectKey(column)}: ${columnExpression(type)},`,
    );
    const relations = Object.entries(table.relations ?? {}).map(([relation, target]) =>
      "rel" in target
        ? `      ${objectKey(relation)}: s.rel(${JSON.stringify(target.rel)}, ${JSON.stringify(target.column)}),`
        : `      ${objectKey(relation)}: s.reverse(${JSON.stringify(target.reverse)}, ${JSON.stringify(target.relation)}),`,
    );
    return [
      `  ${objectKey(name)}: s.table(`,
      "    {",
      ...columns,
      "    },",
      relations.length > 0 ? ["    {", ...relations, "    },"].join("\n") : "    {},",
      "  ),",
    ].join("\n");
  });
  const deny = "{ read: false, insert: false, update: false, delete: false }";
  return [
    "// Generated by Better Auth's `generate` command through the Jazz adapter: rerun it",
    "// rather than editing. Spread both exports into your schema.ts:",
    "//   s.defineSchema({ ...betterAuth.tables, … })",
    "//   definePermissions({ ...betterAuth.permissions, … })",
    'import { schema as s } from "jazz-tools";',
    "",
    "export const tables = {",
    ...blocks,
    "};",
    "",
    "// Only the backend's adapter reads and writes these tables.",
    "export const permissions = {",
    ...names.map((name) => `  ${objectKey(name)}: ${deny},`),
    "} as const;",
    "",
  ].join("\n");
}

export function buildBetterAuthSchemaSourceFromTables(args: {
  tables: BetterAuthDBSchema;
  usePlural?: boolean;
}): string {
  const resolvers = createSchemaNameResolvers({ tables: args.tables, usePlural: args.usePlural });
  return buildBetterAuthSchemaSource({ tables: args.tables, ...resolvers });
}

/**
 * What the adapter's `createSchema` gives Better Auth's `generate` command:
 * `schema-better-auth/schema.ts` (or `file`), to import from the app's
 * schema.ts. Then `npx jazz-tools generate` writes the app's files.
 */
export function createJazzSchemaSourceFile(args: {
  file?: string;
  tables: BetterAuthDBSchema;
  getModelName: (model: string) => string;
  getFieldName: ({ model, field }: { model: string; field: string }) => string;
}): DBAdapterSchemaCreation {
  return {
    path: args.file ?? DEFAULT_SCHEMA_FILE_PATH,
    overwrite: true,
    code: buildBetterAuthSchemaSource(args),
  };
}

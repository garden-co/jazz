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
// jazz.app.json generation, for Better Auth's `generate` command.
// ---------------------------------------------------------------------------

const DEFAULT_APP_FILE_PATH = "./jazz.app.json";
const APP_FILE_SCHEMA_URL = "https://jazz.tools/schemas/jazz.app.schema.json";
const APP_FILE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
const MAX_LINE = 100;

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

/**
 * A Better Auth table merged over the app's existing one: Better Auth owns
 * the columns; relations the app added stay while their column does, and the
 * generated forward relation is only added for a column no relation names yet.
 */
function mergeTable(existing: unknown, generated: AppFileTable): AppFileTable {
  if (typeof existing !== "object" || existing === null || Array.isArray(existing)) {
    return generated;
  }
  const { columns: _old, relations: oldRelations, indexes, ...rest } = existing as AppFileTable;
  const columns = generated.columns;
  const relations: NonNullable<AppFileTable["relations"]> = {};
  const covered = new Set<string>();
  for (const [name, relation] of Object.entries(oldRelations ?? {})) {
    if ("column" in relation) {
      if (!(relation.column in columns)) continue;
      covered.add(relation.column);
    }
    relations[name] = relation;
  }
  for (const [name, relation] of Object.entries(generated.relations ?? {})) {
    if ("column" in relation && !covered.has(relation.column) && !(name in relations)) {
      relations[name] = relation;
    }
  }
  const keptIndexes = Array.isArray(indexes)
    ? indexes.filter((index: unknown) =>
        (Array.isArray(index) ? index : [index]).every(
          (column) => typeof column === "string" && (column.startsWith("$") || column in columns),
        ),
      )
    : undefined;
  return {
    columns,
    ...(Object.keys(relations).length > 0 ? { relations } : {}),
    ...(keptIndexes && keptIndexes.length > 0 ? { indexes: keptIndexes } : {}),
    ...rest,
  };
}

/** Where a JSON value starting at `start` ends (exclusive). Expects valid JSON. */
function skipJsonValue(text: string, start: number): number {
  let depth = 0;
  let i = start;
  do {
    const c = text[i]!;
    if (c === '"') {
      i++;
      while (text[i] !== '"') i += text[i] === "\\" ? 2 : 1;
    } else if (c === "{" || c === "[") depth++;
    else if (c === "}" || c === "]") depth--;
    else if (depth === 0) {
      // A scalar at the top: runs until a delimiter.
      while (i < text.length && !/[\s,}\]]/.test(text[i]!)) i++;
      return i;
    }
    i++;
  } while (depth > 0);
  return i;
}

function skipSpace(text: string, i: number): number {
  while (/\s/.test(text[i] ?? "")) i++;
  return i;
}

/** The entries of the JSON object at `start`, with each value's source span. */
function objectEntries(text: string, start: number) {
  const entries: { key: string; valueStart: number; valueEnd: number }[] = [];
  let i = skipSpace(text, start + 1);
  while (text[i] !== "}") {
    const keyEnd = skipJsonValue(text, i);
    const key = JSON.parse(text.slice(i, keyEnd)) as string;
    const valueStart = skipSpace(text, skipSpace(text, keyEnd) + 1);
    const valueEnd = skipJsonValue(text, valueStart);
    entries.push({ key, valueStart, valueEnd });
    i = skipSpace(text, valueEnd);
    if (text[i] === ",") i = skipSpace(text, i + 1);
  }
  return { entries, end: i + 1 };
}

/** A value as the app-file writer lays it out: on one line when it fits, expanded otherwise. */
function formatJson(value: unknown, indent: number, prefix: number): string {
  const inline = formatInline(value);
  if (indent + prefix + inline.length <= MAX_LINE || typeof value !== "object" || value === null) {
    return inline;
  }
  const pad = " ".repeat(indent + 2);
  if (Array.isArray(value)) {
    if (value.length === 0) return "[]";
    const items = value.map((item) => pad + formatJson(item, indent + 2, 1));
    return `[\n${items.join(",\n")}\n${" ".repeat(indent)}]`;
  }
  const entries = Object.entries(value);
  if (entries.length === 0) return "{}";
  const lines = entries.map(([key, item]) => {
    const head = `${JSON.stringify(key)}: `;
    return pad + head + formatJson(item, indent + 2, head.length + 1);
  });
  return `{\n${lines.join(",\n")}\n${" ".repeat(indent)}}`;
}

function formatInline(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(formatInline).join(", ")}]`;
  if (typeof value === "object" && value !== null) {
    const entries = Object.entries(value);
    if (entries.length === 0) return "{}";
    return `{ ${entries.map(([k, v]) => `${JSON.stringify(k)}: ${formatInline(v)}`).join(", ")} }`;
  }
  return JSON.stringify(value);
}

/**
 * `jazz.app.json` with the Better Auth tables written into it.
 *
 * Each Better Auth table replaces the app's table of that name (its columns
 * become exactly what Better Auth writes; see {@link mergeTable} for what the
 * app keeps). Every other table and every other key (`history`, `migrations`,
 * …) is kept as written; tables stay sorted by name. A Better Auth table the
 * app has but Better Auth no longer generates is kept. With no file yet, it
 * writes a new one holding only the Better Auth tables.
 *
 * The tables get no entry in `jazz.permissions.json`: a table without
 * policies denies every read and write, which is what Better Auth's tables
 * need (only the backend's adapter touches them).
 */
export function mergeBetterAuthTables(args: {
  appFile: string | null;
  tables: BetterAuthDBSchema;
  getModelName: (model: string) => string;
  getFieldName: ({ model, field }: { model: string; field: string }) => string;
}): string {
  const generated = buildBetterAuthAppTables(args);
  const formatTables = (tables: Record<string, string>) => {
    const names = Object.keys(tables).sort();
    if (names.length === 0) return "{}";
    return `{\n${names.map((name) => `    ${JSON.stringify(name)}: ${tables[name]}`).join(",\n")}\n  }`;
  };
  const formatTable = (name: string, table: AppFileTable) =>
    formatJson(table, 4, JSON.stringify(name).length + 2);

  if (args.appFile === null) {
    const tables = Object.fromEntries(
      Object.entries(generated).map(([name, table]) => [name, formatTable(name, table)]),
    );
    return `{\n  "$schema": ${JSON.stringify(APP_FILE_SCHEMA_URL)},\n  "format": 1,\n  "tables": ${formatTables(tables)}\n}\n`;
  }

  const text = args.appFile.replace(/\r\n/g, "\n");
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    throw new Error(`jazz.app.json is not valid JSON: ${(error as Error).message}`);
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("jazz.app.json must hold an object.");
  }
  const tablesValue = (parsed as { tables?: unknown }).tables;
  if (typeof tablesValue !== "object" || tablesValue === null || Array.isArray(tablesValue)) {
    throw new Error('jazz.app.json has no "tables" object.');
  }

  // Splice: only the `tables` object is rewritten, and within it only the
  // Better Auth tables, so the rest of the file stays exactly as written and
  // regenerating unchanged tables gives back the same bytes.
  const top = objectEntries(text, skipSpace(text, 0));
  const tablesEntry = top.entries.find((entry) => entry.key === "tables")!;
  const existing = objectEntries(text, tablesEntry.valueStart);
  const tables: Record<string, string> = {};
  for (const entry of existing.entries) {
    tables[entry.key] = text.slice(entry.valueStart, entry.valueEnd);
  }
  for (const [name, table] of Object.entries(generated)) {
    const merged = mergeTable((tablesValue as Record<string, unknown>)[name], table);
    tables[name] = formatTable(name, merged);
  }
  const result =
    text.slice(0, tablesEntry.valueStart) + formatTables(tables) + text.slice(existing.end);
  return result.endsWith("\n") ? result : `${result}\n`;
}

/**
 * What the adapter's `createSchema` gives Better Auth's `generate` command:
 * the app's `jazz.app.json` (`file`, default `./jazz.app.json`) with the
 * Better Auth tables written into it.
 */
export async function createJazzAppFile(args: {
  file?: string;
  tables: BetterAuthDBSchema;
  getModelName: (model: string) => string;
  getFieldName: ({ model, field }: { model: string; field: string }) => string;
}): Promise<DBAdapterSchemaCreation> {
  const path = args.file ?? DEFAULT_APP_FILE_PATH;
  const { readFile } = await import("node:fs/promises");
  let appFile: string | null = null;
  try {
    appFile = await readFile(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const code = mergeBetterAuthTables({ ...args, appFile });
  console.log(betterAuthAppFileNotes(Object.keys(buildBetterAuthAppTables(args))));
  return { path, overwrite: true, code };
}

/**
 * What the user does after `generate` wrote the tables (JSON has no comments
 * to carry it), with the `jazz.permissions.json` entries to paste.
 */
export function betterAuthAppFileNotes(tableNames: readonly string[]): string {
  const deny = '{ "read": "false", "insert": "false", "update": "false", "delete": "false" }';
  return [
    `Jazz: wrote the Better Auth tables (${tableNames.join(", ")}) into jazz.app.json.`,
    "",
    "Next, run `npx jazz-tools generate`. If it says the schema changed, run",
    "`npx jazz-tools migrations create --name <name>` and review the migration it writes.",
    "",
    "Only the backend's adapter touches these tables: a table without policies denies every client",
    "read and write. To say so explicitly (and silence the deploy warning about tables without",
    'policies), paste these under "tables" in jazz.permissions.json:',
    "",
    ...tableNames.map(
      (name, i) => `    ${JSON.stringify(name)}: ${deny}${i < tableNames.length - 1 ? "," : ""}`,
    ),
    "",
  ].join("\n");
}

export function mergeBetterAuthTablesFromTables(args: {
  appFile: string | null;
  tables: BetterAuthDBSchema;
  usePlural?: boolean;
}): string {
  const resolvers = createSchemaNameResolvers({ tables: args.tables, usePlural: args.usePlural });
  return mergeBetterAuthTables({ ...args, ...resolvers });
}

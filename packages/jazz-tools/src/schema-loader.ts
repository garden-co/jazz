import { existsSync } from "fs";
import { access } from "fs/promises";
import { basename, dirname, join, resolve } from "path";
import { pathToFileURL } from "url";
import { register as registerCjs } from "tsx/cjs/api";
import { register as registerEsm } from "tsx/esm/api";
import { schemaToWasm } from "./codegen/schema-reader.js";
import { getCollectedSchema, resetCollectedState } from "./dsl.js";
import type { Column, OperationPolicy, Schema, SqlType, TablePolicies } from "./schema.js";
import type { ColumnDescriptor, ColumnType, TableSchema, WasmSchema } from "./drivers/types.js";
import { schemaDefinitionToAst } from "./migrations.js";
import type { CompiledPermissionsMap } from "./schema-permissions.js";
import { validatePermissionsAgainstSchema } from "./schema-permissions.js";

registerEsm();

let importCounter = 0;

export interface LoadedSchemaProject {
  rootDir: string;
  schemaFile: string;
  permissionsFile?: string;
  permissions?: CompiledPermissionsMap;
  schema: Schema;
  wasmSchema: WasmSchema;
}

function requireTsModule<T>(filePath: string, namespace: string): T {
  const loader = registerCjs({ namespace: `${namespace}-${++importCounter}` });
  try {
    return loader.require(resolve(filePath), import.meta.url) as T;
  } finally {
    loader.unregister();
  }
}

async function loadTsModule(filePath: string): Promise<Record<string, unknown>> {
  resetCollectedState();
  const url = pathToFileURL(filePath).href + `?v=${++importCounter}`;
  return (await import(url)) as Record<string, unknown>;
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

function columnTypeToSqlType(columnType: ColumnType): SqlType {
  switch (columnType.type) {
    case "Text":
      return "TEXT";
    case "Boolean":
      return "BOOLEAN";
    case "Integer":
      return "INTEGER";
    case "Double":
      return "REAL";
    case "Timestamp":
      return "TIMESTAMP";
    case "Uuid":
      return "UUID";
    case "Bytea":
      return "BYTEA";
    case "Json":
      return columnType.schema ? { kind: "JSON", schema: columnType.schema } : { kind: "JSON" };
    case "Enum":
      return { kind: "ENUM", variants: [...columnType.variants] };
    case "Array":
      return { kind: "ARRAY", element: columnTypeToSqlType(columnType.element) };
    case "BigInt":
      throw new Error("Root schema loading does not yet support BIGINT columns.");
    case "Row":
      throw new Error("Root schema loading does not yet support row-valued columns.");
  }
}

function wasmColumnToAst(column: ColumnDescriptor): Column {
  return {
    name: column.name,
    sqlType: columnTypeToSqlType(column.column_type),
    nullable: column.nullable,
    references: column.references,
  };
}

function wasmTableToAst(name: string, table: TableSchema): Schema["tables"][number] {
  return {
    name,
    columns: table.columns.map(wasmColumnToAst),
    policies: table.policies as TablePolicies | undefined,
  };
}

function wasmSchemaToAst(wasmSchema: WasmSchema): Schema {
  return {
    tables: Object.entries(wasmSchema).map(([tableName, table]) =>
      wasmTableToAst(tableName, table),
    ),
  };
}

function isTypedAppLike(value: Record<string, unknown>): value is { wasmSchema: WasmSchema } {
  if (!("wasmSchema" in value)) {
    return false;
  }

  const schema = value.wasmSchema;
  return typeof schema === "object" && schema !== null && !Array.isArray(schema);
}

function schemaFromLoadedModule(loaded: Record<string, unknown>): Schema | null {
  const collected = getCollectedSchema();
  if (collected.tables.length > 0) {
    return collected;
  }

  const candidates = [loaded.schema, loaded.schemaDef, loaded.default, loaded.app].filter(
    (candidate): candidate is Record<string, unknown> =>
      typeof candidate === "object" && candidate !== null,
  );

  for (const candidate of candidates) {
    if (isTypedAppLike(candidate)) {
      return wasmSchemaToAst(candidate.wasmSchema);
    }

    try {
      return schemaDefinitionToAst(candidate as any);
    } catch {
      // Try the next supported export shape.
    }
  }

  return null;
}

async function loadSchemaAst(filePath: string): Promise<Schema> {
  const loaded = await loadTsModule(filePath);
  const directSchema = schemaFromLoadedModule(loaded);
  if (directSchema) {
    return directSchema;
  }

  resetCollectedState();
  const required = requireTsModule<Record<string, unknown>>(filePath, "jazz-tools-schema");
  const requiredSchema = schemaFromLoadedModule(required);
  if (requiredSchema) {
    return requiredSchema;
  }

  throw new Error(
    `Could not find a schema export in ${basename(filePath)}. ` +
      "Use side-effect table(...) declarations, or export schema/app/default from schema.ts.",
  );
}

function isOperationPolicyLike(input: unknown): input is OperationPolicy {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    return false;
  }
  const opPolicy = input as Record<string, unknown>;
  return Object.keys(opPolicy).every((key) => key === "using" || key === "with_check");
}

function isTablePoliciesLike(input: unknown): input is TablePolicies {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    return false;
  }
  const tablePolicy = input as Record<string, unknown>;
  const validOperationKeys = ["select", "insert", "update", "delete"];
  return Object.entries(tablePolicy).every(([key, value]) => {
    if (!validOperationKeys.includes(key)) {
      return false;
    }
    return isOperationPolicyLike(value);
  });
}

function isPermissionsMap(input: unknown): input is Record<string, TablePolicies> {
  if (typeof input !== "object" || input === null) {
    return false;
  }
  return Object.values(input).every((value) => isTablePoliciesLike(value));
}

async function loadPermissionsModule(filePath: string): Promise<Record<string, TablePolicies>> {
  const module = requireTsModule<Record<string, unknown>>(filePath, "jazz-tools-permissions");
  const candidate = module.default ?? module.permissions ?? null;
  if (!candidate) {
    throw new Error(
      `Missing permissions export in ${basename(filePath)}. ` +
        `Export default definePermissions(...) (or export const permissions = definePermissions(...)).`,
    );
  }
  if (!isPermissionsMap(candidate)) {
    throw new Error(
      `Invalid permissions export in ${basename(filePath)}. Expected default export from definePermissions(...).`,
    );
  }
  return candidate;
}

function findInlinePolicyTables(schema: Schema): string[] {
  return schema.tables.filter((table) => table.policies).map((table) => table.name);
}

function resolveRootSchemaFiles(schemaDir: string): { rootDir: string; schemaFile: string } | null {
  const directRootSchemaFile = join(schemaDir, "schema.ts");
  if (existsSync(directRootSchemaFile)) {
    return {
      rootDir: schemaDir,
      schemaFile: directRootSchemaFile,
    };
  }

  if (basename(schemaDir) !== "schema") {
    return null;
  }

  const appRoot = dirname(schemaDir);
  const parentRootSchemaFile = join(appRoot, "schema.ts");
  if (existsSync(parentRootSchemaFile)) {
    return {
      rootDir: appRoot,
      schemaFile: parentRootSchemaFile,
    };
  }

  return null;
}

export async function hasRootSchema(schemaDir: string): Promise<boolean> {
  return resolveRootSchemaFiles(schemaDir) !== null;
}

export async function loadCompiledSchema(schemaDir: string): Promise<LoadedSchemaProject> {
  const resolved = resolveRootSchemaFiles(schemaDir);
  if (!resolved) {
    throw new Error(`Schema file not found. Expected ${join(schemaDir, "schema.ts")}.`);
  }

  let schema = await loadSchemaAst(resolved.schemaFile);
  const tablesWithInlinePolicies = findInlinePolicyTables(schema);
  if (tablesWithInlinePolicies.length > 0) {
    throw new Error(
      `Inline table permissions in ${basename(resolved.schemaFile)} are no longer supported. ` +
        "Move policies to permissions.ts. " +
        `Tables: ${tablesWithInlinePolicies.join(", ")}.`,
    );
  }

  const permissionsFile = join(resolved.rootDir, "permissions.ts");
  let permissions: CompiledPermissionsMap | undefined;
  if (await pathExists(permissionsFile)) {
    permissions = await loadPermissionsModule(permissionsFile);
    validatePermissionsAgainstSchema(
      schema.tables.map((table) => table.name),
      permissions,
    );
  }

  return {
    rootDir: resolved.rootDir,
    schemaFile: resolved.schemaFile,
    permissionsFile: (await pathExists(permissionsFile)) ? permissionsFile : undefined,
    permissions,
    schema,
    wasmSchema: schemaToWasm(schema),
  };
}

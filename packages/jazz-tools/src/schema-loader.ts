import { existsSync } from "fs";
import { access, rm } from "fs/promises";
import { basename, dirname, join, resolve } from "path";
import { pathToFileURL } from "url";
import { build } from "esbuild";
import { schemaToWasm } from "./codegen/schema-reader.js";
import { getCollectedSchema, resetCollectedState } from "./dsl.js";
import type { Column, OperationPolicy, Schema, SqlType, TablePolicies } from "./schema.js";
import type {
  ColumnDescriptor,
  ColumnType,
  TableSchema,
  Value,
  WasmSchema,
} from "./drivers/types.js";
import { schemaDefinitionToAst } from "./migrations.js";
import type { CompiledPermissionsMap } from "./schema-permissions.js";
import { validatePermissionsAgainstSchema } from "./schema-permissions.js";

let importCounter = 0;

export interface LoadedSchemaProject {
  rootDir: string;
  schemaFile: string;
  permissionsFile?: string;
  permissions?: CompiledPermissionsMap;
  schema: Schema;
  wasmSchema: WasmSchema;
}

async function bundleToTempFile(filePath: string): Promise<string> {
  const sourceDir = dirname(resolve(filePath));
  const outFile = join(sourceDir, `.jazz-schema-${++importCounter}.mjs`);

  await build({
    entryPoints: [resolve(filePath)],
    bundle: true,
    format: "esm",
    platform: "node",
    outfile: outFile,
    packages: "external",
  });

  return outFile;
}

async function loadTsModule(filePath: string): Promise<Record<string, unknown>> {
  resetCollectedState();
  const outFile = await bundleToTempFile(filePath);
  try {
    return (await import(pathToFileURL(outFile).href)) as Record<string, unknown>;
  } finally {
    await rm(outFile, { force: true }).catch(() => undefined);
  }
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

function wasmValueToDefault(value: Value, columnType: ColumnType): unknown {
  switch (value.type) {
    case "Null":
      return null;
    case "Integer":
    case "BigInt":
    case "Double":
    case "Boolean":
    case "Text":
    case "Timestamp":
    case "Uuid":
      if (columnType.type === "Json") {
        return JSON.parse(String(value.value));
      }
      return value.value;
    case "Bytea":
      return new Uint8Array(value.value);
    case "Array": {
      if (columnType.type !== "Array") {
        throw new Error("Array default does not match column type.");
      }
      return value.value.map((inner) => wasmValueToDefault(inner, columnType.element));
    }
    case "Row":
      throw new Error("Root schema loading does not yet support row-valued defaults.");
  }
}

function columnMergeStrategyToAst(
  strategy: ColumnDescriptor["merge_strategy"],
): Column["mergeStrategy"] {
  switch (strategy) {
    case undefined:
      return undefined;
    case "Counter":
      return "counter";
  }
}

function wasmColumnToAst(column: ColumnDescriptor): Column {
  return {
    name: column.name,
    sqlType: columnTypeToSqlType(column.column_type),
    nullable: column.nullable,
    default:
      column.default === undefined
        ? undefined
        : wasmValueToDefault(column.default, column.column_type),
    references: column.references,
    mergeStrategy: columnMergeStrategyToAst(column.merge_strategy),
  };
}

function wasmTableToAst(name: string, table: TableSchema): Schema["tables"][number] {
  return {
    name,
    columns: table.columns.map(wasmColumnToAst),
    indexedColumns: table.indexed_columns ? [...table.indexed_columns] : undefined,
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

  const collected = getCollectedSchema();
  if (collected.tables.length > 0) {
    return collected;
  }

  return null;
}

async function loadSchemaAst(filePath: string): Promise<Schema> {
  const loaded = await loadTsModule(filePath);
  const directSchema = schemaFromLoadedModule(loaded);
  if (directSchema) {
    return directSchema;
  }

  throw new Error(
    `Could not find a schema in ${filePath}. ` +
      `Define tables with side-effect table(...) calls at module scope, ` +
      `or export const schema / app / default. ` +
      `By convention, schema.ts (and permissions.ts) live at the project root ` +
      `(or src/lib/ for SvelteKit). See https://jazz.tools/docs/schemas/defining-tables.`,
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
  const module = await loadTsModule(filePath);
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

async function tryLoadPermissionsFromSchemaModule(
  filePath: string,
): Promise<Record<string, TablePolicies> | undefined> {
  const module = await loadTsModule(filePath);
  const candidate = module.permissions ?? null;
  if (!candidate) {
    return undefined;
  }
  if (!isPermissionsMap(candidate)) {
    throw new Error(
      `Invalid permissions export in ${basename(filePath)}. Expected definePermissions(...).`,
    );
  }
  return candidate;
}

function findInlinePolicyTables(schema: Schema): string[] {
  return schema.tables.filter((table) => table.policies).map((table) => table.name);
}

interface SchemaRootCandidate {
  rootDir: string;
  schemaFile: string;
}

function schemaRootCandidates(appRoot: string): SchemaRootCandidate[] {
  return [
    {
      rootDir: appRoot,
      schemaFile: join(appRoot, "schema.ts"),
    },
    {
      rootDir: join(appRoot, "src"),
      schemaFile: join(appRoot, "src", "schema.ts"),
    },
    {
      rootDir: join(appRoot, "src", "lib"),
      schemaFile: join(appRoot, "src", "lib", "schema.ts"),
    },
  ];
}

function resolveSchemaRootCandidate(appRoot: string): SchemaRootCandidate | null {
  const matches = schemaRootCandidates(appRoot).filter((candidate) =>
    existsSync(candidate.schemaFile),
  );
  if (matches.length === 0) return null;
  if (matches.length > 1) {
    throw new Error(
      `Ambiguous schema location: found ${matches
        .map((m) => m.schemaFile)
        .join(" and ")}. Delete all but one so the schema root is unambiguous.`,
    );
  }
  return matches[0];
}

function describeExpectedSchemaFiles(schemaDir: string): string {
  return schemaRootCandidates(schemaDir)
    .map((candidate) => candidate.schemaFile)
    .join(" or ");
}

function resolveRootSchemaFiles(schemaDir: string): SchemaRootCandidate | null {
  return resolveSchemaRootCandidate(schemaDir);
}

export async function hasRootSchema(schemaDir: string): Promise<boolean> {
  return resolveRootSchemaFiles(schemaDir) !== null;
}

export async function loadCompiledSchema(schemaDir: string): Promise<LoadedSchemaProject> {
  const resolved = resolveRootSchemaFiles(schemaDir);
  if (!resolved) {
    throw new Error(
      `Schema file not found. Jazz looks for schema.ts (and, optionally, permissions.ts alongside it) ` +
        `at the project root, ./src/, or ./src/lib/ (SvelteKit). ` +
        `Searched: ${describeExpectedSchemaFiles(schemaDir)}. ` +
        `Pass --schema-dir <path> to point at a different root.`,
    );
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
  let resolvedPermissionsFile: string | undefined;
  if (await pathExists(permissionsFile)) {
    resolvedPermissionsFile = permissionsFile;
    permissions = await loadPermissionsModule(permissionsFile);
    validatePermissionsAgainstSchema(
      schema.tables.map((table) => table.name),
      permissions,
    );
  } else {
    const schemaModulePermissions = await tryLoadPermissionsFromSchemaModule(resolved.schemaFile);
    if (schemaModulePermissions) {
      resolvedPermissionsFile = resolved.schemaFile;
      permissions = schemaModulePermissions;
      validatePermissionsAgainstSchema(
        schema.tables.map((table) => table.name),
        permissions,
      );
    }
  }

  return {
    rootDir: resolved.rootDir,
    schemaFile: resolved.schemaFile,
    permissionsFile: resolvedPermissionsFile,
    permissions,
    schema,
    wasmSchema: schemaToWasm(schema),
  };
}

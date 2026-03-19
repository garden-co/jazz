#!/usr/bin/env node

// CLI for jazz-tools schema tooling

import { spawn } from "child_process";
import { access, mkdir, readFile, readdir, writeFile } from "fs/promises";
import { join, basename, dirname, resolve } from "path";
import { pathToFileURL } from "url";
import { register as registerCjs } from "tsx/cjs/api";
import { register as registerEsm } from "tsx/esm/api";
import { schemaToSql, lensesToSql } from "./sql-gen.js";
import { getCollectedSchema, getCollectedMigrations, resetCollectedState } from "./dsl.js";
import { generateClient } from "./codegen/index.js";
import type { Lens, Schema, SqlType, TablePolicies, OperationPolicy, Column } from "./schema.js";
import type { ColumnDescriptor, ColumnType, WasmSchema, TableSchema } from "./drivers/types.js";
import { buildEndpointUrl } from "./runtime/sync-transport.js";
import { fetchStoredWasmSchema } from "./runtime/schema-fetch.js";
import { schemaDefinitionToAst } from "./migrations.js";
import type { DefinedMigration } from "./migrations.js";

export interface BuildOptions {
  jazzBin: string;
  schemaDir: string;
}

function parseArgs(): { command: string; options: BuildOptions } {
  const args = process.argv.slice(2);
  const command = args[0] || "";
  let jazzBin = "jazz-tools";
  let schemaDir = join(process.cwd(), "schema");

  for (let i = 1; i < args.length; i++) {
    const arg = args[i];
    const nextArg = args[i + 1];
    if (arg === "--jazz-bin" && nextArg) {
      jazzBin = nextArg;
      i += 1;
    } else if (arg === "--schema-dir" && nextArg) {
      schemaDir = nextArg;
      i += 1;
    }
  }

  return { command, options: { jazzBin, schemaDir } };
}

// Allow loading `.ts` schema files when invoked via `node dist/cli.js`.
registerEsm();

// Counter for cache-busting module loads.
let importCounter = 0;

function requirePermissionsModule<T>(filePath: string): T {
  const loader = registerCjs({ namespace: `jazz-tools-cli-permissions-${++importCounter}` });
  try {
    return loader.require(resolve(filePath), import.meta.url) as T;
  } finally {
    loader.unregister();
  }
}

function requireTsModule<T>(filePath: string, namespace: string): T {
  const loader = registerCjs({ namespace: `${namespace}-${++importCounter}` });
  try {
    return loader.require(resolve(filePath), import.meta.url) as T;
  } finally {
    loader.unregister();
  }
}

async function loadSchemaModule(filePath: string): Promise<Record<string, unknown>> {
  resetCollectedState();
  const url = pathToFileURL(filePath).href + `?v=${++importCounter}`;
  return (await import(url)) as Record<string, unknown>;
}

async function loadSchema(filePath: string): Promise<Schema> {
  const loaded = await loadSchemaModule(filePath);
  const directSchema = schemaFromLoadedModule(loaded);
  if (directSchema) {
    return directSchema;
  }

  resetCollectedState();
  const required = requireTsModule<Record<string, unknown>>(filePath, "jazz-tools-cli-schema");
  const requiredSchema = schemaFromLoadedModule(required);
  if (requiredSchema) {
    return requiredSchema;
  }

  throw new Error(
    `Could not find a schema export in ${basename(filePath)}. ` +
      "Use side-effect table(...) declarations, or export schema/app/default from schema.ts.",
  );
}

async function loadMigrationModule(filePath: string): Promise<Lens[]> {
  resetCollectedState();
  const url = pathToFileURL(filePath).href + `?v=${++importCounter}`;
  await import(url);
  return getCollectedMigrations();
}

async function generateSqlFile(sqlFile: string, schema: Schema): Promise<void> {
  await mkdir(dirname(sqlFile), { recursive: true });
  const sql = schemaToSql(schema);
  await writeFile(sqlFile, sql);
  console.log(`Generated: ${basename(sqlFile)}`);
}

async function generateAppTs(schemaDir: string, schema: Schema): Promise<void> {
  const output = generateClient(schema);
  const appTsPath = join(schemaDir, "app.ts");
  await writeFile(appTsPath, output);
  console.log(`Generated: app.ts`);
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

/**
 * Check if a filename is a migration TypeScript stub.
 *
 * Valid format: `migration_v1_v2_455a1f10a158_357c464c4c43.ts`
 */
function isMigrationTsStub(filename: string): boolean {
  const pattern = /^migration_v\d+_v\d+_[0-9a-f]{12}_[0-9a-f]{12}\.ts$/;
  return pattern.test(filename);
}

/**
 * Generate migration SQL filename with direction before hashes.
 *
 * Input: migration_v1_v2_455a1f10a158_357c464c4c43.ts
 * Output: migration_v1_v2_fwd_455a1f10a158_357c464c4c43.sql
 */
function migrationSqlFilename(tsFile: string, direction: "fwd" | "bwd"): string {
  const dir = tsFile.substring(0, tsFile.lastIndexOf("/") + 1);
  const name = basename(tsFile, ".ts");

  const match = name.match(/^(migration_v\d+_v\d+)_([0-9a-f]{12})_([0-9a-f]{12})$/);
  if (!match) {
    return tsFile.replace(/\.ts$/, `_${direction}.sql`);
  }

  const [, prefix, hash1, hash2] = match;
  return `${dir}${prefix}_${direction}_${hash1}_${hash2}.sql`;
}

async function generateSqlForMigrationFile(tsFile: string): Promise<void> {
  const lenses = await loadMigrationModule(tsFile);

  if (lenses.length === 0) {
    console.error(`No migration found in ${basename(tsFile)}`);
    return;
  }

  const fwdSql = lensesToSql(lenses, "fwd");
  const bwdSql = lensesToSql(lenses, "bwd");

  const fwdFile = migrationSqlFilename(tsFile, "fwd");
  const bwdFile = migrationSqlFilename(tsFile, "bwd");

  await writeFile(fwdFile, fwdSql);
  await writeFile(bwdFile, bwdSql);

  console.log(`Generated: ${basename(fwdFile)}`);
  console.log(`Generated: ${basename(bwdFile)}`);
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
  const module = requirePermissionsModule<Record<string, unknown>>(filePath);
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

function mergePermissionsIntoSchema(
  schema: Schema,
  compiledPermissions: Record<string, TablePolicies>,
): Schema {
  const schemaTableNames = new Set(schema.tables.map((table) => table.name));
  const unknownTables = Object.keys(compiledPermissions).filter(
    (tableName) => !schemaTableNames.has(tableName),
  );
  if (unknownTables.length > 0) {
    throw new Error(
      `permissions.ts defines permissions for unknown table(s): ${unknownTables.join(", ")}.`,
    );
  }

  return {
    tables: schema.tables.map((table) => {
      const external = compiledPermissions[table.name];
      if (!external) {
        return table;
      }
      return {
        ...table,
        policies: external,
      };
    }),
  };
}

/**
 * Check if a path exists
 */
const pathExists = async (path: string): Promise<boolean> => {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
};

const findMonorepoJazzBinary = async (): Promise<string | null> => {
  let currentDir = process.cwd();
  while (true) {
    const cargoTomlPath = join(currentDir, "Cargo.toml");
    const monorepoJazzToolsPath = join(currentDir, "target", "debug", "jazz-tools");
    const monorepoJazzPath = join(currentDir, "target", "debug", "jazz");

    if ((await pathExists(cargoTomlPath)) && (await pathExists(monorepoJazzToolsPath))) {
      return monorepoJazzToolsPath;
    }

    // Backward compatibility for older local builds.
    if ((await pathExists(cargoTomlPath)) && (await pathExists(monorepoJazzPath))) {
      return monorepoJazzPath;
    }

    const parentDir = dirname(currentDir);
    if (parentDir === currentDir) {
      return null;
    }
    currentDir = parentDir;
  }
};

async function ensurePermissionsTestStub(schemaDir: string): Promise<void> {
  const permissionsFile = join(schemaDir, "permissions.ts");
  if (!(await pathExists(permissionsFile))) {
    return;
  }

  const testFile = join(schemaDir, "permissions.test.ts");
  if (await pathExists(testFile)) {
    return;
  }

  const template = `/**
 * Permissions test starter.
 *
 * Suggested shape (jazz-tools/testing):
 * - createPolicyTestApp(...) for an isolated test app
 * - testApp.seed(...) to set up synthetic fixtures
 * - testApp.as(...) for request-scoped user clients
 * - testApp.expectAllowed(...) / testApp.expectDenied(...) assertions
 */
import { describe, it } from "vitest";

describe.skip("permissions", () => {
  it("add policy tests", () => {
    // Starter file generated by jazz-tools build.
  });
});
`;

  await writeFile(testFile, template);
  console.log(`Generated: permissions.test.ts`);
}

type SchemaLayout =
  | {
      mode: "legacy";
      schemaFile: string;
      permissionsDir: string;
      outputDir: string;
    }
  | {
      mode: "root";
      schemaFile: string;
      permissionsDir: string;
      outputDir: string;
    };

async function resolveSchemaLayout(schemaDir: string): Promise<SchemaLayout | null> {
  const legacySchemaFile = join(schemaDir, "current.ts");
  if (await pathExists(legacySchemaFile)) {
    return {
      mode: "legacy",
      schemaFile: legacySchemaFile,
      permissionsDir: schemaDir,
      outputDir: schemaDir,
    };
  }

  const directRootSchemaFile = join(schemaDir, "schema.ts");
  if (await pathExists(directRootSchemaFile)) {
    return {
      mode: "root",
      schemaFile: directRootSchemaFile,
      permissionsDir: schemaDir,
      outputDir: join(schemaDir, "schema"),
    };
  }

  if (basename(schemaDir) !== "schema") {
    return null;
  }

  const appRoot = dirname(schemaDir);
  const parentRootSchemaFile = join(appRoot, "schema.ts");
  if (!(await pathExists(parentRootSchemaFile))) {
    return null;
  }

  return {
    mode: "root",
    schemaFile: parentRootSchemaFile,
    permissionsDir: appRoot,
    outputDir: schemaDir,
  };
}

type JazzBuildResult = { type: "close"; code: number | null } | { type: "error"; error: Error };

async function runJazzBuild(
  jazzBin: string,
  schemaDir: string,
  searchJazzBinOnError: boolean = true,
): Promise<void> {
  const result: JazzBuildResult = await new Promise((resolve) => {
    console.log(`\nRunning: ${jazzBin} build --ts --schema-dir ${schemaDir}`);
    const child = spawn(jazzBin, ["build", "--ts", "--schema-dir", schemaDir], {
      stdio: "inherit",
    });
    child.on("close", (code) => {
      resolve({ type: "close", code });
    });
    child.on("error", (error) => {
      resolve({ type: "error", error });
    });
  });
  if (result.type === "close") {
    if (result.code !== 0) {
      console.warn(
        `jazz-tools build exited with code ${result.code} (versioned schemas not generated)`,
      );
    }
    return;
  }

  const error = result.error as NodeJS.ErrnoException;
  if (error.code === "ENOENT") {
    const monorepoJazzPath = searchJazzBinOnError ? await findMonorepoJazzBinary() : null;
    if (monorepoJazzPath) {
      console.log(
        `jazz-tools binary not found at '${jazzBin}'. Using monorepo binary at '${monorepoJazzPath}'`,
      );
      return runJazzBuild(monorepoJazzPath, schemaDir, false);
    } else {
      console.warn(
        `jazz-tools binary not found at '${jazzBin}'. Use --jazz-bin to specify the path.\n` +
          `Versioned schemas will not be generated.`,
      );
    }
  }

  console.warn(`jazz-tools build failed: ${error.message}`);
}

export async function build(options: BuildOptions): Promise<void> {
  const { jazzBin, schemaDir } = options;
  const layout = await resolveSchemaLayout(schemaDir);
  if (!layout) {
    console.error(
      `Schema file not found. Expected either ${join(schemaDir, "current.ts")} or ${join(schemaDir, "schema.ts")}.`,
    );
    process.exit(1);
  }

  if (layout.mode === "legacy") {
    const files = await readdir(layout.outputDir);
    const tsFiles = files.filter((f) => f.endsWith(".ts"));
    for (const file of tsFiles.filter((name) => isMigrationTsStub(name))) {
      await generateSqlForMigrationFile(join(layout.outputDir, file));
    }
  }

  let schema = await loadSchema(layout.schemaFile);
  const tablesWithInlinePolicies = schema.tables
    .filter((table) => table.policies)
    .map((t) => t.name);
  if (tablesWithInlinePolicies.length > 0) {
    throw new Error(
      `Inline table permissions in ${basename(layout.schemaFile)} are no longer supported. ` +
        "Move policies to permissions.ts. " +
        `Tables: ${tablesWithInlinePolicies.join(", ")}.`,
    );
  }

  if (layout.mode === "legacy") {
    // Generate app.ts before loading permissions.ts so permissions can import it for typing.
    await generateAppTs(layout.outputDir, schema);
  }

  const permissionsFile = join(layout.permissionsDir, "permissions.ts");
  if (await pathExists(permissionsFile)) {
    const permissions = await loadPermissionsModule(permissionsFile);
    schema = mergePermissionsIntoSchema(schema, permissions);
  }

  await generateSqlFile(join(layout.outputDir, "current.sql"), schema);
  if (layout.mode === "legacy") {
    await generateAppTs(layout.outputDir, schema);
  }
  await ensurePermissionsTestStub(layout.permissionsDir);

  if (layout.mode === "legacy") {
    await runJazzBuild(jazzBin, layout.outputDir);
  }
}

export interface MigrationCommandOptions {
  serverUrl: string;
  adminSecret: string;
  migrationsDir: string;
}

export interface CreateMigrationOptions extends MigrationCommandOptions {
  fromHash: string;
  toHash: string;
}

export interface PushMigrationOptions extends MigrationCommandOptions {
  fromHash: string;
  toHash: string;
}

function getFlagValue(args: string[], flag: string): string | undefined {
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (!arg) {
      continue;
    }
    if (arg === flag) {
      return args[i + 1];
    }
    const prefix = `${flag}=`;
    if (arg.startsWith(prefix)) {
      return arg.slice(prefix.length);
    }
  }
  return undefined;
}

function resolveMigrationOptions(args: string[]): MigrationCommandOptions {
  const serverUrl = getFlagValue(args, "--server-url") ?? process.env.JAZZ_SERVER_URL;
  const adminSecret = getFlagValue(args, "--admin-secret") ?? process.env.JAZZ_ADMIN_SECRET;
  const migrationsDir = resolve(
    process.cwd(),
    getFlagValue(args, "--migrations-dir") ?? join(process.cwd(), "migrations"),
  );

  if (!serverUrl) {
    throw new Error("Missing server URL. Pass --server-url <url> or set JAZZ_SERVER_URL.");
  }

  if (!adminSecret) {
    throw new Error("Missing admin secret. Pass --admin-secret <secret> or set JAZZ_ADMIN_SECRET.");
  }

  return {
    serverUrl,
    adminSecret,
    migrationsDir,
  };
}

function normalizeFullSchemaHash(hash: string, label: string): string {
  const normalized = hash.trim().toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(normalized)) {
    throw new Error(`${label} must be a 64-character lowercase hex schema hash.`);
  }
  return normalized;
}

function columnTypeSignature(columnType: ColumnType): string {
  return JSON.stringify(columnType);
}

function columnsEqual(left: ColumnDescriptor, right: ColumnDescriptor): boolean {
  return (
    left.name === right.name &&
    left.nullable === right.nullable &&
    left.references === right.references &&
    columnTypeSignature(left.column_type) === columnTypeSignature(right.column_type)
  );
}

function tableSchemasEqual(
  left: WasmSchema[string] | undefined,
  right: WasmSchema[string] | undefined,
): boolean {
  if (!left || !right) {
    return false;
  }

  if (left.columns.length !== right.columns.length) {
    return false;
  }

  return left.columns.every((column, index) => columnsEqual(column, right.columns[index]!));
}

function changedTableNames(fromSchema: WasmSchema, toSchema: WasmSchema): string[] {
  const names = new Set([...Object.keys(fromSchema), ...Object.keys(toSchema)]);
  return [...names].filter(
    (tableName) => !tableSchemasEqual(fromSchema[tableName], toSchema[tableName]),
  );
}

function pickWitnessSchema(schema: WasmSchema, tableNames: readonly string[]): WasmSchema {
  return Object.fromEntries(
    tableNames
      .filter((tableName) => schema[tableName])
      .map((tableName) => [tableName, schema[tableName]!]),
  );
}

function indentBlock(text: string, indent: number): string {
  const prefix = " ".repeat(indent);
  return text
    .split("\n")
    .map((line) => (line.length === 0 ? line : `${prefix}${line}`))
    .join("\n");
}

function baseBuilderExpression(columnType: ColumnType, references?: string): string {
  switch (columnType.type) {
    case "Text":
      return "col.string()";
    case "Boolean":
      return "col.boolean()";
    case "Integer":
      return "col.int()";
    case "Double":
      return "col.float()";
    case "Timestamp":
      return "col.timestamp()";
    case "Bytea":
      return "col.bytes()";
    case "Json":
      return columnType.schema ? `col.json(${JSON.stringify(columnType.schema)})` : "col.json()";
    case "Enum":
      return `col.enum(${columnType.variants.map((variant) => JSON.stringify(variant)).join(", ")})`;
    case "Uuid":
      if (!references) {
        throw new Error("Migration stub generation does not yet support bare UUID columns.");
      }
      return `col.ref(${JSON.stringify(references)})`;
    case "Array":
      return `col.array(${baseBuilderExpression(columnType.element, references)})`;
    case "BigInt":
      throw new Error("Migration stub generation does not yet support BIGINT columns.");
    case "Row":
      throw new Error("Migration stub generation does not yet support row-valued columns.");
  }
}

function builderExpressionForColumn(column: ColumnDescriptor): string {
  const base = baseBuilderExpression(column.column_type, column.references);
  return column.nullable ? `${base}.optional()` : base;
}

function renderSchemaWitness(schema: WasmSchema): string {
  const tableEntries = Object.entries(schema)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([tableName, tableSchema]) => {
      const columnLines = tableSchema.columns.map(
        (column) => `${JSON.stringify(column.name)}: ${builderExpressionForColumn(column)},`,
      );
      return `${JSON.stringify(tableName)}: {\n${indentBlock(columnLines.join("\n"), 2)}\n}`;
    });

  if (tableEntries.length === 0) {
    return "{}";
  }

  return `{\n${indentBlock(tableEntries.join(",\n"), 2)}\n}`;
}

type TableSuggestion = {
  tableName: string;
  comments: string[];
  operations: string[];
};

function inferTableSuggestions(
  tableName: string,
  fromTable: WasmSchema[string],
  toTable: WasmSchema[string],
): TableSuggestion {
  const fromColumns = new Map(fromTable.columns.map((column) => [column.name, column]));
  const toColumns = new Map(toTable.columns.map((column) => [column.name, column]));
  const comments: string[] = [];
  const operations: string[] = [];

  const removedColumns = [...fromColumns.keys()].filter((name) => !toColumns.has(name));
  const addedColumns = [...toColumns.keys()].filter((name) => !fromColumns.has(name));

  if (removedColumns.length === 1 && addedColumns.length === 1) {
    const removed = fromColumns.get(removedColumns[0]!)!;
    const added = toColumns.get(addedColumns[0]!)!;
    if (
      removed.nullable === added.nullable &&
      removed.references === added.references &&
      columnTypeSignature(removed.column_type) === columnTypeSignature(added.column_type)
    ) {
      comments.push(
        `Possible rename detected: ${JSON.stringify(removed.name)} -> ${JSON.stringify(added.name)}.`,
      );
    }
  }

  for (const columnName of addedColumns) {
    const column = toColumns.get(columnName)!;
    if (column.nullable) {
      operations.push(`t.add(${JSON.stringify(columnName)}, { default: null });`);
    } else {
      comments.push(
        `Added required column ${JSON.stringify(columnName)} needs an explicit default.`,
      );
    }
  }

  for (const columnName of removedColumns) {
    const column = fromColumns.get(columnName)!;
    if (column.nullable) {
      operations.push(`t.drop(${JSON.stringify(columnName)}, { backwardsDefault: null });`);
    } else {
      comments.push(
        `Removed required column ${JSON.stringify(columnName)} needs an explicit backwardsDefault.`,
      );
    }
  }

  return {
    tableName,
    comments,
    operations,
  };
}

function renderMigrationBody(
  fromSchema: WasmSchema,
  toSchema: WasmSchema,
): { body: string; witnessFrom: WasmSchema; witnessTo: WasmSchema } {
  const changedTables = changedTableNames(fromSchema, toSchema);
  const migratableTables = changedTables.filter(
    (tableName) => fromSchema[tableName] !== undefined && toSchema[tableName] !== undefined,
  );
  const witnessFrom = pickWitnessSchema(fromSchema, migratableTables);
  const witnessTo = pickWitnessSchema(toSchema, migratableTables);
  const lines: string[] = [];

  for (const tableName of migratableTables) {
    const fromTable = fromSchema[tableName]!;
    const toTable = toSchema[tableName]!;

    const suggestion = inferTableSuggestions(tableName, fromTable, toTable);
    lines.push(`m.table(${JSON.stringify(tableName)}, (t) => {`);
    for (const comment of suggestion.comments) {
      lines.push(`  // TODO: ${comment}`);
    }
    for (const operation of suggestion.operations) {
      lines.push(`  ${operation}`);
    }
    if (suggestion.comments.length === 0 && suggestion.operations.length === 0) {
      lines.push("  // TODO: No safe migration steps were inferred automatically.");
    }
    lines.push("});");
    lines.push("");
  }

  if (lines.length === 0) {
    lines.push(
      changedTables.length === 0
        ? "// TODO: No schema differences were detected."
        : "// TODO: No column-level migration steps were required for the detected schema changes.",
    );
  }

  return {
    body: lines.join("\n").trimEnd(),
    witnessFrom,
    witnessTo,
  };
}

async function packageVersion(): Promise<string> {
  const packageJson = JSON.parse(
    await readFile(new URL("../package.json", import.meta.url), "utf8"),
  ) as { version?: string };
  return packageJson.version ?? "unknown";
}

function createDateStamp(now: Date = new Date()): string {
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  return `${year}${month}${day}`;
}

function migrationFilename(migrationsDir: string, fromHash: string, toHash: string): string {
  return join(migrationsDir, `${createDateStamp()}-unnamed-${fromHash}-${toHash}.ts`);
}

function renderMigrationStub(input: {
  fromHash: string;
  toHash: string;
  fromSchema: WasmSchema;
  toSchema: WasmSchema;
}): string {
  const rendered = renderMigrationBody(input.fromSchema, input.toSchema);
  return `import { col, defineMigration } from "jazz-tools";

export default defineMigration({
  fromHash: ${JSON.stringify(input.fromHash)},
  toHash: ${JSON.stringify(input.toHash)},
  from: ${renderSchemaWitness(rendered.witnessFrom)},
  to: ${renderSchemaWitness(rendered.witnessTo)},
  migrate: (m) => {
${indentBlock(rendered.body, 4)}
  },
});
`;
}

function isDefinedMigration(value: unknown): value is DefinedMigration {
  if (typeof value !== "object" || value === null) {
    return false;
  }

  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.fromHash === "string" &&
    typeof candidate.toHash === "string" &&
    typeof candidate.from === "object" &&
    candidate.from !== null &&
    typeof candidate.to === "object" &&
    candidate.to !== null &&
    Array.isArray(candidate.forward)
  );
}

async function loadDefinedMigration(filePath: string): Promise<DefinedMigration> {
  const url = pathToFileURL(filePath).href + `?v=${++importCounter}`;
  const loaded = (await import(url)) as { default?: unknown; migration?: unknown };
  const migration = loaded.default ?? loaded.migration;
  if (!isDefinedMigration(migration)) {
    throw new Error(
      `Invalid migration export in ${basename(filePath)}. Export default defineMigration(...).`,
    );
  }
  return migration;
}

async function findMigrationFile(
  migrationsDir: string,
  fromHash: string,
  toHash: string,
): Promise<string> {
  const files = await readdir(migrationsDir);
  const matches = files
    .filter((file) => file.endsWith(".ts"))
    .filter((file) => file.includes(`-${fromHash}-${toHash}.ts`));

  if (matches.length === 0) {
    throw new Error(`No migration file found in ${migrationsDir} for ${fromHash} -> ${toHash}.`);
  }

  if (matches.length > 1) {
    throw new Error(
      `Multiple migration files found for ${fromHash} -> ${toHash}: ${matches.join(", ")}`,
    );
  }

  return join(migrationsDir, matches[0]!);
}

export async function createMigration(options: CreateMigrationOptions): Promise<string> {
  const fromHash = normalizeFullSchemaHash(options.fromHash, "fromHash");
  const toHash = normalizeFullSchemaHash(options.toHash, "toHash");

  await mkdir(options.migrationsDir, { recursive: true });

  const [{ schema: fromSchema }, { schema: toSchema }] = await Promise.all([
    fetchStoredWasmSchema(options.serverUrl, {
      adminSecret: options.adminSecret,
      schemaHash: fromHash,
    }),
    fetchStoredWasmSchema(options.serverUrl, {
      adminSecret: options.adminSecret,
      schemaHash: toHash,
    }),
  ]);

  const filePath = migrationFilename(options.migrationsDir, fromHash, toHash);
  if (await pathExists(filePath)) {
    throw new Error(`Migration stub already exists: ${filePath}`);
  }

  const stub = renderMigrationStub({ fromHash, toHash, fromSchema, toSchema });
  await writeFile(filePath, stub);

  const version = await packageVersion();
  console.log(`Generated: ${filePath}`);
  console.log("");
  console.log("Next steps:");
  console.log("1. Fill in migrate().");
  console.log("2. Rename the file by replacing 'unnamed'.");
  console.log(`3. Run npx jazz-tools@${version} migrations push ${fromHash} ${toHash}`);

  return filePath;
}

export async function pushMigration(options: PushMigrationOptions): Promise<void> {
  const fromHash = normalizeFullSchemaHash(options.fromHash, "fromHash");
  const toHash = normalizeFullSchemaHash(options.toHash, "toHash");
  const filePath = await findMigrationFile(options.migrationsDir, fromHash, toHash);
  const migration = await loadDefinedMigration(filePath);

  if (migration.fromHash !== fromHash || migration.toHash !== toHash) {
    throw new Error(
      `Migration ${basename(filePath)} exports ${migration.fromHash} -> ${migration.toHash}, expected ${fromHash} -> ${toHash}.`,
    );
  }

  // Validate that the schema witnesses are valid definitions before publishing.
  schemaDefinitionToAst(migration.from as any);
  schemaDefinitionToAst(migration.to as any);

  if (migration.forward.length === 0) {
    throw new Error(`Migration ${basename(filePath)} has no steps. Fill in migrate() before push.`);
  }

  const forwardSql = lensesToSql(migration.forward, "fwd");
  const response = await fetch(buildEndpointUrl(options.serverUrl, "/admin/migrations"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Jazz-Admin-Secret": options.adminSecret,
    },
    body: JSON.stringify({
      fromHash,
      toHash,
      forwardSql,
    }),
  });

  if (!response.ok) {
    const bodyText = await response.text().catch(() => "");
    const detail = bodyText ? ` - ${bodyText}` : "";
    throw new Error(`Migration push failed: ${response.status} ${response.statusText}${detail}`);
  }

  console.log(`Pushed migration ${fromHash} -> ${toHash} from ${basename(filePath)}.`);
}

function isMainModule(): boolean {
  const entry = process.argv[1];
  if (!entry) {
    return false;
  }
  return pathToFileURL(entry).href === import.meta.url;
}

if (isMainModule()) {
  const command = process.argv[2] ?? "";

  if (command === "build") {
    const { options } = parseArgs();
    build(options).catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
  } else if (command === "migrations") {
    const subcommand = process.argv[3] ?? "";
    const fromHash = process.argv[4];
    const toHash = process.argv[5];
    const sharedArgs = process.argv.slice(6);

    if (!fromHash || !toHash) {
      console.error(
        "Usage: node dist/cli.js migrations <create|push> <fromHash> <toHash> [options]",
      );
      process.exit(1);
    }

    const options = resolveMigrationOptions(sharedArgs);
    const task =
      subcommand === "create"
        ? createMigration({ ...options, fromHash, toHash })
        : subcommand === "push"
          ? pushMigration({ ...options, fromHash, toHash })
          : Promise.reject(
              new Error(
                "Usage: node dist/cli.js migrations <create|push> <fromHash> <toHash> [options]",
              ),
            );

    task.catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
  } else {
    console.log("Usage: node <path-to-jazz-tools>/dist/cli.js <command> [options]");
    console.log("\nCommands:");
    console.log(
      "  build                 Generate SQL from TypeScript schemas and run jazz-tools build",
    );
    console.log(
      "  migrations create     Generate a typed migration stub from two known schema hashes",
    );
    console.log("  migrations push       Push a reviewed migration edge to the server");
    console.log("\nBuild options:");
    console.log("  --jazz-bin <path>     Path to jazz binary (default: jazz-tools)");
    console.log("  --schema-dir <path>   Path to schema directory (default: ./schema)");
    console.log("\nMigration options:");
    console.log("  --server-url <url>    Jazz server URL (or set JAZZ_SERVER_URL)");
    console.log("  --admin-secret <sec>  Admin secret (or set JAZZ_ADMIN_SECRET)");
    console.log("  --migrations-dir <p>  Path to migrations directory (default: ./migrations)");
    process.exit(command ? 1 : 0);
  }
}

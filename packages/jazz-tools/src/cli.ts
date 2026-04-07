#!/usr/bin/env node

// CLI for jazz-tools schema tooling

import { access, mkdir, readFile, readdir, writeFile } from "fs/promises";
import { basename, join, resolve } from "path";
import { pathToFileURL } from "url";
import { register as registerEsm } from "tsx/esm/api";
import type {
  ColumnDescriptor,
  ColumnType as WasmColumnType,
  WasmSchema,
} from "./drivers/types.js";
import type { DefinedMigration } from "./migrations.js";
import { schemaDefinitionToAst } from "./migrations.js";
import type { Lens, SqlType } from "./schema.js";
import { loadCompiledSchema, type LoadedSchemaProject } from "./schema-loader.js";
import {
  encodePublishedMigrationValue,
  fetchPermissionsHead,
  fetchSchemaHashes,
  fetchStoredWasmSchema,
  publishStoredPermissions,
  publishStoredMigration,
  type PublishedTableLens,
  type StoredPermissionsHead,
} from "./runtime/schema-fetch.js";
import { toValue } from "./runtime/value-converter.js";

export interface BuildOptions {
  jazzBin?: string;
  schemaDir: string;
}

export interface SchemaExportOptions {
  schemaDir: string;
  format: "json";
}

const PERMISSIONS_LIFECYCLE_NOTE =
  "Permission-only changes do not create schema hashes or require migrations.";

function parseArgs(): { command: string; options: BuildOptions } {
  const args = process.argv.slice(2);
  const command = args[0] || "";
  let schemaDir = process.cwd();
  let jazzBin: string | undefined;

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

registerEsm();

let importCounter = 0;

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

export async function validate(options: BuildOptions): Promise<void> {
  const compiled = await loadCompiledSchema(options.schemaDir);
  const tableCount = compiled.schema.tables.length;
  console.log(`Loaded structural schema from ${compiled.schemaFile}.`);
  if (compiled.permissionsFile) {
    console.log(`Loaded current permissions from ${compiled.permissionsFile}.`);
    console.log(PERMISSIONS_LIFECYCLE_NOTE);
    console.log(
      "Use `jazz-tools permissions status` or `jazz-tools permissions push` for auth publication.",
    );
  }
  console.log(`Validated ${tableCount} table${tableCount === 1 ? "" : "s"} in schema.ts.`);
}

export async function exportSchema(options: SchemaExportOptions): Promise<void> {
  if (options.format !== "json") {
    throw new Error(`Unsupported schema export format: ${options.format}`);
  }

  const compiled = await loadCompiledSchema(options.schemaDir);
  process.stdout.write(`${JSON.stringify(compiled.wasmSchema, null, 2)}\n`);
}

export interface MigrationCommandOptions {
  serverUrl: string;
  adminSecret: string;
  migrationsDir: string;
}

export interface PermissionsCommandOptions {
  serverUrl: string;
  adminSecret: string;
  schemaDir: string;
}

export interface CreateMigrationOptions extends MigrationCommandOptions {
  fromHash: string;
  toHash: string;
}

export interface PushMigrationOptions extends MigrationCommandOptions {
  fromHash: string;
  toHash: string;
}

const SHORT_SCHEMA_HASH_LENGTH = 12;

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

function resolvePermissionsOptions(args: string[]): PermissionsCommandOptions {
  const serverUrl = getFlagValue(args, "--server-url") ?? process.env.JAZZ_SERVER_URL;
  const adminSecret = getFlagValue(args, "--admin-secret") ?? process.env.JAZZ_ADMIN_SECRET;
  const schemaDir = resolve(process.cwd(), getFlagValue(args, "--schema-dir") ?? process.cwd());

  if (!serverUrl) {
    throw new Error("Missing server URL. Pass --server-url <url> or set JAZZ_SERVER_URL.");
  }

  if (!adminSecret) {
    throw new Error("Missing admin secret. Pass --admin-secret <secret> or set JAZZ_ADMIN_SECRET.");
  }

  return {
    serverUrl,
    adminSecret,
    schemaDir,
  };
}

function normalizeSchemaHashInput(hash: string, label: string): string {
  const normalized = hash.trim().toLowerCase();
  if (!/^[0-9a-f]{12,64}$/.test(normalized)) {
    throw new Error(`${label} must be a 12-64 character lowercase hex schema hash.`);
  }
  return normalized;
}

function shortSchemaHash(hash: string): string {
  return normalizeSchemaHashInput(hash, "schema hash").slice(0, SHORT_SCHEMA_HASH_LENGTH);
}

function hashMatchesFullSchema(hash: string, fullHash: string): boolean {
  return fullHash.startsWith(normalizeSchemaHashInput(hash, "schema hash"));
}

function resolveKnownSchemaHash(
  hash: string,
  label: string,
  knownHashes: readonly string[],
): string {
  const normalized = normalizeSchemaHashInput(hash, label);

  if (normalized.length === 64) {
    if (!knownHashes.includes(normalized)) {
      throw new Error(`No stored schema found for ${label} ${normalized}.`);
    }
    return normalized;
  }

  const matches = knownHashes.filter((candidate) => candidate.startsWith(normalized));
  if (matches.length === 0) {
    throw new Error(`No stored schema found for ${label} prefix ${normalized}.`);
  }
  if (matches.length > 1) {
    throw new Error(
      `${label} prefix ${normalized} is ambiguous: ${matches
        .map((candidate) => shortSchemaHash(candidate))
        .join(", ")}`,
    );
  }
  return matches[0]!;
}

function columnTypeSignature(columnType: WasmColumnType): string {
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

  const leftColumns = [...left.columns].sort((a, b) => a.name.localeCompare(b.name));
  const rightColumns = [...right.columns].sort((a, b) => a.name.localeCompare(b.name));

  return leftColumns.every((column, index) => columnsEqual(column, rightColumns[index]!));
}

function wasmSchemasEqual(left: WasmSchema, right: WasmSchema): boolean {
  const leftTableNames = Object.keys(left).sort();
  const rightTableNames = Object.keys(right).sort();

  if (leftTableNames.length !== rightTableNames.length) {
    return false;
  }

  return leftTableNames.every((tableName, index) => {
    if (tableName !== rightTableNames[index]) {
      return false;
    }
    return tableSchemasEqual(left[tableName], right[tableName]);
  });
}

function changedTableNames(fromSchema: WasmSchema, toSchema: WasmSchema): string[] {
  const names = new Set([...Object.keys(fromSchema), ...Object.keys(toSchema)]);
  return [...names].filter(
    (tableName) => !tableSchemasEqual(fromSchema[tableName], toSchema[tableName]),
  );
}

type TableRenameSuggestion = {
  oldTableName: string;
  newTableName: string;
};

function detectPossibleTableRenames(
  fromSchema: WasmSchema,
  toSchema: WasmSchema,
): TableRenameSuggestion[] {
  const removedTables = Object.keys(fromSchema)
    .filter((tableName) => !toSchema[tableName])
    .sort();
  const addedTables = Object.keys(toSchema)
    .filter((tableName) => !fromSchema[tableName])
    .sort();

  const removedCandidates = removedTables.map((oldTableName) =>
    addedTables
      .map((newTableName, addedIndex) =>
        tableSchemasEqual(fromSchema[oldTableName], toSchema[newTableName]) ? addedIndex : -1,
      )
      .filter((addedIndex) => addedIndex >= 0),
  );
  const addedCandidates = addedTables.map((newTableName) =>
    removedTables
      .map((oldTableName, removedIndex) =>
        tableSchemasEqual(fromSchema[oldTableName], toSchema[newTableName]) ? removedIndex : -1,
      )
      .filter((removedIndex) => removedIndex >= 0),
  );

  return removedCandidates.flatMap((candidateAddedTables, removedIndex) => {
    if (candidateAddedTables.length !== 1) {
      return [];
    }

    const addedIndex = candidateAddedTables[0]!;
    const candidateRemovedTables = addedCandidates[addedIndex]!;
    if (candidateRemovedTables.length !== 1 || candidateRemovedTables[0] !== removedIndex) {
      return [];
    }

    return [
      {
        oldTableName: removedTables[removedIndex]!,
        newTableName: addedTables[addedIndex]!,
      },
    ];
  });
}

function ensurePermissionsProject(compiled: LoadedSchemaProject): LoadedSchemaProject & {
  permissions: NonNullable<LoadedSchemaProject["permissions"]>;
  permissionsFile: string;
} {
  if (!compiled.permissions || !compiled.permissionsFile) {
    throw new Error(
      "No permissions.ts found for this app. Create permissions.ts before using permissions commands.",
    );
  }

  return compiled as LoadedSchemaProject & {
    permissions: NonNullable<LoadedSchemaProject["permissions"]>;
    permissionsFile: string;
  };
}

async function resolveStoredStructuralSchemaHash(
  serverUrl: string,
  adminSecret: string,
  wasmSchema: WasmSchema,
): Promise<string> {
  const { hashes } = await fetchSchemaHashes(serverUrl, { adminSecret });
  const storedSchemas = await Promise.all(
    hashes.map(async (hash) => ({
      hash,
      schema: (await fetchStoredWasmSchema(serverUrl, { adminSecret, schemaHash: hash })).schema,
    })),
  );

  const match = storedSchemas.find(({ schema }) => wasmSchemasEqual(schema, wasmSchema));
  if (!match) {
    throw new Error(
      "No stored structural schema matches the local schema.ts. Publish the structural schema before pushing permissions.",
    );
  }

  return match.hash;
}

function pickWitnessSchema(schema: WasmSchema, tableNames: readonly string[]): WasmSchema {
  const uniqueTableNames = [...new Set(tableNames)];
  return Object.fromEntries(
    uniqueTableNames
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

function baseBuilderExpression(columnType: WasmColumnType, references?: string): string {
  switch (columnType.type) {
    case "Text":
      return "s.string()";
    case "Boolean":
      return "s.boolean()";
    case "Integer":
      return "s.int()";
    case "Double":
      return "s.float()";
    case "Timestamp":
      return "s.timestamp()";
    case "Bytea":
      return "s.bytes()";
    case "Json":
      return columnType.schema ? `s.json(${JSON.stringify(columnType.schema)})` : "s.json()";
    case "Enum":
      return `s.enum(${columnType.variants.map((variant) => JSON.stringify(variant)).join(", ")})`;
    case "Uuid":
      if (!references) {
        throw new Error("Migration stub generation does not yet support bare UUID columns.");
      }
      return `s.ref(${JSON.stringify(references)})`;
    case "Array":
      return `s.array(${baseBuilderExpression(columnType.element, references)})`;
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

function sqlTypeToWasmColumnType(sqlType: SqlType): WasmColumnType {
  if (typeof sqlType === "string") {
    switch (sqlType) {
      case "TEXT":
        return { type: "Text" };
      case "BOOLEAN":
        return { type: "Boolean" };
      case "INTEGER":
        return { type: "Integer" };
      case "REAL":
        return { type: "Double" };
      case "TIMESTAMP":
        return { type: "Timestamp" };
      case "UUID":
        return { type: "Uuid" };
      case "BYTEA":
        return { type: "Bytea" };
    }
  }

  if (sqlType.kind === "ENUM") {
    return {
      type: "Enum",
      variants: [...sqlType.variants],
    };
  }

  if (sqlType.kind === "JSON") {
    return {
      type: "Json",
      schema: sqlType.schema,
    };
  }

  return {
    type: "Array",
    element: sqlTypeToWasmColumnType(sqlType.element),
  };
}

function serializeForwardLenses(forward: readonly Lens[]): PublishedTableLens[] {
  return forward.map((tableLens) => ({
    table: tableLens.table,
    added: tableLens.added,
    removed: tableLens.removed,
    renamedFrom: tableLens.renamedFrom,
    operations: tableLens.operations.map((op) => {
      if (op.type === "rename") {
        return op;
      }

      const columnType = sqlTypeToWasmColumnType(op.sqlType);
      const value = encodePublishedMigrationValue(toValue(op.value, columnType));

      return {
        type: op.type,
        column: op.column,
        columnType,
        value,
      };
    }),
  }));
}

function renderSchemaWitness(schema: WasmSchema): string {
  const tableEntries = Object.entries(schema)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([tableName, tableSchema]) => {
      const columnLines = tableSchema.columns.map(
        (column) => `${JSON.stringify(column.name)}: ${builderExpressionForColumn(column)},`,
      );
      return `${JSON.stringify(tableName)}: s.table({\n${indentBlock(columnLines.join("\n"), 2)}\n})`;
    });

  if (tableEntries.length === 0) {
    return "{}";
  }

  return `{\n${indentBlock(tableEntries.join(",\n"), 2)}\n}`;
}

type TableSuggestion = {
  tableName: string;
  comments: string[];
  properties: string[];
};

function renderArrayElementExpression(columnType: WasmColumnType, references?: string): string {
  return baseBuilderExpression(columnType, references);
}

function renderAddOperationExpression(column: ColumnDescriptor, defaultExpression: string): string {
  switch (column.column_type.type) {
    case "Text":
      return `s.add.string({ default: ${defaultExpression} })`;
    case "Boolean":
      return `s.add.boolean({ default: ${defaultExpression} })`;
    case "Integer":
      return `s.add.int({ default: ${defaultExpression} })`;
    case "Double":
      return `s.add.float({ default: ${defaultExpression} })`;
    case "Timestamp":
      return `s.add.timestamp({ default: ${defaultExpression} })`;
    case "Bytea":
      return `s.add.bytes({ default: ${defaultExpression} })`;
    case "Json":
      return column.column_type.schema
        ? `s.add.json({ default: ${defaultExpression}, schema: ${JSON.stringify(column.column_type.schema)} })`
        : `s.add.json({ default: ${defaultExpression} })`;
    case "Enum":
      return `s.add.enum(${column.column_type.variants
        .map((variant) => JSON.stringify(variant))
        .join(", ")}, { default: ${defaultExpression} })`;
    case "Uuid":
      if (column.references) {
        return `s.add.ref(${JSON.stringify(column.references)}, { default: ${defaultExpression} })`;
      }
      return `s.add.ref("TODO_TABLE", { default: ${defaultExpression} })`;
    case "Array":
      return `s.add.array({ of: ${renderArrayElementExpression(column.column_type.element, column.references)}, default: ${defaultExpression} })`;
    case "BigInt":
      throw new Error("Migration stub generation does not yet support BIGINT columns.");
    case "Row":
      throw new Error("Migration stub generation does not yet support row-valued columns.");
  }
}

function renderDropOperationExpression(
  column: ColumnDescriptor,
  defaultExpression: string,
): string {
  switch (column.column_type.type) {
    case "Text":
      return `s.drop.string({ backwardsDefault: ${defaultExpression} })`;
    case "Boolean":
      return `s.drop.boolean({ backwardsDefault: ${defaultExpression} })`;
    case "Integer":
      return `s.drop.int({ backwardsDefault: ${defaultExpression} })`;
    case "Double":
      return `s.drop.float({ backwardsDefault: ${defaultExpression} })`;
    case "Timestamp":
      return `s.drop.timestamp({ backwardsDefault: ${defaultExpression} })`;
    case "Bytea":
      return `s.drop.bytes({ backwardsDefault: ${defaultExpression} })`;
    case "Json":
      return column.column_type.schema
        ? `s.drop.json({ backwardsDefault: ${defaultExpression}, schema: ${JSON.stringify(column.column_type.schema)} })`
        : `s.drop.json({ backwardsDefault: ${defaultExpression} })`;
    case "Enum":
      return `s.drop.enum(${column.column_type.variants
        .map((variant) => JSON.stringify(variant))
        .join(", ")}, { backwardsDefault: ${defaultExpression} })`;
    case "Uuid":
      if (column.references) {
        return `s.drop.ref(${JSON.stringify(column.references)}, { backwardsDefault: ${defaultExpression} })`;
      }
      return `s.drop.ref("TODO_TABLE", { backwardsDefault: ${defaultExpression} })`;
    case "Array":
      return `s.drop.array({ of: ${renderArrayElementExpression(column.column_type.element, column.references)}, backwardsDefault: ${defaultExpression} })`;
    case "BigInt":
      throw new Error("Migration stub generation does not yet support BIGINT columns.");
    case "Row":
      throw new Error("Migration stub generation does not yet support row-valued columns.");
  }
}

function inferTableSuggestions(
  tableName: string,
  fromTable: WasmSchema[string],
  toTable: WasmSchema[string],
): TableSuggestion {
  const fromColumns = new Map(fromTable.columns.map((column) => [column.name, column]));
  const toColumns = new Map(toTable.columns.map((column) => [column.name, column]));
  const comments: string[] = [];
  const properties: string[] = [];

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
      properties.push(
        `${JSON.stringify(columnName)}: ${renderAddOperationExpression(column, "null")},`,
      );
    } else {
      comments.push(
        `Added required column ${JSON.stringify(columnName)} needs an explicit default.`,
      );
    }
  }

  for (const columnName of removedColumns) {
    const column = fromColumns.get(columnName)!;
    if (column.nullable) {
      properties.push(
        `${JSON.stringify(columnName)}: ${renderDropOperationExpression(column, "null")},`,
      );
    } else {
      comments.push(
        `Removed required column ${JSON.stringify(columnName)} needs an explicit backwardsDefault.`,
      );
    }
  }

  return {
    tableName,
    comments,
    properties,
  };
}

function renderMigrationBody(
  fromSchema: WasmSchema,
  toSchema: WasmSchema,
): {
  migrateBody?: string;
  renameTablesBody?: string;
  createTablesBody?: string;
  dropTablesBody?: string;
  witnessFrom: WasmSchema;
  witnessTo: WasmSchema;
} {
  const renameSuggestions = detectPossibleTableRenames(fromSchema, toSchema);
  const renamedOldTables = new Set(renameSuggestions.map((suggestion) => suggestion.oldTableName));
  const renamedNewTables = new Set(renameSuggestions.map((suggestion) => suggestion.newTableName));
  const addedTables = Object.keys(toSchema)
    .filter((tableName) => !fromSchema[tableName])
    .sort();
  const removedTables = Object.keys(fromSchema)
    .filter((tableName) => !toSchema[tableName])
    .sort();
  const explicitAddedTables = addedTables.filter((tableName) => !renamedNewTables.has(tableName));
  const explicitRemovedTables = removedTables.filter(
    (tableName) => !renamedOldTables.has(tableName),
  );
  const changedTables = changedTableNames(fromSchema, toSchema);
  const migratableTables = changedTables.filter(
    (tableName) => fromSchema[tableName] !== undefined && toSchema[tableName] !== undefined,
  );
  const witnessFromTables = [...migratableTables, ...explicitRemovedTables];
  const witnessToTables = [...migratableTables, ...explicitAddedTables];
  for (const renameSuggestion of renameSuggestions) {
    witnessFromTables.push(renameSuggestion.oldTableName);
    witnessToTables.push(renameSuggestion.newTableName);
  }
  const witnessFrom = pickWitnessSchema(fromSchema, witnessFromTables);
  const witnessTo = pickWitnessSchema(toSchema, witnessToTables);
  const lines: string[] = [];

  for (const tableName of migratableTables) {
    const fromTable = fromSchema[tableName]!;
    const toTable = toSchema[tableName]!;

    const suggestion = inferTableSuggestions(tableName, fromTable, toTable);
    lines.push(`${JSON.stringify(tableName)}: {`);
    for (const comment of suggestion.comments) {
      lines.push(`  // TODO: ${comment}`);
    }
    for (const property of suggestion.properties) {
      lines.push(`  ${property}`);
    }
    if (suggestion.comments.length === 0 && suggestion.properties.length === 0) {
      lines.push("  // TODO: No safe migration steps were inferred automatically.");
    }
    lines.push("},");
    lines.push("");
  }

  if (lines.length === 0) {
    if (
      renameSuggestions.length === 0 &&
      explicitAddedTables.length === 0 &&
      explicitRemovedTables.length === 0
    ) {
      lines.push(
        changedTables.length === 0
          ? "// TODO: No schema differences were detected."
          : "// TODO: No column-level migration steps were required for the detected schema changes.",
      );
    }
  }

  return {
    migrateBody: lines.length > 0 ? lines.join("\n").trimEnd() : undefined,
    createTablesBody:
      explicitAddedTables.length > 0
        ? explicitAddedTables.map((tableName) => `${JSON.stringify(tableName)}: true,`).join("\n")
        : undefined,
    dropTablesBody:
      explicitRemovedTables.length > 0
        ? explicitRemovedTables.map((tableName) => `${JSON.stringify(tableName)}: true,`).join("\n")
        : undefined,
    renameTablesBody:
      renameSuggestions.length > 0
        ? renameSuggestions
            .map(
              (renameSuggestion) =>
                `${renameSuggestion.newTableName}: s.renameTableFrom(${JSON.stringify(renameSuggestion.oldTableName)}),`,
            )
            .join("\n")
        : undefined,
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
  return join(
    migrationsDir,
    `${createDateStamp()}-unnamed-${shortSchemaHash(fromHash)}-${shortSchemaHash(toHash)}.ts`,
  );
}

function renderMigrationStub(input: {
  fromHash: string;
  toHash: string;
  fromSchema: WasmSchema;
  toSchema: WasmSchema;
}): string {
  const rendered = renderMigrationBody(input.fromSchema, input.toSchema);
  const sections: string[] = [];

  if (rendered.renameTablesBody) {
    sections.push(`  renameTables: {\n${indentBlock(rendered.renameTablesBody, 4)}\n  },`);
  }

  if (rendered.createTablesBody) {
    sections.push(`  createTables: {\n${indentBlock(rendered.createTablesBody, 4)}\n  },`);
  }

  if (rendered.dropTablesBody) {
    sections.push(`  dropTables: {\n${indentBlock(rendered.dropTablesBody, 4)}\n  },`);
  }

  if (rendered.migrateBody) {
    sections.push(`  migrate: {\n${indentBlock(rendered.migrateBody, 4)}\n  },`);
  }

  sections.push(`  fromHash: ${JSON.stringify(shortSchemaHash(input.fromHash))},`);
  sections.push(`  toHash: ${JSON.stringify(shortSchemaHash(input.toHash))},`);
  sections.push(`  from: ${renderSchemaWitness(rendered.witnessFrom)},`);
  sections.push(`  to: ${renderSchemaWitness(rendered.witnessTo)},`);

  return `import { schema as s } from "jazz-tools";

export default s.defineMigration({
${sections.join("\n")}
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
  const fromShortHash = shortSchemaHash(fromHash);
  const toShortHash = shortSchemaHash(toHash);
  const files = await readdir(migrationsDir);
  const matches = files
    .filter((file) => file.endsWith(".ts"))
    .filter(
      (file) =>
        file.includes(`-${fromShortHash}-${toShortHash}.ts`) ||
        file.includes(`-${fromHash}-${toHash}.ts`),
    );

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
  const { hashes } = await fetchSchemaHashes(options.serverUrl, {
    adminSecret: options.adminSecret,
  });
  const fromHash = resolveKnownSchemaHash(options.fromHash, "fromHash", hashes);
  const toHash = resolveKnownSchemaHash(options.toHash, "toHash", hashes);

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
  console.log("Migration stubs are only for structural schema changes.");
  console.log(PERMISSIONS_LIFECYCLE_NOTE);
  console.log("");
  console.log("Next steps:");
  console.log("1. Fill in migrate.");
  console.log("2. Rename the file by replacing 'unnamed'.");
  console.log(
    `3. Run npx jazz-tools@${version} migrations push ${shortSchemaHash(fromHash)} ${shortSchemaHash(toHash)}`,
  );

  return filePath;
}

export async function pushMigration(options: PushMigrationOptions): Promise<void> {
  const { hashes } = await fetchSchemaHashes(options.serverUrl, {
    adminSecret: options.adminSecret,
  });
  const fromHash = resolveKnownSchemaHash(options.fromHash, "fromHash", hashes);
  const toHash = resolveKnownSchemaHash(options.toHash, "toHash", hashes);
  const filePath = await findMigrationFile(options.migrationsDir, fromHash, toHash);
  const migration = await loadDefinedMigration(filePath);

  if (
    !hashMatchesFullSchema(migration.fromHash, fromHash) ||
    !hashMatchesFullSchema(migration.toHash, toHash)
  ) {
    throw new Error(
      `Migration ${basename(filePath)} exports ${migration.fromHash} -> ${migration.toHash}, expected ${shortSchemaHash(fromHash)} -> ${shortSchemaHash(toHash)}.`,
    );
  }

  schemaDefinitionToAst(migration.from as any);
  schemaDefinitionToAst(migration.to as any);

  if (migration.forward.length === 0) {
    throw new Error(`Migration ${basename(filePath)} has no steps. Fill in migrate before push.`);
  }

  const forward = serializeForwardLenses(migration.forward);
  await publishStoredMigration(options.serverUrl, {
    adminSecret: options.adminSecret,
    fromHash,
    toHash,
    forward,
  });

  console.log(
    `Pushed migration ${shortSchemaHash(fromHash)} -> ${shortSchemaHash(toHash)} from ${basename(filePath)}.`,
  );
}

function describePermissionsHead(head: StoredPermissionsHead): string {
  return `v${head.version} on ${shortSchemaHash(head.schemaHash)}`;
}

export async function permissionsStatus(options: PermissionsCommandOptions): Promise<void> {
  const compiled = ensurePermissionsProject(await loadCompiledSchema(options.schemaDir));
  const localSchemaHash = await resolveStoredStructuralSchemaHash(
    options.serverUrl,
    options.adminSecret,
    compiled.wasmSchema,
  );
  const { head } = await fetchPermissionsHead(options.serverUrl, {
    adminSecret: options.adminSecret,
  });

  console.log(`Loaded structural schema from ${compiled.schemaFile}.`);
  console.log(`Loaded current permissions from ${compiled.permissionsFile}.`);
  console.log(`Local structural schema matches stored hash ${shortSchemaHash(localSchemaHash)}.`);
  console.log(PERMISSIONS_LIFECYCLE_NOTE);

  if (!head) {
    console.log("Server has no published permissions head yet.");
    console.log("Next push will publish version 1.");
    return;
  }

  console.log(`Server permissions head is ${describePermissionsHead(head)}.`);
  if (head.schemaHash === localSchemaHash) {
    console.log("Current server permissions already target this structural schema.");
  } else {
    console.log(
      `Current server permissions target ${shortSchemaHash(head.schemaHash)}; pushing will retarget the head to ${shortSchemaHash(localSchemaHash)}.`,
    );
  }
  console.log(`Next push will require parent bundle ${head.bundleObjectId}.`);
}

export async function pushPermissions(options: PermissionsCommandOptions): Promise<void> {
  const compiled = ensurePermissionsProject(await loadCompiledSchema(options.schemaDir));
  const localSchemaHash = await resolveStoredStructuralSchemaHash(
    options.serverUrl,
    options.adminSecret,
    compiled.wasmSchema,
  );
  const { head: currentHead } = await fetchPermissionsHead(options.serverUrl, {
    adminSecret: options.adminSecret,
  });
  const { head: publishedHead } = await publishStoredPermissions(options.serverUrl, {
    adminSecret: options.adminSecret,
    schemaHash: localSchemaHash,
    permissions: compiled.permissions,
    expectedParentBundleObjectId: currentHead?.bundleObjectId ?? null,
  });

  console.log(`Loaded structural schema from ${compiled.schemaFile}.`);
  console.log(`Loaded current permissions from ${compiled.permissionsFile}.`);
  console.log(`Resolved structural schema hash ${shortSchemaHash(localSchemaHash)}.`);
  if (currentHead) {
    console.log(`Publishing from parent ${describePermissionsHead(currentHead)}.`);
  } else {
    console.log("Publishing first permissions head for this app.");
  }

  const nextHead = publishedHead ?? {
    schemaHash: localSchemaHash,
    version: currentHead ? currentHead.version + 1 : 1,
    parentBundleObjectId: currentHead?.bundleObjectId ?? null,
    bundleObjectId: currentHead?.bundleObjectId ?? "",
  };
  console.log(`Published permissions head ${describePermissionsHead(nextHead)}.`);
  console.log(PERMISSIONS_LIFECYCLE_NOTE);
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

  if (command === "validate") {
    const { options } = parseArgs();
    validate(options).catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
  } else if (command === "schema") {
    const subcommand = process.argv[3] ?? "";
    if (subcommand !== "export") {
      console.error("Usage: node dist/cli.js schema export [--schema-dir <path>] [--format json]");
      process.exit(1);
    }

    const args = process.argv.slice(4);
    const schemaDir = getFlagValue(args, "--schema-dir") ?? process.cwd();
    const formatValue = getFlagValue(args, "--format") ?? "json";
    if (formatValue !== "json") {
      console.error(`Unsupported schema export format: ${formatValue}`);
      process.exit(1);
    }

    exportSchema({ schemaDir, format: "json" }).catch((err) => {
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
  } else if (command === "permissions") {
    const subcommand = process.argv[3] ?? "";
    const options = resolvePermissionsOptions(process.argv.slice(4));
    const task =
      subcommand === "status"
        ? permissionsStatus(options)
        : subcommand === "push"
          ? pushPermissions(options)
          : Promise.reject(
              new Error("Usage: node dist/cli.js permissions <status|push> [options]"),
            );

    task.catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
  } else {
    console.log("Usage: node <path-to-jazz-tools>/dist/cli.js <command> [options]");
    console.log("\nCommands:");
    console.log("  validate              Validate root schema.ts and optional permissions.ts");
    console.log("  schema export         Print the compiled structural schema as JSON");
    console.log("  permissions status    Show the current server permissions head for this app");
    console.log(
      "  permissions push      Publish the current permissions.ts with head-parent checks",
    );
    console.log(
      "  migrations create     Generate a typed structural migration stub from two known schema hashes",
    );
    console.log("  migrations push       Push a reviewed migration edge to the server");
    console.log("\nValidation options:");
    console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
    console.log("\nSchema export options:");
    console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
    console.log("  --format json         Output the compiled schema as JSON");
    console.log("\nPermissions options:");
    console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
    console.log("  --server-url <url>    Jazz server URL (or set JAZZ_SERVER_URL)");
    console.log("  --admin-secret <sec>  Admin secret (or set JAZZ_ADMIN_SECRET)");
    console.log("\nMigration options:");
    console.log("  --server-url <url>    Jazz server URL (or set JAZZ_SERVER_URL)");
    console.log("  --admin-secret <sec>  Admin secret (or set JAZZ_ADMIN_SECRET)");
    console.log("  --migrations-dir <p>  Path to migrations directory (default: ./migrations)");
    process.exit(command ? 1 : 0);
  }
}

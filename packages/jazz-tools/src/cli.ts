#!/usr/bin/env node

// CLI for jazz-tools schema tooling

import { spawn } from "child_process";
import { access, readdir, writeFile } from "fs/promises";
import { join, basename, dirname, resolve } from "path";
import { pathToFileURL } from "url";
import { register as registerCjs } from "tsx/cjs/api";
import { register as registerEsm } from "tsx/esm/api";
import { schemaToSql, lensesToSql } from "./sql-gen.js";
import { getCollectedSchema, getCollectedMigrations, resetCollectedState } from "./dsl.js";
import { generateClient } from "./codegen/index.js";
import type { Lens, Schema, TablePolicies, OperationPolicy } from "./schema.js";

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

async function loadSchemaModule(filePath: string): Promise<void> {
  resetCollectedState();
  const url = pathToFileURL(filePath).href + `?v=${++importCounter}`;
  await import(url);
}

async function loadSchema(filePath: string): Promise<Schema> {
  await loadSchemaModule(filePath);
  return getCollectedSchema();
}

async function loadMigrationModule(filePath: string): Promise<Lens[]> {
  resetCollectedState();
  const url = pathToFileURL(filePath).href + `?v=${++importCounter}`;
  await import(url);
  return getCollectedMigrations();
}

async function generateSqlForSchemaFile(tsFile: string, schema: Schema): Promise<void> {
  const sql = schemaToSql(schema);
  const sqlFile = tsFile.replace(/\.ts$/, ".sql");
  await writeFile(sqlFile, sql);
  console.log(`Generated: ${basename(sqlFile)}`);
}

async function generateAppTs(schemaDir: string, schema: Schema): Promise<void> {
  const output = generateClient(schema);
  const appTsPath = join(schemaDir, "app.ts");
  await writeFile(appTsPath, output);
  console.log(`Generated: app.ts`);
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

  let files: string[];
  try {
    files = await readdir(schemaDir);
  } catch {
    console.error(`Schema directory not found: ${schemaDir}`);
    process.exit(1);
  }

  const tsFiles = files.filter((f) => f.endsWith(".ts"));

  for (const file of tsFiles.filter((name) => isMigrationTsStub(name))) {
    await generateSqlForMigrationFile(join(schemaDir, file));
  }

  const schemaFile = join(schemaDir, "current.ts");
  if (!(await pathExists(schemaFile))) {
    console.error(`Schema file not found: ${schemaFile}`);
    process.exit(1);
  }

  let schema = await loadSchema(schemaFile);
  const tablesWithInlinePolicies = schema.tables
    .filter((table) => table.policies)
    .map((t) => t.name);
  if (tablesWithInlinePolicies.length > 0) {
    throw new Error(
      "Inline table permissions in current.ts are no longer supported. " +
        "Move policies to schema/permissions.ts. " +
        `Tables: ${tablesWithInlinePolicies.join(", ")}.`,
    );
  }

  // Generate app.ts before loading permissions.ts so permissions can import it for typing.
  await generateAppTs(schemaDir, schema);

  const permissionsFile = join(schemaDir, "permissions.ts");
  if (await pathExists(permissionsFile)) {
    const permissions = await loadPermissionsModule(permissionsFile);
    schema = mergePermissionsIntoSchema(schema, permissions);
  }

  await generateSqlForSchemaFile(schemaFile, schema);
  await generateAppTs(schemaDir, schema);
  await ensurePermissionsTestStub(schemaDir);

  await runJazzBuild(jazzBin, schemaDir);
}

function isMainModule(): boolean {
  const entry = process.argv[1];
  if (!entry) {
    return false;
  }
  return pathToFileURL(entry).href === import.meta.url;
}

if (isMainModule()) {
  const { command, options } = parseArgs();

  switch (command) {
    case "build":
      build(options).catch((err) => {
        console.error(err.message);
        process.exit(1);
      });
      break;
    default:
      console.log("Usage: node <path-to-jazz-tools>/dist/cli.js build [options]");
      console.log("\nCommands:");
      console.log("  build    Generate SQL from TypeScript schemas and run jazz-tools build");
      console.log("\nOptions:");
      console.log("  --jazz-bin <path>    Path to jazz binary (default: jazz-tools)");
      console.log("  --schema-dir <path>  Path to schema directory (default: ./schema)");
      process.exit(command ? 1 : 0);
  }
}

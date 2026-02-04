#!/usr/bin/env node

// CLI for jazz-ts schema tooling

import { spawn } from "child_process";
import { readdir, writeFile } from "fs/promises";
import { join, basename } from "path";
import { pathToFileURL } from "url";
import { schemaToSql, lensToSql } from "./sql-gen.js";
import { getCollectedSchema, getCollectedMigration, resetCollectedState } from "./dsl.js";
import { generateClient } from "./codegen/index.js";
import type { Lens } from "./schema.js";

interface BuildOptions {
  jazzBin: string;
  schemaDir: string;
}

function parseArgs(): { command: string; options: BuildOptions } {
  const args = process.argv.slice(2);
  const command = args[0] || "";
  let jazzBin = "jazz";
  let schemaDir = join(process.cwd(), "schema");

  for (let i = 1; i < args.length; i++) {
    if (args[i] === "--jazz-bin" && args[i + 1]) {
      jazzBin = args[++i];
    } else if (args[i] === "--schema-dir" && args[i + 1]) {
      schemaDir = args[++i];
    }
  }

  return { command, options: { jazzBin, schemaDir } };
}

// Counter for cache-busting dynamic imports
let importCounter = 0;

async function loadSchemaModule(filePath: string): Promise<void> {
  resetCollectedState();
  // Add cache-busting query param since Node.js caches dynamic imports
  const url = pathToFileURL(filePath).href + `?v=${++importCounter}`;
  await import(url);
}

async function loadMigrationModule(filePath: string): Promise<Lens | null> {
  resetCollectedState();
  // Add cache-busting query param since Node.js caches dynamic imports
  const url = pathToFileURL(filePath).href + `?v=${++importCounter}`;
  await import(url);
  return getCollectedMigration();
}

async function generateSqlForSchemaFile(tsFile: string): Promise<void> {
  await loadSchemaModule(tsFile);
  const schema = getCollectedSchema();
  const sql = schemaToSql(schema);
  const sqlFile = tsFile.replace(/\.ts$/, ".sql");
  await writeFile(sqlFile, sql);
  console.log(`Generated: ${basename(sqlFile)}`);
}

async function generateAppTs(schemaDir: string): Promise<void> {
  // Reload the schema since SQL generation consumed the collected state
  const schemaFile = join(schemaDir, "current.ts");
  await loadSchemaModule(schemaFile);
  const schema = getCollectedSchema();
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
  const lens = await loadMigrationModule(tsFile);

  if (!lens) {
    console.error(`No migration found in ${basename(tsFile)}`);
    return;
  }

  const fwdSql = lensToSql(lens, "fwd");
  const bwdSql = lensToSql(lens, "bwd");

  const fwdFile = migrationSqlFilename(tsFile, "fwd");
  const bwdFile = migrationSqlFilename(tsFile, "bwd");

  await writeFile(fwdFile, fwdSql);
  await writeFile(bwdFile, bwdSql);

  console.log(`Generated: ${basename(fwdFile)}`);
  console.log(`Generated: ${basename(bwdFile)}`);
}

async function runJazzBuild(jazzBin: string, schemaDir: string): Promise<void> {
  return new Promise((resolve, reject) => {
    console.log(`\nRunning: ${jazzBin} build --ts --schema-dir ${schemaDir}`);
    const child = spawn(jazzBin, ["build", "--ts", "--schema-dir", schemaDir], {
      stdio: "inherit",
    });
    child.on("close", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`jazz build exited with code ${code}`));
      }
    });
    child.on("error", (err) => {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") {
        reject(
          new Error(`jazz binary not found at '${jazzBin}'. Use --jazz-bin to specify the path.`),
        );
      } else {
        reject(err);
      }
    });
  });
}

async function build(options: BuildOptions): Promise<void> {
  const { jazzBin, schemaDir } = options;

  let files: string[];
  try {
    files = await readdir(schemaDir);
  } catch {
    console.error(`Schema directory not found: ${schemaDir}`);
    process.exit(1);
  }

  const tsFiles = files.filter((f) => f.endsWith(".ts"));

  for (const file of tsFiles) {
    const filePath = join(schemaDir, file);

    if (isMigrationTsStub(file)) {
      await generateSqlForMigrationFile(filePath);
    } else if (file === "current.ts") {
      await generateSqlForSchemaFile(filePath);
      await generateAppTs(schemaDir);
    }
  }

  await runJazzBuild(jazzBin, schemaDir);
}

const { command, options } = parseArgs();

switch (command) {
  case "build":
    build(options).catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
    break;
  default:
    console.log("Usage: jazz-ts build [options]");
    console.log("\nCommands:");
    console.log("  build    Generate SQL from TypeScript schemas and run jazz build");
    console.log("\nOptions:");
    console.log("  --jazz-bin <path>    Path to jazz binary (default: jazz)");
    console.log("  --schema-dir <path>  Path to schema directory (default: ./schema)");
    process.exit(command ? 1 : 0);
}

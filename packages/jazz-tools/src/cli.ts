#!/usr/bin/env node

// CLI for jazz-tools schema tooling

import { existsSync, readFileSync, realpathSync } from "fs";
import { readFile } from "fs/promises";
import { basename, join, resolve } from "path";
import { fileURLToPath } from "url";
import {
  createMigration as createCatalogueMigration,
  deploy as deployCatalogue,
  exportSchema as exportCatalogueSchema,
  shortSchemaHash,
  validateProject,
} from "./dev/catalogue-project.js";
import type { StoredPermissionsHead } from "./runtime/schema-fetch.js";

export interface BuildOptions {
  jazzBin?: string;
  schemaDir: string;
  strictProvenance?: boolean;
}

export interface SchemaExportOptions {
  schemaDir: string;
  migrationsDir?: string;
}

const PERMISSIONS_LIFECYCLE_NOTE =
  "Permission-only changes do not create schema hashes or require migrations.";

function parseArgs(args: string[]): { command: string; options: BuildOptions } {
  const command = args[0] || "";
  const schemaDir = getFlagValue(args, "--schema-dir", "last") ?? process.cwd();
  const jazzBin = getFlagValue(args, "--jazz-bin", "last");
  const strictProvenance = args.includes("--strict-provenance");

  return { command, options: { jazzBin, schemaDir, strictProvenance } };
}

export async function validate(options: BuildOptions): Promise<void> {
  const result = await validateProject(options);
  const provenanceWarnings = result.warnings.filter((warning) => warning.includes("built-in $"));
  if (options.strictProvenance && provenanceWarnings.length > 0) {
    throw new Error(
      `Conventional provenance columns are forbidden by --strict-provenance:\n${provenanceWarnings.join("\n")}`,
    );
  }
  console.log(`Loaded schema from ${result.schemaFile}.`);
  if (result.permissionsFile) {
    console.log(`Loaded current permissions from ${result.permissionsFile}.`);
    console.log(PERMISSIONS_LIFECYCLE_NOTE);
    console.log("Use `jazz-tools deploy <appId>` to publish schema, permissions, and migrations.");
  }
  for (const warning of result.warnings) {
    console.warn(`\x1b[33m${warning}\x1b[0m`);
  }
  console.log(
    `Validated ${result.tableCount} table${result.tableCount === 1 ? "" : "s"} in schema.ts.`,
  );
}

export async function exportSchema(options: SchemaExportOptions): Promise<void> {
  const result = await exportCatalogueSchema(options);
  process.stdout.write(`${JSON.stringify(result.schema, null, 2)}\n`);
}

export interface MigrationCommandOptions {
  appId?: string;
  serverUrl?: string;
  adminSecret?: string;
  migrationsDir: string;
  schemaDir?: string;
}

export interface CreateMigrationOptions extends MigrationCommandOptions {
  schemaDir: string;
  fromHash?: string;
  toHash?: string;
  name?: string;
}

export interface DeployOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  schemaDir: string;
  migrationsDir: string;
}

// Framework bundlers (Vite, SvelteKit, Next.js, Expo) expose public env vars
// under their own prefix so the client bundle can read them. The CLI often
// runs in the same project, so accept those prefixed names as fallbacks for
// the canonical JAZZ_ form. The unprefixed JAZZ_ name always wins — it's the
// explicit opt-in when the framework var points somewhere else (e.g. prod).
// Admin/backend secrets stay unprefixed by design: a PUBLIC_/VITE_/NEXT_PUBLIC_
// prefix would leak them into the client bundle.
export const SERVER_URL_ENV_VARS = [
  "JAZZ_SERVER_URL",
  "PUBLIC_JAZZ_SERVER_URL",
  "VITE_JAZZ_SERVER_URL",
  "NEXT_PUBLIC_JAZZ_SERVER_URL",
  "EXPO_PUBLIC_JAZZ_SERVER_URL",
] as const;

export const APP_ID_ENV_VARS = [
  "JAZZ_APP_ID",
  "PUBLIC_JAZZ_APP_ID",
  "VITE_JAZZ_APP_ID",
  "NEXT_PUBLIC_JAZZ_APP_ID",
  "EXPO_PUBLIC_JAZZ_APP_ID",
] as const;

// Real environment variables always win — `.env` is a fallback only.
// Uses Node's built-in `process.loadEnvFile` when operating on the real
// process.env; falls back to a small parser for tests and older Node.
export function loadEnvFile(
  envPath: string,
  env: Record<string, string | undefined> = process.env,
): void {
  if (!existsSync(envPath)) return;
  if (env === process.env && typeof process.loadEnvFile === "function") {
    process.loadEnvFile(envPath);
    return;
  }
  const content = readFileSync(envPath, "utf8");
  for (let line of content.split("\n")) {
    if (line.endsWith("\r")) line = line.slice(0, -1);
    if (!line || line.startsWith("#")) continue;
    const eq = line.indexOf("=");
    if (eq === -1) continue;
    const key = line.slice(0, eq).trim();
    let value = line.slice(eq + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    if (env[key] === undefined) env[key] = value;
  }
}

export function loadDotEnv(
  cwd: string = process.cwd(),
  env: Record<string, string | undefined> = process.env,
): void {
  loadEnvFile(join(cwd, ".env"), env);
}

// Normalize env-file flags in the same pass that records the files to load.
// This keeps command and operand positions stable for every dispatch path.
function normalizeArgs(args: string[]): { args: string[]; envFiles: string[] } {
  const normalized: string[] = [];
  const envFiles: string[] = [];
  const prefix = "--env-file=";

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--env-file") {
      const value = args[i + 1];
      if (!value || value.startsWith("-")) {
        throw new Error("Missing value for --env-file.");
      }
      envFiles.push(value);
      i += 1;
      continue;
    }
    if (arg.startsWith(prefix)) {
      const value = arg.slice(prefix.length);
      if (!value || value.startsWith("-")) {
        throw new Error("Missing value for --env-file.");
      }
      envFiles.push(value);
      continue;
    }
    normalized.push(arg);
  }

  return { args: normalized, envFiles };
}

// Collect every `--env-file=PATH` and `--env-file PATH` from argv, in
// the order they appear. Earlier files take precedence over later ones
// because loadEnvFile only fills in keys that are still undefined.
export function readEnvFiles(args: string[]): string[] {
  return normalizeArgs(args).envFiles;
}
export function resolveEnvVar(
  names: readonly string[],
  env: Record<string, string | undefined> = process.env,
): string | undefined {
  for (const name of names) {
    const value = env[name];
    if (value) return value;
  }
  return undefined;
}

function getFlagValue(
  args: string[],
  flag: string,
  selection: "first" | "last" = "first",
): string | undefined {
  const prefix = `${flag}=`;
  let selected: string | undefined;
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (!arg) {
      continue;
    }
    if (arg === flag) {
      const value = args[i + 1];
      if (!value || value.startsWith("-")) {
        throw new Error(`Missing value for ${flag}.`);
      }
      if (selected === undefined || selection === "last") {
        selected = value;
      }
      continue;
    }
    if (arg.startsWith(prefix)) {
      const value = arg.slice(prefix.length);
      if (!value || value.startsWith("-")) {
        throw new Error(`Missing value for ${flag}.`);
      }
      if (selected === undefined || selection === "last") {
        selected = value;
      }
    }
  }
  return selected;
}

function hasFlag(args: string[], flag: string): boolean {
  return args.includes(flag);
}

function splitLeadingAppId(args: string[]): { appId?: string; args: string[] } {
  const first = args[0];
  if (!first || first.startsWith("-")) {
    return { args: args, appId: resolveEnvVar(APP_ID_ENV_VARS) };
  }

  return {
    appId: first,
    args: args.slice(1),
  };
}

function resolveMigrationOptions(args: string[]): MigrationCommandOptions {
  const serverUrl = getFlagValue(args, "--server-url") ?? resolveEnvVar(SERVER_URL_ENV_VARS);
  const adminSecret = getFlagValue(args, "--admin-secret") ?? process.env.JAZZ_ADMIN_SECRET;
  const migrationsDir = resolve(
    process.cwd(),
    getFlagValue(args, "--migrations-dir") ?? join(process.cwd(), "migrations"),
  );
  const schemaDir = resolve(process.cwd(), getFlagValue(args, "--schema-dir") ?? process.cwd());

  return {
    serverUrl,
    adminSecret,
    migrationsDir,
    schemaDir,
  };
}

function requireSchemaExportServerValue(
  value: string | undefined,
  kind: "serverUrl" | "adminSecret",
): string {
  if (value) {
    return value;
  }

  if (kind === "serverUrl") {
    throw new Error(
      "Missing server URL. Pass --server-url <url> or set JAZZ_SERVER_URL (or a framework-prefixed form such as VITE_JAZZ_SERVER_URL).",
    );
  }

  throw new Error("Missing admin secret. Pass --admin-secret <secret> or set JAZZ_ADMIN_SECRET.");
}

function requireAppId(appId: string | undefined): string {
  if (appId) {
    return appId;
  }

  throw new Error(
    "Missing app ID. Pass an <appId> positional argument or set JAZZ_APP_ID (or a framework-prefixed form such as VITE_JAZZ_APP_ID).",
  );
}

function requireMigrationServerOptions(options: MigrationCommandOptions): {
  appId: string;
  serverUrl: string;
  adminSecret: string;
} {
  return {
    appId: requireAppId(options.appId),
    serverUrl: requireSchemaExportServerValue(options.serverUrl, "serverUrl"),
    adminSecret: requireSchemaExportServerValue(options.adminSecret, "adminSecret"),
  };
}

async function packageVersion(): Promise<string> {
  const packageJson = JSON.parse(
    await readFile(new URL("../package.json", import.meta.url), "utf8"),
  ) as { version?: string };
  return packageJson.version ?? "unknown";
}

export async function createMigration(options: CreateMigrationOptions): Promise<string | null> {
  const result = await createCatalogueMigration(options);

  switch (result.status) {
    case "initial-snapshot":
      console.log("Wrote initial schema snapshot: " + result.snapshotPath);
      console.log("No migration created because there was no previous local schema baseline.");
      return null;
    case "unchanged":
      console.log("No schema changes detected.");
      return null;
    case "migration-not-required": {
      const version = await packageVersion();
      console.log(
        "No reviewed migration file needed because this schema change does not require row transformations.",
      );
      console.log(
        "Next step: Run npx jazz-tools@" + version + " deploy " + (options.appId ?? "<appId>"),
      );
      return null;
    }
    case "generated": {
      const version = await packageVersion();
      console.log("Generated: " + result.filePath);
      console.log("");
      console.log("Migration stubs are only for schema changes.");
      console.log(PERMISSIONS_LIFECYCLE_NOTE);
      console.log("");
      console.log("Next steps:");
      console.log("1. Fill in migrate.");
      if (result.needsRename) {
        console.log("2. Rename the file by replacing 'unnamed'.");
      }
      console.log(
        (result.needsRename ? "3" : "2") +
          ". Run npx jazz-tools@" +
          version +
          " deploy " +
          (options.appId ?? "<appId>"),
      );
      return result.filePath;
    }
  }
}

function describePermissionsHead(head: StoredPermissionsHead): string {
  return `v${head.version} on ${shortSchemaHash(head.schemaHash)}`;
}

function logDeployWarning(message: string): void {
  if (message.startsWith("Warning: table ")) {
    console.warn(`\x1b[33m${message}\x1b[0m`);
    return;
  }

  if (message.startsWith("Warning: ")) {
    console.warn(message);
    return;
  }

  console.warn(`Warning: ${message}`);
}

export async function deploy(options: DeployOptions): Promise<void> {
  const result = await deployCatalogue({
    ...options,
    onEvent: (event) => {
      switch (event.type) {
        case "schema-loaded":
          console.log(`Loaded current schema from ${event.schemaFile}.`);
          break;
        case "warning":
          logDeployWarning(event.message);
          break;
        case "schema-published":
          console.log(`Published the current schema as ${shortSchemaHash(event.hash)}.`);
          break;
        case "schema-skipped":
          console.log(
            `The current schema is already stored in the server as ${shortSchemaHash(event.hash)}; skipping publish.`,
          );
          break;
        case "permissions-loaded":
          console.log(`Loaded current permissions from ${event.permissionsFile}.`);
          break;
        case "migration-published":
          if (event.filePath) {
            console.log(
              `Pushed migration ${shortSchemaHash(event.fromHash)} -> ${shortSchemaHash(event.toHash)} from ${basename(event.filePath)}.`,
            );
          } else {
            console.log(
              `Pushed migration ${shortSchemaHash(event.fromHash)} -> ${shortSchemaHash(event.toHash)} without a reviewed migration file because no row transformations are required.`,
            );
          }
          break;
        case "permissions-published":
          break;
      }
    },
  });

  if (!result.permissions) {
    return;
  }

  const previousHead = result.permissions.previousHead;
  const nextHead = result.permissions.head ?? {
    schemaHash: result.permissions.schemaHash,
    version: previousHead ? previousHead.version + 1 : 1,
    parentBundleObjectId: previousHead?.bundleObjectId ?? null,
    bundleObjectId: previousHead?.bundleObjectId ?? "",
  };

  console.log(`Published permissions as ${describePermissionsHead(nextHead)}.`);
}

function realpathOrSelf(path: string): string {
  try {
    return realpathSync(path);
  } catch {
    return path;
  }
}

function isMainModule(): boolean {
  const entry = process.argv[1];
  if (!entry) {
    return false;
  }
  // pnpm reaches the CLI through a symlinked package path, so argv[1] and
  // import.meta.url differ only by symlink resolution. Compare realpaths.
  return realpathOrSelf(entry) === realpathOrSelf(fileURLToPath(import.meta.url));
}

function printHelp(): void {
  console.log("Usage: node <path-to-jazz-tools>/dist/cli.js <command> [options]");
  console.log("\nCommands:");
  console.log("  validate              Validate root schema.ts and permissions.ts");
  console.log("  schema export         Print the compiled schema as JSON");
  console.log("  deploy <appId>        Publish schema, permissions, and required migrations");
  console.log("  migrations create     Generate a migration stub between two schema versions");
  console.log("\nValidation options:");
  console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
  console.log("  --strict-provenance   Reject conventional duplicates of Jazz provenance");
  console.log("\nSchema export options:");
  console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
  console.log("  --migrations-dir <p>  Path to migrations directory (default: ./migrations)");
  console.log("\nMigration options:");
  console.log(
    "  <appId>               Required for remote migration creation and deploy (or set JAZZ_APP_ID / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_APP_ID)",
  );
  console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
  console.log(
    "  --server-url <url>    Jazz server URL (or set JAZZ_SERVER_URL / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_SERVER_URL)",
  );
  console.log("  --admin-secret <sec>  Admin secret (or set JAZZ_ADMIN_SECRET)");
  console.log("  --migrations-dir <p>  Path to migrations directory (default: ./migrations)");
  console.log("  --fromHash <hash>     Optional source schema hash (defaults to latest snapshot)");
  console.log("  --toHash <hash>       Optional target schema hash (defaults to current schema)");
  console.log("  --name <name>         Optional migration filename label (default: unnamed)");
  console.log("\nGlobal options:");
  console.log(
    "  --env-file <path>     Load env vars from this file (repeatable; first file wins per key). Defaults to .env in cwd.",
  );
}

if (isMainModule()) {
  const { args, envFiles } = normalizeArgs(process.argv.slice(2));
  if (args.some((arg) => arg === "--help" || arg === "-h")) {
    printHelp();
    process.exit(0);
  }
  if (envFiles.length > 0) {
    for (const file of envFiles) {
      loadEnvFile(resolve(process.cwd(), file));
    }
  } else {
    loadDotEnv();
  }
  const command = args[0] ?? "";

  if (command === "validate") {
    const { options } = parseArgs(args);
    validate(options).catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
  } else if (command === "schema") {
    const subcommand = args[1] ?? "";
    if (subcommand === "export") {
      const commandArgs = args.slice(2);
      for (let i = 0; i < commandArgs.length; i += 2) {
        if (!["--schema-dir", "--migrations-dir"].includes(commandArgs[i]!)) {
          console.error(
            `Unknown schema export argument: ${commandArgs[i]}. Only local schema export is supported.`,
          );
          process.exit(1);
        }
        if (!commandArgs[i + 1] || commandArgs[i + 1]!.startsWith("--")) {
          console.error(`Missing value for ${commandArgs[i]}.`);
          process.exit(1);
        }
      }
      const schemaDirFlag = getFlagValue(commandArgs, "--schema-dir");
      const schemaDir = resolve(process.cwd(), schemaDirFlag ?? process.cwd());
      exportSchema({
        schemaDir,
        migrationsDir: getFlagValue(commandArgs, "--migrations-dir")
          ? resolve(process.cwd(), getFlagValue(commandArgs, "--migrations-dir")!)
          : undefined,
      }).catch((err) => {
        console.error(err.message);
        process.exit(1);
      });
    } else {
      console.error("Usage: node dist/cli.js schema export [--schema-dir <path>] [...]");
      process.exit(1);
    }
  } else if (command === "migrations") {
    const subcommand = args[1] ?? "";
    let task: Promise<unknown>;

    if (subcommand === "create") {
      const { appId, args: commandArgs } = splitLeadingAppId(args.slice(2));
      const options = resolveMigrationOptions(commandArgs);
      task = createMigration({
        ...options,
        appId,
        schemaDir: options.schemaDir ?? process.cwd(),
        fromHash: getFlagValue(commandArgs, "--fromHash"),
        toHash: getFlagValue(commandArgs, "--toHash"),
        name: getFlagValue(commandArgs, "--name"),
      });
    } else {
      task = Promise.reject(
        new Error(
          "Use `jazz-tools migrations create` to prepare migrations and `jazz-tools deploy <appId>` to publish them.",
        ),
      );
    }

    task.catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
  } else if (command === "deploy") {
    if (hasFlag(args, "--no-verify")) {
      console.error(
        "--no-verify is no longer supported; deploy requires a complete migration path.",
      );
      process.exit(1);
    }
    const { appId, args: commandArgs } = splitLeadingAppId(args.slice(1));
    const options = { ...resolveMigrationOptions(commandArgs), appId };
    deploy({
      ...requireMigrationServerOptions(options),
      schemaDir: options.schemaDir ?? process.cwd(),
      migrationsDir: options.migrationsDir,
    }).catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
  } else {
    printHelp();
    process.exit(command ? 1 : 0);
  }
}

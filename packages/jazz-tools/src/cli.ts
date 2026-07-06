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
  getCurrentSchemaHash,
  getPermissionsStatus,
  pushMigration as pushCatalogueMigration,
  shortSchemaHash,
  validateProject,
} from "./dev/catalogue-project.js";
import type { StoredPermissionsHead } from "./runtime/schema-fetch.js";

export interface BuildOptions {
  jazzBin?: string;
  schemaDir: string;
}

export interface SchemaExportOptions {
  schemaDir: string;
  migrationsDir?: string;
  schemaHash?: string;
  appId?: string;
  serverUrl?: string;
  adminSecret?: string;
}

export interface SchemaHashOptions {
  schemaDir: string;
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

export async function validate(options: BuildOptions): Promise<void> {
  const result = await validateProject(options);
  console.log(`Loaded structural schema from ${result.schemaFile}.`);
  if (result.permissionsFile) {
    console.log(`Loaded current permissions from ${result.permissionsFile}.`);
    console.log(PERMISSIONS_LIFECYCLE_NOTE);
    console.log(
      "Use `jazz-tools permissions status <appId>` or `jazz-tools deploy <appId>` for auth publication.",
    );
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

export async function schemaHash(options: SchemaHashOptions): Promise<void> {
  const result = await getCurrentSchemaHash(options);
  console.log(`Loaded structural schema from ${result.schemaFile}.`);
  console.log(`Current schema hash: ${shortSchemaHash(result.hash)}`);
}

export interface MigrationCommandOptions {
  appId?: string;
  serverUrl?: string;
  adminSecret?: string;
  migrationsDir: string;
  schemaDir?: string;
}

export interface PermissionsCommandOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  schemaDir: string;
}

export interface CreateMigrationOptions extends MigrationCommandOptions {
  schemaDir: string;
  fromHash?: string;
  toHash?: string;
  name?: string;
}

export interface PushMigrationOptions extends MigrationCommandOptions {
  // Can be a full hash or short hash prefix
  fromHash: string;
  // Can be a full hash or short hash prefix
  toHash: string;
}

export interface DeployOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  schemaDir: string;
  migrationsDir: string;
  noVerify?: boolean;
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

// Collect every `--env-file=PATH` and `--env-file PATH` from argv, in
// the order they appear. Earlier files take precedence over later ones
// because loadEnvFile only fills in keys that are still undefined.
export function readEnvFiles(args: string[]): string[] {
  const files: string[] = [];
  const prefix = "--env-file=";
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--env-file") {
      const value = args[i + 1];
      if (value) files.push(value);
    } else if (arg.startsWith(prefix)) {
      files.push(arg.slice(prefix.length));
    }
  }
  return files;
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

function resolvePermissionsOptions(args: string[]): Omit<PermissionsCommandOptions, "appId"> {
  const serverUrl = getFlagValue(args, "--server-url") ?? resolveEnvVar(SERVER_URL_ENV_VARS);
  const adminSecret = getFlagValue(args, "--admin-secret") ?? process.env.JAZZ_ADMIN_SECRET;
  const schemaDir = resolve(process.cwd(), getFlagValue(args, "--schema-dir") ?? process.cwd());

  if (!serverUrl) {
    throw new Error(
      "Missing server URL. Pass --server-url <url> or set JAZZ_SERVER_URL (or a framework-prefixed form such as VITE_JAZZ_SERVER_URL).",
    );
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
      console.log("No structural schema changes detected.");
      return null;
    case "migration-not-required": {
      const version = await packageVersion();
      console.log(
        "No reviewed migration file needed because this schema change does not require row transformations.",
      );
      console.log(
        "Next step: Run npx jazz-tools@" +
          version +
          " migrations push " +
          (options.appId ?? "<appId>") +
          " " +
          shortSchemaHash(result.fromHash) +
          " " +
          shortSchemaHash(result.toHash),
      );
      return null;
    }
    case "generated": {
      const version = await packageVersion();
      console.log("Generated: " + result.filePath);
      console.log("");
      console.log("Migration stubs are only for structural schema changes.");
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
          " migrations push " +
          (options.appId ?? "<appId>") +
          " " +
          shortSchemaHash(result.fromHash) +
          " " +
          shortSchemaHash(result.toHash),
      );
      return result.filePath;
    }
  }
}

export async function pushMigration(options: PushMigrationOptions): Promise<void> {
  const { appId, serverUrl, adminSecret } = requireMigrationServerOptions(options);
  const result = await pushCatalogueMigration({
    appId,
    serverUrl,
    adminSecret,
    migrationsDir: options.migrationsDir,
    fromHash: options.fromHash,
    toHash: options.toHash,
  });

  if (result.filePath) {
    console.log(
      `Pushed migration ${shortSchemaHash(result.fromHash)} -> ${shortSchemaHash(result.toHash)} from ${basename(result.filePath)}.`,
    );
    return;
  }

  console.log(
    `Pushed migration ${shortSchemaHash(result.fromHash)} -> ${shortSchemaHash(result.toHash)} without a reviewed migration file because no row transformations are required.`,
  );
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

export async function permissionsStatus(options: PermissionsCommandOptions): Promise<void> {
  const result = await getPermissionsStatus(options);

  console.log(`Loaded structural schema from ${result.schemaFile}.`);
  console.log(`Loaded current permissions from ${result.permissionsFile}.`);
  console.log(
    `Local structural schema matches stored hash ${shortSchemaHash(result.localSchemaHash)}.`,
  );
  console.log(PERMISSIONS_LIFECYCLE_NOTE);

  if (!result.head) {
    console.log("Server has no published permissions head yet.");
    console.log("Next push will publish version 1.");
    return;
  }

  console.log(`Server permissions head is ${describePermissionsHead(result.head)}.`);
  if (result.head.schemaHash === result.localSchemaHash) {
    console.log("Current server permissions already target this structural schema.");
  } else {
    console.log(
      `Current server permissions target ${shortSchemaHash(result.head.schemaHash)}; pushing will retarget the head to ${shortSchemaHash(result.localSchemaHash)}.`,
    );
  }
  console.log(`Next push will require parent bundle ${result.head.bundleObjectId}.`);
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
        case "permissions-skipped":
          console.log("No permissions.ts found; skipping permissions publish.");
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

if (isMainModule()) {
  const envFiles = readEnvFiles(process.argv.slice(2));
  if (envFiles.length > 0) {
    for (const file of envFiles) {
      loadEnvFile(resolve(process.cwd(), file));
    }
  } else {
    loadDotEnv();
  }
  const command = process.argv[2] ?? "";

  if (command === "validate") {
    const { options } = parseArgs();
    validate(options).catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
  } else if (command === "schema") {
    const subcommand = process.argv[3] ?? "";
    if (subcommand === "hash") {
      const args = process.argv.slice(4);
      const schemaDirFlag = getFlagValue(args, "--schema-dir");
      const schemaDir = resolve(process.cwd(), schemaDirFlag ?? process.cwd());
      schemaHash({ schemaDir }).catch((err) => {
        console.error(err.message);
        process.exit(1);
      });
    } else if (subcommand === "export") {
      const { appId, args } = splitLeadingAppId(process.argv.slice(4));
      const schemaDirFlag = getFlagValue(args, "--schema-dir");
      const schemaHashFlag = getFlagValue(args, "--schema-hash");
      if (schemaDirFlag && schemaHashFlag) {
        console.error("--schema-dir and --schema-hash are mutually exclusive.");
        process.exit(1);
      }

      const schemaDir = resolve(process.cwd(), schemaDirFlag ?? process.cwd());
      exportSchema({
        schemaDir,
        migrationsDir: getFlagValue(args, "--migrations-dir")
          ? resolve(process.cwd(), getFlagValue(args, "--migrations-dir")!)
          : undefined,
        schemaHash: schemaHashFlag,
        appId,
        serverUrl: getFlagValue(args, "--server-url") ?? resolveEnvVar(SERVER_URL_ENV_VARS),
        adminSecret: getFlagValue(args, "--admin-secret") ?? process.env.JAZZ_ADMIN_SECRET,
      }).catch((err) => {
        console.error(err.message);
        process.exit(1);
      });
    } else {
      console.error("Usage: node dist/cli.js schema <hash|export> [--schema-dir <path>] [...]");
      process.exit(1);
    }
  } else if (command === "migrations") {
    const subcommand = process.argv[3] ?? "";
    let task: Promise<unknown>;

    if (subcommand === "create") {
      const { appId, args } = splitLeadingAppId(process.argv.slice(4));
      const options = resolveMigrationOptions(args);
      task = createMigration({
        ...options,
        appId,
        schemaDir: options.schemaDir ?? process.cwd(),
        fromHash: getFlagValue(args, "--fromHash"),
        toHash: getFlagValue(args, "--toHash"),
        name: getFlagValue(args, "--name"),
      });
    } else if (subcommand === "push") {
      const appId = process.argv[4];
      const fromHash = process.argv[5];
      const toHash = process.argv[6];
      const sharedArgs = process.argv.slice(7);

      if (!appId || !fromHash || !toHash) {
        console.error(
          "Usage: node dist/cli.js migrations push <appId> <fromHash> <toHash> [options]",
        );
        process.exit(1);
      }

      const options = resolveMigrationOptions(sharedArgs);
      task = pushMigration({ ...options, appId, fromHash, toHash });
    } else {
      task = Promise.reject(
        new Error("Usage: node dist/cli.js migrations <create|push> [<appId>] [options]"),
      );
    }

    task.catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
  } else if (command === "permissions") {
    const subcommand = process.argv[3] ?? "";
    const { appId, args } = splitLeadingAppId(process.argv.slice(4));
    const options = { ...resolvePermissionsOptions(args), appId: requireAppId(appId) };
    const task =
      subcommand === "status"
        ? permissionsStatus(options)
        : Promise.reject(new Error("Usage: node dist/cli.js permissions status <appId> [options]"));

    task.catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
  } else if (command === "deploy") {
    const { appId, args } = splitLeadingAppId(process.argv.slice(3));
    const options = { ...resolveMigrationOptions(args), appId };
    deploy({
      ...requireMigrationServerOptions(options),
      schemaDir: options.schemaDir ?? process.cwd(),
      migrationsDir: options.migrationsDir,
      noVerify: hasFlag(args, "--no-verify"),
    }).catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
  } else {
    console.log("Usage: node <path-to-jazz-tools>/dist/cli.js <command> [options]");
    console.log("\nCommands:");
    console.log("  validate              Validate root schema.ts and optional permissions.ts");
    console.log("  schema hash           Print the short hash of the current schema.ts");
    console.log("  schema export         Print the compiled structural schema as JSON");
    console.log("  deploy <appId>        Publish the current schema.ts and permissions.ts");
    console.log(
      "  permissions status <appId> Show the current server permissions head for this app",
    );
    console.log(
      "  migrations create     Generate a typed structural migration stub between two schema versions",
    );
    console.log(
      "  migrations push <appId> <fromHash> <toHash> Push a reviewed migration edge to the server",
    );
    console.log("\nValidation options:");
    console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
    console.log("\nSchema hash options:");
    console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
    console.log("\nSchema export options:");
    console.log(
      "  <appId>               Required for server-backed schema export by hash (or set JAZZ_APP_ID / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_APP_ID)",
    );
    console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
    console.log("  --schema-hash <hash>  Export a stored structural schema by hash");
    console.log("  --migrations-dir <p>  Path to migrations directory (default: ./migrations)");
    console.log(
      "  --server-url <url>    Jazz server URL (or set JAZZ_SERVER_URL / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_SERVER_URL)",
    );
    console.log("  --admin-secret <sec>  Admin secret (or set JAZZ_ADMIN_SECRET)");
    console.log("\nPermissions options:");
    console.log(
      "  <appId>               Required (or set JAZZ_APP_ID / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_APP_ID)",
    );
    console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
    console.log(
      "  --server-url <url>    Jazz server URL (or set JAZZ_SERVER_URL / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_SERVER_URL)",
    );
    console.log("  --admin-secret <sec>  Admin secret (or set JAZZ_ADMIN_SECRET)");
    console.log("\nMigration options:");
    console.log(
      "  <appId>               Required for remote create/push commands (or set JAZZ_APP_ID / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_APP_ID)",
    );
    console.log("  --schema-dir <path>   Path to app root containing schema.ts (default: .)");
    console.log(
      "  --server-url <url>    Jazz server URL (or set JAZZ_SERVER_URL / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_SERVER_URL)",
    );
    console.log("  --admin-secret <sec>  Admin secret (or set JAZZ_ADMIN_SECRET)");
    console.log("  --migrations-dir <p>  Path to migrations directory (default: ./migrations)");
    console.log(
      "  --fromHash <hash>     Optional source schema hash (defaults to latest snapshot)",
    );
    console.log("  --toHash <hash>       Optional target schema hash (defaults to current schema)");
    console.log("  --name <name>         Optional migration filename label (default: unnamed)");
    console.log("\nGlobal options:");
    console.log(
      "  --env-file <path>     Load env vars from this file (repeatable; first file wins per key). Defaults to .env in cwd.",
    );
    process.exit(command ? 1 : 0);
  }
}

#!/usr/bin/env node

// CLI for jazz-tools schema tooling

import { existsSync, readFileSync, realpathSync } from "fs";
import { join, relative, resolve } from "path";
import { fileURLToPath } from "url";
import { createAppMigration, localMigrationGraph, generateApp } from "./dev/project.js";
import {
  combineMigrationGraphs,
  fetchMigrationGraph,
  renderMigrationGraph,
} from "./dev/migration-graph.js";
import { readAppProject } from "./dev/app-files.js";
import { compileApp } from "./dev/codegen-napi.js";
import { isCloudCommand, runCloudCommand } from "./cloud/commands.js";
import { runInspectCommand } from "./inspect/cli.js";

export interface BuildOptions {
  /** The project directory: jazz.app.json in it, ./src or ./src/lib. */
  schemaDir: string;
  /** Write nothing; fail if jazz.generated.ts isn't what generate would write. */
  check?: boolean;
}

export interface SchemaCompileOptions {
  schemaDir: string;
}

// createJazz publishes the app's bundle when the backend starts, so there is
// no separate deploy step.
const DEPLOY_NOTE =
  "Deploy your backend to publish them: createJazz publishes the schema, its permissions and its migrations when it starts.";

const DEPLOY_REMOVED =
  "`jazz-tools deploy` was removed. " +
  "createJazz publishes your schema, its permissions and its migrations when your backend starts, " +
  "so deploying the backend is the deployment.";

const SERVER_REMOVED =
  "`jazz-tools server` was removed. Jazz runs inside your backend: " +
  "`createJazz({ store: vercel() })` runs it on Vercel, and " +
  "`createJazz({ store: sqlite({ path }) })` serves sync from your own server. " +
  "Jazz Cloud is deprecated for new apps.";

export const VALIDATE_RENAMED =
  "`jazz-tools validate` was renamed to `jazz-tools generate` in alpha.65: it checks jazz.app.json and " +
  "jazz.permissions.json and writes jazz.generated.ts. Use `jazz-tools generate --check` in CI.";

const CREATE_REMOVED = "`jazz-tools create` was removed. Start a project with `npm create jazz`.";

const MIGRATE_ALPHA59_MOVED =
  "`jazz-tools migrate-alpha59` moved to `jazz-alpha59-alpha60-import`, a native binary attached to each " +
  "GitHub release next to `jazz-alpha59-alpha60-export`. Download both for your platform from " +
  "https://github.com/garden-co/jazz/releases and run `jazz-alpha59-alpha60-import import --dump <dump> " +
  "--data-dir <dir> --app-id <app id>`.";

function parseArgs(args: string[]): { command: string; options: BuildOptions } {
  const command = args[0] || "";
  const schemaDir = getFlagValue(args, "--schema-dir", "last") ?? process.cwd();
  if (args.includes("--strict-provenance")) {
    throw new Error(
      "--strict-provenance was removed with schema.ts: jazz.app.json's columns are taken as written. Use Jazz's $createdAt, $createdBy, $updatedAt and $updatedBy instead of columns that duplicate them.",
    );
  }
  return {
    command,
    options: { schemaDir: resolve(process.cwd(), schemaDir), check: args.includes("--check") },
  };
}

/** `jazz-tools generate [--check]`: check jazz.app.json and jazz.permissions.json, write the bindings. */
export async function generate(options: BuildOptions): Promise<void> {
  const result = await generateApp({ cwd: options.schemaDir, check: options.check });
  for (const warning of result.warnings) {
    console.warn(`\x1b[33m${warning}\x1b[0m`);
  }
  const shown = result.bindings
    .map((file) => relative(process.cwd(), join(result.dir, file)) || file)
    .join(" and ");
  console.log(
    options.check
      ? `jazz.app.json, jazz.permissions.json and ${shown} are up to date.`
      : result.wrote
        ? `Checked jazz.app.json and jazz.permissions.json; wrote ${shown}. Commit it.`
        : `Checked jazz.app.json and jazz.permissions.json; ${shown} is up to date.`,
  );
  console.log(DEPLOY_NOTE);
}

/** `jazz-tools schema compile`: print what jazz.generated.ts passes to bindApp, as JSON. */
export async function compileSchema(options: SchemaCompileOptions): Promise<void> {
  const project = await readAppProject(options.schemaDir);
  process.stdout.write(`${JSON.stringify(await compileApp(project.app), null, 2)}\n`);
}

export function requireServerValue(
  value: string | undefined,
  kind: "serverUrl" | "adminSecret",
): string {
  if (value) return value;
  if (kind === "serverUrl") {
    throw new Error(
      "Missing server URL. Pass --server-url <url> or set JAZZ_SERVER_URL (or a framework-prefixed form such as VITE_JAZZ_SERVER_URL).",
    );
  }
  throw new Error("Missing admin secret. Pass --admin-secret <secret> or set JAZZ_ADMIN_SECRET.");
}

export function requireAppId(appId: string | undefined): string {
  if (appId) return appId;
  throw new Error(
    "Missing app ID. Pass an <appId> positional argument or set JAZZ_APP_ID (or a framework-prefixed form such as VITE_JAZZ_APP_ID).",
  );
}

export interface MigrationCommandOptions {
  appId?: string;
  serverUrl?: string;
  adminSecret?: string;
  schemaDir?: string;
}

export interface CreateMigrationOptions {
  schemaDir: string;
  name?: string;
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

// The value of `flag` at args[i], given as `flag value` or `flag=value`, and
// how many args it spans.
function readFlagAt(
  args: string[],
  i: number,
  flag: string,
): { value: string; width: number } | undefined {
  const arg = args[i];
  const width = arg === flag ? 2 : 1;
  if (width === 1 && !arg?.startsWith(`${flag}=`)) return undefined;
  const value = width === 2 ? args[i + 1] : arg!.slice(flag.length + 1);
  if (!value || value.startsWith("-")) {
    throw new Error(`Missing value for ${flag}.`);
  }
  return { value, width };
}

// Normalize env-file flags in the same pass that records the files to load.
// This keeps command and operand positions stable for every dispatch path.
function normalizeArgs(args: string[]): { args: string[]; envFiles: string[] } {
  const normalized: string[] = [];
  const envFiles: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const found = readFlagAt(args, i, "--env-file");
    if (found) {
      envFiles.push(found.value);
      i += found.width - 1;
    } else {
      normalized.push(args[i]!);
    }
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
  const values: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const found = readFlagAt(args, i, flag);
    if (found) {
      values.push(found.value);
      i += found.width - 1;
    }
  }
  return selection === "first" ? values[0] : values.at(-1);
}

// Accept only `--flag value` pairs drawn from `allowed`.
function assertFlagPairs(
  args: string[],
  allowed: readonly string[],
  unknown: (arg: string) => string,
): void {
  for (let i = 0; i < args.length; i += 2) {
    if (!allowed.includes(args[i]!)) {
      throw new Error(unknown(args[i]!));
    }
    if (!args[i + 1] || args[i + 1]!.startsWith("--")) {
      throw new Error(`Missing value for ${args[i]}.`);
    }
  }
}

function exitOnError(task: Promise<unknown>): void {
  task.catch((err) => {
    console.error(err.message);
    process.exit(1);
  });
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
  const schemaDir = resolve(process.cwd(), getFlagValue(args, "--schema-dir") ?? process.cwd());

  return {
    serverUrl,
    adminSecret,
    schemaDir,
  };
}

function requireMigrationServerOptions(options: MigrationCommandOptions): {
  appId: string;
  serverUrl: string;
  adminSecret: string;
} {
  return {
    appId: requireAppId(options.appId),
    serverUrl: requireServerValue(options.serverUrl, "serverUrl"),
    adminSecret: requireServerValue(options.adminSecret, "adminSecret"),
  };
}

/** `jazz-tools migrations create --name <name>`: record the schema change in jazz.app.json. */
export async function createMigration(options: CreateMigrationOptions): Promise<string | null> {
  const result = await createAppMigration({ cwd: options.schemaDir, name: options.name ?? "" });
  const file = relative(process.cwd(), join(result.dir, "jazz.app.json")) || "jazz.app.json";
  if (!result.changed) {
    console.log("No schema changes since jazz.generated.ts was generated: no migration needed.");
    return null;
  }
  console.log(
    `Recorded the previous version in ${file}'s history and wrote the migration "${options.name}".`,
  );
  if (result.todos.length > 0) {
    console.log("");
    console.log(
      `Resolve ${result.todos.length} TODO${result.todos.length === 1 ? "" : "s"} in ${file}, then run \`jazz-tools generate\`:`,
    );
    for (const todo of result.todos) console.log(`  ${todo}`);
  } else {
    console.log("Review the migration, then commit jazz.app.json and jazz.generated.ts.");
  }
  console.log(DEPLOY_NOTE);
  return join(result.dir, "jazz.app.json");
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
  console.log(
    "  generate              Check jazz.app.json and jazz.permissions.json and write jazz.generated.ts",
  );
  console.log(
    "                        (--check: write nothing, fail if the bindings are out of date; for CI)",
  );
  console.log("  migrations create     Record a schema change in jazz.app.json (--name <name>)");
  console.log("  migrations graph      Visualize the full migration graph");
  console.log(
    "  upgrade               Convert an alpha.64 app (schema.ts, permissions.ts, migrations/) to the files",
  );
  console.log(
    "  schema compile        Print the compiled schema jazz.generated.ts embeds, as JSON",
  );
  console.log("  login | logout | whoami (deprecated)");
  console.log("                        Sign in to Jazz Cloud with your dashboard account");
  console.log(
    "  teams, apps           (deprecated) Manage Jazz Cloud teams and apps (see `apps --help`)",
  );
  console.log(
    "  inspect <command>     Read and change an app's data through its backend (see `inspect --help`)",
  );
  console.log("\nOptions:");
  console.log(
    "  --schema-dir <path>   Project directory holding jazz.app.json (default: .; also ./src, ./src/lib)",
  );
  console.log("  --name <name>         migrations create: the migration's name");
  console.log("\nUpgrade options:");
  console.log("  --dir <path>          The app's directory (default: .)");
  console.log(
    "  --schema-dir <path>   Where schema.ts is, if not ., ./src, ./src/lib or found below --dir",
  );
  console.log("  --dry-run             Report what would change, write nothing");
  console.log("  --skip-typecheck      Don't typecheck the converted project");
  console.log("\nMigration graph options:");
  console.log(
    "  <appId>               Required (or set JAZZ_APP_ID / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_APP_ID)",
  );
  console.log(
    "  --server-url <url>    Jazz server URL (or set JAZZ_SERVER_URL / {VITE,PUBLIC,NEXT_PUBLIC,EXPO_PUBLIC}_JAZZ_SERVER_URL)",
  );
  console.log("  --admin-secret <sec>  Admin secret (or set JAZZ_ADMIN_SECRET)");
  console.log("\nGlobal options:");
  console.log(
    "  --env-file <path>     Load env vars from this file (repeatable; first file wins per key). Defaults to .env in cwd.",
  );
}

if (isMainModule()) {
  const { args, envFiles } = normalizeArgs(process.argv.slice(2));
  const command = args[0] ?? "";
  // Cloud commands (login, apps, ...) have their own help.
  const cloudCommand = isCloudCommand(command);
  // Cloud commands and `inspect` have their own help.
  const ownHelp = cloudCommand || command === "inspect";
  if (!ownHelp && args.some((arg) => arg === "--help" || arg === "-h")) {
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

  if (command === "inspect" && args[1] === "mcp") {
    const rest = args.slice(2);
    if (rest.some((arg) => arg === "--help" || arg === "-h")) {
      console.log(
        "Usage: jazz-tools inspect mcp [--app <url>]\n\nServe the app's Inspector API as an MCP server over stdio.\nAdd it to an agent with e.g. `claude mcp add jazz -- npx jazz-tools inspect mcp --app <url>`.",
      );
      process.exit(0);
    }
    const appIndex = rest.findIndex((arg) => arg === "--app" || arg.startsWith("--app="));
    const app =
      appIndex === -1
        ? undefined
        : rest[appIndex]!.startsWith("--app=")
          ? rest[appIndex]!.slice("--app=".length)
          : rest[appIndex + 1];
    void import("./inspect/mcp.js").then(({ runMcpServer }) =>
      runMcpServer(app ? { app } : {}).then(
        () => process.exit(0),
        (error: unknown) => {
          console.error(error);
          process.exit(1);
        },
      ),
    );
  } else if (command === "inspect") {
    runInspectCommand(args.slice(1), {
      stdout: (text) => process.stdout.write(text),
      stderr: (text) => process.stderr.write(text),
      env: process.env,
    }).then((code) => process.exit(code));
  } else if (cloudCommand) {
    runCloudCommand(args, {
      stdout: (text) => process.stdout.write(text),
      stderr: (text) => process.stderr.write(text),
      env: process.env,
    }).then((code) => process.exit(code));
  } else if (command === "generate") {
    exitOnError(Promise.resolve().then(() => generate(parseArgs(args).options)));
  } else if (command === "upgrade") {
    const commandArgs = args.slice(1);
    const dryRun = commandArgs.includes("--dry-run");
    exitOnError(
      import("./upgrade/index.js").then(async ({ upgradeProject, formatReport }) => {
        const report = await upgradeProject({
          dir: resolve(process.cwd(), getFlagValue(commandArgs, "--dir") ?? "."),
          dryRun,
          skipTypecheck: commandArgs.includes("--skip-typecheck"),
          schemaDir: getFlagValue(commandArgs, "--schema-dir"),
        });
        console.log(formatReport(report, dryRun));
        if (report.typeErrors.length > 0 || report.todos > 0) process.exitCode = 1;
      }),
    );
  } else if (command === "validate") {
    console.error(VALIDATE_RENAMED);
    process.exit(1);
  } else if (command === "schema") {
    const subcommand = args[1] ?? "";
    if (subcommand === "compile") {
      const commandArgs = args.slice(2);
      try {
        assertFlagPairs(
          commandArgs,
          ["--schema-dir"],
          (arg) =>
            `Unknown schema compile argument: ${arg}. Only local schema compilation is supported.`,
        );
      } catch (err) {
        console.error((err as Error).message);
        process.exit(1);
      }
      const schemaDir = resolve(process.cwd(), getFlagValue(commandArgs, "--schema-dir") ?? ".");
      exitOnError(compileSchema({ schemaDir }));
    } else {
      console.error("Usage: node dist/cli.js schema compile [--schema-dir <path>] [...]");
      process.exit(1);
    }
  } else if (command === "migrations") {
    const subcommand = args[1] ?? "";
    let task: Promise<unknown>;

    if (subcommand === "create") {
      const commandArgs = args.slice(2);
      task = (async () => {
        assertFlagPairs(commandArgs, ["--schema-dir", "--name"], (arg) =>
          ["--fromHash", "--toHash", "--migrations-dir"].includes(arg)
            ? `${arg} was removed: migrations go from the version in jazz.generated.ts to jazz.app.json's tables.`
            : `Unknown migrations create argument: ${arg}.`,
        );
        await createMigration({
          schemaDir: resolve(process.cwd(), getFlagValue(commandArgs, "--schema-dir") ?? "."),
          name: getFlagValue(commandArgs, "--name"),
        });
      })();
    } else if (subcommand === "graph") {
      task = (async () => {
        const { appId, args: commandArgs } = splitLeadingAppId(args.slice(2));
        assertFlagPairs(
          commandArgs,
          ["--server-url", "--admin-secret", "--schema-dir"],
          (arg) => `Unknown migrations graph argument: ${arg}.`,
        );
        const localOptions = resolveMigrationOptions(commandArgs);
        const options = requireMigrationServerOptions({ ...localOptions, appId });
        const graph = combineMigrationGraphs(
          await fetchMigrationGraph(options),
          await localMigrationGraph(localOptions.schemaDir ?? process.cwd()),
        );
        const color =
          Boolean(process.stdout.isTTY) &&
          process.env.NO_COLOR === undefined &&
          process.env.TERM !== "dumb";
        console.log(renderMigrationGraph(graph, color));
      })();
    } else {
      task = Promise.reject(
        new Error(
          `Use \`jazz-tools migrations create\` to prepare migrations and \`jazz-tools migrations graph <appId>\` to inspect the migration graph. ${DEPLOY_NOTE}`,
        ),
      );
    }

    exitOnError(task);
  } else if (command === "deploy") {
    console.error(DEPLOY_REMOVED);
    process.exit(1);
  } else if (command === "server") {
    console.error(SERVER_REMOVED);
    process.exit(1);
  } else if (command === "create") {
    console.error(CREATE_REMOVED);
    process.exit(1);
  } else if (command === "migrate-alpha59") {
    console.error(MIGRATE_ALPHA59_MOVED);
    process.exit(1);
  } else {
    printHelp();
    process.exit(command ? 1 : 0);
  }
}

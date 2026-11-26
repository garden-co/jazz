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
  /** Write nothing; fail if jazz.app.json, jazz.permissions.json or the bindings aren't what generate would write. */
  check?: boolean;
  /** Overwrite a jazz.app.json or jazz.permissions.json edited by hand. */
  force?: boolean;
  /** `--lang <lang>` (repeatable): the bindings' languages; by default each the app's files name. */
  lang?: string[];
  /** `--effect-schema`: also write Effect Schemas for the rows, jazz.effect.generated.ts (kept current once there). */
  effectSchema?: boolean;
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

const UPGRADE_GUIDE_URL =
  "https://jazz.tools/docs/operating/backups-and-upgrades#upgrading-from-alpha70-to-beta0";

const UPGRADE_REMOVED =
  "`jazz-tools upgrade` was removed in 2.0.0-beta.0. To convert an alpha.65 (or older) app, " +
  "run `npx jazz-tools@2.0.0-alpha.70 upgrade`, then follow the upgrade guide: " +
  UPGRADE_GUIDE_URL;

const CREATE_REMOVED = "`jazz-tools create` was removed. Start a project with `npm create jazz`.";

const MIGRATE_ALPHA59_MOVED =
  "`jazz-tools migrate-alpha59` was removed. Move alpha.59 data with the exporter and importer from " +
  "https://github.com/garden-co/jazz/releases/tag/v2.0.0-alpha.65 (`jazz-alpha59-alpha60-export-<platform>` " +
  "and `jazz-alpha59-alpha60-import-<platform>`), then upgrade from alpha.65.";

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
    options: {
      schemaDir: resolve(process.cwd(), schemaDir),
      check: args.includes("--check"),
      force: args.includes("--force"),
      lang: langFlags(args),
      effectSchema: args.includes("--effect-schema"),
    },
  };
}

/** `jazz-tools generate [--check] [--force]`: write jazz.app.json, jazz.permissions.json and the bindings from the app's TS. */
export async function generate(options: BuildOptions): Promise<void> {
  const result = await generateApp({
    cwd: options.schemaDir,
    check: options.check,
    force: options.force,
    ...(options.lang?.length ? { lang: options.lang } : {}),
    ...(options.effectSchema ? { effectSchema: true } : {}),
  });
  for (const warning of result.warnings) {
    console.warn(`\x1b[33m${warning}\x1b[0m`);
  }
  const shown = result.bindings
    .map((file) => relative(process.cwd(), join(result.dir, file)) || file)
    .join(" and ");
  const files = [
    ...(result.wroteFiles ? ["jazz.app.json", "jazz.permissions.json"] : []),
    ...(result.wrote ? [shown] : []),
  ];
  console.log(
    options.check
      ? `jazz.app.json, jazz.permissions.json and ${shown} are up to date with the app's TS.`
      : files.length > 0
        ? `Wrote ${files.join(", ")} from the app's TS. Commit them.`
        : `jazz.app.json, jazz.permissions.json and ${shown} are up to date.`,
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
  /** `--lang <lang>` (repeatable), as for `generate`. */
  lang?: string[];
}

/** Every `--lang <lang>` (or `--lang=<lang>`) given; one without a value is refused. */
export function langFlags(args: readonly string[]): string[] {
  const langs: string[] = [];
  args.forEach((arg, i) => {
    if (arg.startsWith("--lang=")) {
      const lang = arg.slice("--lang=".length);
      if (!lang) throw new Error("Missing value for --lang (e.g. --lang ts).");
      langs.push(lang);
    } else if (arg === "--lang") {
      const lang = args[i + 1];
      if (!lang || lang.startsWith("--"))
        throw new Error("Missing value for --lang (e.g. --lang ts).");
      langs.push(lang);
    }
  });
  return langs;
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

/** Run a `store` subcommand, which needs the store stopped. */
function storeCommand(path: string, run: () => void): void {
  try {
    run();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (/already open|jazz\.lock/i.test(message)) {
      throw new Error(`${path} is in use: stop the backend or server using it first.`);
    }
    throw error;
  }
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

/** `jazz-tools migrations create --name <name>`: write a migration stub for the schema change. */
export async function createMigration(options: CreateMigrationOptions): Promise<string | null> {
  const result = await createAppMigration({
    cwd: options.schemaDir,
    name: options.name ?? "",
  });
  if (!result.file) {
    console.log(
      result.snapshot
        ? `Wrote the snapshot ${relative(process.cwd(), result.snapshot) || result.snapshot}: no migration is needed for this change.`
        : "No schema changes since the latest snapshot in migrations/snapshots: no migration needed.",
    );
    return null;
  }
  const file = relative(process.cwd(), result.file) || result.file;
  console.log(
    `Wrote the migration ${file}${result.snapshot ? ` and the snapshot ${relative(process.cwd(), result.snapshot) || result.snapshot}` : ""}.`,
  );
  if (result.todos.length > 0) {
    console.log("");
    console.log(
      `Resolve ${result.todos.length} TODO${result.todos.length === 1 ? "" : "s"} in ${file}, then run \`jazz-tools generate\`:`,
    );
    for (const todo of result.todos) console.log(`  ${todo}`);
  } else {
    console.log("Review the migration, then run `jazz-tools generate` and commit the results.");
  }
  console.log(DEPLOY_NOTE);
  return result.file;
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
    "  generate              Write jazz.app.json, jazz.permissions.json and jazz.generated.ts from schema.ts",
  );
  console.log(
    "                        (--check: write nothing, fail if anything is out of date; for CI;",
  );
  console.log(
    "                        --force: overwrite a jazz.app.json or jazz.permissions.json edited by hand;",
  );
  console.log(
    "                        --lang <ts|rust|…>: the bindings' languages, by default each the app's files name;",
  );
  console.log(
    "                        --effect-schema: also write Effect Schemas for the rows, jazz.effect.generated.ts,",
  );
  console.log("                        which generate then keeps current and --check checks)");
  console.log(
    "  migrations create     Write a migration stub for a schema change to migrations/ (--name <name>)",
  );
  console.log("  migrations graph      Visualize the full migration graph");
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
  console.log("  store detach <path>   Make a stopped copy of a store a store of its own");
  console.log("  store set-app-id <path> <id>");
  console.log("                        Make a stopped store serve another app id");
  console.log("\nOptions:");
  console.log(
    "  --schema-dir <path>   Project directory holding schema.ts (default: .; also ./src, ./src/lib)",
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
  // Cloud commands and `inspect` have their own help; `upgrade` only refuses.
  const ownHelp = cloudCommand || command === "inspect" || command === "upgrade";
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
    console.error(UPGRADE_REMOVED);
    process.exit(1);
  } else if (command === "print-ts") {
    // Hidden: the examples' TS from their JSON (src/printer/cli.ts).
    exitOnError(import("./printer/cli.js").then(({ runPrintTs }) => runPrintTs(args.slice(1))));
  } else if (command === "store") {
    const [subcommand, path, ...rest] = args.slice(1);
    const usage =
      "Usage: jazz-tools store detach <path>\n       jazz-tools store set-app-id <path> <app id>\n\n" +
      "detach: make a stopped copy of a store a store of its own, so it claims its own chunk directory or s3() prefix instead of the original's.\n" +
      "set-app-id: make a stopped store serve another app id from its next start.";
    if (subcommand === "detach" && path && rest.length === 0) {
      exitOnError(
        import("jazz-napi").then(({ detachStore }) => {
          storeCommand(path, () => detachStore(resolve(process.cwd(), path)));
          console.log(
            `${path} is now a store of its own; it claims its large values' place on its next start.`,
          );
        }),
      );
    } else if (subcommand === "set-app-id" && path && rest.length === 1) {
      const appId = rest[0]!;
      exitOnError(
        import("jazz-napi").then(({ setStoreAppId }) => {
          storeCommand(path, () => setStoreAppId(resolve(process.cwd(), path), appId));
          console.log(`${path} serves app ${appId} from its next start.`);
        }),
      );
    } else {
      console.error(usage);
      process.exit(1);
    }
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
      // `--lang=x` is `--lang x`, so the pairs below check it like any flag.
      const commandArgs = args
        .slice(2)
        .flatMap((arg) =>
          arg.startsWith("--lang=") ? ["--lang", arg.slice("--lang=".length)] : [arg],
        );
      task = (async () => {
        assertFlagPairs(commandArgs, ["--schema-dir", "--name", "--lang"], (arg) =>
          ["--fromHash", "--toHash", "--migrations-dir"].includes(arg)
            ? `${arg} was removed: migrations go from the latest snapshot in migrations/snapshots to schema.ts's tables.`
            : `Unknown migrations create argument: ${arg}.`,
        );
        await createMigration({
          schemaDir: resolve(process.cwd(), getFlagValue(commandArgs, "--schema-dir") ?? "."),
          name: getFlagValue(commandArgs, "--name"),
          lang: langFlags(commandArgs),
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

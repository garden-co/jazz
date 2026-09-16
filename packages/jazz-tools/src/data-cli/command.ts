import { readFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import { resolve } from "node:path";
import type { WasmSchema } from "../drivers/types.js";
import { loadCompiledSchema } from "../schema-loader.js";
import { fetchPermissionsHead, fetchStoredWasmSchema } from "../runtime/schema-fetch.js";
import {
  compileSql,
  describePlan,
  executeSql,
  isSchemaStatement,
  parseSql,
  type DataResult,
  type Statement,
} from "./sql.js";
import {
  DATA_FORMATS,
  formatData,
  formatError,
  isDataFormat,
  resolveFormat,
  type DataFormat,
} from "./output.js";
import { DataError, classifyError } from "./errors.js";
import { schemaHashOf } from "./schema.js";
import { capabilities } from "./capabilities.js";

export type DataCommandMode = "sql" | "tables" | "describe";
export type DataAuth =
  | { backendSecret: string; jwt?: never }
  | { jwt: string; backendSecret?: never };

/** Where the rows and schema come from. Exactly one source per invocation. */
export type SchemaSource =
  | { kind: "local"; dir: string }
  | { kind: "remote-head" }
  | { kind: "remote-hash"; hash: string };

/**
 * Flag-level view of an invocation. Pure: no I/O, no schema, no credentials are
 * resolved here, so it is cheap to test and cannot depend on a connection.
 */
export interface DataFlags {
  mode: DataCommandMode;
  appId?: string;
  serverUrl?: string;
  adminSecret?: string;
  jwt?: string;
  backendSecret?: string;
  sql?: string;
  file?: string;
  schemaDir?: string;
  schemaHashMode?: "current" | "explicit";
  schemaHash?: string;
  idSeed?: string;
  write: boolean;
  explain: boolean;
  capabilities: boolean;
  verbose: boolean;
  formatRequested?: DataFormat;
  maxCellWidth: number;
  timeout: number;
  isTty: boolean;
}

/** A resolved, validated invocation. */
export interface DataOptions extends DataFlags {
  statement?: Statement;
  auth?: DataAuth;
  schemaSource: SchemaSource;
  format: DataFormat;
}

const DATA_AUTH_REQUIRED =
  "Data access requires --backend-secret / JAZZ_BACKEND_SECRET or --jwt / JAZZ_JWT_TOKEN. An admin secret grants catalogue access only";

function catalogAuthRequired(): DataError {
  return new DataError(
    "AUTH_REQUIRED",
    "Reading the deployed schema requires --admin-secret / JAZZ_ADMIN_SECRET",
    { hint: "Use --schema-dir <path> to inspect a local schema.ts instead." },
  );
}

function connection(options: Pick<DataFlags, "appId" | "serverUrl">) {
  if (!options.appId)
    throw new DataError("USAGE", "Missing app ID. Pass --app-id or set JAZZ_APP_ID", {
      hint: "Schema-discovery statements (SHOW TABLES, DESCRIBE) still need the app identity to open a session.",
    });
  if (!options.serverUrl)
    throw new DataError("USAGE", "Missing server URL. Pass --server-url or set JAZZ_SERVER_URL");
  return { appId: options.appId, serverUrl: options.serverUrl };
}

export function parseDataArgs(
  args: string[],
  defaults: { appId?: string; serverUrl?: string },
  env: Record<string, string | undefined> = process.env,
  mode: DataCommandMode = "sql",
): DataFlags {
  const { values, positionals } = (() => {
    try {
      return parseArgs({
        args,
        allowPositionals: true,
        strict: true,
        options: {
          "app-id": { type: "string" },
          sql: { type: "string" },
          file: { type: "string" },
          "schema-dir": { type: "string" },
          "schema-hash": { type: "string" },
          "server-url": { type: "string" },
          "backend-secret": { type: "string" },
          jwt: { type: "string" },
          "admin-secret": { type: "string" },
          "id-seed": { type: "string" },
          "max-cell-width": { type: "string" },
          write: { type: "boolean", default: false },
          explain: { type: "boolean", default: false },
          capabilities: { type: "boolean", default: false },
          verbose: { type: "boolean", default: false },
          format: { type: "string" },
          timeout: { type: "string", default: "30000" },
          "env-file": { type: "string", multiple: true },
        },
      });
    } catch (error) {
      // Flags are user input: an unknown option or malformed value is a usage
      // failure (exit 2), never an internal error.
      throw new DataError("USAGE", error instanceof Error ? error.message : String(error));
    }
  })();
  const capabilities = values.capabilities === true;
  if (capabilities && mode !== "sql")
    throw new DataError("USAGE", "--capabilities is only available on `jazz-tools sql`");
  let sql = values.sql;

  if (mode === "sql") {
    if (positionals.length > 1)
      throw new DataError(
        "USAGE",
        "Quote SQL as one argument; pass the application with --app-id or JAZZ_APP_ID",
      );
    if (positionals[0] !== undefined && (sql !== undefined || values.file !== undefined))
      throw new DataError("USAGE", "Pass exactly one of positional SQL, --sql, or --file");
    sql = positionals[0] ?? sql;
    if (capabilities) {
      if (sql !== undefined || values.file !== undefined)
        throw new DataError("USAGE", "--capabilities takes no statement");
      if (values.write || values.explain)
        throw new DataError("USAGE", "--capabilities cannot be combined with --write or --explain");
      if (values["schema-dir"] !== undefined || values["schema-hash"] !== undefined)
        throw new DataError(
          "USAGE",
          "--capabilities needs no schema; drop --schema-dir/--schema-hash",
        );
    } else if ((sql === undefined) === (values.file === undefined)) {
      throw new DataError("USAGE", "Pass exactly one of positional SQL, --sql, or --file");
    }
  } else {
    if (sql !== undefined || values.file !== undefined)
      throw new DataError(
        "USAGE",
        "Schema discovery accepts no --sql or --file; use `jazz-tools sql`",
      );
    if (values.write)
      throw new DataError(
        "USAGE",
        "Schema discovery is read-only; --write only applies to `jazz-tools sql`",
      );
    if (mode === "tables") {
      if (positionals.length !== 0)
        throw new DataError("USAGE", "Usage: jazz-tools schema tables [options]");
      sql = "SHOW TABLES";
    } else {
      if (positionals.length !== 1 || !positionals[0])
        throw new DataError("USAGE", "Usage: jazz-tools schema describe <table> [options]");
      sql = `DESCRIBE "${positionals[0].replaceAll('"', '""')}"`;
    }
  }

  if (values["schema-dir"] !== undefined && values["schema-hash"] !== undefined)
    throw new DataError("USAGE", "--schema-dir and --schema-hash are mutually exclusive");
  if (
    values["schema-hash"] !== undefined &&
    values["schema-hash"] !== "current" &&
    !/^[a-f0-9]{64}$/i.test(values["schema-hash"])
  )
    throw new DataError(
      "USAGE",
      "--schema-hash must be current or a full 64-character hash from the schema catalogue",
    );
  if (values.jwt !== undefined && values["backend-secret"] !== undefined)
    throw new DataError("USAGE", "Choose --jwt or --backend-secret, not both");

  const serverUrl = values["server-url"] ?? defaults.serverUrl;
  if (serverUrl) {
    let url: URL;
    try {
      url = new URL(serverUrl);
    } catch {
      throw new DataError("USAGE", "Server URL must be an absolute HTTP(S) URL");
    }
    if (
      !["http:", "https:"].includes(url.protocol) ||
      url.username ||
      url.password ||
      url.search ||
      url.hash
    )
      throw new DataError(
        "USAGE",
        "Server URL must be HTTP(S), without credentials, query, or fragment",
      );
  }

  const formatRequested = isDataFormat(values.format ?? "")
    ? (values.format as DataFormat)
    : undefined;
  if (values.format !== undefined && formatRequested === undefined)
    throw new DataError("USAGE", `--format must be ${DATA_FORMATS.join(", ")}`);

  const maxCellWidth = Number(values["max-cell-width"] ?? "80");
  if (
    !/^\d+$/.test(values["max-cell-width"] ?? "80") ||
    !Number.isSafeInteger(maxCellWidth) ||
    maxCellWidth < 0
  )
    throw new DataError(
      "USAGE",
      "--max-cell-width must be a nonnegative integer (0 disables truncation)",
    );

  const timeout = Number(values.timeout);
  if (
    !/^\d+$/.test(values.timeout) ||
    !Number.isSafeInteger(timeout) ||
    timeout < 1 ||
    timeout > 2147483647
  )
    throw new DataError(
      "USAGE",
      "--timeout must be an integer between 1 and 2147483647 milliseconds",
    );

  // An explicit flag wins over the environment; the other credential's ambient
  // value is suppressed so `--jwt` never silently falls back to backend rights.
  // Blank values count as absent: shell blanks and `KEY=` in an env file are the
  // usual way to clear a credential, not a request for an empty one.
  const nonEmpty = (value: string | undefined): string | undefined =>
    value === undefined || value === "" ? undefined : value;
  const flagJwt = nonEmpty(values.jwt);
  const flagBackend = nonEmpty(values["backend-secret"]);
  const jwt = flagJwt ?? (flagBackend === undefined ? nonEmpty(env.JAZZ_JWT_TOKEN) : undefined);
  const backendSecret =
    flagBackend ?? (flagJwt === undefined ? nonEmpty(env.JAZZ_BACKEND_SECRET) : undefined);

  return {
    mode,
    appId: values["app-id"] ?? defaults.appId,
    serverUrl,
    adminSecret: values["admin-secret"] ?? env.JAZZ_ADMIN_SECRET,
    jwt,
    backendSecret,
    sql,
    file: values.file,
    schemaDir: values["schema-dir"] === undefined ? undefined : resolve(values["schema-dir"]),
    schemaHashMode:
      values["schema-hash"] === undefined
        ? undefined
        : values["schema-hash"] === "current"
          ? "current"
          : "explicit",
    schemaHash:
      values["schema-hash"] === undefined || values["schema-hash"] === "current"
        ? undefined
        : values["schema-hash"].toLowerCase(),
    idSeed: values["id-seed"],
    write: values.write,
    explain: values.explain === true,
    capabilities,
    verbose: values.verbose === true,
    formatRequested,
    maxCellWidth,
    timeout,
    isTty: process.stdout.isTTY === true,
  };
}

/**
 * The single place that decides schema source, credentials, and connection
 * requirements. Flag parsing and statement parsing never duplicate this policy.
 */
export function resolveDataOptions(
  flags: DataFlags,
  statement: Statement | undefined,
): DataOptions {
  const format = resolveFormat(flags.formatRequested, flags.isTty);
  if (flags.capabilities)
    return {
      ...flags,
      format,
      statement: undefined,
      auth: undefined,
      schemaSource: { kind: "local", dir: flags.schemaDir ?? process.cwd() },
    };
  if (!statement)
    throw new DataError("USAGE", "Pass exactly one of positional SQL, --sql, or --file");
  const metadata = isSchemaStatement(statement);
  const preview = flags.explain;
  if (flags.idSeed !== undefined && statement.kind !== "insert")
    throw new DataError("USAGE", "--id-seed only applies to INSERT");

  const schemaSource: SchemaSource =
    flags.schemaDir !== undefined
      ? { kind: "local", dir: flags.schemaDir }
      : flags.schemaHashMode === "explicit"
        ? { kind: "remote-hash", hash: flags.schemaHash! }
        : flags.schemaHashMode === "current"
          ? // An explicit `--schema-hash current` names the deployed head even
            // for `schema tables`/`describe`, whose default is local.
            { kind: "remote-head" }
          : metadata && flags.mode !== "sql"
            ? // `schema tables`/`schema describe` read a local schema by default.
              { kind: "local", dir: process.cwd() }
            : { kind: "remote-head" };

  const remote = schemaSource.kind !== "local";
  // A connection is only needed when the command will actually open a session.
  // Local metadata (`SHOW TABLES`/`DESCRIBE`) and any `--explain` are offline;
  // a remote schema still has to be fetched even for a preview.
  if (!(remote === false && (metadata || preview))) connection(flags);

  let auth: DataAuth | undefined;
  if (!metadata && !preview) {
    if (flags.jwt !== undefined && flags.backendSecret !== undefined)
      throw new DataError(
        "AUTH_CONFLICT",
        "Both data credentials are set; explicitly choose --jwt or --backend-secret",
      );
    if (flags.jwt !== undefined) auth = { jwt: flags.jwt };
    else if (flags.backendSecret !== undefined) auth = { backendSecret: flags.backendSecret };
    else throw new DataError("AUTH_REQUIRED", DATA_AUTH_REQUIRED);
  }
  // The catalogue credential is checked last so a row command reports the data
  // credential choice (its primary decision) before the schema-fetch secret.
  if (remote && !flags.adminSecret) throw catalogAuthRequired();
  return { ...flags, format, statement, auth, schemaSource };
}

interface ResolvedSchema {
  schema: WasmSchema;
  schemaSource: string;
  schemaHash: string;
}

async function resolveSchema(options: DataOptions): Promise<ResolvedSchema> {
  if (options.schemaSource.kind === "local") {
    const loaded = await loadCompiledSchema(options.schemaSource.dir);
    return {
      schema: loaded.wasmSchema,
      schemaSource: `local:${loaded.schemaFile}`,
      schemaHash: schemaHashOf(loaded.wasmSchema),
    };
  }
  const { appId, serverUrl } = connection(options);
  const adminSecret = options.adminSecret!;
  let schemaHash =
    options.schemaSource.kind === "remote-hash" ? options.schemaSource.hash : undefined;
  if (schemaHash === undefined) {
    const { head } = await fetchPermissionsHead(serverUrl, { appId, adminSecret });
    if (!head)
      throw new DataError(
        "SCHEMA_NOT_FOUND",
        "No deployed schema: this app has no permissions head",
        {
          hint: "Deploy with `jazz-tools deploy`, pass --schema-hash <full-hash>, or use --schema-dir for a local schema.",
        },
      );
    schemaHash = head.schemaHash.toLowerCase();
  }
  const { schema } = await fetchStoredWasmSchema(serverUrl, { appId, adminSecret, schemaHash });
  return { schema, schemaSource: `remote:${schemaHash}`, schemaHash };
}

export interface DataRunResult {
  result: DataResult;
  context: Record<string, unknown>;
}

async function readSqlText(options: DataFlags): Promise<string> {
  if (options.sql !== undefined) return options.sql;
  if (options.file === "-") return readStdin();
  return readFile(options.file!, "utf8");
}

export async function runDataQuery(flags: DataFlags): Promise<DataRunResult> {
  if (flags.capabilities)
    return {
      result: { kind: "object", value: capabilities() },
      context: { mode: "capabilities" },
    };
  const text = await readSqlText(flags);
  const statement = parseSql(text, { write: flags.write || flags.explain, idSeed: flags.idSeed });
  const options = resolveDataOptions(flags, statement);
  const metadata = isSchemaStatement(statement);
  const resolved = await resolveSchema(options);
  const compiled = compileSql(statement, resolved.schema, {
    schemaHash: resolved.schemaHash,
    schemaSource: resolved.schemaSource,
  });
  const authLabel =
    options.auth === undefined
      ? metadata
        ? "none (catalogue)"
        : "none (not executed)"
      : options.auth.jwt !== undefined
        ? "jwt"
        : "backend-secret";
  const context: Record<string, unknown> = {
    schemaSource: resolved.schemaSource,
    schemaHash: resolved.schemaHash,
    statement: statement.kind,
    table: "table" in statement ? statement.table : null,
    auth: authLabel,
    appId: options.appId ?? null,
    serverUrl: options.serverUrl ?? null,
    write:
      statement.kind === "insert" || statement.kind === "update" || statement.kind === "delete",
  };
  if (options.explain) {
    const plan = describePlan(statement, compiled, resolved.schema, {
      schemaSource: resolved.schemaSource,
      schemaHash: resolved.schemaHash,
      auth: authLabel,
      appId: options.appId ?? null,
      serverUrl: options.serverUrl ?? null,
    });
    return { result: { kind: "object", value: plan }, context: { ...context, mode: "explain" } };
  }
  if (compiled.kind === "schema") return { result: compiled.result, context };

  const auth = options.auth;
  if (!auth) throw new DataError("AUTH_REQUIRED", DATA_AUTH_REQUIRED);
  const { appId, serverUrl } = connection(options);
  const { createJazzSession } = await import("../backend/create-jazz-session.js");
  const session = await createJazzSession({
    app: resolved.schema,
    appId,
    serverUrl,
    driver: { type: "memory" },
  });
  try {
    if (auth.jwt !== undefined) await session.loginJWT(auth.jwt);
    else await session.becomeBackend({ backendSecret: auth.backendSecret });
    const client = session.getSnapshot().client;
    if (!client) throw new DataError("INTERNAL", "Data session did not become ready");
    const result = await executeSql(client.db, compiled);
    return { result, context };
  } finally {
    await session.close();
  }
}

async function readStdin(): Promise<string> {
  process.stdin.setEncoding("utf8");
  let sql = "";
  for await (const chunk of process.stdin) sql += chunk;
  return sql;
}

export interface DataCommandIO {
  stdout: (text: string) => void;
  stderr: (text: string) => void;
  isTty: boolean;
}

/**
 * Run one data command and return its documented exit code. Never throws: every
 * failure is reported through the coded error contract.
 */
export async function dataCommand(
  args: string[],
  defaults: { appId?: string; serverUrl?: string },
  mode: DataCommandMode = "sql",
  io: DataCommandIO = {
    stdout: (text) => void process.stdout.write(text),
    stderr: (text) => void process.stderr.write(text),
    isTty: process.stdout.isTTY === true,
  },
): Promise<number> {
  let format: DataFormat = io.isTty ? "table" : "json";
  let flags: DataFlags | undefined;
  try {
    flags = parseDataArgs(args, defaults, process.env, mode);
    format = resolveFormat(flags.formatRequested, io.isTty);
    // The session API has no per-operation cancellation. A CLI process deadline
    // bounds connection, schema loading, writes, and shutdown together. Never
    // continue an abandoned operation or automatically retry a timed-out write.
    const timer = setTimeout(() => {
      const failure = new DataError(
        "TIMEOUT",
        flags!.write
          ? `Timed out after ${flags!.timeout}ms. A submitted write may have committed; inspect the data before retrying`
          : `Timed out after ${flags!.timeout}ms`,
        flags!.write
          ? {
              hint: "Retry with `INSERT ... WITH ID SEED '<seed>'`: a retry then fails with ALREADY_EXISTS (exit 6) instead of duplicating the row.",
            }
          : undefined,
      );
      io.stderr(formatError(failure, format));
      process.exit(failure.exitCode);
    }, flags.timeout);
    try {
      const { result, context } = await runDataQuery(flags);
      if (flags.verbose) io.stderr(`${JSON.stringify(context)}\n`);
      io.stdout(formatData(result, format, { maxCellWidth: flags.maxCellWidth }));
      return 0;
    } finally {
      clearTimeout(timer);
    }
  } catch (error) {
    const failure = classifyError(error);
    io.stderr(formatError(failure, format));
    return failure.exitCode;
  }
}

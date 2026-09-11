import { readFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import { resolve } from "node:path";
import type { WasmSchema } from "../drivers/types.js";
import { loadCompiledSchema } from "../schema-loader.js";
import { fetchPermissionsHead, fetchStoredWasmSchema } from "../runtime/schema-fetch.js";
import { compileSql, executeSql, isSchemaStatement, parseSql, type DataResult } from "./sql.js";
import { formatData, type DataFormat } from "./output.js";

export const DATA_HELP = `Usage:
  jazz-tools sql '<statement>' --app-id <id> [options]
  jazz-tools schema tables --app-id <id> [options]
  jazz-tools schema describe <table> --app-id <id> [options]
  jazz-tools data query [appId] --sql <statement> [options]  (compatibility)

Run one Jazz SQL statement. Read-only unless --write is supplied.
  --app-id <id>          Application ID (or JAZZ_APP_ID)
  --sql <statement>      Alternative to the positional SQL statement
  --file <path|->        Read SQL from a UTF-8 file or stdin instead
  --schema-dir <path>    Load a local app's schema.ts
  --schema-hash <hash|current> Load a deployed schema; current uses the permissions head
  --server-url <url>     Server URL (or JAZZ_SERVER_URL)
  --backend-secret <s>   Unrestricted data access (or JAZZ_BACKEND_SECRET)
  --jwt <token>          Login as an existing user (or JAZZ_JWT_TOKEN)
  --admin-secret <s>     Catalogue access only (or JAZZ_ADMIN_SECRET)
  --write               Enable mutations; UPDATE/DELETE require WHERE id = 'uuid'
  --format <format>     table (default), json, or jsonl
  --timeout <ms>        Whole-command deadline (default: 30000)
  --env-file <path>     Load environment file (repeatable; existing env wins)

sql and schema discovery default to the current deployed schema. Local schema
discovery (--schema-dir) needs no app ID, server, or credentials. Compatibility
data query still defaults row queries to schema.ts in the current directory.
SHOW TABLES and DESCRIBE <table> also work as SQL. Schema discovery uses only
catalogue/local schema information and never opens a row-data session.

Remote schema loading requires an admin secret. SELECT and mutations also
require exactly one data credential. Explicit --jwt overrides an environment
backend secret; explicit --backend-secret overrides an environment JWT.
--schema-dir and --schema-hash are mutually exclusive. schema list is an alias
for schema tables. Do not insert an extra -- before command options.
SELECT supports projection, AND comparisons, IS [NOT] NULL, ORDER BY,
LIMIT, and OFFSET. INSERT accepts one VALUES tuple. No joins or expressions.
`;

type DataAuth = { backendSecret: string; jwt?: never } | { jwt: string; backendSecret?: never };
export type DataCommandMode = "data" | "sql" | "tables" | "describe";
export interface DataOptions {
  appId?: string;
  serverUrl?: string;
  auth?: DataAuth;
  sql?: string;
  file?: string;
  schemaDir?: string;
  schemaHash?: string;
  adminSecret?: string;
  write: boolean;
  format: DataFormat;
  timeout: number;
}

const DATA_AUTH_REQUIRED =
  "Data access requires --backend-secret / JAZZ_BACKEND_SECRET or --jwt / JAZZ_JWT_TOKEN. An admin secret grants catalogue access only";

function connection(options: Pick<DataOptions, "appId" | "serverUrl">) {
  if (!options.appId) throw new Error("Missing appId. Pass --app-id or set JAZZ_APP_ID");
  if (!options.serverUrl)
    throw new Error("Missing server URL. Pass --server-url or set JAZZ_SERVER_URL");
  return { appId: options.appId, serverUrl: options.serverUrl };
}

export function parseDataArgs(
  args: string[],
  defaults: { appId?: string; serverUrl?: string },
  env: NodeJS.ProcessEnv = process.env,
  mode: DataCommandMode = "data",
): DataOptions {
  const { values, positionals } = parseArgs({
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
      write: { type: "boolean", default: false },
      format: { type: "string", default: "table" },
      timeout: { type: "string", default: "30000" },
      "env-file": { type: "string", multiple: true },
    },
  });
  let appId = values["app-id"] ?? defaults.appId;
  let sql = values.sql;
  if (mode === "data") {
    if (positionals.length > 1)
      throw new Error(
        "Expected at most one appId; pass SQL with --sql or --file. Do not insert an extra -- before options",
      );
    if (positionals[0] !== undefined && values["app-id"] !== undefined)
      throw new Error("Pass appId positionally or with --app-id, not both");
    appId = positionals[0] ?? appId;
  } else if (mode === "sql") {
    if (positionals.length > 1)
      throw new Error("Quote SQL as one argument; pass the application with --app-id");
    if (positionals[0] !== undefined && (sql !== undefined || values.file !== undefined))
      throw new Error("Pass exactly one of positional SQL, --sql, or --file");
    sql = positionals[0] ?? sql;
  } else {
    if (sql !== undefined || values.file !== undefined || values.write)
      throw new Error("Schema discovery accepts no --sql, --file, or --write; use jazz-tools sql");
    if (mode === "tables") {
      if (positionals.length !== 0)
        throw new Error("Usage: jazz-tools schema tables --app-id <id> [options]");
      sql = "SHOW TABLES";
    } else {
      if (positionals.length !== 1 || !positionals[0])
        throw new Error("Usage: jazz-tools schema describe <table> --app-id <id> [options]");
      sql = `DESCRIBE "${positionals[0].replaceAll('"', '""')}"`;
    }
  }
  const serverUrl = values["server-url"] ?? defaults.serverUrl;
  if (serverUrl) {
    const url = new URL(serverUrl);
    if (
      !["http:", "https:"].includes(url.protocol) ||
      url.username ||
      url.password ||
      url.search ||
      url.hash
    )
      throw new Error("Server URL must be HTTP(S), without credentials, query, or fragment");
  }
  if ((sql === undefined) === (values.file === undefined))
    throw new Error("Pass exactly one of --sql or --file (or positional SQL with jazz-tools sql)");
  if (values["schema-dir"] !== undefined && values["schema-hash"] !== undefined)
    throw new Error("--schema-dir and --schema-hash are mutually exclusive");
  if (
    values["schema-hash"] !== undefined &&
    values["schema-hash"] !== "current" &&
    !/^[a-f0-9]{64}$/i.test(values["schema-hash"])
  )
    throw new Error(
      "--schema-hash must be current or a full 64-character hash from the schema catalogue",
    );
  if (values.jwt !== undefined && values["backend-secret"] !== undefined)
    throw new Error("Choose --jwt or --backend-secret, not both");
  const jwt =
    values.jwt ?? (values["backend-secret"] === undefined ? env.JAZZ_JWT_TOKEN : undefined);
  const backendSecret =
    values["backend-secret"] ?? (values.jwt === undefined ? env.JAZZ_BACKEND_SECRET : undefined);
  const statement = sql === undefined ? undefined : parseSql(sql, values.write);
  const metadata = statement !== undefined && isSchemaStatement(statement);
  if (jwt && backendSecret && !metadata)
    throw new Error("Both data credentials are set; explicitly choose --jwt or --backend-secret");
  if (!jwt && !backendSecret && statement && !metadata) throw new Error(DATA_AUTH_REQUIRED);
  const schemaHash =
    values["schema-hash"] ??
    (mode !== "data" && values["schema-dir"] === undefined ? "current" : undefined);
  const adminSecret = values["admin-secret"] ?? env.JAZZ_ADMIN_SECRET;
  if (schemaHash && !adminSecret)
    throw new Error(
      "--schema-hash / remote schema discovery requires --admin-secret / JAZZ_ADMIN_SECRET for catalogue access",
    );
  if (statement && !(metadata && values["schema-dir"] !== undefined))
    connection({ appId, serverUrl });
  if (!["table", "json", "jsonl"].includes(values.format))
    throw new Error("--format must be table, json, or jsonl");
  const timeout = Number(values.timeout);
  if (
    !/^\d+$/.test(values.timeout) ||
    !Number.isSafeInteger(timeout) ||
    timeout < 1 ||
    timeout > 2147483647
  )
    throw new Error("--timeout must be an integer between 1 and 2147483647 milliseconds");
  return {
    appId,
    serverUrl,
    auth: metadata ? undefined : jwt ? { jwt } : backendSecret ? { backendSecret } : undefined,
    sql,
    file: values.file,
    schemaDir: values["schema-dir"],
    schemaHash,
    adminSecret,
    write: values.write,
    format: values.format as DataFormat,
    timeout,
  };
}

async function loadCommandSchema(options: DataOptions, metadata: boolean): Promise<WasmSchema> {
  let schemaHash =
    options.schemaHash ?? (metadata && options.schemaDir === undefined ? "current" : undefined);
  if (!schemaHash)
    return (await loadCompiledSchema(resolve(options.schemaDir ?? process.cwd()))).wasmSchema;
  const { appId, serverUrl } = connection(options);
  const adminSecret = options.adminSecret;
  if (!adminSecret)
    throw new Error(
      "Catalogue access requires --admin-secret / JAZZ_ADMIN_SECRET; use --schema-dir to inspect a local schema",
    );
  if (schemaHash === "current") {
    const { head } = await fetchPermissionsHead(serverUrl, { appId, adminSecret });
    if (!head)
      throw new Error(
        "No current deployed schema: this app has no permissions head. Pass --schema-hash with an explicit stored schema hash or --schema-dir",
      );
    schemaHash = head.schemaHash;
  }
  return (await fetchStoredWasmSchema(serverUrl, { appId, adminSecret, schemaHash })).schema;
}

export async function runDataQuery(options: DataOptions): Promise<DataResult> {
  const sql =
    options.sql ??
    (options.file === "-" ? await readStdin() : await readFile(options.file!, "utf8"));
  // Parse and enforce read-only before loading schema code or opening a session.
  const statement = parseSql(sql, options.write);
  const metadata = isSchemaStatement(statement);
  if (!metadata && !options.auth) throw new Error(DATA_AUTH_REQUIRED);
  const schema = await loadCommandSchema(options, metadata);
  const compiled = compileSql(statement, schema);
  if (compiled.kind === "schema") return compiled.result;
  const auth = options.auth;
  if (!auth) throw new Error(DATA_AUTH_REQUIRED);
  const { appId, serverUrl } = connection(options);
  const { createJazzSession } = await import("../backend/create-jazz-session.js");
  const session = await createJazzSession({
    app: schema,
    appId,
    serverUrl,
    driver: { type: "memory" },
  });
  try {
    if (auth.jwt !== undefined) await session.loginJWT(auth.jwt);
    else await session.becomeBackend({ backendSecret: auth.backendSecret });
    const client = session.getSnapshot().client;
    if (!client) throw new Error("Data session did not become ready");
    return await executeSql(client.db, compiled);
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

export async function dataCommand(
  args: string[],
  defaults: { appId?: string; serverUrl?: string },
  mode: DataCommandMode = "data",
): Promise<void> {
  const options = parseDataArgs(args, defaults, process.env, mode);
  // The session API has no per-operation cancellation. A CLI process deadline
  // bounds connection, schema loading, writes, and shutdown together. Never
  // continue an abandoned operation or automatically retry a timed-out write.
  const timer = setTimeout(() => {
    console.error(
      options.write
        ? "Data command timed out. A submitted write may have committed; inspect the data before retrying."
        : "Command timed out.",
    );
    process.exit(1);
  }, options.timeout);
  try {
    const result = await runDataQuery(options);
    process.stdout.write(formatData(result, options.format));
  } finally {
    clearTimeout(timer);
  }
}

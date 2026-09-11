import { readFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import { resolve } from "node:path";
import type { WasmSchema } from "../drivers/types.js";
import { loadCompiledSchema } from "../schema-loader.js";
import { fetchStoredWasmSchema } from "../runtime/schema-fetch.js";
import { compileSql, executeSql, parseSql, type DataResult } from "./sql.js";
import { formatData, type DataFormat } from "./output.js";

export const DATA_HELP = `Usage: jazz-tools data query [appId] --sql <statement> [options]

Run one Jazz SQL statement. Read-only unless --write is supplied.
  --sql <statement>       SELECT, INSERT, UPDATE, or DELETE
  --file <path|->         Read SQL from a UTF-8 file or stdin instead of --sql
  --schema-dir <path>     App containing schema.ts (default: current directory)
  --schema-hash <hash>    Load a deployed schema from the catalogue instead
  --server-url <url>      Server URL (or JAZZ_SERVER_URL)
  --backend-secret <s>   Unrestricted data access (or JAZZ_BACKEND_SECRET)
  --jwt <token>          Login as an existing user (or JAZZ_JWT_TOKEN)
  --admin-secret <s>     Catalogue access only (or JAZZ_ADMIN_SECRET)
  --write               Enable mutations; UPDATE/DELETE require WHERE id = 'uuid'
  --format <format>      table (default), json, or jsonl
  --timeout <ms>         Whole-command deadline (default: 30000)
  --env-file <path>      Load environment file (repeatable; existing env wins)

Use exactly one data credential. An explicit --jwt overrides an environment
backend secret; an explicit --backend-secret overrides an environment JWT.
--schema-dir and --schema-hash are mutually exclusive. Catalogue loading
requires an admin secret in addition to the selected data credential.
SELECT supports projection, AND comparisons, IS [NOT] NULL, ORDER BY,
LIMIT, and OFFSET. INSERT accepts one VALUES tuple. No joins or expressions.
`;

type DataAuth = { backendSecret: string; jwt?: never } | { jwt: string; backendSecret?: never };
export interface DataOptions {
  appId: string;
  serverUrl: string;
  auth: DataAuth;
  sql?: string;
  file?: string;
  schemaDir?: string;
  schemaHash?: string;
  adminSecret?: string;
  write: boolean;
  format: DataFormat;
  timeout: number;
}

export function parseDataArgs(
  args: string[],
  defaults: { appId?: string; serverUrl?: string },
  env: NodeJS.ProcessEnv = process.env,
): DataOptions {
  const { values, positionals } = parseArgs({
    args,
    allowPositionals: true,
    strict: true,
    options: {
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
  if (positionals.length > 1)
    throw new Error("Expected at most one appId; pass SQL with --sql or --file");
  const appId = positionals[0] ?? defaults.appId;
  const serverUrl = values["server-url"] ?? defaults.serverUrl;
  if (!appId) throw new Error("Missing appId. Pass appId or set JAZZ_APP_ID");
  if (!serverUrl) throw new Error("Missing server URL. Pass --server-url or set JAZZ_SERVER_URL");
  const url = new URL(serverUrl);
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  )
    throw new Error("Server URL must be HTTP(S), without credentials, query, or fragment");
  if ((values.sql === undefined) === (values.file === undefined))
    throw new Error("Pass exactly one of --sql or --file");
  if (values["schema-dir"] !== undefined && values["schema-hash"] !== undefined)
    throw new Error("--schema-dir and --schema-hash are mutually exclusive");
  if (values["schema-hash"] !== undefined && !/^[a-f0-9]{64}$/i.test(values["schema-hash"]))
    throw new Error("--schema-hash must be a full 64-character hash from the schema catalogue");
  if (values.jwt !== undefined && values["backend-secret"] !== undefined)
    throw new Error("Choose --jwt or --backend-secret, not both");
  const jwt =
    values.jwt ?? (values["backend-secret"] === undefined ? env.JAZZ_JWT_TOKEN : undefined);
  const backendSecret =
    values["backend-secret"] ?? (values.jwt === undefined ? env.JAZZ_BACKEND_SECRET : undefined);
  if (jwt && backendSecret)
    throw new Error("Both data credentials are set; explicitly choose --jwt or --backend-secret");
  if (!jwt && !backendSecret)
    throw new Error(
      "Data access requires --backend-secret / JAZZ_BACKEND_SECRET or --jwt / JAZZ_JWT_TOKEN. An admin secret grants catalogue access only",
    );
  const adminSecret = values["admin-secret"] ?? env.JAZZ_ADMIN_SECRET;
  if (values["schema-hash"] && !adminSecret)
    throw new Error(
      "--schema-hash requires --admin-secret / JAZZ_ADMIN_SECRET for catalogue access",
    );
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
    auth: jwt ? { jwt } : { backendSecret: backendSecret! },
    sql: values.sql,
    file: values.file,
    schemaDir: values["schema-dir"],
    schemaHash: values["schema-hash"],
    adminSecret,
    write: values.write,
    format: values.format as DataFormat,
    timeout,
  };
}

export async function runDataQuery(options: DataOptions): Promise<DataResult> {
  const sql =
    options.sql ??
    (options.file === "-" ? await readStdin() : await readFile(options.file!, "utf8"));
  // Parse and enforce read-only before loading schema code or opening a session.
  const statement = parseSql(sql, options.write);
  let schema: WasmSchema;
  if (options.schemaHash) {
    schema = (
      await fetchStoredWasmSchema(options.serverUrl, {
        appId: options.appId,
        adminSecret: options.adminSecret!,
        schemaHash: options.schemaHash,
      })
    ).schema;
  } else {
    schema = (await loadCompiledSchema(resolve(options.schemaDir ?? process.cwd()))).wasmSchema;
  }
  const compiled = compileSql(statement, schema);
  const { createJazzSession } = await import("../backend/create-jazz-session.js");
  const session = await createJazzSession({
    app: schema,
    appId: options.appId,
    serverUrl: options.serverUrl,
    driver: { type: "memory" },
  });
  try {
    if (options.auth.jwt !== undefined) await session.loginJWT(options.auth.jwt);
    else await session.becomeBackend({ backendSecret: options.auth.backendSecret });
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
): Promise<void> {
  const options = parseDataArgs(args, defaults);
  // The session API has no per-operation cancellation. A CLI process deadline
  // bounds connection, schema loading, writes, and shutdown together. Never
  // continue an abandoned operation or automatically retry a timed-out write.
  const timer = setTimeout(() => {
    console.error(
      "Data command timed out. A submitted write may have committed; inspect the data before retrying.",
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

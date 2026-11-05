// The Inspector and agent debug access, served by the app's own backend.
//
// Every way in ends here: the Inspector UI the backend serves, the
// `jazz-tools inspect` CLI and the `jazz-tools inspect mcp` server all call the one
// JSON API at `<basePath>/inspect/api`. A request is let in by exactly one of:
//
// - the app's `inspect` hook, which decides from the request (usually the
//   person's own sign-in, or an infrastructure gate in front of the app) who
//   is inspecting and whether they may write;
// - an agent token a person minted after passing the hook, which is
//   time-limited, read-only unless the person allowed writes, can be limited
//   to some tables, and has redacted columns hidden;
// - with no hook at all, the local development default: a request from this
//   machine to a backend running with NODE_ENV=development.
//
// Whoever is let in reads and writes with the backend's own authority (every
// row, permissions not checked) unless they ask to act as a user. That is
// the point of the Inspector, and it is why the hook must be narrow.

import { createHmac, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import type { ColumnType, WasmSchema } from "../drivers/types.js";
import type { Db, QueryBuilder, TableProxy } from "../runtime/db.js";
import { historySourceTable, historyTableName } from "../runtime/history-relation.js";
import {
  INSPECT_API_PATH,
  INSPECT_ERRORS,
  INSPECT_PATH,
  INSPECT_PROTOCOL_VERSION,
  INSPECT_TOKEN_PREFIX,
  InspectError,
  MAX_ANSWER_BYTES,
  buildQueryJson,
  fromJsonForColumn,
  inspectorLink,
  rowLimit,
  toJsonValue,
  type InspectAs,
  type InspectErrorCode,
  type InspectQuery,
  type InspectRequest,
  type InspectSessionInfo,
  type InspectVia,
  type InspectWhoami,
  type InspectWriteOp,
} from "../inspect/protocol.js";

/** Who is inspecting, as the app's `inspect` hook decides. */
export interface InspectGrant {
  /** Who this is, for the audit log (an email, a staff id). */
  by: string;
  /** Whether they may write. Defaults to false. */
  write?: boolean;
}

/**
 * Decide whether `request` may inspect this app's data, and as whom. Return
 * null to refuse. **A grant reads and, with `write`, changes every row of
 * every table, with your permissions not checked.** Grant it only to people
 * who may see all of your users' data, and prefer a gate in front of the app
 * that is separate from your users' sign-in (Vercel deployment protection,
 * Cloudflare Access, a VPN or SSO proxy) over a flag on a user account.
 */
export type InspectHook = (
  request: Request,
) => InspectGrant | null | undefined | Promise<InspectGrant | null | undefined>;

/** One audited Inspector call. */
export interface InspectEvent {
  /** Unix milliseconds. */
  at: number;
  by: string;
  via: InspectVia;
  op: InspectRequest["op"];
  table?: string;
  /** The user whose permissions the call ran under, if not the backend's. */
  as?: string;
  /** Rows the answer carried, or rows the write touched. */
  rows?: number;
  write: boolean;
  /** The id of the agent token, when one was used. */
  tokenId?: string;
  /** Set when the call was refused or failed. */
  error?: InspectErrorCode;
}

export interface InspectorOptions {
  /**
   * Columns agent tokens never see, by table (e.g. `{ users: ["email"] }`).
   * Their values read as `"[redacted]"`, and a token can't filter, sort or
   * write by them. People let in by the hook see them.
   */
  redact?: Record<string, string[]>;
  /**
   * Called for every Inspector call, allowed or refused. Defaults to one
   * `console.info` line per call. Send it to your own audit log in production.
   */
  audit?: (event: InspectEvent) => void;
  /**
   * The secret agent tokens are signed with, shared by every instance of this
   * backend. Needed where the store has no secret of its own to derive it from
   * (`vercel()`, and `remote()` with a `backendCredential`). Changing it
   * revokes every token.
   */
  tokenSecret?: string;
  /** Tokens issued before this moment no longer work: revoke all tokens without changing the secret. */
  tokensIssuedAfter?: Date | number;
  /** The longest a token may live. Defaults to one day. */
  maxTokenTtlSeconds?: number;
}

/** What the inspector needs from the createJazz it serves. */
export interface InspectorDeps {
  appId: string;
  basePath: string;
  /** The URLs browsers reach this backend's `basePath` at, when configured. */
  publicUrls: string[];
  schema(): WasmSchema;
  permissions: unknown;
  /** The backend's own database (backend authority). */
  db(): Db;
  /** The database as `userId`, judged by the app's permissions. */
  dbAs(user: { userId: string; claims?: Record<string, unknown> }): Promise<Db>;
  sessions(): InspectSessionInfo[];
  hook: InspectHook | undefined;
  options: InspectorOptions;
  /** The secret agent tokens are signed with, or null where the store has none. */
  tokenSecret(): Promise<string | null>;
  /** The peer address a request came from, where the host knows it. */
  remoteAddress(request: Request): string | undefined;
  /** The Inspector UI's file at `path` (`/`, or below it), or null when there is none. */
  serveUi(request: Request, path: string): Promise<Response | null>;
  env?: NodeJS.ProcessEnv;
  now?: () => number;
}

interface Admitted {
  by: string;
  via: InspectVia;
  write: boolean;
  tables?: string[];
  expiresAt?: number;
  tokenId?: string;
}

interface TokenClaims {
  v: 1;
  app: string;
  by: string;
  write: boolean;
  tables?: string[];
  label?: string;
  iat: number;
  exp: number;
  jti: string;
}

const DEFAULT_TOKEN_TTL_SECONDS = 3600;
const DEFAULT_MAX_TOKEN_TTL_SECONDS = 24 * 3600;
const MAX_REQUEST_BYTES = 1024 * 1024;
const REDACTED = "[redacted]";
const PROVENANCE = ["$createdBy", "$createdAt", "$updatedBy", "$updatedAt"];

export function createInspector(deps: InspectorDeps) {
  const now = deps.now ?? Date.now;
  const env = deps.env ?? process.env;
  const redact = new Map(
    Object.entries(deps.options.redact ?? {}).map(([table, columns]) => [table, new Set(columns)]),
  );
  const audit =
    deps.options.audit ??
    ((event: InspectEvent) => {
      const parts = [
        `jazz inspect: ${event.by} (${event.via})`,
        event.op,
        event.table,
        event.as && `as ${event.as}`,
        event.rows !== undefined && `${event.rows} row(s)`,
        event.error && `refused: ${event.error}`,
      ].filter(Boolean);
      console.info(parts.join(" "));
    });
  const issuedAfter =
    deps.options.tokensIssuedAfter === undefined
      ? undefined
      : Math.floor(new Date(deps.options.tokensIssuedAfter).getTime() / 1000);
  const maxTtl = deps.options.maxTokenTtlSeconds ?? DEFAULT_MAX_TOKEN_TTL_SECONDS;
  if (!Number.isSafeInteger(maxTtl) || maxTtl < 60) {
    throw new Error("createJazz: inspector.maxTokenTtlSeconds must be at least 60");
  }

  let signingKey: Promise<Buffer | null> | undefined;
  function tokenKey(): Promise<Buffer | null> {
    signingKey ??= deps
      .tokenSecret()
      .then((secret) =>
        secret === null
          ? null
          : createHmac("sha256", secret).update(`jazz-inspect-token-v1\0${deps.appId}`).digest(),
      );
    return signingKey;
  }

  // ---------------------------------------------------------------- admission

  /**
   * The local development default: no hook, NODE_ENV=development, and a
   * request from this machine. "This machine" is the socket's peer address,
   * which only a host that hands Jazz its Node request knows (`attach()`,
   * `nodeHandler`). The Host header can't stand in for it: anyone who reaches
   * a dev server listening on every interface (as `next dev` does) can send
   * `Host: localhost`. So on other hosts the default is off, and says so.
   */
  function devAllowed(request: Request): boolean | "no_peer" {
    if (env.NODE_ENV !== "development") return false;
    // A proxy in front of the backend makes every request look local.
    for (const header of ["x-forwarded-for", "x-forwarded-host", "x-real-ip", "forwarded"]) {
      if (request.headers.has(header)) return false;
    }
    const remote = deps.remoteAddress(request);
    if (remote === undefined) return "no_peer";
    if (!isLoopbackAddress(remote)) return false;
    const host = request.headers.get("host") ?? new URL(request.url).host;
    let hostname: string;
    try {
      hostname = new URL(`http://${host}`).hostname;
    } catch {
      return false;
    }
    return isLoopbackHost(hostname);
  }

  async function admit(request: Request): Promise<Admitted> {
    const authorization = request.headers.get("authorization");
    if (authorization?.startsWith(`Bearer ${INSPECT_TOKEN_PREFIX}`)) {
      return verifyToken(authorization.slice("Bearer ".length));
    }
    if (deps.hook) {
      const grant = await deps.hook(request);
      if (!grant)
        throw new InspectError("unauthorized", "the app's inspect hook refused this request");
      if (typeof grant.by !== "string" || !grant.by.trim()) {
        throw new InspectError("failed", "the app's inspect hook returned a grant without `by`");
      }
      return { by: grant.by, via: "hook", write: grant.write === true };
    }
    const dev = devAllowed(request);
    if (dev === true) return { by: "local development", via: "dev", write: true };
    if (dev === "no_peer") {
      throw new InspectError(
        "unauthorized",
        "Local inspection without an `inspect` hook is not supported on this host yet: it doesn't tell Jazz the request's peer address, and the Host header proves nothing (anyone who reaches a dev server listening on every interface can send `Host: localhost`). Serve Jazz through `attach()` or `nodeHandler`, or pass an `inspect` hook that names who may inspect from your own sign-in",
      );
    }
    throw new InspectError(
      "unauthorized",
      "Inspecting this app needs an `inspect` hook in createJazz (or, in local development, NODE_ENV=development and a request from this machine)",
    );
  }

  async function mintToken(
    admitted: Admitted,
    request: Extract<InspectRequest, { op: "mint_token" }>,
  ): Promise<{ token: string; expiresAt: number; tokenId: string }> {
    // A token is a person's delegation: another token can't make one.
    if (admitted.via === "token") {
      throw new InspectError(
        "forbidden",
        "an agent token can't mint tokens; a person must approve",
      );
    }
    if (request.write && !admitted.write) {
      throw new InspectError("forbidden", "you may not write, so you can't grant writes");
    }
    const ttl = request.ttlSeconds ?? DEFAULT_TOKEN_TTL_SECONDS;
    if (!Number.isSafeInteger(ttl) || ttl < 60 || ttl > maxTtl) {
      throw new InspectError("bad_request", `ttlSeconds must be between 60 and ${maxTtl}`);
    }
    const tables = request.tables;
    if (tables !== undefined) {
      if (!Array.isArray(tables) || tables.length === 0) {
        throw new InspectError("bad_request", "tables must list at least one table");
      }
      const schema = deps.schema();
      for (const table of tables) {
        if (typeof table !== "string" || !Object.hasOwn(schema, table)) {
          throw new InspectError("bad_request", `no table ${JSON.stringify(table)}`);
        }
      }
    }
    const key = await tokenKey();
    if (!key) {
      throw new InspectError(
        "not_supported",
        "Agent tokens on this store need a signing secret shared by every backend instance: set `inspector: { tokenSecret }` in createJazz",
      );
    }
    const iat = Math.floor(now() / 1000);
    const claims: TokenClaims = {
      v: 1,
      app: deps.appId,
      by: admitted.by,
      write: request.write === true,
      ...(tables ? { tables: [...new Set(tables)] } : {}),
      ...(request.label ? { label: String(request.label).slice(0, 200) } : {}),
      iat,
      exp: iat + ttl,
      jti: randomUUID(),
    };
    const payload = Buffer.from(JSON.stringify(claims)).toString("base64url");
    const mac = createHmac("sha256", key).update(payload).digest("base64url");
    return {
      token: `${INSPECT_TOKEN_PREFIX}${payload}.${mac}`,
      expiresAt: claims.exp,
      tokenId: claims.jti,
    };
  }

  async function verifyToken(token: string): Promise<Admitted> {
    const refused = (why: string) =>
      new InspectError("unauthorized", `agent token refused: ${why}`);
    const key = await tokenKey();
    if (!key) throw refused("this backend has no token signing secret");
    const body = token.slice(INSPECT_TOKEN_PREFIX.length);
    const [payload, mac, extra] = body.split(".");
    if (!payload || !mac || extra !== undefined || body.length > 4096) throw refused("malformed");
    const expected = Buffer.from(createHmac("sha256", key).update(payload).digest("base64url"));
    const given = Buffer.from(mac);
    if (expected.length !== given.length || !timingSafeEqual(expected, given)) {
      throw refused("not signed by this backend");
    }
    let claims: TokenClaims;
    try {
      claims = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")) as TokenClaims;
    } catch {
      throw refused("malformed");
    }
    const at = Math.floor(now() / 1000);
    if (claims.v !== 1 || claims.app !== deps.appId) throw refused("for another app");
    if (typeof claims.exp !== "number" || claims.exp <= at) throw refused("expired");
    if (issuedAfter !== undefined && claims.iat < issuedAfter) throw refused("revoked");
    return {
      by: claims.by,
      via: "token",
      write: claims.write === true,
      ...(claims.tables ? { tables: claims.tables } : {}),
      expiresAt: claims.exp,
      tokenId: claims.jti,
    };
  }

  // ---------------------------------------------------------------- reads and writes

  function inspectorUrl(request: Request): string {
    if (deps.publicUrls[0]) return `${deps.publicUrls[0]}${INSPECT_PATH}/`;
    const url = new URL(request.url);
    return `${url.origin}${deps.basePath}${INSPECT_PATH}/`;
  }

  function tableOf(name: string): {
    columns: Map<string, ColumnType>;
    source: string;
  } {
    const schema = deps.schema();
    const source = historySourceTable(name) ?? name;
    const table = Object.hasOwn(schema, source) ? schema[source] : undefined;
    if (!table) throw new InspectError("not_found", `no table ${JSON.stringify(name)}`);
    return {
      source,
      columns: new Map(table.columns.map((column) => [column.name, column.column_type])),
    };
  }

  function checkTable(admitted: Admitted, name: string): void {
    const { source } = tableOf(name);
    if (admitted.tables && !admitted.tables.includes(source)) {
      throw new InspectError("forbidden", `this token is limited to ${admitted.tables.join(", ")}`);
    }
  }

  function redactedFor(admitted: Admitted, table: string): Set<string> | undefined {
    if (admitted.via !== "token") return undefined;
    return redact.get(historySourceTable(table) ?? table);
  }

  function queryBuilder(table: string, json: string): QueryBuilder<Record<string, unknown>> {
    return {
      _table: table,
      _schema: deps.schema(),
      _build: () => json,
      _rowType: undefined as unknown as Record<string, unknown>,
    };
  }

  function tableProxy(table: string): TableProxy<Record<string, unknown>, Record<string, unknown>> {
    return {
      _table: table,
      _schema: deps.schema(),
      _rowType: undefined,
      _initType: undefined,
    } as unknown as TableProxy<Record<string, unknown>, Record<string, unknown>>;
  }

  async function dbFor(as: InspectAs | undefined): Promise<{ db: Db; as?: string }> {
    if (as === undefined) return { db: deps.db() };
    const user = typeof as === "string" ? { userId: as } : as;
    if (!user || typeof user.userId !== "string" || !user.userId) {
      throw new InspectError("bad_request", "as must name a user id");
    }
    return { db: await deps.dbAs(user), as: user.userId };
  }

  async function readRows(
    admitted: Admitted,
    query: InspectQuery,
    as: InspectAs | undefined,
  ): Promise<{
    rows: Record<string, unknown>[];
    truncated: boolean;
    as?: string;
  }> {
    checkTable(admitted, query?.table);
    const hidden = redactedFor(admitted, query.table);
    if (hidden) {
      const named = [
        ...Object.keys(query.where ?? {}),
        ...(query.orderBy ?? []).map(([column]) => column),
      ].filter((column) => hidden.has(column));
      if (named.length) {
        throw new InspectError(
          "forbidden",
          `${named.join(", ")} ${named.length === 1 ? "is" : "are"} redacted for agents: a token can't filter or sort by ${named.length === 1 ? "it" : "them"}`,
        );
      }
    }
    const limit = rowLimit(query.limit);
    const { columns } = tableOf(query.table);
    // One more than the limit, to tell whether more matched.
    const json = buildQueryJson(query, limit + 1, (column, value) =>
      fromJsonForColumn(columns.get(column), value),
    );
    const { db, as: asUser } = await dbFor(as);
    const found = await db.all(queryBuilder(query.table, json), {
      tier: "remote",
    });
    let truncated = found.length > limit;
    const rows: Record<string, unknown>[] = [];
    let bytes = 0;
    for (const row of found.slice(0, limit)) {
      const shown = toJsonValue(row) as Record<string, unknown>;
      if (hidden) for (const column of hidden) if (column in shown) shown[column] = REDACTED;
      bytes += JSON.stringify(shown).length;
      if (bytes > MAX_ANSWER_BYTES) {
        truncated = true;
        break;
      }
      rows.push(shown);
    }
    return { rows, truncated, ...(asUser ? { as: asUser } : {}) };
  }

  async function write(
    admitted: Admitted,
    ops: InspectWriteOp[],
    as: InspectAs | undefined,
  ): Promise<{ ids: string[]; as?: string }> {
    if (!admitted.write) {
      throw new InspectError(
        "forbidden",
        admitted.via === "token"
          ? "this token is read-only; ask for one that allows writes"
          : "you may read but not write (the inspect hook's grant has no `write: true`)",
      );
    }
    if (!Array.isArray(ops) || ops.length === 0 || ops.length > 1000) {
      throw new InspectError("bad_request", "ops must list between 1 and 1000 writes");
    }
    const prepared = ops.map((op) => {
      if (!op || !["insert", "update", "delete"].includes(op.kind)) {
        throw new InspectError("bad_request", "each op is an insert, update or delete");
      }
      if (historySourceTable(op.table) !== undefined) {
        throw new InspectError("bad_request", `${op.table} is a history table and is read-only`);
      }
      checkTable(admitted, op.table);
      const { columns } = tableOf(op.table);
      if (op.kind === "delete") {
        if (typeof op.id !== "string") throw new InspectError("bad_request", "delete needs an id");
        return { op, values: undefined };
      }
      if (!op.values || typeof op.values !== "object" || Array.isArray(op.values)) {
        throw new InspectError("bad_request", `${op.kind} needs values`);
      }
      if (op.kind === "update" && typeof op.id !== "string") {
        throw new InspectError("bad_request", "update needs an id");
      }
      const hidden = redactedFor(admitted, op.table);
      const values: Record<string, unknown> = {};
      for (const [column, value] of Object.entries(op.values)) {
        if (!columns.has(column)) {
          throw new InspectError("bad_request", `${op.table} has no column ${column}`);
        }
        if (hidden?.has(column)) {
          throw new InspectError("forbidden", `${op.table}.${column} is redacted for agents`);
        }
        values[column] = fromJsonForColumn(columns.get(column), value);
      }
      return { op, values };
    });
    const { db, as: asUser } = await dbFor(as);
    const ids: string[] = [];
    let result;
    try {
      result = await db.transaction((tx) => {
        for (const { op, values } of prepared) {
          const table = tableProxy(op.table);
          if (op.kind === "insert") {
            const inserted = tx.insert(table, values!, op.id ? { id: op.id } : undefined);
            ids.push(String((inserted as { id?: unknown }).id ?? op.id ?? ""));
          } else if (op.kind === "update") {
            tx.update(table, op.id, values!);
            ids.push(op.id);
          } else {
            tx.delete(table, op.id);
            ids.push(op.id);
          }
        }
      });
      // The answer says the write stands: the core accepted it.
      await result.wait({ tier: "global" });
    } catch (error) {
      if (error instanceof InspectError) throw error;
      throw new InspectError("forbidden", `the write was refused: ${errorMessage(error)}`);
    }
    return { ids, ...(asUser ? { as: asUser } : {}) };
  }

  function schemaAnswer(admitted: Admitted) {
    const schema = deps.schema();
    const tables: Record<string, unknown> = {};
    for (const [name, table] of Object.entries(schema)) {
      if (admitted.tables && !admitted.tables.includes(name)) continue;
      const hidden = redactedFor(admitted, name);
      tables[name] = {
        ...table,
        ...(hidden ? { redacted: [...hidden] } : {}),
      };
    }
    return {
      tables,
      // What agents may query besides each table: its row history.
      history: Object.keys(tables).map(historyTableName),
      provenanceColumns: PROVENANCE,
      ...(admitted.via === "token" && admitted.tables ? {} : { permissions: deps.permissions }),
    };
  }

  async function answer(
    request: Request,
    admitted: Admitted,
    body: InspectRequest,
  ): Promise<{ answer: unknown; event: Partial<InspectEvent> }> {
    const url = inspectorUrl(request);
    switch (body.op) {
      case "whoami": {
        const whoami: InspectWhoami = {
          appId: deps.appId,
          by: admitted.by,
          via: admitted.via,
          write: admitted.write,
          ...(admitted.tables ? { tables: admitted.tables } : {}),
          ...(admitted.expiresAt ? { expiresAt: admitted.expiresAt } : {}),
          inspectorUrl: url,
          protocol: INSPECT_PROTOCOL_VERSION,
        };
        return { answer: whoami, event: {} };
      }
      case "schema":
        return { answer: schemaAnswer(admitted), event: {} };
      case "query": {
        const read = await readRows(admitted, body.query, body.as);
        return {
          answer: {
            rows: read.rows,
            truncated: read.truncated,
            inspectorUrl: inspectorLink(url, {
              query: body.query,
              as: body.as,
            }),
          },
          event: {
            table: body.query.table,
            rows: read.rows.length,
            as: read.as,
          },
        };
      }
      case "get": {
        if (typeof body.id !== "string") throw new InspectError("bad_request", "get needs an id");
        const read = await readRows(
          admitted,
          {
            table: body.table,
            where: { id: body.id },
            select: ["*", ...PROVENANCE],
            limit: 1,
          },
          body.as,
        );
        return {
          answer: {
            row: read.rows[0] ?? null,
            inspectorUrl: inspectorLink(url, {
              table: body.table,
              row: body.id,
              as: body.as,
            }),
          },
          event: { table: body.table, rows: read.rows.length, as: read.as },
        };
      }
      case "history": {
        if (typeof body.id !== "string") {
          throw new InspectError("bad_request", "history needs an id");
        }
        const query: InspectQuery = {
          table: historyTableName(body.table),
          where: { id: body.id },
          select: ["*", "$updatedBy", "$updatedAt"],
          orderBy: [["$acceptedAt", "asc"]],
          limit: body.limit ?? 100,
        };
        const read = await readRows(admitted, query, undefined);
        return {
          answer: {
            versions: read.rows,
            truncated: read.truncated,
            inspectorUrl: inspectorLink(url, { query, row: body.id }),
          },
          event: { table: body.table, rows: read.rows.length },
        };
      }
      case "write": {
        const done = await write(admitted, body.ops, body.as);
        return {
          answer: { ids: done.ids },
          event: {
            table: [...new Set(body.ops.map((op) => op.table))].join(","),
            rows: done.ids.length,
            as: done.as,
            write: true,
          },
        };
      }
      case "sessions":
        return {
          answer: {
            // Each backend instance knows only the sessions it serves.
            scope: "this backend instance",
            sessions: deps.sessions(),
          },
          event: {},
        };
      case "explain":
        throw new InspectError(
          "not_supported",
          "explain (which policy allowed or denied each row) is not supported yet; compare `query` with and without `as` meanwhile",
        );
      case "link": {
        if (body.query) checkTable(admitted, body.query.table);
        else if (body.table) checkTable(admitted, body.table);
        return {
          answer: {
            inspectorUrl: inspectorLink(url, {
              query: body.query,
              table: body.table,
              row: body.row,
              as: body.as,
            }),
          },
          event: {},
        };
      }
      case "mint_token": {
        const minted = await mintToken(admitted, body);
        return {
          answer: { token: minted.token, expiresAt: minted.expiresAt },
          event: { tokenId: minted.tokenId, write: body.write === true },
        };
      }
      default:
        throw new InspectError(
          "bad_request",
          `unknown op ${JSON.stringify((body as { op?: unknown }).op)}`,
        );
    }
  }

  function errorResponse(error: InspectError): Response {
    return Response.json(
      { error: error.code, message: error.message },
      {
        status: INSPECT_ERRORS[error.code],
        headers: { "Cache-Control": "no-store" },
      },
    );
  }

  async function handleApi(request: Request): Promise<Response> {
    if (request.method !== "POST") {
      return new Response(null, { status: 405, headers: { Allow: "POST" } });
    }
    // A form or a plain-text POST from another page can't be JSON: this
    // keeps cross-site requests that a browser sends without asking out.
    if (!request.headers.get("content-type")?.toLowerCase().startsWith("application/json")) {
      return errorResponse(new InspectError("bad_request", "send application/json"));
    }
    let admitted: Admitted;
    try {
      admitted = await admit(request);
    } catch (error) {
      const refused = asInspectError(error);
      audit({
        at: now(),
        by: "?",
        via: "hook",
        op: "whoami",
        write: false,
        error: refused.code,
      });
      return errorResponse(refused);
    }
    let body: InspectRequest;
    try {
      const text = await request.text();
      if (text.length > MAX_REQUEST_BYTES) throw new InspectError("too_large", "request too large");
      body = JSON.parse(text) as InspectRequest;
      if (!body || typeof body !== "object" || typeof body.op !== "string") {
        throw new InspectError("bad_request", "send { op, ... }");
      }
    } catch (error) {
      return errorResponse(
        error instanceof InspectError ? error : new InspectError("bad_request", "invalid JSON"),
      );
    }
    const base: InspectEvent = {
      at: now(),
      by: admitted.by,
      via: admitted.via,
      op: body.op,
      write: false,
      ...(admitted.tokenId ? { tokenId: admitted.tokenId } : {}),
    };
    try {
      const { answer: result, event } = await answer(request, admitted, body);
      audit(clean({ ...base, ...event }));
      return Response.json(result, {
        headers: { "Cache-Control": "no-store" },
      });
    } catch (error) {
      const failed = asInspectError(error);
      audit(clean({ ...base, write: body.op === "write", error: failed.code }));
      if (!(error instanceof InspectError)) console.error("jazz inspect: call failed", error);
      return errorResponse(failed);
    }
  }

  return {
    /** Answer a request for `path` (below `basePath`), which starts with `/inspect`. */
    async handle(request: Request, path: string): Promise<Response> {
      if (path === INSPECT_API_PATH) return handleApi(request);
      if (path === INSPECT_PATH || path.startsWith(`${INSPECT_PATH}/`)) {
        if (request.method !== "GET" && request.method !== "HEAD") {
          return new Response(null, { status: 405 });
        }
        if (path === INSPECT_PATH) {
          // The page's assets are relative to it, so it lives at `/inspect/`.
          // A redirect keeps the fragment, which carries any deep link.
          return new Response(null, {
            status: 308,
            headers: { Location: `${deps.basePath}${INSPECT_PATH}/` },
          });
        }
        const served = await deps.serveUi(request, path.slice(INSPECT_PATH.length));
        return (
          served ??
          new Response(
            "This jazz-tools build has no Inspector UI. The API at " +
              `${deps.basePath}${INSPECT_API_PATH} works: try \`npx jazz-tools inspect\`.`,
            {
              status: 404,
              headers: { "Content-Type": "text/plain; charset=utf-8" },
            },
          )
        );
      }
      return new Response("Not found", { status: 404 });
    },
    // For tests.
    mintToken,
    verifyToken,
  };
}

export type Inspector = ReturnType<typeof createInspector>;

function asInspectError(error: unknown): InspectError {
  if (error instanceof InspectError) return error;
  // Whoever got this far may read every row, so the runtime's reason (an
  // unknown column, a bad operator) is theirs to see.
  return new InspectError("failed", errorMessage(error));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function clean(event: InspectEvent): InspectEvent {
  return Object.fromEntries(
    Object.entries(event).filter(([, value]) => value !== undefined && value !== ""),
  ) as unknown as InspectEvent;
}

function isLoopbackHost(hostname: string): boolean {
  return (
    hostname === "localhost" ||
    hostname.endsWith(".localhost") ||
    hostname === "[::1]" ||
    /^127\.\d+\.\d+\.\d+$/.test(hostname)
  );
}

function isLoopbackAddress(address: string): boolean {
  return (
    address === "::1" ||
    /^127\.\d+\.\d+\.\d+$/.test(address) ||
    /^::ffff:127\.\d+\.\d+\.\d+$/.test(address)
  );
}

/** A random secret for stores that keep one beside their data. */
export function newTokenSecret(): string {
  return randomBytes(32).toString("base64url");
}

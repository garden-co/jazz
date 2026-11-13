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

import { NoJazzAccountError } from "./no-account-error.js";
import { createHmac, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import type { ColumnType, TablePolicies, WasmSchema } from "../drivers/types.js";
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
  isLoopbackHost,
  policyClauses,
  readsClaims,
  renderPolicy,
  rowLimit,
  toJsonValue,
  type InspectAs,
  type InspectErrorCode,
  type InspectExplainAnswer,
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
  /** The names (not values) of the claims given for `as`, when some were. */
  asClaims?: string[];
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

function defaultAudit(event: InspectEvent): void {
  const parts = [
    `jazz inspect: ${event.by} (${event.via})`,
    event.op,
    event.table,
    event.as && `as ${event.as}`,
    event.rows !== undefined && `${event.rows} row(s)`,
    event.error && `refused: ${event.error}`,
  ].filter(Boolean);
  console.info(parts.join(" "));
}

/** {@link InspectorOptions} as the Inspector applies them. */
interface InspectorSettings {
  redact: Map<string, Set<string>>;
  audit: (event: InspectEvent) => void;
  /** Tokens issued before this (Unix seconds) are refused. */
  issuedAfter: number | undefined;
  maxTtl: number;
}

/**
 * Check `options` and read them as the Inspector applies them. createJazz
 * calls it when it starts and on each reload, so bad options fail there,
 * not on the next Inspector call.
 */
export function inspectorSettings(options: InspectorOptions): InspectorSettings {
  const maxTtl = options.maxTokenTtlSeconds ?? DEFAULT_MAX_TOKEN_TTL_SECONDS;
  if (!Number.isSafeInteger(maxTtl) || maxTtl < 60) {
    throw new Error("createJazz: inspector.maxTokenTtlSeconds must be at least 60");
  }
  let issuedAfter: number | undefined;
  if (options.tokensIssuedAfter !== undefined) {
    const at = new Date(options.tokensIssuedAfter).getTime();
    if (Number.isNaN(at)) {
      throw new Error("createJazz: inspector.tokensIssuedAfter must be a date");
    }
    issuedAfter = Math.floor(at / 1000);
  }
  if (options.tokenSecret !== undefined && !options.tokenSecret.trim()) {
    throw new Error("createJazz: inspector.tokenSecret is empty");
  }
  return {
    redact: new Map(
      Object.entries(options.redact ?? {}).map(([table, columns]) => [table, new Set(columns)]),
    ),
    audit: options.audit ?? defaultAudit,
    issuedAfter,
    maxTtl,
  };
}

export function createInspector(deps: InspectorDeps) {
  const now = deps.now ?? (() => Date.now());
  const env = deps.env ?? process.env;

  // The options follow a reload of createJazz: they are read again whenever
  // the options object changes.
  let settingsOf: { options: InspectorOptions; settings: InspectorSettings } | undefined;
  function settings(): InspectorSettings {
    const options = deps.options;
    if (settingsOf?.options !== options) {
      settingsOf = { options, settings: inspectorSettings(options) };
    }
    return settingsOf.settings;
  }
  settings();
  const audit = (event: InspectEvent) => settings().audit(event);

  // The signing key follows the secret: a reload that rotates it revokes
  // every token signed with the old one.
  let keyOf: { secret: string; key: Buffer } | undefined;
  async function tokenKey(): Promise<Buffer | null> {
    const secret = await deps.tokenSecret();
    if (secret === null) return null;
    if (keyOf?.secret !== secret) {
      keyOf = {
        secret,
        key: createHmac("sha256", secret).update(`jazz-inspect-token-v1\0${deps.appId}`).digest(),
      };
    }
    return keyOf.key;
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
    // A browser names the page a request comes from. A dev proxy that adds
    // no forwarding headers (Vite's `server.proxy` with `changeOrigin`, on a
    // dev server started with `--host`) passes another machine's request on
    // from 127.0.0.1 with a loopback Host, but keeps the browser's Origin
    // and Sec-Fetch-Site: only a page on this machine is let in.
    const origin = request.headers.get("origin");
    if (origin !== null) {
      let originHost: string;
      try {
        originHost = new URL(origin).hostname;
      } catch {
        return false;
      }
      if (!isLoopbackHost(originHost)) return false;
    }
    if (request.headers.get("sec-fetch-site") === "cross-site") return false;
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
      let grant: InspectGrant | null | undefined;
      try {
        grant = await deps.hook(request);
      } catch (error) {
        // Its reason is the app's (a sign-in service's error, a stack), and
        // this caller isn't let in: it goes to the log, not the answer.
        console.error("jazz inspect: the app's inspect hook threw", error);
        throw new InspectError(
          "failed",
          "the app's inspect hook failed; the backend's log has the error",
        );
      }
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
    const { maxTtl } = settings();
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
    const { issuedAfter } = settings();
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
    if (typeof name !== "string" || !name) throw new InspectError("bad_request", "name a table");
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
    return settings().redact.get(historySourceTable(table) ?? table);
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

  let claimsReadFor: { permissions: unknown; reads: boolean } | undefined;
  /** Whether the app's permissions read anything from `session.claims`. */
  function permissionsReadClaims(): boolean {
    if (!claimsReadFor || claimsReadFor.permissions !== deps.permissions) {
      claimsReadFor = { permissions: deps.permissions, reads: readsClaims(deps.permissions) };
    }
    return claimsReadFor.reads;
  }

  async function dbFor(
    as: InspectAs | undefined,
  ): Promise<{ db: Db; as?: string; asClaims?: string[]; warning?: string }> {
    if (as === undefined) return { db: deps.db() };
    const user = typeof as === "string" ? { userId: as } : as;
    if (!user || typeof user.userId !== "string" || !user.userId) {
      throw new InspectError("bad_request", "as must name a user id");
    }
    if (
      user.claims !== undefined &&
      (user.claims === null || typeof user.claims !== "object" || Array.isArray(user.claims))
    ) {
      throw new InspectError("bad_request", "as.claims must be an object of the user's claims");
    }
    // A user's claims come from their sign-in, which the Inspector doesn't
    // have: viewing as them without claims sees what a session with none
    // would, and policies that read claims then quietly allow less.
    const warning =
      user.claims === undefined && permissionsReadClaims()
        ? `This app's permissions read session claims, and none were given for ${user.userId}, so this ran as a session with no claims: rows or writes a claim would allow are missing or refused. Pass the user's claims with as: { userId, claims } (CLI: --claims '<json>'; MCP: claims; Inspector: the claims field next to "View as user").`
        : undefined;
    try {
      return {
        db: await deps.dbAs(user),
        as: user.userId,
        ...(user.claims ? { asClaims: Object.keys(user.claims).sort() } : {}),
        ...(warning ? { warning } : {}),
      };
    } catch (error) {
      // Viewing as a user who never signed in: there is no one to be.
      if (error instanceof NoJazzAccountError) throw new InspectError("not_found", error.message);
      throw error;
    }
  }

  async function readRows(
    admitted: Admitted,
    query: InspectQuery,
    as: InspectAs | undefined,
  ): Promise<{
    rows: Record<string, unknown>[];
    truncated: boolean;
    as?: string;
    asClaims?: string[];
    warning?: string;
  }> {
    if (!query || typeof query !== "object") {
      throw new InspectError("bad_request", "query needs { table, ... }");
    }
    checkTable(admitted, query.table);
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
    const { db, as: asUser, asClaims, warning } = await dbFor(as);
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
    return {
      rows,
      truncated,
      ...(asUser ? { as: asUser } : {}),
      ...(asClaims ? { asClaims } : {}),
      ...(warning ? { warning } : {}),
    };
  }

  async function write(
    admitted: Admitted,
    ops: InspectWriteOp[],
    as: InspectAs | undefined,
  ): Promise<{ ids: string[]; as?: string; asClaims?: string[]; warning?: string }> {
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
      if (!op || typeof op !== "object" || !["insert", "update", "delete"].includes(op.kind)) {
        throw new InspectError("bad_request", "each op is an insert, update or delete");
      }
      if (typeof op.table !== "string" || !op.table) {
        throw new InspectError("bad_request", `${op.kind} needs a table`);
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
    const { db, as: asUser, asClaims, warning } = await dbFor(as);
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
      throw new InspectError(
        "forbidden",
        `the write was refused: ${errorMessage(error)}${warning ? `. ${warning}` : ""}`,
      );
    }
    return {
      ids,
      ...(asUser ? { as: asUser } : {}),
      ...(asClaims ? { asClaims } : {}),
      ...(warning ? { warning } : {}),
    };
  }

  /**
   * Which read-policy clause let `as` see each row `query` matches with the
   * backend's authority, or that none did. The clauses are the arms of the
   * table's policy's top-level `or`.
   */
  async function explain(admitted: Admitted, query: InspectQuery, as: InspectAs | undefined) {
    if (as === undefined) {
      throw new InspectError("bad_request", "explain needs as: the user to explain for");
    }
    if (!query || typeof query !== "object" || typeof query.table !== "string") {
      throw new InspectError("bad_request", "query needs { table, ... }");
    }
    if (historySourceTable(query.table) !== undefined) {
      throw new InspectError("bad_request", "explain needs a table, not its history");
    }
    checkTable(admitted, query.table);
    if (deps.schema()[query.table]?.branchBy?.length) {
      throw new InspectError(
        "not_supported",
        "explain for a table with branches is not supported yet",
      );
    }
    // Every row the query matches, whoever may see it; then, as the user,
    // which clauses hold for each.
    const read = await readRows(admitted, query, undefined);
    const { db, as: asUser, asClaims, warning } = await dbFor(as);
    const policies = (deps.permissions as Record<string, TablePolicies> | undefined)?.[query.table];
    const policy = policies?.select?.using ?? policies?.select?.with_check;
    const clauses = policyClauses(policy);
    const ids = read.rows.map((row) => String(row.id));
    const engine = db.explainReadClauses(tableProxy(query.table), ids);
    // The clause numbers are the engine's, the text ours: both must split
    // the same policy.
    if (engine.clauses !== clauses.length) {
      throw new InspectError(
        "failed",
        `the read policy this backend was given for ${query.table} has ${clauses.length} clause(s), but the store judges reads by one with ${engine.clauses}: explain can't say which is which`,
      );
    }
    const found = new Map(engine.rows.map((row) => [row.id, row.holds]));
    // A table-scoped token doesn't see the app's permissions (see `schema`),
    // so it gets the clauses' numbers without their text. Nor which clause
    // let a row through, for it or a token with redacted columns here: a
    // clause's verdict tells about what it reads (a redacted column's
    // value, a table outside the token's scope).
    const scoped = admitted.via === "token" && admitted.tables !== undefined;
    const showPolicy = !scoped;
    const showWhy = !scoped && !redactedFor(admitted, query.table);
    const answer: Omit<InspectExplainAnswer, "inspectorUrl"> = {
      table: query.table,
      as: asUser!,
      clauses: clauses.map((clause, index) => ({
        index,
        ...(showPolicy ? { text: renderPolicy(clause) } : {}),
      })),
      rows: read.rows.flatMap((row) => {
        const holds = found.get(String(row.id));
        // Gone between the two reads.
        if (!holds) return [];
        const by = holds.flatMap((held, index) => (held ? [index] : []));
        return [{ id: String(row.id), allowed: by.length > 0, ...(showWhy ? { by } : {}), row }];
      }),
      truncated: read.truncated,
      ...(policy === undefined
        ? { note: `${query.table} has no read policy, so every row is denied` }
        : {}),
      ...(warning ? { warning } : {}),
    };
    return { answer, as: asUser, asClaims };
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
            ...(read.warning ? { warning: read.warning } : {}),
            inspectorUrl: inspectorLink(url, {
              query: body.query,
              as: body.as,
            }),
          },
          event: {
            table: body.query.table,
            rows: read.rows.length,
            as: read.as,
            asClaims: read.asClaims,
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
            ...(read.warning ? { warning: read.warning } : {}),
            inspectorUrl: inspectorLink(url, {
              table: body.table,
              row: body.id,
              as: body.as,
            }),
          },
          event: {
            table: body.table,
            rows: read.rows.length,
            as: read.as,
            asClaims: read.asClaims,
          },
        };
      }
      case "history": {
        if (typeof body.table !== "string" || !body.table) {
          throw new InspectError("bad_request", "history needs a table");
        }
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
          answer: { ids: done.ids, ...(done.warning ? { warning: done.warning } : {}) },
          event: {
            table: [...new Set(body.ops.map((op) => op.table))].join(","),
            rows: done.ids.length,
            as: done.as,
            asClaims: done.asClaims,
            write: true,
          },
        };
      }
      case "sessions":
        // Who is connected is about the whole app, not one table.
        if (admitted.tables) {
          throw new InspectError(
            "forbidden",
            `this token is limited to ${admitted.tables.join(", ")}; listing sessions needs one for the whole app`,
          );
        }
        return {
          answer: {
            // Each backend instance knows only the sessions it serves.
            scope: "this backend instance",
            sessions: deps.sessions(),
          },
          event: {},
        };
      case "explain": {
        const explained = await explain(admitted, body.query, body.as);
        return {
          answer: {
            ...explained.answer,
            inspectorUrl: inspectorLink(url, { query: body.query, as: body.as }),
          },
          event: {
            table: body.query?.table,
            rows: explained.answer.rows.length,
            as: explained.as,
            asClaims: explained.asClaims,
          },
        };
      }
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
        via: request.headers.get("authorization")?.startsWith(`Bearer ${INSPECT_TOKEN_PREFIX}`)
          ? "token"
          : deps.hook
            ? "hook"
            : "dev",
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

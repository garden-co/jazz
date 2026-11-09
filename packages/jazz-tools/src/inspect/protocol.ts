// The Inspector protocol: one JSON API at `<basePath>/inspect/api` on the
// app's own backend, spoken by the Inspector UI the backend serves, the
// `jazz-tools inspect` CLI and the `jazz-tools inspect mcp` server alike. Whatever a
// person can see in the Inspector, an agent can see through the CLI or MCP,
// and the other way round.
//
// This module holds only what both ends share: the request and answer
// shapes, how a query is written, how values cross JSON, and the deep links
// that open the Inspector on a query. It has no Node or browser dependency.

import type { ColumnType } from "../drivers/types.js";

/** The version of the request and answer shapes below. */
export const INSPECT_PROTOCOL_VERSION = 1;

/** Where the Inspector lives under the backend's `basePath`. */
export const INSPECT_PATH = "/inspect";
/** The JSON API under {@link INSPECT_PATH}. */
export const INSPECT_API_PATH = `${INSPECT_PATH}/api`;

/** The prefix every agent token carries, so a token is recognised as one in logs and headers. */
export const INSPECT_TOKEN_PREFIX = "jzi1_";

/** Rows an answer carries unless the request asks for fewer or more. */
export const DEFAULT_ROW_LIMIT = 100;
/** The most rows one answer carries. */
export const MAX_ROW_LIMIT = 1000;
/** The most bytes of JSON one answer carries; rows past it are left out and the answer says so. */
export const MAX_ANSWER_BYTES = 1024 * 1024;

/**
 * A condition on one column, written the way `where()` takes it:
 * `{ done: false }`, `{ title: { contains: "milk" } }`,
 * `{ dueAt: { lt: "2026-11-01T00:00:00Z" } }`.
 */
export type InspectWhere = Record<string, unknown>;

/**
 * A query, in the form agents and the Inspector write it. `where` uses the
 * same operators as the app's own `where()`; `select` defaults to every
 * column (`"*"`), and can add provenance columns such as `$createdBy` or
 * `$updatedAt`. Read a table's row history by querying `history.<table>`.
 */
export interface InspectQuery {
  table: string;
  where?: InspectWhere;
  select?: string[];
  orderBy?: Array<[column: string, direction: "asc" | "desc"]>;
  limit?: number;
  offset?: number;
}

/** Whose permissions a read or write runs under: the app's user id, and the claims `authenticate` would give them. */
export type InspectAs = string | { userId: string; claims?: Record<string, unknown> };

/** One write, as the Inspector's save or an agent sends it. */
export type InspectWriteOp =
  | {
      kind: "insert";
      table: string;
      values: Record<string, unknown>;
      id?: string;
    }
  | {
      kind: "update";
      table: string;
      id: string;
      values: Record<string, unknown>;
    }
  | { kind: "delete"; table: string; id: string };

/** Every request names its operation in `op`. */
export type InspectRequest =
  | { op: "whoami" }
  | { op: "schema" }
  | { op: "query"; query: InspectQuery; as?: InspectAs }
  | { op: "get"; table: string; id: string; as?: InspectAs }
  | { op: "history"; table: string; id: string; limit?: number }
  | { op: "write"; ops: InspectWriteOp[]; as?: InspectAs }
  | { op: "sessions" }
  | { op: "explain"; query: InspectQuery; as: InspectAs }
  | {
      op: "link";
      query?: InspectQuery;
      table?: string;
      row?: string;
      as?: InspectAs;
    }
  | {
      op: "mint_token";
      write?: boolean;
      ttlSeconds?: number;
      tables?: string[];
      label?: string;
    };

export type InspectOp = InspectRequest["op"];

/** How a request was let in: the app's `inspect` hook, an agent token, or the local development default. */
export type InspectVia = "hook" | "token" | "dev";

export interface InspectWhoami {
  appId: string;
  by: string;
  via: InspectVia;
  write: boolean;
  /** The tables an agent token is limited to, if it is. */
  tables?: string[];
  /** When an agent token stops working (Unix seconds). */
  expiresAt?: number;
  /** Where the Inspector UI for this backend is. */
  inspectorUrl: string;
  protocol: typeof INSPECT_PROTOCOL_VERSION;
}

export interface InspectRowsAnswer {
  rows: Record<string, unknown>[];
  /** More rows matched than this answer carries (the row limit, or the byte limit). */
  truncated: boolean;
  /** The Inspector, open on exactly this query. */
  inspectorUrl: string;
}

export interface InspectSessionInfo {
  /** The app's user id, or null for a guest or a trusted backend client. */
  userId: string | null;
  kind: "user" | "guest" | "backend-client";
  transport: "socket" | "stream";
  /** Unix milliseconds. */
  openedAt: number;
}

/** The error codes an answer can carry, with the HTTP status it comes with. */
export const INSPECT_ERRORS = {
  bad_request: 400,
  unauthorized: 401,
  forbidden: 403,
  not_found: 404,
  not_supported: 501,
  too_large: 413,
  failed: 500,
} as const;
export type InspectErrorCode = keyof typeof INSPECT_ERRORS;

export interface InspectErrorAnswer {
  error: InspectErrorCode;
  message: string;
}

/** An error answer, raised by either end. */
export class InspectError extends Error {
  constructor(
    readonly code: InspectErrorCode,
    message: string,
  ) {
    super(message);
    this.name = "InspectError";
  }
}

const UNSAFE_KEYS = new Set(["__proto__", "constructor", "prototype"]);

/**
 * `query` as the JSON a query builder sends the runtime, after checking its
 * shape. Unknown keys are refused rather than ignored, so a typo never reads
 * more than was asked for.
 */
export function buildQueryJson(
  query: InspectQuery,
  limit: number,
  convert: (column: string, value: unknown) => unknown = (_column, value) => fromJsonValue(value),
): string {
  if (!query || typeof query !== "object" || Array.isArray(query)) {
    throw new InspectError("bad_request", "query must be an object like { table, where }");
  }
  for (const key of Object.keys(query)) {
    if (!["table", "where", "select", "orderBy", "limit", "offset"].includes(key)) {
      throw new InspectError(
        "bad_request",
        `query.${key} is not supported (use table, where, select, orderBy, limit, offset)`,
      );
    }
  }
  if (typeof query.table !== "string" || !query.table) {
    throw new InspectError("bad_request", "query.table must name a table");
  }
  const conditions: Array<{ column: string; op: string; value: unknown }> = [];
  for (const [column, value] of Object.entries(query.where ?? {})) {
    if (UNSAFE_KEYS.has(column)) throw new InspectError("bad_request", `bad column ${column}`);
    if (value === undefined) continue;
    if (isPlainObject(value) && !("$bytes" in value)) {
      for (const [op, operand] of Object.entries(value)) {
        if (operand !== undefined)
          conditions.push({
            column,
            op,
            value: convertOperand(column, op, operand),
          });
      }
    } else {
      conditions.push({ column, op: "eq", value: convert(column, value) });
    }
  }
  function convertOperand(column: string, op: string, operand: unknown): unknown {
    // `in` takes a list of the column's values; `isNull` a boolean.
    if ((op === "in" || op === "notIn") && Array.isArray(operand))
      return operand.map((entry) => convert(column, entry));
    if (op === "isNull") return operand;
    return convert(column, operand);
  }
  const select = query.select ?? ["*"];
  if (!Array.isArray(select) || !select.every((column) => typeof column === "string")) {
    throw new InspectError("bad_request", "query.select must be a list of column names");
  }
  const orderBy = query.orderBy ?? [];
  if (
    !Array.isArray(orderBy) ||
    !orderBy.every(
      (entry) =>
        Array.isArray(entry) &&
        typeof entry[0] === "string" &&
        (entry[1] === "asc" || entry[1] === "desc"),
    )
  ) {
    throw new InspectError("bad_request", 'query.orderBy must be a list like [["title", "asc"]]');
  }
  const offset = query.offset ?? 0;
  if (!Number.isSafeInteger(offset) || offset < 0) {
    throw new InspectError("bad_request", "query.offset must be a whole number");
  }
  return JSON.stringify({
    table: query.table,
    conditions,
    includes: {},
    select,
    orderBy,
    limit,
    offset: offset || undefined,
    hops: [],
  });
}

/** The row limit a request gets: its own, within {@link MAX_ROW_LIMIT}. */
export function rowLimit(requested: unknown): number {
  if (requested === undefined) return DEFAULT_ROW_LIMIT;
  if (!Number.isSafeInteger(requested) || (requested as number) < 1) {
    throw new InspectError("bad_request", "limit must be a whole number of at least 1");
  }
  return Math.min(requested as number, MAX_ROW_LIMIT);
}

/**
 * A value as JSON carries it: dates as ISO strings, 64-bit integers as
 * decimal strings, bytes as `{ "$bytes": "<base64>" }`.
 */
export function toJsonValue(value: unknown): unknown {
  if (value === null || value === undefined) return value ?? null;
  if (typeof value === "bigint") return value.toString();
  if (value instanceof Date) return value.toISOString();
  if (value instanceof Uint8Array) return { $bytes: bytesToBase64(value) };
  if (Array.isArray(value)) return value.map(toJsonValue);
  if (typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
      out[key] = toJsonValue(entry);
    }
    return out;
  }
  return value;
}

/** The inverse of {@link toJsonValue} for bytes; dates and 64-bit integers are converted by column type on write. */
export function fromJsonValue(value: unknown): unknown {
  if (isPlainObject(value) && Object.keys(value).length === 1 && typeof value.$bytes === "string") {
    return base64ToBytes(value.$bytes);
  }
  if (Array.isArray(value)) return value.map(fromJsonValue);
  return value;
}

/**
 * Convert a JSON value back to what a column of `type` holds: ISO strings to
 * `Date` for timestamps, decimal strings to `bigint`, `{ $bytes }` to bytes.
 */
export function fromJsonForColumn(type: ColumnType | undefined, value: unknown): unknown {
  value = fromJsonValue(value);
  if (value === null || value === undefined || !type) return value;
  switch (type.type) {
    case "Timestamp":
      return typeof value === "string" ? new Date(value) : value;
    case "BigInt":
      return typeof value === "string" && /^-?\d+$/.test(value) ? BigInt(value) : value;
    case "Array":
      return Array.isArray(value)
        ? value.map((entry) => fromJsonForColumn(type.element, entry))
        : value;
    case "Row": {
      if (typeof value !== "object" || Array.isArray(value)) return value;
      const columns = new Map(type.columns.map((column) => [column.name, column.column_type]));
      return Object.fromEntries(
        Object.entries(value as Record<string, unknown>).map(([key, entry]) => [
          key,
          fromJsonForColumn(columns.get(key), entry),
        ]),
      );
    }
    default:
      return value;
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
  );
}

function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  for (let index = 0; index < bytes.length; index += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(index, index + 0x8000));
  }
  return btoa(binary);
}

function base64ToBytes(text: string): Uint8Array {
  const binary = atob(text);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
  return bytes;
}

function base64UrlEncode(text: string): string {
  return bytesToBase64(new TextEncoder().encode(text))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

function base64UrlDecode(text: string): string {
  const padded = text.replace(/-/g, "+").replace(/_/g, "/");
  return new TextDecoder().decode(
    base64ToBytes(padded + "=".repeat((4 - (padded.length % 4)) % 4)),
  );
}

/** What a deep link opens the Inspector on. It never carries a credential. */
export interface InspectorLinkTarget {
  query?: InspectQuery;
  table?: string;
  row?: string;
  as?: InspectAs;
}

/**
 * The Inspector at `inspectorUrl`, open on `target`. Everything rides in the
 * fragment, which browsers never send to the server, so queries don't end
 * up in access logs; whoever opens the link must pass the app's `inspect`
 * hook on their own.
 */
export function inspectorLink(inspectorUrl: string, target: InspectorLinkTarget = {}): string {
  const params = new URLSearchParams();
  if (target.query) params.set("q", base64UrlEncode(JSON.stringify(target.query)));
  const table = target.table ?? target.query?.table;
  if (table) params.set("table", table);
  if (target.row) params.set("row", target.row);
  if (target.as !== undefined) {
    params.set("as", typeof target.as === "string" ? target.as : target.as.userId);
  }
  const fragment = params.toString();
  return fragment ? `${inspectorUrl}#${fragment}` : inspectorUrl;
}

/** The target a deep link's fragment names (the inverse of {@link inspectorLink}). */
export function parseInspectorLink(fragment: string): InspectorLinkTarget & { grant?: string } {
  const params = new URLSearchParams(fragment.replace(/^#/, ""));
  const target: InspectorLinkTarget & { grant?: string } = {};
  const q = params.get("q");
  if (q) {
    try {
      target.query = JSON.parse(base64UrlDecode(q)) as InspectQuery;
    } catch {
      // A damaged link opens the Inspector without its query.
    }
  }
  const table = params.get("table");
  if (table) target.table = table;
  const row = params.get("row");
  if (row) target.row = row;
  const as = params.get("as");
  if (as) target.as = as;
  const grant = params.get("grant");
  if (grant) target.grant = grant;
  return target;
}

/** What an agent asks a person to approve: the Inspector reads it from `#grant=`. */
export interface InspectGrantRequest {
  write?: boolean;
  ttlSeconds?: number;
  tables?: string[];
  label?: string;
  /** Where the browser hands the token over: a loopback URL on the asking machine. */
  callback?: string;
  state: string;
}

/** Decode a `#grant=` value; null when it is damaged. */
export function decodeGrant(text: string): InspectGrantRequest | null {
  try {
    const grant = JSON.parse(base64UrlDecode(text)) as InspectGrantRequest;
    if (!isPlainObject(grant) || typeof grant.state !== "string" || !grant.state) return null;
    if (grant.write !== undefined && typeof grant.write !== "boolean") return null;
    if (grant.ttlSeconds !== undefined && !Number.isSafeInteger(grant.ttlSeconds)) return null;
    if (
      grant.tables !== undefined &&
      !(Array.isArray(grant.tables) && grant.tables.every((table) => typeof table === "string"))
    )
      return null;
    if (grant.label !== undefined && typeof grant.label !== "string") return null;
    if (grant.callback !== undefined && typeof grant.callback !== "string") return null;
    return grant;
  } catch {
    return null;
  }
}

/** Whether `hostname` (as a URL has it) names this machine. */
export function isLoopbackHost(hostname: string): boolean {
  return (
    hostname === "localhost" ||
    hostname.endsWith(".localhost") ||
    hostname === "[::1]" ||
    /^127\.\d+\.\d+\.\d+$/.test(hostname)
  );
}

/**
 * Whether the Inspector may hand a token to `callback` by itself: only a
 * plain-http loopback URL ending in `/callback`, which reaches nothing but
 * the machine the browser runs on. Anything else, and the Inspector shows the
 * token for the person to paste instead.
 */
export function isLoopbackCallback(callback: string): boolean {
  let url: URL;
  try {
    url = new URL(callback);
  } catch {
    return false;
  }
  return (
    url.protocol === "http:" &&
    (url.hostname === "127.0.0.1" || url.hostname === "localhost" || url.hostname === "[::1]") &&
    url.port !== "" &&
    url.username === "" &&
    url.password === "" &&
    url.pathname === "/callback" &&
    url.search === "" &&
    url.hash === ""
  );
}

/**
 * Jazz Lite wire protocol (experimental, unversioned, JSON text frames).
 *
 * The browser holds no storage and runs no query engine. It sends query JSON
 * produced by the normal typed query builder, and the server-side client proxy
 * executes it under the caller's session and streams per-subscription results.
 */

export type WriteOp = "insert" | "update" | "upsert" | "delete";
export type TxKind = "mergeable" | "exclusive";
export type WriteTier = "local" | "global";

export type ClientMessage =
  /** First frame. Browsers cannot set WebSocket headers, so the bearer token travels here. */
  | { t: "auth"; token: string }
  | { t: "query"; id: number; table: string; query: string; opts?: unknown; tx?: number }
  | { t: "sub"; id: number; table: string; query: string; opts?: unknown }
  | { t: "unsub"; id: number }
  | {
      t: "write";
      id: number;
      op: WriteOp;
      table: string;
      rowId?: string;
      data?: unknown;
      /** Tier the proxy waits for before acking. Ignored inside a transaction. */
      tier?: WriteTier;
      tx?: number;
    }
  | { t: "begin"; id: number; kind: TxKind }
  | { t: "commit"; id: number; tx: number; tier?: WriteTier }
  | { t: "rollback"; id: number; tx: number };

export interface WireError {
  message: string;
  name?: string;
  code?: string;
}

export type ServerMessage =
  | { t: "ready"; account: string | null }
  | { t: "ok"; id: number; v?: unknown }
  | { t: "err"; id: number; e: WireError }
  /**
   * Subscription result change. `set` carries rows that are new or changed,
   * `del` ids that left the result, and `order` the full id order whenever it
   * differs from what the client would get by applying set/del in place.
   */
  | { t: "rows"; id: number; set?: unknown[]; del?: string[]; order?: string[] }
  | { t: "suberr"; id: number; e: WireError }
  | { t: "fatal"; e: WireError };

// ---------------------------------------------------------------------------
// Value codec: JSON plus Date, bigint, Uint8Array and undefined-in-arrays.
// Plain objects whose only key is "$" are reserved for tags; a user object that
// happens to have that shape is escaped as {"$": "o", v: {...}}.
// ---------------------------------------------------------------------------

type Tagged =
  | { $: "d"; v: number }
  | { $: "n"; v: string }
  | { $: "b"; v: string }
  | { $: "u" }
  | { $: "o"; v: Record<string, unknown> };

function isTag(value: Record<string, unknown>): boolean {
  return typeof value.$ === "string" && Object.keys(value).every((k) => k === "$" || k === "v");
}

function bytesToBase64(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]!);
  return btoa(s);
}

function base64ToBytes(b64: string): Uint8Array {
  const s = atob(b64);
  const out = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i);
  return out;
}

export function encodeValue(value: unknown): unknown {
  if (value === null || typeof value !== "object") {
    if (typeof value === "bigint") return { $: "n", v: value.toString() } satisfies Tagged;
    return value;
  }
  if (value instanceof Date) return { $: "d", v: value.getTime() } satisfies Tagged;
  if (value instanceof Uint8Array) return { $: "b", v: bytesToBase64(value) } satisfies Tagged;
  if (Array.isArray(value)) {
    return value.map((item) =>
      item === undefined ? ({ $: "u" } satisfies Tagged) : encodeValue(item),
    );
  }
  const out: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value)) {
    if (item !== undefined) out[key] = encodeValue(item);
  }
  return isTag(out) ? ({ $: "o", v: out } satisfies Tagged) : out;
}

export function decodeValue(value: unknown): unknown {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(decodeValue);
  const record = value as Record<string, unknown>;
  if (isTag(record)) {
    const tagged = record as Tagged;
    switch (tagged.$) {
      case "d":
        return new Date(tagged.v);
      case "n":
        return BigInt(tagged.v);
      case "b":
        return base64ToBytes(tagged.v);
      case "u":
        return undefined;
      case "o":
        return decodeObject(tagged.v);
    }
  }
  return decodeObject(record);
}

function decodeObject(record: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(record)) out[key] = decodeValue(item);
  return out;
}

export function toWireError(error: unknown): WireError {
  if (error instanceof Error) {
    const code = (error as { code?: unknown }).code;
    return {
      message: error.message,
      name: error.name,
      ...(typeof code === "string" ? { code } : {}),
    };
  }
  return { message: String(error) };
}

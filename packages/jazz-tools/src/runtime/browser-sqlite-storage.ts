import { sha256 } from "@noble/hashes/sha2.js";

/** The identity/lease sidecar and SQLite file are one logical browser root. */
export const SQLITE_BROWSER_DATABASE_VERSION = 3;
export const SQLITE_BROWSER_ROOT_KEY = "sqlite-file-owner-v2";
export const SQLITE_BROWSER_ROOT_FORMAT = "jazz.browser.sqlite-root.v2";

export type SqliteBrowserRoot = {
  name: string;
  owner: string;
  node: string;
  state: "opening" | "ready" | "resetting";
};

/** Canonical UTF-8 JSON tuple, stored as bytes rather than an object layout. */
export function encodeSqliteBrowserRoot(root: SqliteBrowserRoot): Uint8Array {
  return new TextEncoder().encode(
    JSON.stringify([SQLITE_BROWSER_ROOT_FORMAT, root.name, root.owner, root.node, root.state]),
  );
}

export function sqliteReplicaKey(node: Uint8Array): string {
  if (node.byteLength !== 16) throw new Error("Invalid SQLite browser replica identity");
  return Array.from(node, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function decodeSqliteBrowserRoot(
  value: unknown,
  name: string,
  owner: string,
  node: Uint8Array,
): SqliteBrowserRoot {
  const fail = () => new Error("Missing or invalid SQLite browser storage manifest");
  if (!(value instanceof ArrayBuffer) && !ArrayBuffer.isView(value)) throw fail();
  const bytes =
    value instanceof ArrayBuffer
      ? new Uint8Array(value)
      : new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  let tuple: unknown;
  try {
    tuple = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  } catch {
    throw fail();
  }
  if (
    !Array.isArray(tuple) ||
    tuple.length !== 5 ||
    tuple[0] !== SQLITE_BROWSER_ROOT_FORMAT ||
    tuple[1] !== name ||
    tuple[2] !== owner ||
    tuple[3] !== sqliteReplicaKey(node) ||
    !["opening", "ready", "resetting"].includes(tuple[4])
  )
    throw fail();
  const root: SqliteBrowserRoot = { name, owner, node: tuple[3], state: tuple[4] };
  const canonical = encodeSqliteBrowserRoot(root);
  if (bytes.length !== canonical.length || bytes.some((byte, i) => byte !== canonical[i]))
    throw fail();
  return root;
}

export function sqliteBrowserDescriptor(root: SqliteBrowserRoot) {
  return {
    backend: "indexeddb" as const,
    // Match upstream StoreSpec's default: SHA-256 of the physical name,
    // truncated to 16 bytes. The node may rotate on reset; the filename does not.
    path: `/jazz-${Array.from(sha256(new TextEncoder().encode(root.name)).slice(0, 16), (byte) => byte.toString(16).padStart(2, "0")).join("")}.sqlite`,
    requireExisting: root.state === "ready",
  };
}

import { parseJwtPayload } from "./client-session.js";
import type { DbConfig } from "./db.js";

export interface RuntimeIdentity {
  node: Uint8Array;
  author: Uint8Array;
}

/** @internal Stable 128-bit identity material used by persistent runtimes. */
export function deterministicRuntimeBytes(seed: string): Uint8Array {
  let hash = 0x811c9dc5;
  const bytes = new Uint8Array(16);
  const view = new DataView(bytes.buffer);

  for (let round = 0; round < 4; round += 1) {
    for (let i = 0; i < seed.length; i += 1) {
      hash ^= seed.charCodeAt(i) + round;
      hash = Math.imul(hash, 0x01000193);
    }
    view.setUint32(round * 4, hash >>> 0, true);
  }

  return bytes;
}

function randomRuntimeBytes(): Uint8Array {
  const bytes = new Uint8Array(16);
  if (globalThis.crypto?.getRandomValues) {
    globalThis.crypto.getRandomValues(bytes);
    return bytes;
  }
  return deterministicRuntimeBytes(`${Date.now()}:${Math.random()}`);
}

function uuidBytes(value: string): Uint8Array | null {
  const hex = value.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) {
    return null;
  }

  const bytes = new Uint8Array(16);
  for (let index = 0; index < 16; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

/** @internal Resolve the authenticated subject used to scope runtime identity. */
export function runtimeSubject(config: DbConfig): string | null {
  if (config.cookieSession?.user_id) {
    return config.cookieSession.user_id;
  }

  const payload = parseJwtPayload(config.jwtToken ?? "");
  return typeof payload?.sub === "string" && payload.sub.trim() ? payload.sub.trim() : null;
}

/** @internal Validate and resolve the shared initial-sync durability cadence. */
export function resolveInitialSyncFlushEvery(config: DbConfig): number {
  const value = config.initialSyncFlushEvery ?? 512;
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error("initialSyncFlushEvery must be a positive integer");
  }
  return value;
}

/**
 * @internal Resolve runtime identity while preserving the browser runtime's
 * existing derivation. A persistent database name makes the node stable;
 * memory runtimes intentionally get a fresh node for each open.
 */
export function resolveRuntimeIdentity(
  config: DbConfig,
  persistentDbName?: string,
): RuntimeIdentity {
  const subject = runtimeSubject(config);
  const seed = `${config.appId}:${config.env ?? "dev"}:${config.userBranch ?? "main"}:${subject ?? "anonymous"}`;
  const node = persistentDbName
    ? deterministicRuntimeBytes(`${seed}:${persistentDbName}:node`)
    : randomRuntimeBytes();
  const author = subject
    ? (uuidBytes(subject) ?? deterministicRuntimeBytes(`${seed}:author`))
    : deterministicRuntimeBytes(`${seed}:author`);

  return { node, author };
}

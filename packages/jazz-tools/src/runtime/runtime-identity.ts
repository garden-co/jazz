import { parseJwtPayload } from "./client-session.js";
import type { DbConfig } from "./db.js";

export interface RuntimeIdentity {
  node: Uint8Array;
  author: Uint8Array;
}

const UUID_URL_NAMESPACE = Uint8Array.from([
  0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
]);

function rotateLeft(value: number, bits: number): number {
  return ((value << bits) | (value >>> (32 - bits))) >>> 0;
}

function sha1(input: Uint8Array): Uint8Array {
  const bitLength = input.length * 8;
  const paddedLength = Math.ceil((input.length + 9) / 64) * 64;
  const padded = new Uint8Array(paddedLength);
  padded.set(input);
  padded[input.length] = 0x80;

  const paddedView = new DataView(padded.buffer);
  paddedView.setUint32(paddedLength - 8, Math.floor(bitLength / 0x1_0000_0000), false);
  paddedView.setUint32(paddedLength - 4, bitLength >>> 0, false);

  let h0 = 0x67452301;
  let h1 = 0xefcdab89;
  let h2 = 0x98badcfe;
  let h3 = 0x10325476;
  let h4 = 0xc3d2e1f0;
  const words = new Uint32Array(80);

  for (let offset = 0; offset < paddedLength; offset += 64) {
    for (let index = 0; index < 16; index += 1) {
      words[index] = paddedView.getUint32(offset + index * 4, false);
    }
    for (let index = 16; index < words.length; index += 1) {
      words[index] = rotateLeft(
        words[index - 3]! ^ words[index - 8]! ^ words[index - 14]! ^ words[index - 16]!,
        1,
      );
    }

    let a = h0;
    let b = h1;
    let c = h2;
    let d = h3;
    let e = h4;

    for (let index = 0; index < words.length; index += 1) {
      let f: number;
      let k: number;
      if (index < 20) {
        f = (b & c) | (~b & d);
        k = 0x5a827999;
      } else if (index < 40) {
        f = b ^ c ^ d;
        k = 0x6ed9eba1;
      } else if (index < 60) {
        f = (b & c) | (b & d) | (c & d);
        k = 0x8f1bbcdc;
      } else {
        f = b ^ c ^ d;
        k = 0xca62c1d6;
      }

      const next = (rotateLeft(a, 5) + f + e + k + words[index]!) >>> 0;
      e = d;
      d = c;
      c = rotateLeft(b, 30);
      b = a;
      a = next;
    }

    h0 = (h0 + a) >>> 0;
    h1 = (h1 + b) >>> 0;
    h2 = (h2 + c) >>> 0;
    h3 = (h3 + d) >>> 0;
    h4 = (h4 + e) >>> 0;
  }

  const digest = new Uint8Array(20);
  const digestView = new DataView(digest.buffer);
  for (const [index, word] of [h0, h1, h2, h3, h4].entries()) {
    digestView.setUint32(index * 4, word, false);
  }
  return digest;
}

function uuidV5UrlBytes(value: string): Uint8Array {
  const valueBytes = new TextEncoder().encode(value);
  const namespaced = new Uint8Array(UUID_URL_NAMESPACE.length + valueBytes.length);
  namespaced.set(UUID_URL_NAMESPACE);
  namespaced.set(valueBytes, UUID_URL_NAMESPACE.length);
  const bytes = sha1(namespaced).slice(0, 16);
  bytes[6] = (bytes[6]! & 0x0f) | 0x50;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  return bytes;
}

/** @internal Canonical core author identity for an authenticated subject. */
export function runtimeAuthorBytesForSubject(subject: string): Uint8Array {
  return uuidBytes(subject) ?? uuidV5UrlBytes(subject);
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
    ? runtimeAuthorBytesForSubject(subject)
    : deterministicRuntimeBytes(`${seed}:author`);

  return { node, author };
}

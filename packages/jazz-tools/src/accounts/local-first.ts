import { sha1 } from "@noble/hashes/legacy.js";
import { parseAuthSecret } from "../runtime/auth-secret-codec.js";
import { generateAuthSecret } from "../runtime/auth-secret-store.js";
import { parseJwtPayload } from "../runtime/client-session.js";
import type { LocalFirstAccountFactory } from "./enrollment.js";

const encoder = new TextEncoder();
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const DNS_NAMESPACE = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
function uuidV5(namespace: string, name: string): string {
  if (!UUID_PATTERN.test(namespace)) throw new Error("Invalid account namespace");
  const raw = namespace.replaceAll("-", "");
  const ns = Uint8Array.from({ length: 16 }, (_, i) =>
    Number.parseInt(raw.slice(i * 2, i * 2 + 2), 16),
  );
  const text = encoder.encode(name);
  const bytes = new Uint8Array(16 + text.length);
  bytes.set(ns);
  bytes.set(text, 16);
  const digest = sha1(bytes).slice(0, 16);
  digest[6] = (digest[6]! & 0x0f) | 0x50;
  digest[8] = (digest[8]! & 0x3f) | 0x80;
  const hex = Array.from(digest, (value) => value.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
/** Same namespace normalization as Rust AppId and local-first JWT admission. */
export function accountAppId(appId: string): string {
  return UUID_PATTERN.test(appId) ? appId.toLowerCase() : uuidV5(DNS_NAMESPACE, appId);
}
/** Deterministic identifier only; not proof of control of the supplied subject. */
export function localFirstAccountId(appId: string, subject: string): string {
  return uuidV5(accountAppId(appId), `jazz-account-founder-v1\0${subject}`);
}

/** Hosts provide the existing Rust-backed synchronous token minting function. */
export function localFirstFactory(options: {
  appId: string;
  mintToken(secret: string, audience: string): string;
  retainSecret(secret: string): void | Promise<void>;
  generateSecret?(): string;
}): LocalFirstAccountFactory {
  const restore = (secret: string) => {
    parseAuthSecret(secret);
    const token = options.mintToken(secret, options.appId);
    const payload = parseJwtPayload(token);
    if (payload?.iss !== "urn:jazz:local-first" || typeof payload.sub !== "string") {
      throw new Error("Native runtime returned an invalid local-first identity");
    }
    const retained = Promise.resolve(options.retainSecret(secret));
    // A synchronous handle may exist before asynchronous platform persistence
    // finishes, but no context can use its key before durable retention.
    // Observe rejection immediately even if the app never opens a context.
    void retained.catch(() => {});
    const getToken = async () => {
      await retained;
      return options.mintToken(secret, options.appId);
    };
    return {
      accountId: localFirstAccountId(options.appId, payload.sub),
      identity: { issuer: payload.iss, subject: payload.sub },
      auth: { getToken },
    };
  };
  return { create: () => restore((options.generateSecret ?? generateAuthSecret)()), restore };
}

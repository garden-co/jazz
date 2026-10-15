import type { AuthorSubject, RowUuid, Value } from "../_extern/db-core.js";
import {
  AuthorSubjectT,
  MAX_SAFE_JS_INTEGER,
  RowUuidT,
  canonicalPolicyBindingClaims,
  jsonValueToPolicyClaim,
} from "../_extern/db-core.js";
import type { ReadTier } from "./db.js";

export type DurabilityTier = "None" | "Local" | "Global";

export function rowUuid(bytes: Uint8Array): RowUuid {
  if (bytes.length !== 16) throw new Error("row id must be 16 bytes");
  return RowUuidT.fromBytes(bytes);
}

export function safeU64(value: number): number | undefined {
  return Number.isFinite(value) &&
    value >= 0 &&
    Number.isInteger(value) &&
    value <= MAX_SAFE_JS_INTEGER
    ? value
    : undefined;
}

export function checkedU64(value: number, name: string): number {
  const v = safeU64(value);
  if (v === undefined) throw new Error(`${name} must be a nonnegative safe integer`);
  return v;
}

export function authorSubject(bytes: Uint8Array): AuthorSubject {
  let canonical: string;
  try {
    canonical = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    throw new Error("author subject must be canonical UTF-8 JSON");
  }
  return AuthorSubjectT.fromUntrustedCanonical(canonical);
}

export function durabilityTier(tier: string): DurabilityTier {
  switch (tier) {
    case "None":
    case "none":
      return "None";
    case "Local":
    case "local":
      return "Local";
    case "Global":
    case "global":
      return "Global";
    default:
      throw new Error(`unknown durability tier ${tier}`);
  }
}

export function readTier(tier: string): ReadTier {
  switch (tier) {
    case "local-first":
    case "LocalFirst":
      return "LocalFirst";
    case "remote":
    case "Remote":
      return "Remote";
    case "local-only":
    case "LocalOnly":
      return "LocalOnly";
    default:
      throw new Error(`unknown read tier ${tier}`);
  }
}

export function policyClaims(author: AuthorSubject, claims: unknown): Map<string, Value> {
  const projected = new Map<string, Value>();
  if (claims !== null && claims !== undefined) {
    if (typeof claims !== "object" || Array.isArray(claims))
      throw new Error("identity claims must be an object");
    for (const key of Object.keys(claims as object).sort()) {
      const value = jsonValueToPolicyClaim((claims as any)[key], "JavaScript");
      if (value !== undefined) projected.set(key, value);
    }
  }
  return canonicalPolicyBindingClaims(author, projected);
}

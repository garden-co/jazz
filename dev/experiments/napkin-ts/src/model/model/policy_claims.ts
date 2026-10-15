// Shared conversion rules for JSON session claims at public transport boundaries.
import { SortedMap, cmpStr } from "../../support/collections.js";
import {
  AuthorSubject,
  RowAuthor,
  providerClaimKey,
  authorClaimPathKey,
  parseUuid,
  gvalueEquals,
  type GValue,
} from "../../_extern/model-core.js";

export type Claims = SortedMap<string, GValue>;

const gstr = (value: string): GValue => ({ kind: "String", value });

/** The one canonical policy-binding vocabulary for an admitted identity. */
export function canonicalPolicyBindingClaims(author: any, providerClaims: Claims): Claims {
  const [issuer, subject] = author.principalParts();
  const admitted: Claims = new SortedMap(cmpStr);
  for (const [name, value] of providerClaims) admitted.set(providerClaimKey(name), value);
  admitted.set(providerClaimKey("iss"), gstr(issuer));
  admitted.set(providerClaimKey("sub"), gstr(subject));
  for (const [k, v] of authorPolicyClaims(author)) admitted.set(k, v);
  admitted.set("authMode", gstr(authModeForAuthorIssuer(issuer)));
  return admitted;
}

/** Reserved structured author bindings, derived only from admitted identity. */
export function authorPolicyClaims(author: any): Claims {
  let rowAuthor: any;
  try {
    rowAuthor = RowAuthor.fromPersistedSubject(author);
  } catch {
    rowAuthor = undefined;
  }
  const value: GValue = rowAuthor ? rowAuthor.toValue() : author.toValue();
  if (value.kind !== "Record") throw new Error("internal error: entered unreachable code");
  const record = value.value;
  let account: GValue = record.get("account");
  if (account === undefined) throw new Error("author account field");
  if (account.kind === "Nullable" && account.value !== undefined) account = account.value;
  const identity: GValue = record.get("identity");
  if (identity === undefined) throw new Error("author identity field");
  if (identity.kind !== "Record") throw new Error("internal error: entered unreachable code");
  const issuer = identity.value.get("issuer");
  if (issuer === undefined) throw new Error("author issuer field");
  const subject = identity.value.get("subject");
  if (subject === undefined) throw new Error("author subject field");
  const claims: Claims = new SortedMap(cmpStr, [
    ["user.identity", identity],
    ["user.identity.issuer", issuer],
    ["user.identity.subject", subject],
  ]);
  if (rowAuthor) {
    claims.set("user", value);
    claims.set("user.account", account);
  }
  return claims;
}

function authModeForAuthorIssuer(issuer: string): string {
  if (issuer === AuthorSubject.LOCAL_FIRST_ISSUER) return "local-first";
  if (issuer === AuthorSubject.ANONYMOUS_ISSUER) return "anonymous";
  return "external";
}

export const MAX_SAFE_JS_INTEGER = 9_007_199_254_740_991;

export type NumericClaimOrigin = "JavaScript" | "ExactJson";

/** Classify a JSON number for policy evaluation. */
export function jsonNumberToPolicyClaim(n: number | bigint, origin: NumericClaimOrigin): GValue {
  if (typeof n === "bigint") {
    if (n >= 0n)
      return origin === "ExactJson" || n <= BigInt(MAX_SAFE_JS_INTEGER)
        ? { kind: "U64", value: n }
        : { kind: "F64", value: Number(n) };
    return origin === "ExactJson" || -n <= BigInt(MAX_SAFE_JS_INTEGER)
      ? { kind: "I64", value: n }
      : { kind: "F64", value: Number(n) };
  }
  if (!Number.isFinite(n)) throw new Error("unsupported numeric claim");
  if (n % 1 === 0 && Math.abs(n) <= MAX_SAFE_JS_INTEGER)
    return n < 0 ? { kind: "I64", value: BigInt(n) } : { kind: "U64", value: BigInt(n) };
  return { kind: "F64", value: n };
}

/** Project JSON provider values into the policy value corpus. */
export function jsonValueToPolicyClaim(
  value: unknown,
  origin: NumericClaimOrigin,
): GValue | undefined {
  if (value === null) return { kind: "Nullable", value: undefined };
  if (typeof value === "boolean") return { kind: "Bool", value };
  if (typeof value === "number" || typeof value === "bigint")
    return jsonNumberToPolicyClaim(value, origin);
  if (typeof value === "string") {
    const uuid = parseUuid(value);
    return uuid !== undefined ? { kind: "Uuid", value: uuid } : gstr(value);
  }
  if (Array.isArray(value)) {
    const projected: GValue[] = [];
    for (const v of value) {
      const p = jsonValueToPolicyClaim(v, origin);
      if (p === undefined) return undefined;
      projected.push(p);
    }
    return { kind: "Array", value: projected };
  }
  if (typeof value === "object") {
    const entries: GValue[] = [];
    for (const name of Object.keys(value as object).sort(cmpStr)) {
      const p = jsonValueToPolicyClaim((value as any)[name], origin);
      if (p === undefined) return undefined;
      entries.push({ kind: "Tuple", value: [gstr(name), p] });
    }
    return { kind: "Tuple", value: entries };
  }
  return undefined;
}

/** Resolve literal path segments, never splitting provider keys on dots. */
export function policyClaimAtPath(claims: Claims, path: string[]): GValue | undefined {
  const authorKey = authorClaimPathKey(path);
  if (authorKey !== undefined) return claims.get(authorKey);
  let root: string;
  let tail: string[];
  if (path.length >= 2 && path[0] === "claims") {
    root = providerClaimKey(path[1]);
    tail = path.slice(2);
  } else if (path.length === 1) {
    root = path[0];
    tail = [];
  } else return undefined;
  let value = claims.get(root);
  for (const segment of tail) {
    if (value?.kind !== "Tuple") return undefined;
    value = (value.value as GValue[]).find(
      (e) => e.kind === "Tuple" && e.value.length === 2 && gvalueEquals(e.value[0], gstr(segment)),
    )?.value[1];
    if (value === undefined) return undefined;
  }
  return value;
}

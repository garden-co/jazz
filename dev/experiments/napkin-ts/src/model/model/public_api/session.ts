import { fromBase64Url, fromUtf8 } from "../../../support/bytes.js";
import {
  AuthorSubject,
  AuthorSubjectError,
  accountIsSystem,
  type AccountId,
} from "../../../_extern/model-core.js";
import { SYSTEM_PRINCIPAL_ID } from "../metadata.js";
import type { OpenTransactionId } from "../transaction.js";

export type AuthMode = "external" | "local-first" | "anonymous";

/**
 * Non-authoritative identity projection used only to partition a local client
 * cache. Decodes the JWT payload without verifying it.
 */
export function unverifiedJwtScopeSubject(jwt: string): [string, string] | undefined {
  const payload = jwt.trim().split(".")[1];
  if (payload === undefined) return undefined;
  try {
    const claims = JSON.parse(fromUtf8(fromBase64Url(payload)));
    if (typeof claims?.iss !== "string" || typeof claims?.sub !== "string") return undefined;
    if (!claims.iss.trim() || !claims.sub.trim()) return undefined;
    return [claims.iss, claims.sub];
  } catch {
    return undefined;
  }
}

const isAuthModePath = (path: readonly string[]) =>
  path.length === 1 && (path[0] === "auth_mode" || path[0] === "authMode");

/** Session context for policy evaluation. */
export class Session {
  account_id?: AccountId;
  claims: any = {};
  auth_mode: AuthMode = "external";
  constructor(
    public issuer: string,
    public user_id: string,
  ) {}

  static fromJson(json: any): Session {
    const s = new Session(json.issuer, json.user_id);
    s.account_id = json.account_id ?? json.accountId ?? undefined;
    s.claims = json.claims;
    s.auth_mode = json.auth_mode ?? json.authMode ?? "external";
    return s;
  }

  withClaims(claims: any): this {
    this.claims = claims;
    return this;
  }
  withAuthMode(mode: AuthMode): this {
    this.auth_mode = mode;
    return this;
  }

  authorSubject(): any {
    let principal;
    if (this.auth_mode === "external")
      principal = AuthorSubject.authenticated(this.issuer, this.user_id);
    else if (this.auth_mode === "local-first" && this.issuer === AuthorSubject.LOCAL_FIRST_ISSUER)
      principal = AuthorSubject.reserved(this.issuer, this.user_id);
    else if (this.auth_mode === "anonymous" && this.issuer === AuthorSubject.ANONYMOUS_ISSUER)
      principal = AuthorSubject.reserved(this.issuer, this.user_id);
    else throw AuthorSubjectError.reservedIssuer(this.issuer);
    if (this.account_id === undefined) return principal;
    if (accountIsSystem(this.account_id)) throw AuthorSubjectError.reservedAccount();
    return principal.withAccount(this.account_id);
  }

  getPath(path: readonly string[]): unknown {
    if (!path.length || isAuthModePath(path)) return undefined;
    if (path[0] !== "claims") return undefined;
    let cur: any = this.claims;
    for (const seg of path.slice(1)) {
      if (cur === null || typeof cur !== "object" || Array.isArray(cur)) return undefined;
      if (!(seg in cur)) return undefined;
      cur = cur[seg];
    }
    return cur;
  }
  getUserId(): string {
    return this.user_id;
  }
  getArray(path: readonly string[]): unknown[] | undefined {
    const v = this.getPath(path);
    return Array.isArray(v) ? v : undefined;
  }
  hasPath(path: readonly string[]): boolean {
    if (!path.length) return false;
    if (isAuthModePath(path)) return true;
    return this.getPath(path) !== undefined;
  }
  getString(path: readonly string[]): string | undefined {
    if (!path.length) return undefined;
    if (isAuthModePath(path)) return this.auth_mode;
    const v = this.getPath(path);
    return typeof v === "string" ? v : undefined;
  }
  arrayContainsString(path: readonly string[], value: string): boolean {
    return this.getArray(path)?.some((v) => v === value) ?? false;
  }
}

/** Write-scoped context for mutations. */
export type WriteContext = {
  session?: Session;
  attribution?: string;
  updated_at?: number;
  transaction_id?: OpenTransactionId;
  target_branch_name?: string;
};

export const writeContextFromSession = (session: Session): WriteContext => ({ session });

export const authorPrincipal = (ctx: WriteContext): string =>
  ctx.attribution ?? ctx.session?.user_id ?? SYSTEM_PRINCIPAL_ID;

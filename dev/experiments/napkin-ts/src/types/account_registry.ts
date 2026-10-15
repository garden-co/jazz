import { SortedMap, cmpStr } from "../support/collections.js";
import { concatBytes, utf8 } from "../support/bytes.js";
import { uuidFromString, uuidToString, uuidV5 } from "../support/hash.js";
import { AuthorSubject, NIL_UUID, tryParseUuid } from "./ids.js";
import { LOCAL_FIRST_ISSUER } from "./identity.js";

export interface Principal {
  issuer: string;
  subject: string;
}
export const comparePrincipal = (a: Principal, b: Principal): number =>
  cmpStr(a.issuer, b.issuer) || cmpStr(a.subject, b.subject);
const principalEq = (a: Principal, b: Principal) =>
  a.issuer === b.issuer && a.subject === b.subject;

export function validatePrincipal(p: Principal, allowLocalFirst: boolean): void {
  if (allowLocalFirst && p.issuer === LOCAL_FIRST_ISSUER) {
    if (tryParseUuid(p.subject) !== undefined) return;
    throw new AccountError("InvalidPrincipal");
  }
  try {
    AuthorSubject.authenticated(p.issuer, p.subject);
  } catch {
    throw new AccountError("InvalidPrincipal");
  }
}

/** Stable account identity (UUID string). */
export type AccountId = string;
export const SYSTEM_ACCOUNT_ID: AccountId = NIL_UUID;
export const accountIsSystem = (a: AccountId): boolean => a === SYSTEM_ACCOUNT_ID;

/** Deterministic founding account, scoped to the application's registry. */
export function localFirstAccountId(app: string, subject: string): AccountId {
  const name = concatBytes(utf8("jazz-account-founder-v1\0"), utf8(subject));
  return uuidToString(uuidV5(uuidFromString(app), name));
}

export interface Assignment {
  account: AccountId;
  active: boolean;
  manageIdentities: boolean;
  generation: number;
}

interface LinkIntent {
  account: AccountId;
  approver: Principal;
  approverGeneration: number;
  candidate: Principal;
  expiresAt: number;
  completed: boolean;
}

export type AccountErrorKind =
  | "InvalidPrincipal"
  | "ReservedAccount"
  | "AlreadyAssigned"
  | "NotAssigned"
  | "NotAuthorized"
  | "InvalidIntent"
  | "Expired"
  | "IdentifierCollision";

const ACCOUNT_ERROR_MESSAGES: Record<AccountErrorKind, string> = {
  InvalidPrincipal: "invalid account principal",
  ReservedAccount: "reserved account identifier",
  AlreadyAssigned: "identity is already assigned",
  NotAssigned: "identity is not assigned",
  NotAuthorized: "identity is not authorized",
  InvalidIntent: "invalid linking intent",
  Expired: "linking intent expired",
  IdentifierCollision: "identifier already exists",
};

export class AccountError extends Error {
  constructor(readonly kind: AccountErrorKind) {
    super(ACCOUNT_ERROR_MESSAGES[kind]);
  }
}

export type AccountCommand =
  | { kind: "FoundLocalFirst"; principal: Principal; app: string }
  | { kind: "Register"; principal: Principal; account: AccountId }
  | {
      kind: "RequestLink";
      approver: Principal;
      candidate: Principal;
      nonce: string;
      now: number;
      expiresAt: number;
    }
  | { kind: "AcceptLink"; candidate: Principal; nonce: string; now: number }
  | { kind: "Revoke"; approver: Principal; target: Principal };

export type AccountCommandResult =
  | { kind: "Assignment"; assignment: Assignment }
  | { kind: "Recorded" };

/** Registry decisions are applied serially by one application authority. */
export class AccountRegistry {
  private assignments = new SortedMap<Principal, Assignment>(comparePrincipal);
  private intents = new Map<string, LinkIntent>();

  clone(): AccountRegistry {
    const c = new AccountRegistry();
    for (const [k, v] of this.assignments) c.assignments.set(k, { ...v });
    for (const [k, v] of this.intents) c.intents.set(k, { ...v });
    return c;
  }

  apply(command: AccountCommand): AccountCommandResult {
    const assignment = (a: Assignment): AccountCommandResult => ({
      kind: "Assignment",
      assignment: a,
    });
    switch (command.kind) {
      case "FoundLocalFirst": {
        const { principal, app } = command;
        validatePrincipal(principal, true);
        if (principal.issuer !== LOCAL_FIRST_ISSUER) throw new AccountError("InvalidPrincipal");
        const account = localFirstAccountId(app, principal.subject);
        const existing = this.assignments.get(principal);
        if (existing) {
          if (existing.account !== account) throw new AccountError("AlreadyAssigned");
          return assignment({ ...this.login(principal) });
        }
        return assignment(this.registerInner(principal, account));
      }
      case "Register":
        return assignment(this.register(command.principal, command.account));
      case "RequestLink":
        this.requestLink(
          command.approver,
          command.candidate,
          command.nonce,
          command.now,
          command.expiresAt,
        );
        return { kind: "Recorded" };
      case "AcceptLink":
        return assignment(this.acceptLink(command.candidate, command.nonce, command.now));
      case "Revoke":
        this.revoke(command.approver, command.target);
        return { kind: "Recorded" };
    }
  }

  login(principal: Principal): Assignment {
    const a = this.assignments.get(principal);
    if (!a) throw new AccountError("NotAssigned");
    if (!a.active) throw new AccountError("NotAuthorized");
    return a;
  }

  register(principal: Principal, account: AccountId): Assignment {
    validatePrincipal(principal, false);
    return this.registerInner(principal, account);
  }

  private registerInner(principal: Principal, account: AccountId): Assignment {
    if (accountIsSystem(account)) throw new AccountError("ReservedAccount");
    if (this.assignments.has(principal)) throw new AccountError("AlreadyAssigned");
    for (const b of this.assignments.valuesArray())
      if (b.account === account) throw new AccountError("IdentifierCollision");
    const a: Assignment = { account, active: true, manageIdentities: true, generation: 0 };
    this.assignments.set(principal, a);
    return { ...a };
  }

  requestLink(
    approver: Principal,
    candidate: Principal,
    nonce: string,
    now: number,
    expiresAt: number,
  ): void {
    validatePrincipal(candidate, false);
    const a = this.login(approver);
    if (!a.manageIdentities) throw new AccountError("NotAuthorized");
    if (expiresAt <= now) throw new AccountError("Expired");
    if (this.assignments.has(candidate)) throw new AccountError("AlreadyAssigned");
    if (this.intents.has(nonce)) throw new AccountError("IdentifierCollision");
    this.intents.set(nonce, {
      account: a.account,
      approver,
      approverGeneration: a.generation,
      candidate,
      expiresAt,
      completed: false,
    });
  }

  acceptLink(candidate: Principal, nonce: string, now: number): Assignment {
    const intent = this.intents.get(nonce);
    if (!intent || !principalEq(intent.candidate, candidate))
      throw new AccountError("InvalidIntent");
    if (intent.completed) return { ...this.login(candidate) };
    if (now >= intent.expiresAt) throw new AccountError("Expired");
    const approver = this.login(intent.approver);
    if (
      !approver.manageIdentities ||
      approver.account !== intent.account ||
      approver.generation !== intent.approverGeneration
    )
      throw new AccountError("NotAuthorized");
    if (this.assignments.has(candidate)) throw new AccountError("AlreadyAssigned");
    const a: Assignment = {
      account: intent.account,
      active: true,
      manageIdentities: true,
      generation: 0,
    };
    this.assignments.set(candidate, a);
    intent.completed = true;
    return { ...a };
  }

  revoke(approver: Principal, target: Principal): void {
    const approval = this.login(approver);
    const binding = this.assignments.get(target);
    if (!binding) throw new AccountError("NotAssigned");
    if (!approval.manageIdentities || approval.account !== binding.account)
      throw new AccountError("NotAuthorized");
    if (binding.active) {
      binding.active = false;
      binding.generation += 1;
    }
  }
}

import { KeyedMap, SortedMap, SortedSet, cmpTuple } from "../support/collections.js";
import { equalBytes } from "../support/bytes.js";
import {
  compareBindingId,
  compareShapeId,
  type BindingId,
  type ShapeId,
} from "../_extern/types-protocol.js";
import type { AuthorSubject } from "../types/ids.js";
import type { GlobalTime } from "../types/time.js";
import {
  authorizationScopeKeyString,
  compareSubscriptionKey,
  type AuthorizationScopeReceipt,
  type AuthorizationSupportScopeKey,
  type SubscriptionKey,
} from "./protocol.js";

export const MAX_AUTHORIZATION_SCOPES = 256;

type Clause = [ShapeId, BindingId];
const cmpClause = cmpTuple(compareShapeId, compareBindingId) as (a: Clause, b: Clause) => number;
const clauseEq = (a: Clause, b: Clause) => cmpClause(a, b) === 0;

export class AuthorityScopeAggregate {
  members = new SortedMap<SubscriptionKey, Clause>(compareSubscriptionKey);
  applied = new SortedMap<SubscriptionKey, [GlobalTime, number]>(compareSubscriptionKey);
  constructor(readonly expectedSupport: SortedSet<Clause>) {}

  register(subscription: SubscriptionKey, clause: Clause): boolean {
    if (
      !this.expectedSupport.has(clause) ||
      this.members.has(subscription) ||
      this.members.valuesArray().some((m) => clauseEq(m, clause))
    )
      return false;
    this.members.set(subscription, clause);
    return true;
  }
  forget(subscription: SubscriptionKey): void {
    this.members.delete(subscription);
    this.applied.delete(subscription);
  }
  hasNoMembers(): boolean {
    return this.members.size === 0;
  }
  apply(
    subscription: SubscriptionKey,
    settledThrough: GlobalTime,
    progress: number,
  ): [GlobalTime, number] | undefined {
    if (!this.members.has(subscription)) return undefined;
    this.applied.set(subscription, [settledThrough, progress]);
    return this.bounds();
  }
  bounds(): [GlobalTime, number] | undefined {
    const members = this.members.valuesArray();
    if (
      this.members.size !== this.expectedSupport.size ||
      this.expectedSupport.toArray().some((e) => !members.some((m) => clauseEq(m, e))) ||
      this.members.keysArray().some((k) => !this.applied.has(k))
    )
      return undefined;
    const applied = this.applied.valuesArray();
    if (applied.length === 0) return undefined;
    let settled = applied[0][0];
    let progress = applied[0][1];
    for (const [s, p] of applied) {
      if (s < settled) settled = s;
      if (p < progress) progress = p;
    }
    return [settled, progress];
  }
}

export interface AuthorityContext {
  authority: Uint8Array;
  link: AuthorSubject;
  connectionId: number;
  connectionEpoch: number;
  claimsRevision: number;
  policyEpoch: number;
  authorizationProgress: number;
  settledThrough: bigint;
}
export const sameAdmittedLink = (a: AuthorityContext, b: AuthorityContext): boolean =>
  equalBytes(a.authority, b.authority) &&
  a.link.equals(b.link) &&
  a.connectionId === b.connectionId &&
  a.connectionEpoch === b.connectionEpoch;

export type AuthorizationScopeOwnerToken = number;
export type AuthorizationScopeAcquisition =
  | { kind: "Owner"; token: AuthorizationScopeOwnerToken }
  | { kind: "Waiting" }
  | { kind: "Proven" };
export type AuthorizationScopeReadiness =
  | { kind: "Proven"; receipt: AuthorizationScopeReceipt }
  | { kind: "Owner"; token: AuthorizationScopeOwnerToken }
  | { kind: "Waiting" };
export type AuthorizationScopeInstall =
  | { kind: "Installed" }
  | { kind: "Retry"; token: AuthorizationScopeOwnerToken }
  | { kind: "Waiting" }
  | { kind: "Rejected" };

type Phase =
  | { kind: "Idle" }
  | { kind: "Hydrating"; generation: number; owner: number }
  | { kind: "Proven" };
interface Entry {
  refs: number;
  phase: Phase;
  receipt?: AuthorizationScopeReceipt;
  waiters: number[];
  nextGeneration: number;
}
interface State {
  scopes: KeyedMap<AuthorizationSupportScopeKey, Entry>;
  nextLease: number;
}

export class AuthorizationScopeLease {
  ownerGeneration?: number;
  constructor(
    readonly state: State,
    readonly key: AuthorizationSupportScopeKey,
    readonly id: number,
  ) {}

  /** Rust: Drop. */
  release(): void {
    const e = this.state.scopes.get(this.key);
    if (!e) return;
    if (this.ownerGeneration === undefined) e.waiters = e.waiters.filter((w) => w !== this.id);
    else if (
      e.phase.kind === "Hydrating" &&
      e.phase.generation === this.ownerGeneration &&
      e.phase.owner === this.id
    )
      e.phase = { kind: "Idle" };
    e.refs -= 1;
    if (e.refs === 0) this.state.scopes.delete(this.key);
  }
}

export class AuthorizationScopeRegistry {
  private readonly state: State = {
    scopes: new KeyedMap(authorizationScopeKeyString),
    nextLease: 0,
  };

  acquire(
    key: AuthorizationSupportScopeKey,
  ): [AuthorizationScopeLease, AuthorizationScopeAcquisition] | undefined {
    const s = this.state;
    if (!s.scopes.has(key) && s.scopes.size >= MAX_AUTHORIZATION_SCOPES) return undefined;
    const id = s.nextLease++;
    const e = s.scopes.getOrInsert(key, () => ({
      refs: 0,
      phase: { kind: "Idle" },
      waiters: [],
      nextGeneration: 1,
    }));
    e.refs += 1;
    const lease = new AuthorizationScopeLease(s, key, id);
    let acquisition: AuthorizationScopeAcquisition;
    switch (e.phase.kind) {
      case "Idle": {
        const token = start(e, id);
        lease.ownerGeneration = token;
        acquisition = { kind: "Owner", token };
        break;
      }
      case "Hydrating":
        e.waiters.push(id);
        acquisition = { kind: "Waiting" };
        break;
      case "Proven":
        acquisition = { kind: "Proven" };
    }
    return [lease, acquisition];
  }

  takePromotion(lease: AuthorizationScopeLease): AuthorizationScopeOwnerToken | undefined {
    if (lease.state !== this.state) return undefined;
    const e = this.state.scopes.get(lease.key);
    if (!e || e.phase.kind !== "Idle" || e.waiters[0] !== lease.id) return undefined;
    e.waiters.shift();
    const token = start(e, lease.id);
    lease.ownerGeneration = token;
    return token;
  }

  install(
    lease: AuthorizationScopeLease,
    token: AuthorizationScopeOwnerToken,
    ctx: AuthorityContext,
    receipt: AuthorizationScopeReceipt,
  ): AuthorizationScopeInstall {
    if (
      lease.state !== this.state ||
      authorizationScopeKeyString(receipt.key) !== authorizationScopeKeyString(lease.key) ||
      lease.ownerGeneration !== token
    )
      return { kind: "Rejected" };
    const e = this.state.scopes.get(receipt.key);
    if (!e) return { kind: "Rejected" };
    if (
      !(e.phase.kind === "Hydrating" && e.phase.generation === token && e.phase.owner === lease.id)
    )
      return { kind: "Rejected" };
    if (receiptMatches(receipt, ctx, ctx.authorizationProgress, ctx.settledThrough)) {
      e.phase = { kind: "Proven" };
      e.receipt = receipt;
      lease.ownerGeneration = undefined;
      return { kind: "Installed" };
    }
    return retryOrIdle(e, lease);
  }

  receipt(
    lease: AuthorizationScopeLease,
    ctx: AuthorityContext,
    requiredProgress: number,
    requiredCut: bigint,
  ): AuthorizationScopeReadiness {
    if (lease.state !== this.state) return { kind: "Waiting" };
    const e = this.state.scopes.get(lease.key);
    if (!e) return { kind: "Waiting" };
    if (e.receipt) {
      if (receiptMatches(e.receipt, ctx, requiredProgress, requiredCut))
        return { kind: "Proven", receipt: e.receipt };
      e.receipt = undefined;
      e.phase = { kind: "Idle" };
    }
    if (e.phase.kind === "Idle" && e.waiters.length === 0) {
      const token = start(e, lease.id);
      lease.ownerGeneration = token;
      return { kind: "Owner", token };
    }
    if (e.phase.kind === "Hydrating" && e.phase.owner === lease.id)
      return { kind: "Owner", token: e.phase.generation };
    return { kind: "Waiting" };
  }

  len(): number {
    return this.state.scopes.size;
  }
  isEmpty(): boolean {
    return this.state.scopes.size === 0;
  }
}

function receiptMatches(
  r: AuthorizationScopeReceipt,
  ctx: AuthorityContext,
  progress: number,
  cut: bigint,
): boolean {
  return (
    equalBytes(r.authority, ctx.authority) &&
    r.link.equals(ctx.link) &&
    r.authorityEpoch === ctx.connectionEpoch &&
    r.claimsRevision === ctx.claimsRevision &&
    r.policyEpoch === ctx.policyEpoch &&
    r.authorizationProgress >= Math.max(ctx.authorizationProgress, progress) &&
    r.settledThrough >= (ctx.settledThrough > cut ? ctx.settledThrough : cut)
  );
}

function start(e: Entry, owner: number): number {
  const generation = e.nextGeneration++;
  e.phase = { kind: "Hydrating", generation, owner };
  return generation;
}

function retryOrIdle(e: Entry, lease: AuthorizationScopeLease): AuthorizationScopeInstall {
  e.receipt = undefined;
  e.phase = { kind: "Idle" };
  if (e.waiters.length === 0) {
    const retry = start(e, lease.id);
    lease.ownerGeneration = retry;
    return { kind: "Retry", token: retry };
  }
  lease.ownerGeneration = undefined;
  return { kind: "Waiting" };
}

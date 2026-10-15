// Peer-local subscription and relay-authority support state.
import { SortedSet } from "../../support/collections.js";
import {
  type AuthorSubject,
  type AuthorityResultKey,
  type GlobalTime,
  type KnownStateDeclaration,
  type ResultMemberEntry,
  type TxId,
  LocalAuthorityReconciliation,
  cmpGlobalTime,
  cmpResultMemberEntry,
  cmpTxId,
} from "../../_extern/node-policy-peer.js";

export function fastCurrentMembershipPosition(
  ks: KnownStateDeclaration | undefined,
): GlobalTime | undefined {
  if (!ks) return undefined;
  switch (ks.kind) {
    case "Fast":
    case "FastWithAuthorizationProgress":
      return ks.completeness === "FastCurrentMembership" ? ks.position : undefined;
    case "Watermark":
      return ks.position;
  }
  return undefined;
}

export function fastAuthorizationProgress(
  ks: KnownStateDeclaration | undefined,
): number | undefined {
  if (!ks) return undefined;
  switch (ks.kind) {
    case "FastWithAuthorizationProgress":
      return ks.completeness === "FastCurrentMembership" ? ks.authorizationProgress : undefined;
    case "Watermark":
      return ks.authorizationProgress;
  }
  return undefined;
}

export function memberSettlePosition(member: ResultMemberEntry): GlobalTime | undefined {
  switch (member.kind) {
    case "Row":
      return member.settlePosition;
    case "TypedRow":
      return member.row.settlePosition;
  }
  return undefined;
}

export function fastCursorRequiresAuthoritativeReset(
  position: GlobalTime,
  previous: SortedSet<ResultMemberEntry>,
  current: SortedSet<ResultMemberEntry>,
): boolean {
  for (const m of previous) if (!current.has(m)) return true;
  for (const m of current) {
    if (previous.has(m)) continue;
    const settled = memberSettlePosition(m);
    if (settled === undefined || cmpGlobalTime(settled, position) <= 0) return true;
  }
  return false;
}

export type PeerRole = { kind: "Relay" } | { kind: "ClientLink"; identity: AuthorSubject };
export const permissionSubjectOfRole = (role: PeerRole): AuthorSubject | undefined =>
  role.kind === "Relay" ? undefined : role.identity;

export type RowKey = [any, string, any];
export type MemberIndexKey = string;
export type MemberSlot = { member: ResultMemberEntry; refcount: number };
export type PolicyBinding = [AuthorSubject, Map<string, any>];

export class PeerSubscriptionState {
  policyBinding?: PolicyBinding;
  authorityResultSource?: AuthorityResultKey;
  awaitingSelectedAuthoritySource = false;
  resultMemberSet = new SortedSet<ResultMemberEntry>(cmpResultMemberEntry);
  supportingRevision?: Uint8Array;
  localAuthority: any = new LocalAuthorityReconciliation();
  memberIndex = new Map<MemberIndexKey, MemberSlot>();
  maintainedSubscriptionView?: MaintainedSubscriptionViewSubscription;
  preparedQuery?: CachedPeerQueryPlan;
  grooveRuntimeToken?: number;
  physicalIdentityGeneration?: number;
  authorizationProgress = 0;
  hasServedAuthorizationProgress = false;

  clearGrooveRuntimeHandles(): void {
    this.supportingRevision = undefined;
    this.maintainedSubscriptionView = undefined;
    this.preparedQuery?.clearRuntimePlan();
    this.grooveRuntimeToken = undefined;
    this.physicalIdentityGeneration = undefined;
  }

  runtimeIsStale(node: any): boolean {
    return (
      (this.grooveRuntimeToken !== undefined &&
        this.grooveRuntimeToken !== node.grooveRuntimeToken()) ||
      (this.physicalIdentityGeneration !== undefined &&
        this.physicalIdentityGeneration !== node.physicalIdentityGeneration())
    );
  }

  acknowledgePublished(revision: Uint8Array): void {
    this.supportingRevision = revision;
    this.maintainedSubscriptionView?.maintained.acknowledgePeerSourceClosure();
  }

  memberResultSet(): SortedSet<ResultMemberEntry> {
    return new SortedSet(cmpResultMemberEntry, this.resultMemberSet);
  }

  supportingRows(): any[] {
    return this.maintainedSubscriptionView
      ? [...this.maintainedSubscriptionView.maintained.supportingRows()]
      : [];
  }

  previousTxIds(): SortedSet<TxId> {
    const out = new SortedSet<TxId>(cmpTxId);
    if (this.supportingRevision === undefined || !this.maintainedSubscriptionView) return out;
    for (const row of this.maintainedSubscriptionView.maintained.acknowledgedSupportingRows())
      out.add(row.version.tx);
    return out;
  }
}

export type MaintainedSubscriptionViewSubscription = {
  subscription: any;
  maintained: any;
  terminalSchemas: any;
  tables: Map<string, any>;
  coveredInputReceiver?: any;
  resultSchemaVersion: any;
  sourceAuthorityResult?: AuthorityResultKey;
  initialReceived: boolean;
};

export type MaintainedRehydrateRequest = {
  shape: any;
  binding: any;
  subscription: any;
  previousMemberResultSet: SortedSet<ResultMemberEntry>;
  resetInputSet: boolean;
  resultTableFilter?: string;
  tier: any;
  readView: any;
  purpose: RehydratePurpose;
};

export type RehydratePurpose = "Query" | "AuthorizationSupport";

export class CachedPeerQueryPlan {
  constructor(
    readonly tier: any,
    readonly readView: any,
    private plan?: any,
  ) {}
  static withPlan(opts: any, plan: any): CachedPeerQueryPlan {
    return new CachedPeerQueryPlan(opts.tier, opts.readView, plan);
  }
  hasRuntimePlan(): boolean {
    return this.plan !== undefined;
  }
  replaceRuntimePlan(plan: any): void {
    this.plan = plan;
  }
  clearRuntimePlan(): void {
    this.plan = undefined;
  }
  context(): [any, any] {
    return [this.tier, this.readView];
  }
}

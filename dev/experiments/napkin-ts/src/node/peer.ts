// Per-peer server-side sync state: shipped-payload dedup and per-subscription
// incremental result maintenance for one downstream peer.
// Merges peer.rs, peer/known_state.rs, peer/repair.rs and the synchronous
// accessors of peer/publication.rs (the async publication paths live in
// ./peer/publication.ts).
import { SortedMap, SortedSet } from "../support/collections.js";
import {
  type AuthorSubject,
  type AuthorityResultKey,
  type KnownStateDeclaration,
  type ResultMemberEntry,
  type SubscriptionKey,
  type SyncMessage,
  type TxId,
  AUTHOR_SYSTEM,
  authorEquals,
  claimsEqual,
  cmpSubscriptionKey,
  cmpTxId,
  coveredInputTrace,
  defaultReadViewKey,
  nodeInvalidStoredValue,
  policyBindingKeyFromCanonicalParts,
  debugFmt,
} from "../_extern/node-policy-peer.js";
import {
  applyContributionAdd,
  applyContributionRemove,
  bundleContainsCompleteTxPayload,
  duplicatePhysicalRowResultSet,
  viewUpdateSingletonBundles,
} from "./peer/delivery.js";
import {
  type MaintainedSubscriptionViewSubscription,
  type PeerRole,
  type PolicyBinding,
  PeerSubscriptionState,
  permissionSubjectOfRole,
} from "./peer/subscription_state.js";

export type { PeerRole } from "./peer/subscription_state.js";

export type RelayTransportCapability =
  | { kind: "OrdinarySession" }
  | {
      kind: "ScopeIsolatedClientRelay";
      binding: { identity: AuthorSubject; claims: Map<string, any> };
      admissionEpoch: number;
    }
  | { kind: "MultiplexedRelay" };

export class FullDiffFallbackMetrics {
  membershipReconciliations = 0;
  queryReopens = 0;
  authorizationSupportReopens = 0;
  runtimeResets = 0;
  total(): number {
    return (
      this.membershipReconciliations +
      this.queryReopens +
      this.authorizationSupportReopens +
      this.runtimeResets
    );
  }
  add(other: FullDiffFallbackMetrics): void {
    this.membershipReconciliations += other.membershipReconciliations;
    this.queryReopens += other.queryReopens;
    this.authorizationSupportReopens += other.authorizationSupportReopens;
    this.runtimeResets += other.runtimeResets;
  }
}

export class MaintainedSubscriptionViewMetrics {
  hitsOut = 0;
  unsupportedSkipsOut = 0;
  deltaBatchesIn = 0;
  fullDiffFallbacks = new FullDiffFallbackMetrics();
}

export class PeerMetrics {
  viewUpdatesOut = 0;
  versionBundlesOut = 0;
  duplicateVersionBundlesOut = 0;
  completeTxPayloadRefsOut = 0;
  resultAddsOut = 0;
  resultRemovesOut = 0;
  maintainedSubscriptionView = new MaintainedSubscriptionViewMetrics();
}

let nextPublicationOwner = 1;

export class PeerState {
  publicationOwner = nextPublicationOwner++;
  role: PeerRole = { kind: "ClientLink", identity: AUTHOR_SYSTEM };
  permissionIdentity?: AuthorSubject;
  transportCapability: RelayTransportCapability = { kind: "OrdinarySession" };
  authorityQueryDelegate = false;
  shippedCompleteTxPayloads = new SortedSet<TxId>(cmpTxId);
  shipCompleteExclusivePayloads = false;
  publicationStates = new SortedMap<SubscriptionKey, PeerSubscriptionState>(cmpSubscriptionKey);
  downstreamKnownStates = new SortedMap<SubscriptionKey, KnownStateDeclaration>(cmpSubscriptionKey);
  announcedCatalogueFingerprint?: Uint8Array;
  metrics = new PeerMetrics();

  static relay(): PeerState {
    const p = new PeerState();
    p.role = { kind: "Relay" };
    p.transportCapability = { kind: "MultiplexedRelay" };
    return p;
  }

  static scopeIsolatedRelay(
    identity: AuthorSubject,
    claims: Map<string, any>,
    admissionEpoch: number,
  ): PeerState {
    const p = new PeerState();
    p.role = { kind: "Relay" };
    p.transportCapability = {
      kind: "ScopeIsolatedClientRelay",
      binding: { identity, claims },
      admissionEpoch,
    };
    return p;
  }

  static clientLink(identity: AuthorSubject): PeerState {
    const p = new PeerState();
    p.role = { kind: "ClientLink", identity };
    return p;
  }

  static clientLinkWithPermissionIdentity(
    identity: AuthorSubject,
    permissionIdentity: AuthorSubject,
  ): PeerState {
    const p = PeerState.clientLink(identity);
    p.permissionIdentity = permissionIdentity;
    return p;
  }

  stateFor(subscription: SubscriptionKey): PeerSubscriptionState {
    return this.publicationStates.getOrInsert(subscription, () => new PeerSubscriptionState());
  }

  // ---- catalogue / role accessors ----

  resetCatalogueSnapshotAnnouncement(): void {
    this.announcedCatalogueFingerprint = undefined;
  }
  hasAnnouncedCatalogueSnapshot(): boolean {
    return this.announcedCatalogueFingerprint !== undefined;
  }
  needsCatalogueSnapshot(fingerprint: Uint8Array): boolean {
    const a = this.announcedCatalogueFingerprint;
    return !a || a.length !== fingerprint.length || a.some((b, i) => b !== fingerprint[i]);
  }
  markCatalogueSnapshotAnnounced(fingerprint: Uint8Array): void {
    this.announcedCatalogueFingerprint = fingerprint;
  }

  admitsRelayBinding(binding: PolicyBinding): boolean {
    const t = this.transportCapability;
    return (
      t.kind === "ScopeIsolatedClientRelay" &&
      authorEquals(t.binding.identity, binding[0]) &&
      claimsEqual(t.binding.claims, binding[1])
    );
  }

  admittedScopeRelayBinding(): { identity: AuthorSubject; claims: Map<string, any> } | undefined {
    const t = this.transportCapability;
    return t.kind === "ScopeIsolatedClientRelay" ? t.binding : undefined;
  }

  refreshScopeRelayAdmissionEpoch(): boolean {
    const t = this.transportCapability;
    if (t.kind !== "ScopeIsolatedClientRelay") return false;
    if (t.admissionEpoch >= Number.MAX_SAFE_INTEGER)
      throw new Error("scope-isolated relay admission epoch exhausted");
    t.admissionEpoch += 1;
    return true;
  }

  rejectsRawSessionClaims(): boolean {
    return this.transportCapability.kind === "ScopeIsolatedClientRelay";
  }

  linkIdentity(): AuthorSubject | undefined {
    return permissionSubjectOfRole(this.role);
  }

  identity(): AuthorSubject {
    const id = this.linkIdentity();
    if (id === undefined)
      throw new Error("relay transport has no identity; use an admitted policy binding");
    return id;
  }

  permissionSubject(): AuthorSubject | undefined {
    return this.permissionIdentity ?? permissionSubjectOfRole(this.role);
  }

  isRelay(): boolean {
    return this.role.kind === "Relay";
  }

  // ---- served policy binding / authority source ----

  setSubscriptionPolicyBinding(subscription: SubscriptionKey, binding: PolicyBinding): void {
    if (coveredInputTrace()) {
      console.error(
        `JAZZ_COVERED_INPUT_TRACE stage=served_policy_binding peer=${this.publicationOwner} owner=${this.publicationOwner} role=${debugFmt(this.role)} subscription=${debugFmt(subscription)} identity=${debugFmt(binding[0])} claims=${debugFmt(binding[1])}`,
      );
    }
    this.stateFor(subscription).policyBinding = binding;
  }

  setSubscriptionAuthorityResultSource(
    subscription: SubscriptionKey,
    key: AuthorityResultKey,
  ): void {
    const state = this.stateFor(subscription);
    state.localAuthority.replaceSource(key, 0);
    state.authorityResultSource = key;
  }

  setSubscriptionAwaitingSelectedAuthoritySource(
    subscription: SubscriptionKey,
    awaiting: boolean,
  ): void {
    this.stateFor(subscription).awaitingSelectedAuthoritySource = awaiting;
  }

  subscriptionAuthorityResultSource(subscription: SubscriptionKey): AuthorityResultKey | undefined {
    return this.publicationStates.get(subscription)?.authorityResultSource;
  }

  subscriptionPolicyBinding(subscription: SubscriptionKey): PolicyBinding | undefined {
    return this.publicationStates.get(subscription)?.policyBinding;
  }

  servedSubscriptionPolicyBinding(subscription: SubscriptionKey): PolicyBinding {
    const binding = this.subscriptionPolicyBinding(subscription);
    if (binding) return binding;
    if (coveredInputTrace()) {
      console.error(
        `JAZZ_COVERED_INPUT_TRACE stage=missing_served_policy_binding peer=${this.publicationOwner} owner=${this.publicationOwner} role=${debugFmt(this.role)} subscription=${debugFmt(subscription)} states=${debugFmt([...this.publicationStates.keysArray()])} caller=${new Error().stack ?? ""}`,
      );
    }
    throw nodeInvalidStoredValue("served subscription is missing its immutable policy binding");
  }

  ensureDirectInternalSubscriptionPolicyBinding(node: any, subscription: SubscriptionKey): void {
    if (this.subscriptionPolicyBinding(subscription)) return;
    if (this.isRelay())
      throw nodeInvalidStoredValue(
        "relay subscription requires an explicit immutable policy binding",
      );
    const identity = this.permissionSubject();
    if (identity === undefined)
      throw nodeInvalidStoredValue(
        "direct subscription is missing a terminated permission subject",
      );
    this.setSubscriptionPolicyBinding(subscription, [identity, node.sessionClaimsFor(identity)]);
  }

  subscriptionAwaitsSelectedAuthoritySource(subscription: SubscriptionKey): boolean {
    return this.publicationStates.get(subscription)?.awaitingSelectedAuthoritySource ?? false;
  }

  maintainedView(
    subscription: SubscriptionKey,
  ): MaintainedSubscriptionViewSubscription | undefined {
    return this.publicationStates.get(subscription)?.maintainedSubscriptionView;
  }

  hasMaintainedSubscription(subscription: SubscriptionKey): boolean {
    return this.maintainedView(subscription) !== undefined;
  }

  hasPendingInitialMaintainedSubscription(subscription: SubscriptionKey): boolean {
    const m = this.maintainedView(subscription);
    return m !== undefined && !m.initialReceived;
  }

  memberResultSet(subscription: SubscriptionKey): SortedSet<ResultMemberEntry> {
    return (
      this.publicationStates.get(subscription)?.memberResultSet() ??
      new PeerSubscriptionState().resultMemberSet
    );
  }

  // ---- known_state.rs ----

  forgetSubscription(subscription: SubscriptionKey): void {
    this.publicationStates.delete(subscription);
    this.downstreamKnownStates.delete(subscription);
  }

  declareKnownState(
    subscription: SubscriptionKey,
    declaration: KnownStateDeclaration | undefined,
  ): void {
    if (declaration) this.downstreamKnownStates.set(subscription, declaration);
    else this.downstreamKnownStates.delete(subscription);
  }

  settleWatermarkDeclaration(subscription: SubscriptionKey): void {
    const ks = this.downstreamKnownStates.get(subscription);
    if (ks?.kind !== "Watermark") return;
    this.downstreamKnownStates.set(
      subscription,
      ks.authorizationProgress !== undefined
        ? {
            kind: "FastWithAuthorizationProgress",
            completeness: "FastCurrentMembership",
            position: ks.position,
            authorizationProgress: ks.authorizationProgress,
          }
        : { kind: "Fast", completeness: "FastCurrentMembership", position: ks.position },
    );
  }

  advanceAuthorizationProgress(): void {
    for (const state of this.publicationStates.valuesArray()) {
      if (state.authorizationProgress >= Number.MAX_SAFE_INTEGER)
        throw new Error("authorization progress overflow must stop reset suppression");
      state.authorizationProgress += 1;
    }
  }

  authorizationProgressForSubscription(subscription: SubscriptionKey): number {
    return this.publicationStates.get(subscription)?.authorizationProgress ?? 0;
  }

  retainAuthorizationProgressForSubscription(
    subscription: SubscriptionKey,
    progress: number,
  ): void {
    const state = this.stateFor(subscription);
    state.authorizationProgress = progress;
    state.hasServedAuthorizationProgress = true;
  }

  forgetSubscriptionWithNode(node: any, subscription: SubscriptionKey): boolean {
    const state = this.publicationStates.delete(subscription);
    this.downstreamKnownStates.delete(subscription);
    if (!state) {
      node.releaseQuerySubscriptionForPeer(this.publicationOwner, subscription);
      return false;
    }
    const admitted = state.policyBinding
      ? policyBindingKeyFromCanonicalParts(state.policyBinding[0], state.policyBinding[1])
      : undefined;
    let unsubscribed = false;
    if (
      state.grooveRuntimeToken === node.grooveRuntimeToken() &&
      state.maintainedSubscriptionView
    ) {
      const maintained = state.maintainedSubscriptionView;
      state.maintainedSubscriptionView = undefined;
      unsubscribed = node.unsubscribeGrooveSubscription(maintained.subscription.id());
    }
    if (admitted)
      node.releaseQuerySubscriptionForPeerWithAdmittedPolicyBinding(
        this.publicationOwner,
        subscription,
        admitted,
      );
    else node.releaseQuerySubscriptionForPeer(this.publicationOwner, subscription);
    return unsubscribed;
  }

  forgetQueryBinding(shape: any, binding: any): void {
    this.forgetSubscription({
      shapeId: shape.shapeId(),
      bindingId: binding.bindingId(),
      readView: defaultReadViewKey(),
    });
  }

  forgetQueryBindingWithNode(node: any, shape: any, binding: any): boolean {
    return this.forgetSubscriptionWithNode(node, {
      shapeId: shape.shapeId(),
      bindingId: binding.bindingId(),
      readView: defaultReadViewKey(),
    });
  }

  acknowledgedCompleteTxPayloads(): SortedSet<TxId> {
    return new SortedSet<TxId>(cmpTxId);
  }

  setShipCompleteExclusivePayloads(enabled: boolean): void {
    this.shipCompleteExclusivePayloads = enabled;
  }

  forgetEvictedVersions(txIds: Iterable<TxId>): number {
    let n = 0;
    for (const tx of txIds) if (this.shippedCompleteTxPayloads.delete(tx)) n++;
    return n;
  }

  subscriptionResultSets(subscription: SubscriptionKey): SortedSet<TxId> | undefined {
    return this.publicationStates.get(subscription)?.previousTxIds();
  }

  maintainedSubscriptionViewMetrics(): MaintainedSubscriptionViewMetrics {
    return this.metrics.maintainedSubscriptionView;
  }

  inspectMaintainedSubscriptionViewFootprint(subscription: SubscriptionKey): any {
    return this.maintainedView(subscription)?.maintained.footprint();
  }

  // ---- repair.rs ----

  recordOutgoingViewUpdateMetadata(update: SyncMessage): void {
    if (update.kind !== "ViewUpdate") return;
    const view = update.payload;
    if (view.supportingRows.isSnapshot() && !view.peerPayloadInventory.openingPending) {
      this.stateFor(view.subscription).supportingRevision = undefined;
    }
    const bundles = viewUpdateSingletonBundles(view.versionCarriers);
    this.metrics.viewUpdatesOut += 1;
    this.metrics.versionBundlesOut += bundles.length;
    this.metrics.completeTxPayloadRefsOut += view.peerPayloadInventory.completeTxPayloads.length;
    this.metrics.duplicateVersionBundlesOut += bundles.filter(
      (b) => bundleContainsCompleteTxPayload(b) && this.shippedCompleteTxPayloads.has(b.tx.txId),
    ).length;
  }

  async proveTerminalCommitAuthorization(
    node: any,
    writer: AuthorSubject,
    claims: Map<string, any>,
    versions: any[],
    candidateTxId: TxId,
  ): Promise<boolean> {
    if (authorEquals(writer, AUTHOR_SYSTEM)) return true;
    const scoped = node.scopedActiveSessionClaims(writer, claims);
    return scoped.commitUnitSatisfiesWritePolicy(versions, writer, candidateTxId);
  }

  recordOutgoingViewUpdate(update: SyncMessage): void {
    this.recordOutgoingViewUpdateMetadata(update);
    if (update.kind === "ViewUpdate")
      this.stateFor(update.payload.subscription).acknowledgePublished(
        update.payload.supportingRows.revision(),
      );
  }

  applyOutgoingViewDelta(
    subscription: SubscriptionKey,
    resetInputSet: boolean,
    adds: ResultMemberEntry[],
    removes: ResultMemberEntry[],
  ): void {
    const state = this.stateFor(subscription);
    if (resetInputSet) {
      state.maintainedSubscriptionView?.maintained.forgetPeerSourceClosureBaseline();
      state.supportingRevision = undefined;
      state.resultMemberSet = new PeerSubscriptionState().resultMemberSet;
      state.memberIndex.clear();
    }
    for (const member of removes) {
      state.resultMemberSet.delete(member);
      applyContributionRemove(state, [member], []);
    }
    for (const member of adds) {
      state.resultMemberSet.add(member);
      applyContributionAdd(state, [member], [], []);
    }
    const dup = duplicatePhysicalRowResultSet(state.resultMemberSet);
    if (dup && dup[1] !== dup[2]) {
      throw new Error(
        `peer subscription ${debugFmt(subscription)} has multiple content versions for physical output row ${debugFmt(dup[0])}: ${debugFmt(dup[1])} and ${debugFmt(dup[2])}`,
      );
    }
  }
}

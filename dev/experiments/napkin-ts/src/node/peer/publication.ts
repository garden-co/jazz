// Maintained-view publication paths for a PeerState (Rust: impl PeerState in
// peer/publication.rs). Synchronous accessors live on the class in ../peer.ts.
import { SortedSet } from "../../support/collections.js";
import { uuidV4 } from "../../support/hash.js";
import { monotonicMs } from "../../support/runtime.js";
import {
  type AuthorSubject,
  type GlobalTime,
  type KnownStateDeclaration,
  type ResultMemberEntry,
  type SubscriptionKey,
  type SyncMessage,
  AuthorityResultKey,
  BindingViewKey,
  NodeErrorKind,
  PeerPayloadInventory,
  ProgramFactEntry,
  RegisterShapeOptions,
  ShapeAst,
  SupportingRowsUpdate,
  cmpGlobalTime,
  coveredInputTrace,
  debugFmt,
  defaultReadViewKey,
  emptyResultTransitions,
  isNodeError,
  nodeInvalidStoredValue,
  policyBindingKeyFromCanonicalParts,
  readViewKeyEquals,
  rehydrateTrace,
  simpleScalarExitQuery,
} from "../../_extern/node-policy-peer.js";
import { PeerState } from "../peer.js";
import {
  bindingValuesInParamOrder,
  memberSet,
  memberStateMap,
  replacementRemovals,
  storageReadMetricsBuckets,
  viewUpdateSingletonBundles,
} from "./delivery.js";
import {
  type MaintainedRehydrateRequest,
  type MaintainedSubscriptionViewSubscription,
  type PolicyBinding,
  type RehydratePurpose,
  CachedPeerQueryPlan,
  fastAuthorizationProgress,
  fastCurrentMembershipPosition,
  fastCursorRequiresAuthoritativeReset,
  memberSettlePosition,
} from "./subscription_state.js";

type MemberSet = SortedSet<ResultMemberEntry>;

function ordinaryCurrentContentMember(member: ResultMemberEntry): boolean {
  return (
    member.kind === "Row" &&
    member.layer === "Content" &&
    member.deletionTx === undefined &&
    member.source === "Current" &&
    member.branchOrPrefix === undefined &&
    member.batch === undefined
  );
}

function ordinaryFlatRowDuplicateView(
  shape: any,
  currentMembers: MemberSet,
  removedMembersAreOrdinary: boolean,
  canonicalReadView: any,
  canonicalProgramFactsEmpty: boolean,
  sourceHadProgramFactTransitions: boolean,
): boolean {
  const q = shape.query();
  if (q.flatJoin || q.includes.length || q.arraySubqueries.length || q.aggregate) return false;
  return (
    readViewKeyEquals(canonicalReadView, defaultReadViewKey()) &&
    canonicalProgramFactsEmpty &&
    !sourceHadProgramFactTransitions &&
    removedMembersAreOrdinary &&
    [...currentMembers].every(ordinaryCurrentContentMember)
  );
}

function publishedMember(
  member: ResultMemberEntry,
  resultTable: string | undefined,
  outputTables: Map<string, any>,
  aggregateIsPolicyScoped: boolean,
): boolean {
  const tableName = member.tableName();
  if (tableName === undefined) return false;
  const synthetic = member.kind === "Synthetic";
  return (
    (synthetic || resultTable === undefined || tableName === resultTable) &&
    (outputTables.has(tableName) || (synthetic && !aggregateIsPolicyScoped))
  );
}

const aggregateIsPolicyScoped = (node: any, shape: any): boolean =>
  shape.query().aggregate !== undefined && node.table(shape.query().table).readPolicy !== undefined;

const tracedBundleCount = (update: SyncMessage): number =>
  update.kind === "ViewUpdate"
    ? viewUpdateSingletonBundles(update.payload.versionCarriers).length
    : 0;

function reconcileRetainedMembersAfterInitialDeletionWitness(
  states: ReturnType<typeof memberStateMap>,
  previous: MemberSet,
  active: MemberSet,
  published: MemberSet,
): void {
  for (const member of memberSet([...previous, ...active])) {
    const wasPublished = previous.has(member);
    states.set(member, [
      wasPublished,
      active.has(member) && (wasPublished || published.has(member)),
    ]);
  }
}

export type ReconciledMaintainedSubscriptionClone = {
  canonicalUpdate?: SyncMessage;
  sourceRemoves: ResultMemberEntry[];
  sourceHadProgramFactTransitions: boolean;
};

type ClonedResetBasis = {
  knownState?: KnownStateDeclaration;
  knownMembershipPosition?: GlobalTime;
  authorizationMatches: boolean;
  removedMembersAreOrdinary: boolean;
  sourceHadProgramFactTransitions: boolean;
  flatRowRemoves?: ResultMemberEntry[];
};

type MaintainedCanonicalUpdate = { changed: boolean; update: SyncMessage };

type PublicationPlan = {
  subscription: SubscriptionKey;
  policy: PolicyBinding;
  tier: any;
  defaultReadView: boolean;
  supportingUpdate?: any;
  settledThrough: GlobalTime;
  knownState?: KnownStateDeclaration;
  leaveScanAfter?: GlobalTime;
  adds: ResultMemberEntry[];
  removes: ResultMemberEntry[];
};

const emptyViewUpdate = (
  subscription: SubscriptionKey,
  settledThrough: GlobalTime,
): SyncMessage => ({
  kind: "ViewUpdate",
  payload: {
    subscription,
    settledThrough,
    versionCarriers: [],
    peerPayloadInventory: PeerPayloadInventory.default(),
    supportingRows: SupportingRowsUpdate.snapshot([]),
  },
});

const gtPositive = (t: GlobalTime): boolean => t[0] > 0;

function hasCurrentPendingInitialMaintainedSubscription(
  self: PeerState,
  node: any,
  sub: SubscriptionKey,
): boolean {
  const state = self.publicationStates.get(sub);
  return (
    !!state &&
    state.grooveRuntimeToken === node.grooveRuntimeToken() &&
    state.physicalIdentityGeneration === node.physicalIdentityGeneration() &&
    !!state.maintainedSubscriptionView &&
    !state.maintainedSubscriptionView.initialReceived
  );
}

function fastCursorAuthorizationMatches(
  self: PeerState,
  sub: SubscriptionKey,
  knownState: KnownStateDeclaration | undefined,
): boolean {
  if (self.isRelay()) return true;
  const state = self.publicationStates.get(sub);
  return (
    !!state &&
    state.hasServedAuthorizationProgress &&
    fastAuthorizationProgress(knownState) === state.authorizationProgress
  );
}

function preparedContext(self: PeerState, sub: SubscriptionKey, missing: string): [any, any] {
  const prepared = self.publicationStates.get(sub)?.preparedQuery;
  if (!prepared) throw nodeInvalidStoredValue(missing);
  return prepared.context();
}

function settlementTimeForPublication(
  self: PeerState,
  node: any,
  sub: SubscriptionKey,
  bindingView: any,
): GlobalTime {
  const source = self.publicationStates.get(sub)?.authorityResultSource;
  if (source) return node.settledThroughForAuthorityResult(source) ?? [0, 0];
  return (
    node.settledThroughForAuthorityResult(AuthorityResultKey.unscoped(bindingView)) ??
    node.committedGlobalTime()
  );
}

export function canonicalSubscriptionSettlementTime(
  self: PeerState,
  node: any,
  sub: SubscriptionKey,
): GlobalTime {
  return settlementTimeForPublication(
    self,
    node,
    sub,
    BindingViewKey.fromCanonicalSubscriptionKey(sub),
  );
}

function maintainedPublicationCut(self: PeerState, node: any, sub: SubscriptionKey): GlobalTime {
  return self.subscriptionAwaitsSelectedAuthoritySource(sub)
    ? canonicalSubscriptionSettlementTime(self, node, sub)
    : node.committedGlobalTime();
}

async function bundlePublication(
  self: PeerState,
  node: any,
  shape: any,
  source: SubscriptionKey,
  maintainedFacts: any,
  plan: PublicationPlan,
): Promise<SyncMessage> {
  const [identity, claims] = plan.policy;
  return node.scopedActiveSessionClaims(identity, claims).viewUpdateForMaintainedResultMembers({
    supportingUpdate: plan.supportingUpdate,
    shape,
    hasDefaultReadView: plan.defaultReadView,
    allowAuthoritativeScalarExitRefresh: !self.subscriptionAwaitsSelectedAuthoritySource(source),
    subscription: plan.subscription,
    settledThrough: plan.settledThrough,
    peerCompleteTxPayloads: self.acknowledgedCompleteTxPayloads(),
    knownState: plan.knownState,
    leaveScanAfter: plan.leaveScanAfter,
    completeExclusivePayloads: self.shipCompleteExclusivePayloads && self.isRelay(),
    previousResultSet: memberSet(),
    resultMemberAdds: plan.adds,
    resultMemberRemoves: plan.removes,
    identity,
    tier: plan.tier,
    maintainedFacts,
    allowStorageWitnessFallback: false,
  });
}

function clearStaleGrooveRuntimeHandles(self: PeerState, node: any, sub: SubscriptionKey): void {
  const currentToken = node.grooveRuntimeToken();
  const state = self.publicationStates.get(sub);
  if (!state?.runtimeIsStale(node)) return;
  if (state.grooveRuntimeToken === currentToken && state.maintainedSubscriptionView) {
    const stale = state.maintainedSubscriptionView;
    state.maintainedSubscriptionView = undefined;
    node.unsubscribeGrooveSubscription(stale.subscription.id());
  }
  state.clearGrooveRuntimeHandles();
}

function replaceMaintainedSubscriptionView(
  self: PeerState,
  node: any,
  sub: SubscriptionKey,
  replacement: MaintainedSubscriptionViewSubscription,
): void {
  const token = node.grooveRuntimeToken();
  const state = self.stateFor(sub);
  const previousToken = state.grooveRuntimeToken;
  const stale = state.maintainedSubscriptionView;
  state.maintainedSubscriptionView = replacement;
  state.grooveRuntimeToken = token;
  state.physicalIdentityGeneration = node.physicalIdentityGeneration();
  if (stale && previousToken === token) node.unsubscribeGrooveSubscription(stale.subscription.id());
}

const requiresSelectedAuthoritySource = (
  self: PeerState,
  sub: SubscriptionKey,
  purpose: RehydratePurpose,
) => purpose === "Query" && self.subscriptionAwaitsSelectedAuthoritySource(sub);

const selectedAuthoritySource = (self: PeerState, sub: SubscriptionKey) =>
  self.publicationStates.get(sub)?.authorityResultSource;

function ensureQuerySubscriptionRegistered(
  self: PeerState,
  node: any,
  subscription: SubscriptionKey,
  shape: any,
  binding: any,
  opts: any,
  policy: PolicyBinding,
): void {
  node.registerQuerySubscriptionForPeer(
    self.publicationOwner,
    shape.shapeId(),
    ShapeAst.fromValidated(shape),
    opts,
    {
      shapeId: shape.shapeId(),
      subscription,
      values: bindingValuesInParamOrder(shape, binding),
      knownState: undefined,
      delegatedSession: undefined,
    },
    policyBindingKeyFromCanonicalParts(policy[0], policy[1]),
  );
}

function installPreparedPlan(
  self: PeerState,
  node: any,
  sub: SubscriptionKey,
  cached: CachedPeerQueryPlan,
) {
  const state = self.stateFor(sub);
  state.preparedQuery = cached;
  state.grooveRuntimeToken = node.grooveRuntimeToken();
  state.physicalIdentityGeneration = node.physicalIdentityGeneration();
  return state;
}

/** Full current-row view update for a whole table. */
export async function currentRowsUpdate(
  self: PeerState,
  node: any,
  table: string,
): Promise<SyncMessage> {
  const [shape, binding] = node.wholeTableShapeBinding(table);
  const opts = RegisterShapeOptions.default();
  const subscription: SubscriptionKey = {
    shapeId: shape.shapeId(),
    bindingId: binding.bindingId(),
    readView: opts.readViewKey(),
  };
  self.ensureDirectInternalSubscriptionPolicyBinding(node, subscription);
  clearStaleGrooveRuntimeHandles(self, node, subscription);
  const policy = self.servedSubscriptionPolicyBinding(subscription);
  ensureQuerySubscriptionRegistered(self, node, subscription, shape, binding, opts, policy);
  const prepared = self.publicationStates.get(subscription)?.preparedQuery;
  if (!prepared || !prepared.hasRuntimePlan()) {
    const plan = node.markPeerMaintainedQueryShapeCache(shape, binding, opts.tier);
    installPreparedPlan(self, node, subscription, CachedPeerQueryPlan.withPlan(opts, plan));
  } else self.stateFor(subscription);
  const [tier, readView] = preparedContext(
    self,
    subscription,
    "maintained subscription view is missing prepared state",
  );
  const previousMemberResultSet = self.memberResultSet(subscription);
  if (!self.maintainedView(subscription)) {
    const update = await rehydrateQueryMaintainedSubscriptionView(self, node, {
      shape,
      binding,
      subscription,
      previousMemberResultSet,
      resetInputSet: true,
      resultTableFilter: table,
      tier,
      readView,
      purpose: "Query",
    });
    if (update) return update;
    await node.driveQueryRuntime();
  }
  if (self.maintainedView(subscription)) {
    const update = await queryUpdateMaintainedSubscriptionView(
      self,
      node,
      shape,
      binding,
      subscription,
      table,
    );
    if (update) return update;
    await node.driveQueryRuntime();
    const again = await queryUpdateMaintainedSubscriptionView(
      self,
      node,
      shape,
      binding,
      subscription,
      table,
    );
    if (!again)
      throw nodeInvalidStoredValue("maintained hydration ended without an initial publication");
    return again;
  }
  throw new Error(
    "internal error: entered unreachable code: maintained subscription view state is either absent or present",
  );
}

export function queryUpdate(
  self: PeerState,
  node: any,
  shape: any,
  binding: any,
): Promise<SyncMessage> {
  const subscription = {
    shapeId: shape.shapeId(),
    bindingId: binding.bindingId(),
    readView: defaultReadViewKey(),
  };
  return queryUpdateForSubscription(self, node, subscription, shape, binding);
}

export function queryUpdateForSubscription(
  self: PeerState,
  node: any,
  subscription: SubscriptionKey,
  shape: any,
  binding: any,
): Promise<SyncMessage> {
  return awaitQueryUpdateForSubscriptionWithOpts(
    self,
    node,
    subscription,
    shape,
    binding,
    RegisterShapeOptions.default(),
  );
}

export async function queryUpdateForSubscriptionWithOpts(
  self: PeerState,
  node: any,
  subscription: SubscriptionKey,
  shape: any,
  binding: any,
  opts: any,
): Promise<SyncMessage | undefined> {
  const progress = await queryUpdateForSubscriptionWithOptsAndWaker(
    self,
    node,
    subscription,
    shape,
    binding,
    opts,
  );
  return progress.ready ? progress.update : undefined;
}

export async function queryUpdateForSubscriptionWithOptsAndWaker(
  self: PeerState,
  node: any,
  subscription: SubscriptionKey,
  shape: any,
  binding: any,
  opts: any,
  waker?: any,
): Promise<{ ready: boolean; update?: SyncMessage }> {
  const update = await queryUpdateInnerForSubscriptionWithMetadata(
    self,
    node,
    subscription,
    shape,
    binding,
    opts,
    waker,
  );
  if (!update) return { ready: false };
  return { ready: true, update: update.changed ? update.update : undefined };
}

async function awaitQueryUpdateForSubscriptionWithOpts(
  self: PeerState,
  node: any,
  subscription: SubscriptionKey,
  shape: any,
  binding: any,
  opts: any,
): Promise<SyncMessage> {
  const first = await queryUpdateInnerForSubscriptionWithMetadata(
    self,
    node,
    subscription,
    shape,
    binding,
    opts,
  );
  if (first) return first.update;
  await node.driveQueryRuntime();
  const second = await queryUpdateInnerForSubscriptionWithMetadata(
    self,
    node,
    subscription,
    shape,
    binding,
    opts,
  );
  if (!second)
    throw nodeInvalidStoredValue("maintained hydration ended without a query publication");
  return second.update;
}

async function queryUpdateInnerForSubscriptionWithMetadata(
  self: PeerState,
  node: any,
  subscription: SubscriptionKey,
  shape: any,
  binding: any,
  opts: any,
  waker?: any,
): Promise<MaintainedCanonicalUpdate | undefined> {
  const metadataWasStale = self.publicationStates.get(subscription)?.runtimeIsStale(node) ?? false;
  if (metadataWasStale && self.maintainedView(subscription)?.initialReceived)
    self.metrics.maintainedSubscriptionView.fullDiffFallbacks.runtimeResets += 1;
  clearStaleGrooveRuntimeHandles(self, node, subscription);
  const policy = self.servedSubscriptionPolicyBinding(subscription);
  ensureQuerySubscriptionRegistered(self, node, subscription, shape, binding, opts, policy);
  const existing = self.publicationStates.get(subscription);
  if (!existing)
    return {
      changed: true,
      update: emptyViewUpdate(subscription, maintainedPublicationCut(self, node, subscription)),
    };
  if (existing.maintainedSubscriptionView)
    return queryUpdateMaintainedSubscriptionViewWithMetadata(
      self,
      node,
      shape,
      binding,
      subscription,
      undefined,
      waker,
    );
  const previousMemberResultSet = self.memberResultSet(subscription);
  const prepared = existing.preparedQuery;
  const [tier, readView] = prepared ? prepared.context() : [opts.tier, opts.readView];
  if (!prepared?.hasRuntimePlan()) {
    const plan = node.markPeerMaintainedQueryShapeCache(shape, binding, tier);
    const state = self.stateFor(subscription);
    if (state.preparedQuery) state.preparedQuery.replaceRuntimePlan(plan);
    else state.preparedQuery = new CachedPeerQueryPlan(tier, readView, plan);
    state.grooveRuntimeToken = node.grooveRuntimeToken();
    state.physicalIdentityGeneration = node.physicalIdentityGeneration();
  }
  const update = await rehydrateQueryMaintainedSubscriptionView(
    self,
    node,
    {
      shape,
      binding,
      subscription,
      previousMemberResultSet,
      resetInputSet: metadataWasStale,
      resultTableFilter: undefined,
      tier,
      readView,
      purpose: "Query",
    },
    waker,
  );
  return update ? { changed: true, update } : undefined;
}

async function queryUpdateMaintainedSubscriptionView(
  self: PeerState,
  node: any,
  shape: any,
  binding: any,
  subscription: SubscriptionKey,
  resultTableFilter?: string,
  waker?: any,
): Promise<SyncMessage | undefined> {
  return (
    await queryUpdateMaintainedSubscriptionViewWithMetadata(
      self,
      node,
      shape,
      binding,
      subscription,
      resultTableFilter,
      waker,
    )
  )?.update;
}

async function queryUpdateMaintainedSubscriptionViewWithMetadata(
  self: PeerState,
  node: any,
  shape: any,
  binding: any,
  subscription: SubscriptionKey,
  resultTableFilter?: string,
  waker?: any,
): Promise<MaintainedCanonicalUpdate | undefined> {
  const trace = rehydrateTrace();
  const traceStart = monotonicMs();
  if (trace) node.resetStorageReadMetrics();
  const initialReceivedBeforeDrain = self.maintainedView(subscription)?.initialReceived ?? false;
  const transitions = await drainMaintainedSubscriptionViewChanges(
    self,
    node,
    shape,
    subscription,
    resultTableFilter,
    waker,
  );
  if (!self.maintainedView(subscription)?.initialReceived) return undefined;
  const initialSnapshotCompleted = !initialReceivedBeforeDrain;
  const drainElapsed = monotonicMs() - traceStart;
  const drainReads = trace ? node.takeStorageReadMetrics() : undefined;
  const adds: ResultMemberEntry[] = transitions.adds;
  let removes: ResultMemberEntry[] = transitions.removes;
  const resultAddCount = adds.length;
  const resultRemoveCount = removes.length;
  if (
    adds.length === 0 &&
    removes.length === 0 &&
    !transitions.supportingChanged &&
    transitions.programFactAdds.length === 0 &&
    transitions.programFactRemoves.length === 0 &&
    (transitions.requiresAuthoritativeMembershipReconcile ||
      transitions.observedResultDeltaBatches > 0)
  ) {
    const [tier, readView] = preparedContext(
      self,
      subscription,
      "maintained subscription view is missing prepared state",
    );
    self.metrics.maintainedSubscriptionView.fullDiffFallbacks.membershipReconciliations += 1;
    const update = await rehydrateQueryMaintainedSubscriptionView(
      self,
      node,
      {
        shape,
        binding,
        subscription,
        previousMemberResultSet: self.memberResultSet(subscription),
        resetInputSet: false,
        resultTableFilter,
        tier,
        readView,
        purpose: "Query",
      },
      waker,
    );
    return update ? { changed: true, update } : undefined;
  }
  const existing = self.publicationStates.get(subscription);
  if (existing) removes.push(...replacementRemovals(existing, adds));
  removes = memberSet(removes).toArray();
  const state = self.publicationStates.get(subscription);
  if (!state) throw nodeInvalidStoredValue("maintained subscription publication state is missing");
  const view = state.maintainedSubscriptionView;
  if (!view)
    throw nodeInvalidStoredValue("maintained subscription view is missing source closure state");
  const maintained = view.maintained;
  let supportingRows: any;
  const delta =
    !initialSnapshotCompleted && state.supportingRevision !== undefined
      ? maintained.unpublishedSupportingDelta()
      : undefined;
  if (delta) {
    const [dAdds, dRemoves] = delta;
    const predecessor = state.supportingRevision!;
    const revision = dAdds.length === 0 && dRemoves.length === 0 ? predecessor : uuidV4();
    supportingRows = SupportingRowsUpdate.delta(predecessor, revision, dAdds, dRemoves);
  } else {
    supportingRows = SupportingRowsUpdate.snapshot([...maintained.supportingRows()]);
  }
  const factAddCount = supportingRows.addedRows().length;
  const factRemoveCount = supportingRows.removedRows().length;
  if (adds.length === 0 && removes.length === 0 && factAddCount === 0 && factRemoveCount === 0) {
    const changed = initialSnapshotCompleted || supportingRows.isSnapshot();
    self.publicationStates.get(subscription)?.acknowledgePublished(supportingRows.revision());
    return {
      changed,
      update: {
        kind: "ViewUpdate",
        payload: {
          subscription,
          settledThrough: maintainedPublicationCut(self, node, subscription),
          versionCarriers: [],
          peerPayloadInventory: PeerPayloadInventory.default(),
          supportingRows,
        },
      },
    };
  }
  const [tier, readView] = preparedContext(
    self,
    subscription,
    "maintained subscription view is missing prepared state",
  );
  const bundleStart = monotonicMs();
  if (trace) node.resetStorageReadMetrics();
  const plan: PublicationPlan = {
    subscription,
    policy: self.servedSubscriptionPolicyBinding(subscription),
    tier,
    defaultReadView: readView.isDefault(),
    supportingUpdate: supportingRows,
    settledThrough: maintainedPublicationCut(self, node, subscription),
    knownState: self.downstreamKnownStates.get(subscription),
    leaveScanAfter: undefined,
    adds: adds.slice(),
    removes: removes.slice(),
  };
  const facts = self.maintainedView(subscription);
  if (!facts) throw nodeInvalidStoredValue("maintained subscription view subscription missing");
  const update = await bundlePublication(self, node, shape, subscription, facts.maintained, plan);
  const bundleElapsed = monotonicMs() - bundleStart;
  if (trace) {
    const bundleReads = node.takeStorageReadMetrics();
    console.error(
      `JAZZ_REHYDRATE_TRACE stage=update table=${shape.query().table} subscription=${debugFmt(subscription)} drain_ms=${Math.floor(drainElapsed)} bundle_ms=${Math.floor(bundleElapsed)} adds=${resultAddCount} removes=${resultRemoveCount} fact_adds=${factAddCount} fact_removes=${factRemoveCount} bundles=${tracedBundleCount(update)} fallback=false drain_reads=${drainReads.total.reads} drain_ranges=${drainReads.total.ranges} bundle_reads=${bundleReads.total.reads} bundle_ranges=${bundleReads.total.ranges}`,
    );
  }
  self.metrics.maintainedSubscriptionView.hitsOut += 1;
  self.recordOutgoingViewUpdateMetadata(update);
  self.applyOutgoingViewDelta(subscription, false, adds, removes);
  if (update.kind === "ViewUpdate")
    self.stateFor(update.payload.subscription).acknowledgePublished(supportingRows.revision());
  return { changed: true, update };
}

export async function drainMaintainedSubscriptionViewChanges(
  self: PeerState,
  node: any,
  shape: any,
  subscription: SubscriptionKey,
  resultTableFilter?: string,
  waker?: any,
): Promise<any> {
  let deletionChanges = false;
  const mv = self.publicationStates.get(subscription)?.maintainedSubscriptionView;
  if (mv?.sourceAuthorityResult && mv.coveredInputReceiver) {
    const source = mv.sourceAuthorityResult;
    const receiver = mv.coveredInputReceiver;
    const schemaVersion = mv.resultSchemaVersion;
    mv.coveredInputReceiver = undefined;
    const due = node.coveredInputReceiverReconciliationDue(receiver, source);
    let replacement: boolean | Error = true;
    if (due) {
      try {
        replacement = await node.replaceCoveredInputReceiver(receiver, schemaVersion, source);
      } catch (e) {
        replacement = e as Error;
      }
    }
    const restored = self.publicationStates.get(subscription)?.maintainedSubscriptionView;
    if (!restored) throw new Error("maintained receiver state survives replacement");
    restored.coveredInputReceiver = receiver;
    if (replacement instanceof Error) throw replacement;
    if (!replacement) return emptyResultTransitions();
    if (due) {
      const witnesses = await node.selectedDeletionWitnesses(source, schemaVersion);
      const view = self.publicationStates.get(subscription)?.maintainedSubscriptionView;
      if (view) deletionChanges = view.maintained.replaceSelectedDeletionWitnesses(witnesses);
    }
  }
  await node.driveReadyQueryRuntimeWithWaker(waker);
  const outputTables = self.maintainedView(subscription)?.tables ?? new Map<string, any>();
  const maintainedSource = self.maintainedView(subscription)?.sourceAuthorityResult;
  let sourceAuthorityResult = maintainedSource;
  if (sourceAuthorityResult === undefined) {
    const selected = selectedAuthoritySource(self, subscription);
    if (selected && node.hasSettledAuthorityResult(selected)) sourceAuthorityResult = selected;
  }
  const policyScoped = aggregateIsPolicyScoped(node, shape);
  const states = memberStateMap();
  const programFactAdds: any[] = [];
  const programFactRemoves: any[] = [];
  let supportingChanged = deletionChanges;
  let observedResultDeltaBatches = 0;
  let requiresReconcile = false;
  let initialDeletionWitness = false;
  const state = self.publicationStates.get(subscription);
  if (!state) return emptyResultTransitions();
  const previous = state.resultMemberSet;
  const view = state.maintainedSubscriptionView;
  if (!view) return emptyResultTransitions();
  for (;;) {
    const deltas = view.subscription.tryRecv();
    if (deltas === undefined) break;
    const initialSnapshot = !view.initialReceived;
    view.initialReceived = true;
    self.metrics.maintainedSubscriptionView.deltaBatchesIn += 1;
    const t = view.maintained.applyMultisinkDeltas(
      deltas,
      view.terminalSchemas,
      view.tables,
      node.nodeAliases,
    );
    if (initialSnapshot) {
      initialDeletionWitness ||= t.requiresAuthoritativeMembershipReconcile;
    } else {
      supportingChanged ||= t.supportingChanged;
      observedResultDeltaBatches += t.observedResultDeltaBatches;
      requiresReconcile ||= t.requiresAuthoritativeMembershipReconcile;
    }
    if (coveredInputTrace() && (t.adds.length || t.programFactAdds.length)) {
      console.error(
        `JAZZ_COVERED_INPUT_TRACE stage=publication_filter subscription=${debugFmt(subscription)} table=${shape.query().table} result_filter=${debugFmt(resultTableFilter)} authority=${debugFmt(sourceAuthorityResult)} members=${debugFmt(t.adds)} raw_facts=${debugFmt(t.programFactAdds)}`,
      );
    }
    programFactAdds.push(...t.programFactAdds.filter(ProgramFactEntry.isPeerSourceClosureFact));
    programFactRemoves.push(
      ...t.programFactRemoves.filter(ProgramFactEntry.isPeerSourceClosureFact),
    );
    for (const member of t.adds) {
      const e = states.get(member);
      if (e) e[1] = true;
      else states.set(member, [previous.has(member), true]);
    }
    for (const member of t.removes) {
      const e = states.get(member);
      if (e) e[1] = false;
      else states.set(member, [previous.has(member), false]);
    }
  }
  if (initialDeletionWitness) {
    const prev = self.memberResultSet(subscription);
    const hydrated = self.maintainedView(subscription);
    if (!hydrated)
      throw nodeInvalidStoredValue(
        "initial maintained subscription snapshot missing after receive",
      );
    reconcileRetainedMembersAfterInitialDeletionWitness(
      states,
      prev,
      memberSet(hydrated.maintained.activeResultMembers()),
      memberSet(hydrated.maintained.publishedResultMembers()),
    );
  }
  const adds: ResultMemberEntry[] = [];
  const removes: ResultMemberEntry[] = [];
  for (const [member, [before, after]] of states) {
    if (!publishedMember(member, resultTableFilter, outputTables, policyScoped)) continue;
    if (!before && after) adds.push(member);
    else if (before && !after) removes.push(member);
  }
  return {
    ...emptyResultTransitions(),
    adds,
    removes,
    programFactAdds,
    programFactRemoves,
    observedResultDeltaBatches,
    requiresAuthoritativeMembershipReconcile: requiresReconcile,
    supportingChanged,
  };
}

async function rehydrateQueryMaintainedSubscriptionView(
  self: PeerState,
  node: any,
  request: MaintainedRehydrateRequest,
  waker?: any,
): Promise<SyncMessage | undefined> {
  const {
    shape,
    binding,
    subscription,
    previousMemberResultSet,
    resultTableFilter,
    tier,
    readView,
    purpose,
  } = request;
  let resetInputSet = request.resetInputSet;
  const trace = rehydrateTrace();
  const openStart = monotonicMs();
  if (trace) node.resetStorageReadMetrics();
  const relayRequiresSource = requiresSelectedAuthoritySource(self, subscription, purpose);
  let sourceKey: any;
  if (relayRequiresSource) {
    const source = selectedAuthoritySource(self, subscription);
    if (!source || !node.hasSettledAuthorityResult(source)) return undefined;
    sourceKey = source;
  }
  const [policyIdentity, policyClaims] = self.servedSubscriptionPolicyBinding(subscription);
  if (coveredInputTrace()) {
    console.error(
      `JAZZ_COVERED_INPUT_TRACE stage=rehydrate peer=${self.publicationOwner} owner=${self.publicationOwner} subscription=${debugFmt(subscription)} identity=${debugFmt(policyIdentity)} source=${debugFmt(sourceKey)} purpose=${purpose}`,
    );
  }
  let opened: any[];
  try {
    const scoped = node.scopedActiveSessionClaims(policyIdentity, policyClaims);
    if (purpose === "Query" && relayRequiresSource) {
      opened = await scoped.openSeededRelaySubscriptionViewWithWaker(
        shape,
        binding,
        policyIdentity,
        readView,
        subscription.readView,
        sourceKey,
        waker,
      );
    } else if (purpose === "Query") {
      opened = [
        ...(await scoped.openSeededMaintainedSubscriptionViewWithWaker(
          shape,
          binding,
          policyIdentity,
          tier,
          readView,
          subscription.readView,
          waker,
        )),
        undefined,
      ];
    } else {
      opened = [
        ...(await scoped.openSeededAuthorizationSupportSubscriptionViewWithWaker(
          shape,
          binding,
          policyIdentity,
          tier,
          readView,
          subscription.readView,
          waker,
        )),
        undefined,
      ];
    }
  } catch (error) {
    if (
      isNodeError(error, NodeErrorKind.AuthorizationSupportMissingClaim) &&
      purpose === "AuthorizationSupport"
    ) {
      if (previousMemberResultSet.size > 0) throw error;
      const update = emptyViewUpdate(
        subscription,
        maintainedPublicationCut(self, node, subscription),
      );
      self.recordOutgoingViewUpdate(update);
      self.stateFor(subscription).hasServedAuthorizationProgress = true;
      return update;
    }
    throw error;
  }
  const [
    receiver,
    maintained,
    terminalSchemas,
    transitions,
    tables,
    initialReceived,
    coveredInputReceiver,
  ] = opened;
  if (coveredInputTrace()) {
    console.error(
      `JAZZ_COVERED_INPUT_TRACE stage=rehydrate_opened owner=${self.publicationOwner} subscription=${debugFmt(subscription)} identity=${debugFmt(policyIdentity)} initial=${initialReceived} adds=${debugFmt(transitions.adds)} facts=${debugFmt(transitions.programFactAdds)}`,
    );
  }
  if (!terminalSchemas.hasRootCollector()) maintained.discardStructuredAppRows();
  const makeSubscription = (initial: boolean): MaintainedSubscriptionViewSubscription => ({
    subscription: receiver,
    maintained,
    terminalSchemas,
    tables,
    coveredInputReceiver,
    resultSchemaVersion: shape.schemaVersion(),
    sourceAuthorityResult: sourceKey,
    initialReceived: initial,
  });
  if (!initialReceived) {
    replaceMaintainedSubscriptionView(self, node, subscription, makeSubscription(false));
    return undefined;
  }
  const openElapsed = monotonicMs() - openStart;
  const openReads = trace ? node.takeStorageReadMetrics() : undefined;
  const rawAddCount = transitions.adds.length;
  const rawRemoveCount = transitions.removes.length;
  const rawFactAddCount = transitions.programFactAdds.length;
  const filterStart = monotonicMs();
  const policyScoped = aggregateIsPolicyScoped(node, shape);
  const knownState = self.downstreamKnownStates.get(subscription);
  const knownPosition = fastCurrentMembershipPosition(knownState);
  const authorizationMatches = fastCursorAuthorizationMatches(self, subscription, knownState);
  const watermark = maintainedPublicationCut(self, node, subscription);
  const simpleMembershipDelta =
    transitions.programFactAdds.length === 0 && transitions.programFactRemoves.length === 0;
  let adds: ResultMemberEntry[] = transitions.adds.filter((m: ResultMemberEntry) =>
    publishedMember(m, resultTableFilter, tables, policyScoped),
  );
  const current = memberSet(adds);
  let removes = [...previousMemberResultSet].filter((m) => !current.has(m));
  if (!resetInputSet) adds = adds.filter((m) => !previousMemberResultSet.has(m));
  const cursorMismatch =
    !authorizationMatches &&
    knownPosition !== undefined &&
    fastCursorRequiresAuthoritativeReset(knownPosition, previousMemberResultSet, current);
  if (
    resetInputSet &&
    simpleMembershipDelta &&
    !cursorMismatch &&
    knownPosition !== undefined &&
    gtPositive(watermark) &&
    cmpGlobalTime(knownPosition, watermark) >= 0
  ) {
    adds = [];
    removes = [];
    resetInputSet = false;
  } else if (
    resetInputSet &&
    !cursorMismatch &&
    simpleMembershipDelta &&
    knownPosition !== undefined &&
    adds.some((m) => memberSettlePosition(m) !== undefined)
  ) {
    adds = adds.filter((m) => {
      const s = memberSettlePosition(m);
      return s === undefined || cmpGlobalTime(s, knownPosition) > 0;
    });
    removes = [];
    resetInputSet = false;
  }
  const catchUp = gtPositive(watermark)
    ? await watermarkCatchUp(
        node,
        knownState,
        readView.isDefault(),
        maintained.singlePhysicalTable(),
        shape.query(),
        maintained.supportingRows(),
        undefined,
        authorizationMatches,
      )
    : undefined;
  if (catchUp) {
    removes = [];
    self.settleWatermarkDeclaration(subscription);
  }
  const bundleKnownState = cursorMismatch && !catchUp ? undefined : knownState;
  const filterElapsed = monotonicMs() - filterStart;
  const resultAddCount = adds.length;
  const resultRemoveCount = removes.length;
  const positioned = trace ? adds.filter((m) => memberSettlePosition(m) !== undefined).length : 0;
  const bundleStart = monotonicMs();
  if (trace) node.resetStorageReadMetrics();
  const plan: PublicationPlan = {
    subscription,
    policy: self.servedSubscriptionPolicyBinding(subscription),
    tier,
    defaultReadView: readView.isDefault(),
    supportingUpdate: catchUp,
    settledThrough: watermark,
    knownState: bundleKnownState,
    leaveScanAfter: knownPosition,
    adds,
    removes,
  };
  let update: SyncMessage;
  try {
    update = await bundlePublication(self, node, shape, subscription, maintained, plan);
  } catch (err) {
    node.unsubscribeGrooveSubscription(receiver.id());
    throw err;
  }
  if (trace) {
    const bundleElapsed = monotonicMs() - bundleStart;
    const bundleReads = node.takeStorageReadMetrics();
    console.error(
      `JAZZ_REHYDRATE_TRACE stage=rehydrate table=${shape.query().table} subscription=${debugFmt(subscription)} reset=${resetInputSet} known_state=${debugFmt(knownState)} positioned_members=${positioned} open_ms=${Math.floor(openElapsed)} filter_ms=${Math.floor(filterElapsed)} bundle_ms=${Math.floor(bundleElapsed)} raw_adds=${rawAddCount} raw_removes=${rawRemoveCount} raw_fact_adds=${rawFactAddCount} adds=${resultAddCount} removes=${resultRemoveCount} bundles=${tracedBundleCount(update)} open_reads=${openReads.total.reads} open_ranges=${openReads.total.ranges} open_read_buckets=${storageReadMetricsBuckets(openReads)} bundle_reads=${bundleReads.total.reads} bundle_ranges=${bundleReads.total.ranges}`,
    );
  }
  self.applyOutgoingViewDelta(subscription, true, current.toArray(), []);
  replaceMaintainedSubscriptionView(self, node, subscription, makeSubscription(true));
  self.recordOutgoingViewUpdate(update);
  self.stateFor(subscription).hasServedAuthorizationProgress = true;
  self.metrics.maintainedSubscriptionView.hitsOut += 1;
  return update;
}

export async function rehydrateCurrentRows(
  self: PeerState,
  node: any,
  table: string,
): Promise<SyncMessage> {
  const [shape, binding] = node.wholeTableShapeBinding(table);
  return rehydrateQuery(self, node, shape, binding);
}

export const rehydrateQuery = (self: PeerState, node: any, shape: any, binding: any) =>
  rehydrateQueryWithOpts(self, node, shape, binding, RegisterShapeOptions.default());

export async function rehydrateQueryWithOpts(
  self: PeerState,
  node: any,
  shape: any,
  binding: any,
  opts: any,
): Promise<SyncMessage> {
  const subscription = {
    shapeId: shape.shapeId(),
    bindingId: binding.bindingId(),
    readView: opts.readViewKey(),
  };
  const update = await rehydrateQueryForSubscriptionWithOpts(
    self,
    node,
    subscription,
    shape,
    binding,
    opts,
  );
  if (update) return update;
  await node.driveQueryRuntime();
  const again = await rehydrateQueryForSubscriptionFromMaintainedSubscription(
    self,
    node,
    subscription,
    subscription,
    shape,
  );
  if (!again) throw nodeInvalidStoredValue("query hydration ended without an initial publication");
  return again;
}

export function rehydrateQueryForSubscriptionWithOpts(
  self: PeerState,
  node: any,
  subscription: SubscriptionKey,
  shape: any,
  binding: any,
  opts: any,
): Promise<SyncMessage | undefined> {
  self.ensureDirectInternalSubscriptionPolicyBinding(node, subscription);
  return rehydrateQueryForSubscriptionWithOptsAndWaker(
    self,
    node,
    subscription,
    shape,
    binding,
    opts,
  );
}

export function rehydrateQueryForSubscriptionWithOptsAndWaker(
  self: PeerState,
  node: any,
  subscription: SubscriptionKey,
  shape: any,
  binding: any,
  opts: any,
  waker?: any,
): Promise<SyncMessage | undefined> {
  return rehydrateQueryForSubscriptionWithPurpose(
    self,
    node,
    subscription,
    shape,
    binding,
    opts,
    "Query",
    waker,
  );
}

function noteFullDiffReopen(
  self: PeerState,
  subscription: SubscriptionKey,
  purpose: RehydratePurpose,
): void {
  if (!self.maintainedView(subscription)?.initialReceived) return;
  const f = self.metrics.maintainedSubscriptionView.fullDiffFallbacks;
  if (purpose === "Query") f.queryReopens += 1;
  else f.authorizationSupportReopens += 1;
}

export const noteClaimRefreshFullDiff = (self: PeerState, subscription: SubscriptionKey) =>
  noteFullDiffReopen(self, subscription, "Query");

async function rehydrateQueryForSubscriptionWithPurpose(
  self: PeerState,
  node: any,
  subscription: SubscriptionKey,
  shape: any,
  binding: any,
  opts: any,
  purpose: RehydratePurpose,
  waker?: any,
): Promise<SyncMessage | undefined> {
  if (requiresSelectedAuthoritySource(self, subscription, purpose)) {
    const source = selectedAuthoritySource(self, subscription);
    if (!source || !node.hasSettledAuthorityResult(source)) return undefined;
  }
  if (
    purpose === "Query" &&
    hasCurrentPendingInitialMaintainedSubscription(self, node, subscription)
  )
    return queryUpdateMaintainedSubscriptionView(
      self,
      node,
      shape,
      binding,
      subscription,
      undefined,
      waker,
    );
  noteFullDiffReopen(self, subscription, purpose);
  clearStaleGrooveRuntimeHandles(self, node, subscription);
  const prior = self.publicationStates.get(subscription);
  const previousMemberResultSet = self.memberResultSet(subscription);
  const previousMemberIndex = new Map(prior?.memberIndex ?? []);
  const previousLocalAuthority = prior?.localAuthority.clone();
  const knownState = self.downstreamKnownStates.get(subscription);
  const retainedAuthorization = prior?.hasServedAuthorizationProgress
    ? prior.authorizationProgress
    : undefined;
  const authorityResultSource = prior?.authorityResultSource;
  const awaiting = prior?.awaitingSelectedAuthoritySource ?? false;
  const policy = self.servedSubscriptionPolicyBinding(subscription);
  self.forgetSubscriptionWithNode(node, subscription);
  ensureQuerySubscriptionRegistered(self, node, subscription, shape, binding, opts, policy);
  if (knownState) self.downstreamKnownStates.set(subscription, knownState);
  const cached = CachedPeerQueryPlan.withPlan(
    opts,
    node.markPeerMaintainedQueryShapeCache(shape, binding, opts.tier),
  );
  const [tier, readView] = cached.context();
  const state = installPreparedPlan(self, node, subscription, cached);
  state.resultMemberSet = memberSet(previousMemberResultSet);
  state.memberIndex = previousMemberIndex;
  if (previousLocalAuthority) state.localAuthority = previousLocalAuthority;
  state.policyBinding = policy;
  state.authorityResultSource = authorityResultSource;
  state.awaitingSelectedAuthoritySource = awaiting;
  if (retainedAuthorization !== undefined) {
    state.authorizationProgress = retainedAuthorization;
    state.hasServedAuthorizationProgress = true;
  }
  return rehydrateQueryMaintainedSubscriptionView(
    self,
    node,
    {
      shape,
      binding,
      subscription,
      previousMemberResultSet,
      resetInputSet: true,
      resultTableFilter: undefined,
      tier,
      readView,
      purpose,
    },
    waker,
  );
}

/** Hydrate an authority-owned authorization proof under its admitted subject. */
export async function rehydrateAuthorizationSupportQueryForIdentity(
  self: PeerState,
  node: any,
  identity: AuthorSubject,
  claims: Map<string, any>,
  subscription: SubscriptionKey,
  shape: any,
  binding: any,
  opts: any,
): Promise<SyncMessage> {
  self.setSubscriptionPolicyBinding(subscription, [identity, claims]);
  const previousRole = self.role;
  const previousPermissionIdentity = self.permissionIdentity;
  let completed = false;
  self.role = { kind: "ClientLink", identity };
  self.permissionIdentity = identity;
  try {
    const opening = await rehydrateQueryForSubscriptionWithPurpose(
      self,
      node,
      subscription,
      shape,
      binding,
      opts,
      "AuthorizationSupport",
    );
    if (opening) {
      completed = true;
      return opening;
    }
    await node.driveQueryRuntime();
    const result = await queryUpdateMaintainedSubscriptionView(
      self,
      node,
      shape,
      binding,
      subscription,
    );
    if (!result)
      throw nodeInvalidStoredValue(
        "authorization hydration missing initial snapshot after storage progress",
      );
    completed = true;
    return result;
  } finally {
    if (!completed) self.forgetSubscriptionWithNode(node, subscription);
    self.role = previousRole;
    self.permissionIdentity = previousPermissionIdentity;
  }
}

export function rehydrateQueryForSubscriptionFromMaintainedSubscription(
  self: PeerState,
  node: any,
  maintainedSubscription: SubscriptionKey,
  targetSubscription: SubscriptionKey,
  shape: any,
): Promise<SyncMessage | undefined> {
  self.ensureDirectInternalSubscriptionPolicyBinding(node, targetSubscription);
  return rehydrateQueryForSubscriptionFromMaintainedSubscriptionAndWaker(
    self,
    node,
    maintainedSubscription,
    targetSubscription,
    shape,
  );
}

export async function rehydrateQueryForSubscriptionFromMaintainedSubscriptionAndWaker(
  self: PeerState,
  node: any,
  maintainedSubscription: SubscriptionKey,
  targetSubscription: SubscriptionKey,
  shape: any,
  waker?: any,
): Promise<SyncMessage | undefined> {
  clearStaleGrooveRuntimeHandles(self, node, maintainedSubscription);
  const t = await drainMaintainedSubscriptionViewChanges(
    self,
    node,
    shape,
    maintainedSubscription,
    undefined,
    waker,
  );
  if (!self.maintainedView(maintainedSubscription)?.initialReceived) return undefined;
  const hadFacts =
    t.supportingChanged || t.programFactAdds.length > 0 || t.programFactRemoves.length > 0;
  const basis = clonedResetBasis(
    self,
    maintainedSubscription,
    targetSubscription,
    t.removes,
    hadFacts,
  );
  if (hadFacts || t.adds.length || t.removes.length)
    self.applyOutgoingViewDelta(maintainedSubscription, false, t.adds, t.removes);
  return clonedSubscriptionReset(
    self,
    node,
    maintainedSubscription,
    targetSubscription,
    shape,
    basis,
  );
}

function bindClonedSupportingRevision(self: PeerState, source: SubscriptionKey, view: any): void {
  const revision = self.publicationStates.get(source)?.supportingRevision;
  if (revision !== undefined && view.supportingRows.kind === "Snapshot")
    view.supportingRows.revision = revision;
}

export async function reconcileMaintainedSubscriptionForClone(
  self: PeerState,
  node: any,
  maintainedSubscription: SubscriptionKey,
  shape: any,
  binding: any,
  opts: any,
  waker?: any,
): Promise<ReconciledMaintainedSubscriptionClone | undefined> {
  const canonical = await queryUpdateInnerForSubscriptionWithMetadata(
    self,
    node,
    maintainedSubscription,
    shape,
    binding,
    opts,
    waker,
  );
  if (!canonical) return undefined;
  return {
    canonicalUpdate: canonical.changed ? canonical.update : undefined,
    sourceRemoves: [],
    sourceHadProgramFactTransitions: canonical.changed,
  };
}

export function rehydrateQueryForSubscriptionFromReconciledMaintainedSubscription(
  self: PeerState,
  node: any,
  maintainedSubscription: SubscriptionKey,
  targetSubscription: SubscriptionKey,
  shape: any,
  reconciled: ReconciledMaintainedSubscriptionClone,
): Promise<SyncMessage> {
  const basis = clonedResetBasis(
    self,
    maintainedSubscription,
    targetSubscription,
    reconciled.sourceRemoves,
    reconciled.sourceHadProgramFactTransitions,
  );
  return clonedSubscriptionReset(
    self,
    node,
    maintainedSubscription,
    targetSubscription,
    shape,
    basis,
  );
}

function clonedResetBasis(
  self: PeerState,
  maintainedSubscription: SubscriptionKey,
  targetSubscription: SubscriptionKey,
  sourceRemoves: ResultMemberEntry[],
  sourceHadProgramFactTransitions: boolean,
): ClonedResetBasis {
  const knownState = self.downstreamKnownStates.get(targetSubscription);
  const knownMembershipPosition = fastCurrentMembershipPosition(knownState);
  const authorizationMatches = fastCursorAuthorizationMatches(
    self,
    maintainedSubscription,
    knownState,
  );
  const removedMembersAreOrdinary = sourceRemoves.every(ordinaryCurrentContentMember);
  const flat =
    !self.isRelay() &&
    authorizationMatches &&
    removedMembersAreOrdinary &&
    readViewKeyEquals(maintainedSubscription.readView, defaultReadViewKey()) &&
    !sourceHadProgramFactTransitions &&
    self.publicationStates.get(maintainedSubscription)!.supportingRows().length === 0;
  return {
    knownState,
    knownMembershipPosition,
    authorizationMatches,
    removedMembersAreOrdinary,
    sourceHadProgramFactTransitions,
    flatRowRemoves: flat ? sourceRemoves.slice() : undefined,
  };
}

async function clonedSubscriptionReset(
  self: PeerState,
  node: any,
  maintainedSubscription: SubscriptionKey,
  targetSubscription: SubscriptionKey,
  shape: any,
  basis: ClonedResetBasis,
): Promise<SyncMessage> {
  const clientLink = !self.isRelay();
  const canonical = self.publicationStates.get(maintainedSubscription);
  if (!canonical) throw nodeInvalidStoredValue("coverage group subscription is missing peer state");
  const currentSet = canonical.resultMemberSet;
  const currentFacts = canonical.supportingRows();
  const canForwardFlatRemovals =
    clientLink &&
    ordinaryFlatRowDuplicateView(
      shape,
      currentSet,
      basis.removedMembersAreOrdinary,
      maintainedSubscription.readView,
      currentFacts.length === 0,
      basis.sourceHadProgramFactTransitions,
    );
  const position = basis.knownMembershipPosition;
  const authorizationMismatch = clientLink && position !== undefined && !basis.authorizationMatches;
  let targetRemoves =
    canForwardFlatRemovals && basis.authorizationMatches ? (basis.flatRowRemoves ?? []) : [];
  const [tier, readView] = preparedContext(
    self,
    maintainedSubscription,
    "coverage group subscription is missing prepared state",
  );
  let adds: ResultMemberEntry[];
  const settlement = canonicalSubscriptionSettlementTime(self, node, maintainedSubscription);
  if (
    currentFacts.length === 0 &&
    !authorizationMismatch &&
    position !== undefined &&
    gtPositive(settlement) &&
    cmpGlobalTime(position, settlement) >= 0
  ) {
    adds = [];
  } else if (
    currentFacts.length === 0 &&
    !authorizationMismatch &&
    position !== undefined &&
    [...currentSet].some((m) => memberSettlePosition(m) !== undefined)
  ) {
    adds = [...currentSet].filter((m) => {
      const s = memberSettlePosition(m);
      return s === undefined || cmpGlobalTime(s, position) > 0;
    });
  } else {
    adds = currentSet.toArray();
  }
  const policy = self.servedSubscriptionPolicyBinding(targetSubscription);
  const settledThrough = maintainedPublicationCut(self, node, maintainedSubscription);
  const catchUpAuthorizationMatches = fastCursorAuthorizationMatches(
    self,
    targetSubscription,
    basis.knownState,
  );
  const view = canonical.maintainedSubscriptionView;
  const catchUp =
    view && gtPositive(settledThrough)
      ? await watermarkCatchUp(
          node,
          basis.knownState,
          readView.isDefault(),
          view.maintained.singlePhysicalTable(),
          shape.query(),
          view.maintained.supportingRows(),
          canonical.supportingRevision,
          catchUpAuthorizationMatches,
        )
      : undefined;
  if (catchUp) self.settleWatermarkDeclaration(targetSubscription);
  const leaveScanAfter = fastCurrentMembershipPosition(basis.knownState);
  let knownState = basis.knownState;
  if (catchUp) targetRemoves = [];
  else if (authorizationMismatch) knownState = undefined;
  const plan: PublicationPlan = {
    subscription: targetSubscription,
    policy,
    tier,
    defaultReadView: readView.isDefault(),
    supportingUpdate: catchUp,
    settledThrough,
    knownState,
    leaveScanAfter,
    adds,
    removes: targetRemoves,
  };
  const maintained = self.maintainedView(maintainedSubscription);
  if (!maintained)
    throw nodeInvalidStoredValue("coverage group subscription is missing maintained state");
  const reset = await bundlePublication(
    self,
    node,
    shape,
    maintainedSubscription,
    maintained.maintained,
    plan,
  );
  if (reset.kind === "ViewUpdate")
    bindClonedSupportingRevision(self, maintainedSubscription, reset.payload);
  self.recordOutgoingViewUpdateMetadata(reset);
  self.metrics.maintainedSubscriptionView.hitsOut += 1;
  return reset;
}

/** Answer a "Q at W" declaration from the by_seq index for single-table views. */
async function watermarkCatchUp(
  node: any,
  knownState: KnownStateDeclaration | undefined,
  defaultReadView: boolean,
  singleTable: [string, any] | undefined,
  query: any,
  rows: Iterable<any>,
  revision: Uint8Array | undefined,
  authorizationMatches: boolean,
): Promise<any> {
  if (knownState?.kind !== "Watermark") return undefined;
  if (!defaultReadView || !singleTable) return undefined;
  const [table, physicalTable] = singleTable;
  if (!rowLocalMembership(query)) return undefined;
  const readPolicy = node.table(table).readPolicy;
  if (readPolicy && !rowLocalMembership(readPolicy)) return undefined;
  if (
    !authorizationMatches &&
    (queryUsesClaims(query) || (readPolicy && queryUsesClaims(readPolicy)))
  )
    return undefined;
  const update = await node.supportingCatchUpAfter(
    table,
    physicalTable,
    rows,
    knownState.position,
    knownState.supportingRevision,
  );
  if (revision !== undefined && update.kind === "CatchUp") update.revision = revision;
  return update;
}

/** A read policy with no conditions. */
export function readPolicyAdmitsEveryRow(policy: any): boolean {
  return (
    policy.joins.length === 0 &&
    !policy.flatJoin &&
    policy.policyBranches.length === 0 &&
    policy.reachable.length === 0 &&
    policy.inherits.length === 0 &&
    !policy.relation &&
    policy.filters.every((f: any) => f.kind === "All" && f.children.length === 0)
  );
}

/** Membership depends only on the row's own image. */
export function rowLocalMembership(query: any): boolean {
  const scalarFilters =
    query.filters.length === 0 || simpleScalarExitQuery({ ...query, select: undefined });
  return (
    scalarFilters &&
    query.joins.length === 0 &&
    !query.flatJoin &&
    query.policyBranches.length === 0 &&
    query.reachable.length === 0 &&
    query.inherits.length === 0 &&
    query.includes.length === 0 &&
    query.arraySubqueries.length === 0 &&
    !query.aggregate &&
    query.limit === undefined &&
    query.offset === 0 &&
    !query.relation
  );
}

export function queryUsesClaims(query: any): boolean {
  const operand = (o: any) => o.kind === "Claim";
  const predicate = (f: any): boolean => {
    switch (f.kind) {
      case "All":
      case "Any":
        return f.children.some(predicate);
      case "Not":
        return predicate(f.child);
      case "Eq":
      case "Ne":
      case "Gt":
      case "Gte":
      case "Lt":
      case "Lte":
      case "Contains":
        return operand(f.a) || operand(f.b);
      case "In":
        return operand(f.a) || f.list.some(operand);
      case "EnumMatch":
        return predicate(f.payload);
      case "IsNull":
        return operand(f.a);
    }
    return false;
  };
  return query.filters.some(predicate);
}

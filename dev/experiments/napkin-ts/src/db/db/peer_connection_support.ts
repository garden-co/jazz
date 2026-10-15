// Free helpers of peer_connection.rs: send paths, flushes, authority batches, scope serving.
import { toHex } from "../../support/bytes.js";
import { blake3, uuidV4 } from "../../support/hash.js";
import {
  AuthorityScopeAggregate,
  DbError,
  dbg,
  DurabilityTier,
  ErrorCode,
  MAX_AUTHORIZATION_SCOPES,
  MergeableCommit,
  PermissionAdvice,
  QueryAuthorizationMode,
  RowUuid,
  ShapeAst,
  TickUrgency,
  demoteAuthorityReceiptSubscriptions,
  encodedSyncMessageLen,
  mutationErrorEvent,
  mutationErrorEventFor,
  queueMutationError,
  registerLocalFateObserver,
  subscriberPermissionsReady,
  type AuthorSubject,
  type AuthorityContext,
  type CoverageKey,
  type SubscriptionKey,
  type SyncMessage,
  type TickScheduler,
  type TxId,
  type Value,
} from "../../_extern/db-runtime.js";
import {
  cmpSubscriptionKey,
  notifyWriteStateWaiters,
  retireRelayUpstreamSubscription,
  subscriptionKeyString,
  type Node,
  type SharedNodeState,
  type Transport,
  type Waker,
} from "./node_runtime.js";
import {
  coverageGroupSubscriptionKey,
  queueAuthorizationScopeSequence,
  queueDirectControl,
  queueRelaySubscriptionRejection,
  routeInvalidAuthoritySourceClosure,
  type PeerConnection,
  type PendingSubscriberControlResponse,
  type SubscriberConnectionState,
} from "./peer_connection.js";
import { SortedSet } from "../../support/collections.js";

type Scheduler = { current: TickScheduler | undefined };

export function scheduleTickIn(scheduler: Scheduler, urgency: any): void {
  scheduler.current?.scheduleTick(urgency);
}

export function serializedSyncMessageLen(message: SyncMessage): number {
  try {
    return encodedSyncMessageLen(message);
  } catch {
    return 0;
  }
}

function viewUpdateParts(message: any): any {
  if (message.kind !== "ViewUpdate") throw new Error("expected view update message");
  const inventory = message.peerPayloadInventory;
  return {
    wireRows: message.supportingRows,
    subscription: message.subscription,
    settledThrough: message.settledThrough,
    deferSettlement: false,
    resetInputSet: true,
    versionCarriers: message.versionCarriers,
    peerCompleteTxPayloadRefs: inventory.completeTxPayloads,
    authorizationProgress: inventory.authorizationProgress,
    openingPending: inventory.openingPending,
    resultMemberAdds: [],
    resultMemberRemoves: [],
  };
}

export interface PendingAuthorityViewUpdate {
  parts: any;
  authorityReceiptEligible: boolean;
}

export type AuthorityViewBatch = {
  pendingViewUpdates: PendingAuthorityViewUpdate[];
  pendingInitialCoverageClears: Set<string>;
};

export function pushViewUpdateMessageForReceiver(
  ready: PendingAuthorityViewUpdate[],
  message: SyncMessage,
  authorityReceiptEligible: boolean,
): void {
  ready.push({ parts: viewUpdateParts(message), authorityReceiptEligible });
}

export function stageInitialCoverageClearForUpdate(
  update: SyncMessage,
  latest: Map<string, SubscriptionKey>,
  clears: Set<string>,
): void {
  const u: any = update;
  if (u.kind !== "ViewUpdate" || u.peerPayloadInventory.openingPending) return;
  const target = subscriptionKeyString(u.subscription);
  for (const [coverage, current] of latest)
    if (subscriptionKeyString(current) === target) {
      clears.add(coverage);
      return;
    }
}

export async function applyPendingAuthorityViewUpdates(
  pc: PeerConnection,
  batch: AuthorityViewBatch,
  _progressWaker: Waker | undefined,
): Promise<void> {
  const rt = pc.rt;
  const node = pc.node;
  const connectionEpoch = pc.connectionEpoch;
  const pending = batch.pendingViewUpdates;
  // Replacement authority selection is connection-scoped; combine it with each frame's eligibility bit.
  const selectedLink =
    !rt.activeAuthorityViewReceipts ||
    rt.activeAuthorityViewReceipts.connectionEpoch === connectionEpoch;
  const isSelected = (u: PendingAuthorityViewUpdate) => selectedLink && u.authorityReceiptEligible;
  const confirmed = pending
    .filter((u) => isSelected(u) && !u.parts.openingPending)
    .map((u) => [u.parts.subscription, u.parts.settledThrough] as const);
  const sourceConfirmations = new Set(
    pending
      .filter(
        (u) =>
          isSelected(u) &&
          !u.parts.openingPending &&
          !u.parts.deferSettlement &&
          (u.parts.resetInputSet ||
            u.parts.supportingAdds().length > 0 ||
            u.parts.supportingRemoves().length > 0),
      )
      .map((u) => subscriptionKeyString(u.parts.subscription)),
  );
  const maxCut = (cuts: any[]) => cuts.reduce((a, b) => (b > a ? b : a), undefined);
  const batchCut = maxCut(pending.map((u) => u.parts.settledThrough)) ?? 0;
  const ineligibleCut = maxCut(
    pending.filter((u) => !isSelected(u)).map((u) => u.parts.settledThrough),
  );
  const publishing = new SortedSet<SubscriptionKey>(
    cmpSubscriptionKey,
    pending.filter(isSelected).map((u) => u.parts.subscription),
  );
  const nodeRef = node.borrow();
  const relayAuthoritySessionOwner = nodeRef.clientRelayScope() !== undefined;
  const confirmedBindingViews: [SubscriptionKey, any, any][] = [];
  for (const [subscription, settledThrough] of confirmed) {
    try {
      confirmedBindingViews.push([
        subscription,
        nodeRef.bindingViewKeyForSubscription(subscription),
        settledThrough,
      ]);
    } catch {
      // Unknown subscriptions have no receipt to record.
    }
  }
  const receipts = rt.activeAuthorityViewReceipts;
  if (receipts) {
    const invalidationCut = receipts.connectionEpoch !== connectionEpoch ? batchCut : ineligibleCut;
    if (invalidationCut !== undefined) {
      // A nonselected update may recompute views beyond the one it names: no receipt stays safe.
      receipts.bindingViews.clear();
      receipts.subscriptions.clear();
      if (invalidationCut > receipts.confirmationFloor)
        receipts.confirmationFloor = invalidationCut;
      demoteAuthorityReceiptSubscriptions(rt.subscriptions, publishing);
    }
  }
  // A nonselected frame is only evidence of staleness, never an input to the selected receipt's graph.
  const unselectedCarriers = pending
    .filter((u) => !isSelected(u))
    .flatMap((u) => u.parts.versionCarriers);
  const updates = pending
    .splice(0)
    .filter(isSelected)
    .map((u) => u.parts);
  if (unselectedCarriers.length)
    await node.with((n) => n.ingestUnselectedAuthorityViewBundles(unselectedCarriers));
  if (updates.length) {
    const owner = await node.lock();
    try {
      await owner.applyViewUpdatesInBatch(updates);
      owner.recordAuthoritativeSettledThrough(maxCut(confirmed.map(([, cut]) => cut)) ?? 0);
    } catch (error: any) {
      if (error?.kind !== "InvalidAuthoritySourceClosure") throw error;
      const publicRejections = routeInvalidAuthoritySourceClosure(rt.subscriptions, error);
      const oneShotOwner = rt.queryCoverageRegistrations.has(
        subscriptionKeyString(error.subscription),
      );
      const relayRejections = queueRelaySubscriptionRejection(rt, error.subscription, {
        kind: "InvalidAuthoritySourceClosure",
        transition: error.transition,
      });
      if (relayRejections > 0) {
        rt.subscriberDirtyEpoch++;
        scheduleTickIn(rt.scheduler, TickUrgency.Immediate);
      }
      // One-shot attachments have no stream sender: surface the protocol error rather than hang.
      if (oneShotOwner || (publicRejections === 0 && relayRejections === 0)) throw error;
      return;
    } finally {
      node.unlock();
    }
  }
  // Only successfully applied selected source receipts advance reconciliation.
  for (const weak of rt.subscriptions) {
    const state = weak.deref();
    if (
      state?.upstreamSubscriptionHandles.some((h: any) =>
        sourceConfirmations.has(subscriptionKeyString(h.subscription)),
      )
    )
      state.scalarAuthorityRevision++;
  }
  for (const owner of rt.relayUpstreamSubscriptionOwners.valuesArray())
    if (sourceConfirmations.has(subscriptionKeyString(owner.request.subscription)))
      owner.scalarAuthorityRevision++;
  if (relayAuthoritySessionOwner) {
    // A relay authority view feeds every locally served Global child: advance after commit.
    rt.subscriberDirtyEpoch++;
    scheduleTickIn(rt.scheduler, TickUrgency.Immediate);
  }
  const active = rt.activeAuthorityViewReceipts;
  if (active && active.connectionEpoch === connectionEpoch)
    for (const [subscription, bindingView, settledThrough] of confirmedBindingViews) {
      if (settledThrough < active.confirmationFloor) continue;
      // Every usage requires its own accepted receipt.
      active.bindingViews.add(bindingView);
      active.subscriptions.add(subscription);
    }
  for (const coverage of batch.pendingInitialCoverageClears) {
    rt.awaitingInitialAuthorityCoverage.delete(coverage);
    // Coalescing is only valid while this relay reply is outstanding.
    if (relayAuthoritySessionOwner) rt.coverageRefreshGenerations.delete(coverage);
  }
  batch.pendingInitialCoverageClears.clear();
}

export function transportError(error: any): DbError {
  return error?.kind === "Backpressure"
    ? new DbError(ErrorCode.Backpressure, "transport backpressure")
    : new DbError(ErrorCode.Protocol, error?.message ?? String(error));
}

export async function evaluateAuthoritativePermissionAdvice(
  node: any,
  identity: AuthorSubject,
  action: any,
): Promise<any> {
  const draft = (row: any, cells: any) =>
    new MergeableCommit(action.table, row, 0)
      .madeBy(identity)
      .permissionSubject(identity)
      .cells(cells);
  let allowed: boolean;
  try {
    switch (action.kind) {
      case "Insert": {
        const policy = node.table(action.table).writePolicies.insertCheck;
        if (policy && queryRootFiltersReferenceId(policy)) return PermissionAdvice.Unknown;
        allowed = await node.dryRunInsertAllows(
          draft(RowUuid.fromBytes(new Uint8Array(16)), action.cells),
        );
        break;
      }
      case "Read":
        allowed = await node.dryRunReadCurrentAllows(action.table, action.row, identity);
        break;
      case "Update":
        allowed =
          (await node.localCurrentRowExists(action.table, action.row)) &&
          (await node.dryRunInsertAllows(draft(action.row, action.patch)));
        break;
      default:
        allowed = await node.dryRunDeleteCurrentAllows(action.table, action.row, identity);
    }
  } catch {
    return PermissionAdvice.Unknown;
  }
  return allowed ? PermissionAdvice.Allowed : PermissionAdvice.Denied;
}

/**
 * Compile and serve an authorization scope entirely at the serving authority, with
 * authority-allocated opaque usage keys; proof receivers are retired once the receipt is built.
 */
export async function serveAuthorizationScopeIntent(
  node: SharedNodeState,
  peer: any,
  pending: PendingSubscriberControlResponse[],
  identity: AuthorSubject,
  sessionClaims: Map<string, Value>,
  connectionEpoch: number,
  requestId: string,
  action: any,
  trust: any,
  state: SubscriberConnectionState,
): Promise<void> {
  const unavailable = () =>
    queueDirectControl(pending, {
      kind: "AuthorizationScopeUnavailable",
      requestId,
    } as SyncMessage);
  const n0 = node.borrow();
  if (!n0.isHistoryComplete() || !subscriberPermissionsReady(n0.permissionsReady(), trust))
    return unavailable();
  let scope: any;
  try {
    scope = n0.authorizationSupportScopeForSession(identity, sessionClaims, action);
  } catch {
    return unavailable();
  }
  const seen = new Set<string>();
  const clauses: [any, any][] = scope.subscriptions.filter(([shape, binding]: [any, any]) => {
    const key = `${dbg(shape.shapeId())}|${dbg(binding.bindingId())}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  if (clauses.length > 0xffff)
    throw new DbError(ErrorCode.Protocol, "authorization scope has too many clauses");
  if (clauses.length === 0) {
    const advice = await node.with((n) =>
      evaluateAuthoritativePermissionAdvice(
        n.scopedActiveSessionClaims(identity, sessionClaims),
        identity,
        action,
      ),
    );
    queueDirectControl(pending, {
      kind: "AuthorizationScopeDecision",
      requestId,
      advice,
    } as SyncMessage);
    return;
  }
  const claimsRevision = n0.sessionClaimRevision(identity);
  const policyEpoch = n0.activeCatalogueSeq();
  const currentCut = n0.committedGlobalTime();
  // A cache entry is evidence, not a generic response cache: prune revision/cut mismatches first.
  const hydrations = state.authorityScopeHydrations;
  for (const [key, hydration] of hydrations)
    if (
      hydration.receipt.claimsRevision !== claimsRevision ||
      hydration.receipt.policyEpoch !== policyEpoch ||
      dbg(hydration.receipt.settledThrough) !== dbg(currentCut)
    )
      hydrations.delete(key);
  const scopeKey = dbg(scope.key);
  const cached = hydrations.get(scopeKey);
  if (cached) return queueAuthorizationScopeSequence(pending, requestId, scope.key, cached);
  state.authorityScopeHydrationCount++;
  const aggregate = new AuthorityScopeAggregate(
    clauses.map(([shape, binding]) => [shape.shapeId(), binding.bindingId()]),
  );
  const owner = await node.lock();
  const receivers: SubscriptionKey[] = [];
  try {
    const served: any[] = [];
    for (let index = 0; index < clauses.length; index++) {
      const [shape, binding] = clauses[index];
      // This usage-site key belongs to the authority; it can't be inferred from the binding identity.
      const subscription: SubscriptionKey = {
        shapeId: shape.shapeId(),
        bindingId: uuidV4(),
        readView: scope.options.readViewKey(),
      } as SubscriptionKey;
      if (!aggregate.register(subscription, [shape.shapeId(), binding.bindingId()]))
        return unavailable();
      const scoped = owner.scopedActiveSessionClaims(identity, sessionClaims);
      try {
        await scoped.ensurePeerMaintainedSubscriptionViewSupported(
          shape,
          binding,
          scope.options.tier,
          identity,
          scope.options.readView,
          QueryAuthorizationMode.TrustedServing,
        );
      } catch {
        return unavailable();
      }
      const register = {
        kind: "RegisterShape",
        shapeId: shape.shapeId(),
        ast: ShapeAst.fromValidated(shape),
        opts: scope.options,
      };
      const subscribe = {
        kind: "Subscribe",
        shapeId: shape.shapeId(),
        subscription,
        values: bindingValuesInParamOrder(shape, binding),
        knownState: undefined,
        delegatedSession: undefined,
      };
      receivers.push(subscription);
      peer.declareKnownState(subscription, undefined);
      // Bind the request's admission snapshot, not the authority transport identity.
      peer.setSubscriptionPolicyBinding(subscription, [identity, sessionClaims]);
      const update = await peer.rehydrateAuthorizationSupportQueryForIdentity(
        owner.scopedActiveSessionClaims(identity, sessionClaims),
        identity,
        sessionClaims,
        subscription,
        shape,
        binding,
        scope.options,
      );
      if (update.kind !== "ViewUpdate")
        throw new DbError(ErrorCode.Protocol, "scope hydration was not a view update");
      const progress = peer.authorizationProgressForSubscription(subscription);
      if (
        aggregate.apply(subscription, update.settledThrough, progress) === undefined &&
        index + 1 === clauses.length
      )
        return unavailable();
      served.push({ subscription, register, subscribe, view: update });
    }
    const bounds = aggregate.bounds();
    if (!bounds) return unavailable();
    const [settledThrough, authorizationProgress] = bounds;
    const hydration = {
      clauses: served,
      receipt: {
        key: scope.key,
        authority: owner.nodeUuid(),
        link: identity,
        authorityEpoch: connectionEpoch,
        claimsRevision,
        policyEpoch,
        settledThrough,
        authorizationProgress,
      },
    };
    if (hydrations.size < MAX_AUTHORIZATION_SCOPES) hydrations.set(scopeKey, hydration);
    queueAuthorizationScopeSequence(pending, requestId, scope.key, hydration);
  } finally {
    // Scope views are proof material, not application subscriptions.
    for (const subscription of receivers) peer.forgetSubscriptionWithNode(owner, subscription);
    node.unlock();
  }
}

/** Check a transported receipt against this connection's authority and the applied support view. */
export function authorizationScopeReceiptMatchesTransportContext(
  receipt: any,
  expected: AuthorityContext,
  expectedSubject: AuthorSubject,
  appliedCut: any,
): boolean {
  return (
    dbg(receipt.link) === dbg(expectedSubject) &&
    dbg(receipt.key.subject) === dbg(expectedSubject) &&
    dbg(receipt.authority) === dbg(expected.authority) &&
    receipt.authorityEpoch === expected.connectionEpoch &&
    receipt.claimsRevision === expected.claimsRevision &&
    receipt.policyEpoch === expected.policyEpoch &&
    receipt.authorizationProgress >= expected.authorizationProgress &&
    receipt.settledThrough[0] >= expected.settledThrough &&
    appliedCut !== undefined &&
    appliedCut >= receipt.settledThrough
  );
}

/**
 * Undo a usage-site admission that failed before its opening was accepted; retire the group
 * and its upstream ownership with the last usage.
 */
export function rollbackRejectedSubscriberAdmission(
  node: SharedNodeState,
  peer: any,
  served: SubscriberConnectionState["served"],
  coverageGroups: SubscriberConnectionState["coverageGroups"],
  relayOwners: Node["relayUpstreamSubscriptionOwners"],
  upstreamSubscriptions: any[],
  connectionEpoch: number,
  subscription: SubscriptionKey,
): void {
  const coverage = served.get(subscription);
  if (!coverage) return;
  served.delete(subscription);
  const n = node.borrow();
  const group = coverageGroups.get(coverage);
  if (!group) {
    n.applyUnsubscribe(subscription);
    peer.forgetSubscription(subscription);
    return;
  }
  const key = subscriptionKeyString(subscription);
  group.subscribers.delete(subscription);
  group.pendingInitialSubscribers.delete(subscription);
  group.pendingIncrementalUpdates = group.pendingIncrementalUpdates.filter(
    ([recipient]: [SubscriptionKey]) => subscriptionKeyString(recipient) !== key,
  );
  if (group.pendingInitialUpdate && subscriptionKeyString(group.pendingInitialUpdate[0]) === key)
    group.pendingInitialUpdate = undefined;
  const owner = relayOwners.get([
    group.upstreamSubscription,
    connectionEpoch,
    coverage.opts.readViewKey(),
  ]);
  if (
    owner &&
    owner.downstreamConnectionEpoch === connectionEpoch &&
    dbg(owner.coverage) === dbg(coverage)
  )
    owner.downstreamSubscriptions.delete(subscription);
  n.applyUnsubscribe(subscription);
  peer.forgetSubscription(subscription);
  if (group.subscribers.size) return;
  const upstream: SubscriptionKey = group.upstreamSubscription;
  peer.forgetSubscriptionWithNode(n, coverageGroupSubscriptionKey(coverage as CoverageKey));
  coverageGroups.delete(coverage);
  if (
    retireRelayUpstreamSubscription(relayOwners, upstream, connectionEpoch, coverage) === undefined
  )
    return;
  const upstreamKey = subscriptionKeyString(upstream);
  const isOpen = (c: any) =>
    c.kind === "Subscribe" && subscriptionKeyString(c.subscription) === upstreamKey;
  const openWasPending = upstreamSubscriptions.some(isOpen);
  for (let i = upstreamSubscriptions.length - 1; i >= 0; i--)
    if (isOpen(upstreamSubscriptions[i])) upstreamSubscriptions.splice(i, 1);
  if (!openWasPending) upstreamSubscriptions.push({ kind: "Unsubscribe", subscription: upstream });
}

function queryRootFiltersReferenceId(query: any): boolean {
  const refs = (q: any) =>
    q.filters.some(predicateReferencesId) ||
    q.reachable.length > 0 ||
    q.joins.some(rootJoinReferencesId);
  return refs(query) || query.policyBranches.some(refs);
}

function rootJoinReferencesId(join: any): boolean {
  return (
    join.sourceColumn === undefined ||
    join.sourceColumn === "id" ||
    join.sourceLookup?.rowIdSourceColumn === "id" ||
    join.correlatedFilters.some((c: any) => c.sourceColumn === "id")
  );
}

function predicateReferencesId(predicate: any): boolean {
  switch (predicate.kind) {
    case "All":
    case "Any":
      return predicate.items.some(predicateReferencesId);
    case "Not":
      return predicateReferencesId(predicate.item);
    case "In":
      return operandIsId(predicate.operand) || predicate.values.some(operandIsId);
    case "IsNull":
      return operandIsId(predicate.operand);
    case "EnumMatch":
      return predicate.column === "id" || predicateReferencesId(predicate.payload);
    default:
      return operandIsId(predicate.left) || operandIsId(predicate.right);
  }
}

const operandIsId = (operand: any): boolean => operand.kind === "Column" && operand.column === "id";

export function dropPeerRequest(node: SharedNodeState): void {
  node.borrow().recordDroppedPeerRequest();
}

export function handleTransportBackpressure(
  node: SharedNodeState,
  scheduler: Scheduler,
  error: any,
): boolean {
  if (error?.kind !== "Backpressure") return false;
  node.borrow().recordTransportBackpressureRetry();
  scheduleTickIn(scheduler, TickUrgency.Deferred);
  return true;
}

export function handleDbBackpressure(
  node: SharedNodeState,
  scheduler: Scheduler,
  error: any,
): boolean {
  if (error?.code !== ErrorCode.Backpressure) return false;
  node.borrow().recordTransportBackpressureRetry();
  scheduleTickIn(scheduler, TickUrgency.Deferred);
  return true;
}

function send(transport: Transport, message: SyncMessage): void {
  try {
    transport.send(message);
  } catch (error) {
    throw transportError(error);
  }
}

export function sendWithSyncContext(
  node: SharedNodeState,
  peer: any,
  transport: Transport,
  message: SyncMessage,
): void {
  sendCatalogueSnapshotIfNeeded(node, peer, transport);
  prepareForSend(peer, message);
  send(transport, message);
}

/** Like `sendWithSyncContext`, but hands a backpressured message back. */
function trySendWithSyncContext(
  node: SharedNodeState,
  peer: any,
  transport: Transport,
  message: SyncMessage,
): SyncMessage | undefined {
  try {
    sendCatalogueSnapshotIfNeeded(node, peer, transport);
  } catch (error: any) {
    if (error?.code === ErrorCode.Backpressure) return message;
    throw error;
  }
  prepareForSend(peer, message);
  try {
    return transport.trySend(message);
  } catch (error) {
    throw transportError(error);
  }
}

/** Stamp and bound an outbound message; idempotent across backpressure retries. */
function prepareForSend(peer: any, message: SyncMessage): void {
  const m: any = message;
  if (m.kind === "ViewUpdate")
    m.peerPayloadInventory.authorizationProgress ??= peer.authorizationProgressForSubscription(
      m.subscription,
    );
}

export function sendSubscriberWithSyncContext(
  node: SharedNodeState,
  peer: any,
  transport: Transport,
  rt: Node,
  downstreamFates: any[],
  message: SyncMessage,
): void {
  stampSubscriberOpeningState(node, peer, message);
  sendPreparedSubscriberWithSyncContext(node, peer, transport, rt, downstreamFates, message);
}

/** A selected-but-unsettled upstream source keeps every route to the foreground opening-pending. */
export function stampSubscriberOpeningState(
  node: SharedNodeState,
  peer: any,
  message: SyncMessage,
): void {
  const m: any = message;
  if (m.kind !== "ViewUpdate" || !peer.subscriptionAwaitsSelectedAuthoritySource(m.subscription))
    return;
  const source = peer.subscriptionAuthorityResultSource(m.subscription);
  if (source !== undefined && !node.borrow().hasSettledAuthorityResult(source))
    m.peerPayloadInventory.openingPending = true;
}

/** A retained publication already owns its envelope; retry must not restamp it. */
export function sendPreparedSubscriberWithSyncContext(
  node: SharedNodeState,
  peer: any,
  transport: Transport,
  rt: Node,
  downstreamFates: any[],
  message: SyncMessage,
): void {
  const pendingTxIds = pendingViewUpdateTxIds(message);
  sendWithSyncContext(node, peer, transport, message);
  for (const txId of pendingTxIds)
    registerLocalFateObserver(rt.localFateRoutes, txId, downstreamFates);
}

export function trySendPreparedSubscriberWithSyncContext(
  node: SharedNodeState,
  peer: any,
  transport: Transport,
  rt: Node,
  downstreamFates: any[],
  message: SyncMessage,
): SyncMessage | undefined {
  const pendingTxIds = pendingViewUpdateTxIds(message);
  const rejected = trySendWithSyncContext(node, peer, transport, message);
  if (!rejected)
    for (const txId of pendingTxIds)
      registerLocalFateObserver(rt.localFateRoutes, txId, downstreamFates);
  return rejected;
}

/** Pending transactions a view update ships, whose fates its receiver waits on. */
function pendingViewUpdateTxIds(message: SyncMessage): Map<string, TxId> {
  const out = new Map<string, TxId>();
  const m: any = message;
  if (m.kind !== "ViewUpdate") return out;
  for (const carrier of m.versionCarriers) {
    let bundles: any[];
    try {
      bundles = carrier.bundleRefs();
    } catch {
      throw new DbError(ErrorCode.Protocol, "malformed version-bundle run");
    }
    for (const bundle of bundles)
      if (bundle.fate.kind === "Pending") out.set(dbg(bundle.tx.txId), bundle.tx.txId);
  }
  return out;
}

/** Whether an authority answer about this link's uploads may change its outbox. */
export function uploadAnswerReceiptEligible(
  authorityReceiptEligible: boolean,
  expected: AuthorityContext | undefined,
  admitted: AuthorityContext | undefined,
): boolean {
  return (
    authorityReceiptEligible &&
    (!expected || (admitted !== undefined && admitted.sameAdmittedLink(expected)))
  );
}

/** A terminal fate no upstream can revise: a rejection or a Global-durable acceptance. */
export function fateUpdateSettlesUpload(message: SyncMessage, txId: TxId): boolean {
  const m: any = message;
  return (
    m.kind === "FateUpdate" &&
    dbg(m.txId) === dbg(txId) &&
    (m.fate.kind === "Rejected" ||
      (m.fate.kind === "Accepted" &&
        m.globalTime !== undefined &&
        m.durability !== undefined &&
        DurabilityTier.cmp(m.durability, DurabilityTier.Global) >= 0))
  );
}

/** Deliver fates FIFO, keeping any unaccepted one at the producer boundary. */
export function flushDownstreamFates(
  node: SharedNodeState,
  peer: any,
  transport: Transport,
  fates: any[],
  readBarrier: any,
  scheduler: Scheduler,
): boolean {
  while (fates.length) {
    try {
      sendWithSyncContext(node, peer, transport, fates[0]);
    } catch (error: any) {
      if (error?.code !== ErrorCode.Backpressure) throw error;
      scheduleTickIn(scheduler, TickUrgency.Deferred);
      return false;
    }
    // Requests a now-answered write released are served on the next turn.
    if (readBarrier.answerSent(fates.shift())) scheduleTickIn(scheduler, TickUrgency.Immediate);
  }
  return true;
}

/** Retry the one ordinary-wire chunk response whose byte admission was refused. */
export function flushPendingChunkResponse(pc: PeerConnection): boolean {
  const response = pc.pendingChunkResponse;
  if (!response) return true;
  pc.pendingChunkResponse = undefined;
  try {
    pc.transport.send({ kind: "ChunkResponseBatch", ...response } as SyncMessage);
    return true;
  } catch (error: any) {
    pc.pendingChunkResponse = response;
    if (error?.kind !== "Backpressure") throw transportError(error);
    scheduleTickIn(pc.rt.scheduler, TickUrgency.Deferred);
    return false;
  }
}

/** Retry subscriber-control replies; one retained message bounds a stalled link. */
export function flushPendingControlResponses(
  node: SharedNodeState,
  peer: any,
  transport: Transport,
  pending: PendingSubscriberControlResponse[],
  scheduler: Scheduler,
): boolean {
  while (pending.length) {
    const response = pending[0];
    try {
      switch (response.kind) {
        case "Direct":
          send(transport, response.message);
          break;
        case "WithSyncContext":
          sendWithSyncContext(node, peer, transport, response.message);
          break;
        case "AuthorizationScopeSequence": {
          const message = response.sequence.nextMessage();
          if (!message) {
            pending.shift();
            continue;
          }
          // Scope hydration carries physical row identities: install the catalogue first.
          sendWithSyncContext(node, peer, transport, message);
          response.sequence.advance();
          if (response.sequence.nextMessage()) continue;
        }
      }
    } catch (error: any) {
      if (error?.code !== ErrorCode.Backpressure) throw error;
      scheduleTickIn(scheduler, TickUrgency.Deferred);
      return false;
    }
    pending.shift();
  }
  return true;
}

/** Send an authority catalogue snapshot exactly once per peer fingerprint. */
export function sendCatalogueSnapshotIfNeeded(
  node: SharedNodeState,
  peer: any,
  transport: Transport,
): void {
  const snapshot = node.borrow().catalogueSnapshot();
  const fingerprint = blake3(new TextEncoder().encode(JSON.stringify(snapshot)));
  if (!peer.needsCatalogueSnapshot(fingerprint)) return;
  send(transport, { kind: "CatalogueSnapshot", snapshot } as SyncMessage);
  peer.markCatalogueSnapshotAnnounced(fingerprint);
}

export function sendWithLocalSyncContext(
  _node: SharedNodeState,
  transport: Transport,
  message: SyncMessage,
): void {
  send(transport, message);
}

export function stampViewUpdateAuthorizationProgressFrom(
  peer: any,
  source: SubscriptionKey,
  message: SyncMessage,
): SyncMessage {
  const m: any = message;
  if (m.kind !== "ViewUpdate") return message;
  const progress = peer.authorizationProgressForSubscription(source);
  if (
    subscriptionKeyString(source) !== subscriptionKeyString(m.subscription) &&
    progress === peer.authorizationProgressForSubscription(m.subscription)
  )
    return message;
  m.peerPayloadInventory.authorizationProgress = progress;
  return message;
}

/** Every sibling but the last gets a clone; the last takes the original. */
export function takeLastOrClone(
  slot: { value: SyncMessage | undefined },
  moreFollow: boolean,
): SyncMessage {
  const value = slot.value;
  if (value === undefined)
    throw new Error(
      moreFollow ? "update remains for later siblings" : "update remains for the last sibling",
    );
  if (moreFollow) return structuredClone(value);
  slot.value = undefined;
  return value;
}

export function retargetViewUpdate(message: SyncMessage, target: SubscriptionKey): SyncMessage {
  const m: any = message;
  if (m.kind === "ViewUpdate") m.subscription = target;
  return message;
}

export function writeStateUpdateTxId(message: SyncMessage): TxId | undefined {
  const m: any = message;
  return m.kind === "FateUpdate" ? m.txId : undefined;
}

export function handleWriteStateUpdate(node: SharedNodeState, rt: Node, txId: TxId): void {
  notifyWriteStateWaiters(rt.writeStateWaiters, txId);
  const rejected = node.borrow().rejectedTransaction(txId);
  if (rejected) {
    queueMutationError(rt.mutationErrors, rt.scheduler, txId, mutationErrorEvent(rejected));
    return;
  }
  // A restarted browser relay's replayed foreground commit still owes one live rejection notice.
  if (!rt.browserRelayRecoveredTxIds.has(txId)) return;
  void node
    .borrow()
    .transactionRecord(txId)
    .then((record: any) => {
      if (!record) return;
      const terminal =
        record.fate.kind === "Rejected" ||
        (record.fate.kind === "Accepted" &&
          record.globalTime !== undefined &&
          DurabilityTier.cmp(record.durability, DurabilityTier.Global) >= 0);
      if (
        !terminal ||
        !rt.browserRelayRecoveredTxIds.delete(txId) ||
        record.fate.kind !== "Rejected"
      )
        return;
      queueMutationError(
        rt.mutationErrors,
        rt.scheduler,
        txId,
        mutationErrorEventFor(txId, record.kind, record.fate.reason),
      );
    });
}

/** Bindings carry values positionally; the shape orders them by param name. */
export function bindingValuesInParamOrder(shape: any, binding: any): Value[] {
  const values = binding.values();
  return [...shape.params().keys()].map((name: string) => {
    if (!values.has(name)) throw new Error("binding is missing a shape parameter value");
    return values.get(name);
  });
}

/** Describe an unavailable upload chunk by locator fingerprint, never raw locator bytes. */
export function largeValueUploadChunkContext(
  txId: TxId,
  valueRef: any,
  nodeRef: any,
  replicaRole: string,
  sourceNode: any,
): string {
  return `role=${replicaRole} source_node=${dbg(sourceNode)} transaction=${dbg(txId)} root_hash=${toHex(valueRef.root.objectHash)} root_locator=${chunkLocatorFingerprint(valueRef.root.locator)} chunk_hash=${toHex(nodeRef.objectHash)} chunk_locator=${chunkLocatorFingerprint(nodeRef.locator)}`;
}

const chunkLocatorFingerprint = (locator: any): string =>
  toHex(blake3(locator.asBytes())).slice(0, 16);

// Subscription opening, query attachment, coverage, and cleanup.
import {
  AUTHOR_SYSTEM,
  AuthorityResultKey,
  BindingViewKey,
  DurabilityTier,
  JazzDbError,
  RelationSnapshotIndex,
  ShapeAst,
  SubscriptionStream,
  applyMaintainedUpdateToSnapshot,
  authorEquals,
  bindingViewKeyEquals,
  ensureSupportedReadView,
  ensureSupportedSubscriptionReadOpts,
  materializeSubscriptionTerminalRecords,
  readCoverageTier,
  registerShapeOptions,
  registerUpstreamSubscriptionOwner,
  relationQueryToQuery,
  relationSnapshotWithDeltaSlack,
  requestCoverageKey,
  simpleScalarExitQuery,
  singleTableScalarQuery,
  subscriptionIsSettled,
  subscriptionOutputsWithOccurrenceSidecar,
  validateShapeAstForRegistration,
} from "../../_extern/db-rest-wasm.js";
import { Channel } from "../../support/runtime.js";
import { registerOpeningGate, resolveFirstLoad } from "./first_load.js";

type Db = any;
type Prepared = any;
type ReadOpts = any;
export type SerializedSubscriptionAuthorization =
  | { kind: "ClientLocal" }
  | { kind: "TrustedServing"; author: any }
  | { kind: "TrustedClient"; author: any };

export interface QueryAttachment {
  subscriptions: any[];
  requiredAfter: [any, number][];
  requiresDeliveryReceipt: boolean;
  requiresCurrentAuthorityReceipt: boolean;
  registrations: any[];
  refreshes: [any, number][];
}

/** Decode, prepare, scope, and open a serialized host subscription. */
export async function subscribeSerializedQuery(
  db: Db,
  query: Uint8Array,
  opts: ReadOpts,
  requestScope: [any, Map<string, any>] | undefined,
  authorization: SerializedSubscriptionAuthorization,
) {
  await db.awaitOpenSchemaForCoverage(readCoverageTier(opts));
  const prepared = await db.prepareSerializedQueryAsync(query, requestScope);
  switch (authorization.kind) {
    case "ClientLocal":
      return subscribe(db, prepared, opts);
    case "TrustedServing":
      return subscribeForIdentity(db, prepared, opts, authorization.author);
    case "TrustedClient": {
      const author = authorization.author;
      const requestIdentity = prepared.requestIdentity();
      if (
        !authorEquals(author, AUTHOR_SYSTEM) &&
        !(requestIdentity && authorEquals(requestIdentity, author))
      )
        throw new JazzDbError(
          "Protocol",
          "trusted client subscription requires immutable request claims",
        );
      return subscribeClientForIdentity(db, prepared, opts, author);
    }
  }
}

export function subscribe(db: Db, prepared: Prepared, opts: ReadOpts) {
  return openSubscription(db, prepared, opts, db.identity.author, "ClientLocal", true);
}

export function subscribeForIdentity(db: Db, prepared: Prepared, opts: ReadOpts, author: any) {
  return openSubscription(db, prepared, opts, author, "TrustedServing", false);
}

/** Subscribe from a trusted backend client on behalf of an admitted author. */
export function subscribeClientForIdentity(
  db: Db,
  prepared: Prepared,
  opts: ReadOpts,
  author: any,
) {
  const mode = readCoverageTier(opts) >= DurabilityTier.Global ? "ClientLocal" : "TrustedServing";
  return openSubscription(db, prepared, opts, author, mode, false);
}

export async function subscribeRelationQuery(db: Db, query: any, opts: ReadOpts) {
  await db.awaitOpenSchemaForCoverage(readCoverageTier(opts));
  ensureSupportedSubscriptionReadOpts(opts);
  const prepared = db.prepareQuery(relationQueryToQuery(query));
  return openSubscription(db, prepared, opts, db.identity.author, "ClientLocal", true);
}

/** Attach a one-shot usage-site query coverage request. */
export function attachQueryWithOpts(db: Db, prepared: Prepared, opts: ReadOpts): QueryAttachment {
  db.ensureOpenSchemaAdmitted();
  ensureSupportedReadView(opts);
  if (opts.tier === "LocalOnly") return localQueryAttachment(db, prepared, opts);
  const upstreamOpts = db.node.upstreamRegisterShapeOptions(readCoverageTier(opts), opts.readView);
  return attachOrRefreshQueryCoverage(
    db,
    db.node.node.borrow(),
    prepared.shape,
    prepared.binding,
    upstreamOpts,
    db.identity.author,
    prepared.requestPolicyBinding(db.identity.author),
    readCoverageTier(opts) >= DurabilityTier.Global,
  );
}

export async function attachQueryWithOptsForIdentity(
  db: Db,
  prepared: Prepared,
  opts: ReadOpts,
  author: any,
): Promise<QueryAttachment> {
  db.ensureOpenSchemaAdmitted();
  ensureSupportedReadView(opts);
  if (opts.tier === "LocalOnly") return localQueryAttachment(db, prepared, opts);
  const upstreamOpts = db.node.upstreamRegisterShapeOptions(readCoverageTier(opts), opts.readView);
  const owner = db.node.node.borrow();
  const node = prepared.scopedNode(owner, author);
  const [shape, binding] = await node.prepareQueryBindingForLink(
    prepared.shape,
    prepared.binding,
    upstreamOpts.tier,
    author,
  );
  return attachOrRefreshQueryCoverage(
    db,
    owner,
    shape,
    binding,
    upstreamOpts,
    author,
    prepared.requestPolicyBinding(author),
    readCoverageTier(opts) >= DurabilityTier.Global,
  );
}

/** LocalOnly owns no remote coverage, even for a memory-only foreground. */
function localQueryAttachment(db: Db, prepared: Prepared, opts: ReadOpts): QueryAttachment {
  return {
    subscriptions: [
      db.node.nextSubscriptionKey(
        prepared.shape,
        registerShapeOptions({ readView: opts.readView }).readViewKey(),
      ),
    ],
    requiredAfter: [],
    requiresDeliveryReceipt: false,
    requiresCurrentAuthorityReceipt: false,
    registrations: [],
    refreshes: [],
  };
}

/** Wait for the node owner before opening ordinary or identity-aware coverage. */
export function attachQueryWithOptsAsync(
  db: Db,
  prepared: Prepared,
  opts: ReadOpts,
  openTxId?: any,
  author?: any,
) {
  return attachQueryWithCoverageAsync(db, prepared, opts, readCoverageTier(opts), openTxId, author);
}

export async function attachQueryWithCoverageAsync(
  db: Db,
  prepared: Prepared,
  opts: ReadOpts,
  coverageTier: number,
  openTxId?: any,
  author?: any,
): Promise<QueryAttachment> {
  await db.awaitOpenSchemaForCoverage(coverageTier);
  ensureSupportedReadView(opts);
  const owner =
    openTxId !== undefined
      ? await db.node.lockForTransactionOperation(openTxId, db.ownerOperationAdmitted)
      : await db.node.node.lock();
  const identity = author ?? db.identity.author;
  const node = prepared.scopedNode(owner, identity);
  if (openTxId !== undefined)
    opts = {
      ...opts,
      readView: { source: { kind: "Snapshot", snapshot: node.openTransactionSnapshot(openTxId) } },
    };
  if (opts.tier === "LocalOnly") return localQueryAttachment(db, prepared, opts);
  const upstreamOpts = db.node.upstreamRegisterShapeOptions(coverageTier, opts.readView);
  let shape = prepared.shape;
  let binding = prepared.binding;
  if (author !== undefined)
    [shape, binding] = await node.prepareQueryBindingForLink(
      prepared.shape,
      prepared.binding,
      upstreamOpts.tier,
      author,
    );
  return attachOrRefreshQueryCoverage(
    db,
    node,
    shape,
    binding,
    upstreamOpts,
    identity,
    prepared.requestPolicyBinding(identity),
    coverageTier >= DurabilityTier.Global,
  );
}

function attachOrRefreshQueryCoverage(
  db: Db,
  node: any,
  shape: any,
  binding: any,
  upstreamOpts: any,
  identity: any,
  policyBinding: any,
  requiresCurrentAuthorityReceipt: boolean,
): QueryAttachment {
  const rt = db.node;
  const requiresDeliveryReceipt =
    requiresCurrentAuthorityReceipt || rt.upstreamDurabilityFloor === DurabilityTier.Local;
  const bindingView = new BindingViewKey(
    shape.shapeId(),
    binding.bindingId(),
    upstreamOpts.readViewKey(),
  );
  const coverage = requestCoverageKey(shape, binding, upstreamOpts, policyBinding);
  const authorityKey = coverage.policyBinding
    ? AuthorityResultKey.policyScoped(bindingView, coverage.policyBinding)
    : AuthorityResultKey.unscoped(bindingView);
  const requiredAfter: number = node.appliedAuthorityResultGeneration(authorityKey);
  const pending = (subscription: any) => ({
    subscription,
    shape,
    binding,
    opts: upstreamOpts,
    identity,
    policyBinding,
  });
  // All live usages pin one stream. One-shot freshness is a newer receipt on it.
  const existing = rt.upstreamCoverageRefcounts.has(coverage)
    ? rt.latestCoverageSubscriptions.get(coverage)
    : undefined;
  if (existing !== undefined) {
    rt.upstreamCoverageRefcounts.set(
      coverage,
      (rt.upstreamCoverageRefcounts.get(coverage) ?? 0) + 1,
    );
    const pendingSubscription = pending(existing);
    registerQueryCoverage(db, coverage, pendingSubscription);
    const refreshes = rt.coverageRefreshGenerations;
    if (refreshes.get(coverage) !== requiredAfter) {
      refreshes.set(coverage, requiredAfter);
      rt.upstreamSubscriptions.push({ kind: "Subscribe", subscription: pendingSubscription });
      rt.scheduleTick("Immediate");
    }
    return {
      subscriptions: [existing],
      requiredAfter: [[bindingView, requiredAfter]],
      requiresDeliveryReceipt,
      requiresCurrentAuthorityReceipt,
      registrations: [existing],
      refreshes: [[coverage, requiredAfter]],
    };
  }
  const subscription = attachQueryShapeBindingWithOpts(
    db,
    shape,
    binding,
    upstreamOpts,
    identity,
    policyBinding,
  );
  rt.upstreamCoverageRefcounts.set(coverage, (rt.upstreamCoverageRefcounts.get(coverage) ?? 0) + 1);
  registerQueryCoverage(db, coverage, pending(subscription));
  return {
    subscriptions: [subscription],
    requiredAfter: [[bindingView, requiredAfter]],
    requiresDeliveryReceipt,
    requiresCurrentAuthorityReceipt,
    registrations: [subscription],
    refreshes: [],
  };
}

function registerQueryCoverage(db: Db, coverage: any, subscription: any): void {
  const registrations = db.node.queryCoverageRegistrations;
  const existing = registrations.get(subscription.subscription);
  if (existing) existing.refCount += 1;
  else registrations.set(subscription.subscription, { coverage, subscription, refCount: 1 });
}

function attachQueryShapeBindingWithOpts(
  db: Db,
  shape: any,
  binding: any,
  opts: any,
  identity: any,
  policyBinding: any,
): any {
  const subscription = db.node.nextSubscriptionKey(shape, opts.readViewKey());
  db.node.upstreamSubscriptions.push({
    kind: "Subscribe",
    subscription: { subscription, shape, binding, opts, identity, policyBinding },
  });
  db.node.latestCoverageSubscriptions.set(
    requestCoverageKey(shape, binding, opts, policyBinding),
    subscription,
  );
  db.node.scheduleTick("Immediate");
  return subscription;
}

export function attachQuery(db: Db, prepared: Prepared): QueryAttachment {
  return attachQueryWithOpts(db, prepared, {});
}

/** LocalOnly attachments are immediately ready against this node's data. */
export function queryAttachmentIsCovered(db: Db, attachment: QueryAttachment): boolean {
  // Local propagation does not gate a durable node's local knowledge.
  if (!attachment.requiresDeliveryReceipt) return true;
  const node = db.node.node.tryLock();
  if (!node) return false;
  const receipts = db.node.activeAuthorityViewReceipts;
  const hasCurrentAuthorityReceipt =
    receipts !== undefined &&
    attachment.requiredAfter.every(([bv]) => receipts.bindingViews.has(bv)) &&
    attachment.subscriptions.every((s) => receipts.subscriptions.has(s));
  const covered =
    attachment.requiredAfter.every(([bindingView, requiredAfter]) => {
      const keys: any[] = [];
      for (const s of attachment.subscriptions) {
        try {
          const key = node.authorityResultKeyForSubscription(s);
          if (bindingViewKeyEquals(key.bindingView, bindingView)) keys.push(key);
        } catch {}
      }
      const receipt = keys[0];
      if (!receipt) return false;
      // Rebuild/eviction preserves sequencing but discards the delivered answer.
      return (
        keys.every((k) => k.equals(receipt)) &&
        node.appliedAuthorityResultGeneration(receipt) > requiredAfter &&
        node.hasSettledAuthorityResult(receipt) &&
        !node.openingPendingForAuthorityResult(receipt)
      );
    }) &&
    (!attachment.requiresCurrentAuthorityReceipt || hasCurrentAuthorityReceipt);
  db.node.node.unlock?.();
  if (covered) {
    const refreshes = db.node.coverageRefreshGenerations;
    for (const [coverage, generation] of attachment.refreshes)
      if (refreshes.get(coverage) === generation) refreshes.delete(coverage);
  }
  return covered;
}

/** Detach a one-shot query coverage request. */
export function detachQuery(db: Db, attachment: QueryAttachment): void {
  detachQueryUsing(db, attachment, (subscription) => {
    const owner = db.node.node.tryLock();
    if (owner) owner.applyUnsubscribe(subscription);
    else
      db.node.enqueueTransactionCleanup(async () => {
        (await db.node.node.lock()).applyUnsubscribe(subscription);
      });
  });
}

/** Release binding-owned coverage after asynchronously acquiring its owner. */
export async function detachQueryAsync(db: Db, attachment: QueryAttachment): Promise<void> {
  const owner = await db.node.node.lock();
  detachQueryUsing(db, attachment, (s) => owner.applyUnsubscribe(s));
}

function detachQueryUsing(
  db: Db,
  attachment: QueryAttachment,
  unsubscribe: (subscription: any) => void,
): void {
  const rt = db.node;
  const removed: [any, any][] = [];
  const registrations = rt.queryCoverageRegistrations;
  for (const subscription of attachment.registrations) {
    const registration = registrations.get(subscription);
    if (!registration) continue;
    const coverage = registration.coverage;
    registration.refCount = Math.max(0, registration.refCount - 1);
    if (registration.refCount === 0) registrations.delete(subscription);
    const count = rt.upstreamCoverageRefcounts.get(coverage);
    if (count === undefined) continue;
    const next = Math.max(0, count - 1);
    rt.upstreamCoverageRefcounts.set(coverage, next);
    if (next === 0) {
      rt.upstreamCoverageRefcounts.delete(coverage);
      rt.awaitingInitialAuthorityCoverage.delete(coverage);
      removed.push([subscription, coverage]);
    }
  }
  for (const [subscription, coverage] of removed) {
    rt.activeAuthorityViewReceipts?.subscriptions.delete(subscription);
    rt.coverageRefreshGenerations.delete(coverage);
    unsubscribe(subscription);
    const latest = rt.latestCoverageSubscriptions.get(coverage);
    if (latest !== undefined && latest.equals?.(subscription))
      rt.latestCoverageSubscriptions.delete(coverage);
    rt.upstreamSubscriptions.push({ kind: "Unsubscribe", subscription });
  }
  rt.scheduleTick("Immediate");
}

async function openSubscription(
  db: Db,
  prepared: Prepared,
  opts: ReadOpts,
  author: any,
  authorizationMode: string,
  allowPendingOverlay: boolean,
) {
  const localFirstOpts = { ...opts, firstLoad: { kind: "Deliver" } };
  const stream = await openGatedSubscription(
    db,
    prepared,
    opts,
    author,
    authorizationMode,
    allowPendingOverlay,
  );
  // A remote window that stops being able to answer before it opens is served
  // by the plain local-first read of the same window, opened now beside it.
  if (stream.isRemoteWindow())
    stream.windowFallback = await openGatedSubscription(
      db,
      prepared,
      localFirstOpts,
      author,
      authorizationMode,
      allowPendingOverlay,
    );
  return stream;
}

/** Internal one-shot remote-answer path. */
export function subscribeRemoteAnswer(db: Db, prepared: Prepared, opts: ReadOpts) {
  opts = { ...opts, firstLoad: { kind: "Deliver" } };
  const coverageTier = opts.tier === "LocalOnly" ? DurabilityTier.Local : DurabilityTier.Global;
  return subscribeOneShotWithCoverage(db, prepared, opts, undefined, coverageTier);
}

export function subscribeOneShotWithCoverage(
  db: Db,
  prepared: Prepared,
  opts: ReadOpts,
  author: any,
  coverageTier: number,
) {
  const mode =
    author !== undefined && coverageTier < DurabilityTier.Global ? "TrustedServing" : "ClientLocal";
  return openGatedSubscription(
    db,
    prepared,
    opts,
    author ?? db.identity.author,
    mode,
    author === undefined,
    coverageTier,
  );
}

async function openGatedSubscription(
  db: Db,
  prepared: Prepared,
  opts: ReadOpts,
  author: any,
  authorizationMode: string,
  allowPendingOverlay: boolean,
  coverageTierIn?: number,
): Promise<any> {
  const rt = db.node;
  let coverageTier = coverageTierIn ?? readCoverageTier(opts);
  await db.awaitOpenSchemaForCoverage(coverageTier);
  ensureSupportedSubscriptionReadOpts(opts);
  await validatePreparedShapeForRegistration(db, prepared);
  const [resolvedTier, openingGate] = resolveFirstLoad(
    db,
    prepared,
    opts,
    authorizationMode,
    coverageTier,
  );
  coverageTier = resolvedTier;
  const requestedReadTier = coverageTier;
  // A non-durable foreground registers Local coverage, so its `settled` bit
  // cannot tell a gated opening that the authority answered: it also holds
  // `Global` witness coverage while the gate is armed.
  const authorityWitnessed =
    opts.tier !== "LocalOnly" &&
    rt.upstreamDurabilityFloor === DurabilityTier.Local &&
    openingGate?.route === "LocalFirst";
  if (authorityWitnessed && openingGate) openingGate.witnessed = true;
  const readTier = requestedReadTier;
  const pendingOverlay =
    allowPendingOverlay &&
    authorizationMode === "ClientLocal" &&
    opts.tier === "LocalFirst" &&
    coverageTier >= DurabilityTier.Global;
  let owner = await rt.node.lock();
  let node = prepared.scopedNode(owner, author);
  const [localShape, localBinding] = node.queryBindingForLink(prepared.shape, prepared.binding);
  // Keep the host scheduler as the cold-storage continuation owner.
  const progressWaker = rt.queryRuntimeWaker();
  let [subscription, snapshot] =
    await node.openMaintainedViewSubscriptionInAuthorizationModeWithWaker(
      localShape,
      localBinding,
      author,
      readTier,
      opts.readView,
      undefined,
      authorizationMode,
      pendingOverlay,
      progressWaker,
    );
  const localRuntimeToken = node.grooveRuntimeToken();
  const localSubscriptionCleanup = {
    value: [localRuntimeToken, subscription.subscriptionId()] as any,
  };
  let openingUpstream: any[] = [];
  let cleanupArmed = true;
  const localCleanup = () => {
    if (!cleanupArmed) return;
    // Opening failed before a public state existed; ID-based cleanup.
    rt.enqueueSubscriptionFinalization({
      state: undefined,
      openingUpstream: openingUpstream.splice(0),
      openingLocal: localSubscriptionCleanup.value,
      acknowledgement: undefined,
    });
    localSubscriptionCleanup.value = undefined;
  };
  try {
    rt.ensureSubscriptionFinalizationOpen();
    const terminalRows = subscription.hasRootCollector();
    let stateShape = localShape;
    let stateBinding = localBinding;
    let remoteReadTier: number | undefined;
    let requiresAuthorityReceipt = false;
    let upstreamSubscriptionHandles: any[] = [];
    let authorityWitness: any[] = [];
    let suppressProvisionalOpening = false;
    // LocalOnly never sends a query to another node.
    const propagatesUpstream = opts.tier !== "LocalOnly";
    if (propagatesUpstream) {
      const upstreamOpts = rt.upstreamRegisterShapeOptions(requestedReadTier, opts.readView);
      let shape = stateShape;
      let binding = stateBinding;
      if (upstreamOpts.tier !== readTier) {
        owner = await rt.node.lock();
        node = prepared.scopedNode(owner, author);
        [shape, binding] = node.queryBindingForLink(prepared.shape, prepared.binding);
      }
      stateShape = shape;
      stateBinding = binding;
      remoteReadTier = upstreamOpts.tier;
      // Global cache possession is never a settlement receipt.
      requiresAuthorityReceipt = upstreamOpts.tier >= DurabilityTier.Global;
      const opened = await openSubscriptionUpstreamCoverage(
        db,
        prepared,
        shape,
        binding,
        upstreamOpts,
        author,
        authorizationMode,
      );
      upstreamSubscriptionHandles = opened.handles;
      openingUpstream = upstreamSubscriptionHandles.slice();
      if (authorityWitnessed) {
        const witness = await openSubscriptionUpstreamCoverage(
          db,
          prepared,
          shape,
          binding,
          rt.upstreamRegisterShapeOptions(DurabilityTier.Global, opts.readView),
          author,
          authorizationMode,
        );
        openingUpstream.push(...witness.handles);
        authorityWitness = witness.handles;
      }
      suppressProvisionalOpening =
        authorizationMode === "ClientLocal" &&
        requestedReadTier >= DurabilityTier.Global &&
        opened.awaitsInitialAuthorityResponse &&
        snapshot.rootCount === 0 &&
        snapshot.edges.length === 0;
    }
    const settledTier = remoteReadTier ?? readTier;
    let settledAuthorityResult: any;
    if (remoteReadTier !== undefined) {
      const bindingViewKey = new BindingViewKey(
        stateShape.shapeId(),
        stateBinding.bindingId(),
        registerShapeOptions({ tier: settledTier, readView: opts.readView }).readViewKey(),
      );
      const n = await rt.node.lock();
      const keys: any[] = [];
      for (const h of upstreamSubscriptionHandles) {
        try {
          const key = n.authorityResultKeyForSubscription(h.subscription);
          if (bindingViewKeyEquals(key.bindingView, bindingViewKey)) keys.push(key);
        } catch {}
      }
      if (keys.length && keys.every((k) => k.equals(keys[0]))) settledAuthorityResult = keys[0];
    }
    // Installation of a received authority closure is folded into the public
    // stream owner's snapshot with the same terminal reducer.
    let snapshotIndex = RelationSnapshotIndex.withRootOccurrences(subscription.rootOccurrenceIds());
    if (authorizationMode === "ClientLocal" && settledAuthorityResult) {
      const [update] = await (
        await rt.node.lock()
      ).drainLocalMaintainedViewSubscriptionPreservingRowsWithWaker(
        subscription,
        settledAuthorityResult,
        new Set(),
        progressWaker,
      );
      if (update)
        applyMaintainedUpdateToSnapshot(
          snapshot,
          snapshotIndex,
          update,
          prepared.shape.query().table,
          readTier,
          false,
          subscription.terminalRootLayout(),
        );
    }
    const n = await rt.node.lock();
    const coveredClosureInstalled =
      !settledAuthorityResult ||
      subscription.hasInstalledCoveredClosure(
        settledAuthorityResult,
        n.appliedAuthorityResultGeneration(settledAuthorityResult),
      );
    let settled =
      subscriptionIsSettled(
        n,
        rt.activeAuthorityViewReceipts,
        stateShape,
        stateBinding,
        settledTier,
        opts.readView,
        requiresAuthorityReceipt,
        settledAuthorityResult,
      ) &&
      (!subscription.hasCoveredInputSources() ||
        // An installed closure still waiting (e.g. on chunks) has no rows yet (#3349).
        (coveredClosureInstalled && !n.coveredReceiverEvaluationPending(subscription)));
    // An empty local opening carries no observable result at a Global request.
    suppressProvisionalOpening ||=
      authorizationMode === "ClientLocal" &&
      requestedReadTier >= DurabilityTier.Global &&
      remoteReadTier !== undefined &&
      !settled &&
      snapshot.rootCount === 0 &&
      snapshot.edges.length === 0;
    const channel = new Channel<any>();
    const sender = rt.newSubscriptionSender(channel, {
      openingGate,
      remoteWindow: openingGate?.route === "RemoteWindow",
      requestedTier: readTier,
    });
    const rootOccurrenceIds = snapshotIndex.roots.flat();
    materializeSubscriptionTerminalRecords(snapshot, snapshotIndex);
    const initialOutputs = subscriptionOutputsWithOccurrenceSidecar(snapshot, rootOccurrenceIds);
    const stateSnapshot = relationSnapshotWithDeltaSlack(snapshot);
    snapshotIndex = RelationSnapshotIndex.withRootOccurrences(rootOccurrenceIds);
    snapshotIndex.terminalRecords = subscription.decodedTerminalRecords();
    const pendingInitialOwnerResult =
      authorizationMode === "ClientLocal" &&
      readTier === DurabilityTier.Local &&
      propagatesUpstream &&
      rt.upstreamDurabilityFloor === DurabilityTier.Local;
    // Even a warm, empty foreground graph is provisional until its owner answered.
    const pendingInitialLocalSnapshot =
      pendingInitialOwnerResult || !subscription.initialSnapshotReceived();
    settled = settled && !pendingInitialOwnerResult;
    const closed = { value: false };
    const scalarReconciliationEnabled =
      readTier < DurabilityTier.Global &&
      remoteReadTier !== undefined &&
      propagatesUpstream &&
      opts.readView.isDefault() &&
      simpleScalarExitQuery(stateShape.query());
    // Only a fresh strict opening can miss a deletion.
    const deletionReconciliation =
      readTier >= DurabilityTier.Global &&
      !settled &&
      authorizationMode === "ClientLocal" &&
      remoteReadTier !== undefined &&
      propagatesUpstream &&
      opts.readView.isDefault() &&
      singleTableScalarQuery(stateShape.query())
        ? "Unchecked"
        : "Disabled";
    const state = {
      closed,
      terminalRows,
      deletionReconciliation,
      scalarReconciliationEnabled,
      scalarAuthorityRevision: 0,
      scalarReconciliation: {},
      kind: {
        kind: "Prepared",
        shape: stateShape,
        binding: stateBinding,
        maintainedSubscription: subscription,
      },
      grooveRuntimeToken: localRuntimeToken,
      localSubscriptionCleanup,
      upstreamSubscriptionHandles,
      propagatesUpstream,
      requestIdentityClaims: prepared.requestIdentityClaims,
      author,
      authorizationMode,
      readTier,
      pendingOverlay,
      remoteReadTier,
      requiresAuthorityReceipt,
      readView: opts.readView,
      snapshot: stateSnapshot,
      snapshotIndex,
      snapshotSource: "LocalMaintained",
      settled,
      pendingInitialLocalSnapshot,
      pendingInitialOwnerResult,
      authorityWitness,
      sender,
    };
    const event = {
      kind: "Delta",
      reset: true,
      publishable: !suppressProvisionalOpening && !pendingInitialLocalSnapshot,
      added: initialOutputs,
      updated: [],
      removed: [],
      terminalOperations: [],
      settled,
      tier: readTier,
    };
    const materialized = sender.materialized(n, prepared.shape, event);
    sender.publish(event, undefined, state.snapshot, state.snapshotIndex, materialized);
    rt.subscriptions.push(new WeakRef(state));
    if (openingGate) registerOpeningGate(db, state);
    cleanupArmed = false;
    registerUpstreamSubscriptionOwner(
      rt.upstreamSubscriptionOwners,
      state.upstreamSubscriptionHandles,
      state,
    );
    const cleanup = (acknowledgement: any) => {
      closed.value = true;
      rt.enqueueSubscriptionFinalization({
        state,
        openingUpstream: [],
        openingLocal: undefined,
        acknowledgement,
      });
      return acknowledgement ? rt.drainSubscriptionFinalizations() : undefined;
    };
    return new SubscriptionStream(channel, state, cleanup);
  } finally {
    localCleanup();
  }
}

async function validatePreparedShapeForRegistration(db: Db, prepared: Prepared): Promise<void> {
  const ast = ShapeAst.fromValidated(prepared.shape);
  const node = await db.node.node.lock();
  validateShapeAstForRegistration(node, prepared.shape.shapeId(), ast);
}

async function openSubscriptionUpstreamCoverage(
  db: Db,
  prepared: Prepared,
  shape: any,
  binding: any,
  opts: any,
  identity: any,
  authorizationMode: string,
): Promise<{ handles: any[]; awaitsInitialAuthorityResponse: boolean }> {
  const rt = db.node;
  const owner = await rt.node.lock();
  await prepared
    .scopedNode(owner, identity)
    .ensurePeerMaintainedSubscriptionViewSupported(
      shape,
      binding,
      opts.tier,
      identity,
      opts.readView,
      authorizationMode,
    );
  const policyBinding = prepared.requestPolicyBinding(identity);
  const coverage = requestCoverageKey(shape, binding, opts, policyBinding);
  if (rt.upstreamCoverageRefcounts.has(coverage)) {
    const subscription = rt.latestCoverageSubscriptions.get(coverage);
    if (subscription !== undefined) {
      rt.upstreamCoverageRefcounts.set(coverage, rt.upstreamCoverageRefcounts.get(coverage) + 1);
      return {
        handles: [{ coverage, subscription }],
        awaitsInitialAuthorityResponse: rt.awaitingInitialAuthorityCoverage.has(coverage),
      };
    }
  }
  const subscription = attachQueryShapeBindingWithOpts(
    db,
    shape,
    binding,
    opts,
    identity,
    policyBinding,
  );
  rt.upstreamCoverageRefcounts.set(coverage, (rt.upstreamCoverageRefcounts.get(coverage) ?? 0) + 1);
  const hasLiveUpstream = rt.connections.some((c: any) => c.link.kind === "Upstream");
  if (hasLiveUpstream) rt.awaitingInitialAuthorityCoverage.add(coverage);
  return {
    handles: [{ coverage, subscription }],
    awaitsInitialAuthorityResponse: hasLiveUpstream,
  };
}

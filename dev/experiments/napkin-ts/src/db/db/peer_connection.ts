// Per-peer synchronization, repair, and resume machinery.
import { KeyedMap, SortedMap, SortedSet } from "../../support/collections.js";
import { uuidFromString, uuidV5 } from "../../support/hash.js";
import { encode, Writer } from "../../support/postcard.js";
import {
  AuthorizationScopeLeaseManager,
  BindingSource,
  CanonicalPolicyClaims,
  CommitUnitTrust,
  CoveragePolicyBindingOrigin,
  DbError,
  dbg,
  DurabilityTier,
  ErrorCode,
  PeerRole,
  PeerState,
  PolicyBindingKey,
  AuthorityResultKey,
  ReadBarrierReceiver,
  ReadBarrierSender,
  TickUrgency,
  ViewUpdatePayload,
  encodePostcardValue,
  finishOpenSchemaConnection,
  registerLocalFateRoute,
  pumpCurrentRows,
  restoreLocalSubscriberReplay,
  upstreamRegisterShapeOptions,
  type AuthorSubject,
  type AuthorityContext,
  type CoverageKey,
  type SubscriptionKey,
  type SyncMessage,
  type TxId,
  type Value,
} from "../../_extern/db-runtime.js";
import {
  cmpSubscriptionKey,
  coverageKeyString,
  makeQueryRuntimeWaker,
  refreshSubscriptionsIn,
  retireRelayUpstreamSubscription,
  subscriptionKeyString,
  takeRelayUpstreamSubscriptionOwner,
  type LocalMutex,
  type Node,
  type SharedNodeState,
  type Transport,
  type Waker,
} from "./node_runtime.js";
import { tickUpstream } from "./peer_connection_upstream.js";
import { tickSubscriber } from "./peer_connection_subscriber.js";
import {
  handleTransportBackpressure,
  sendSubscriberWithSyncContext,
  serializedSyncMessageLen,
  stampViewUpdateAuthorizationProgressFrom,
  takeLastOrClone,
  retargetViewUpdate,
  transportError,
  scheduleTickIn,
} from "./peer_connection_support.js";

/** Wire rejections and receiver validation failures terminate the same downstream usages. */
export function queueRelaySubscriptionRejection(
  rt: Node,
  subscription: SubscriptionKey,
  reason: any,
): number {
  const removed = takeRelayUpstreamSubscriptionOwner(
    rt.relayUpstreamSubscriptionOwners,
    subscription,
  );
  const retired = new KeyedMap<SubscriptionKey, true>(subscriptionKeyString);
  for (const [handle, owner] of removed) {
    retired.set(handle, true);
    let queue = rt.pendingRelaySubscriptionRejections.get(owner.downstreamConnectionEpoch);
    if (!queue)
      rt.pendingRelaySubscriptionRejections.set(owner.downstreamConnectionEpoch, (queue = []));
    queue.push({
      coverage: owner.coverage,
      policyBinding: owner.policyBinding,
      downstreamSubscriptions: owner.downstreamSubscriptions,
      reason,
    });
  }
  for (const [subscription] of retired)
    rt.upstreamSubscriptions.push({ kind: "Unsubscribe", subscription });
  return removed.length;
}

export function routeSubscriptionRefreshFailure(
  subscriptions: WeakRef<any>[],
  error: unknown,
): number {
  console.error(`jazz subscription refresh failed: ${error}`);
  let delivered = 0;
  for (const weak of subscriptions) {
    const state = weak.deref();
    if (!state || state.closed.value) continue;
    const event = { kind: "Rejected", reason: { kind: "ServerFailure", code: "Internal" } };
    if (state.sendRejection(event)) delivered++;
  }
  return delivered;
}

/** Deliver a receiver-side authority-closure rejection to the named usage site only. */
export function routeInvalidAuthoritySourceClosure(
  subscriptions: WeakRef<any>[],
  error: any,
): number {
  if (error?.kind !== "InvalidAuthoritySourceClosure") return 0;
  const reason = { kind: "InvalidAuthoritySourceClosure", transition: error.transition };
  const target = subscriptionKeyString(error.subscription);
  let delivered = 0;
  for (const weak of subscriptions) {
    const state = weak.deref();
    if (
      !state ||
      state.closed.value ||
      !state.upstreamSubscriptionHandles.some(
        (h: any) => subscriptionKeyString(h.subscription) === target,
      )
    )
      continue;
    if (state.sendRejection({ kind: "Rejected", reason })) delivered++;
  }
  return delivered;
}

/** Namespace for relay-owned usage-site subscription handles. */
const RELAY_UPSTREAM_SUBSCRIPTION_NAMESPACE = uuidFromString(
  "ae3eb9f7-65cc-528d-8f3e-a772fb6f68fe",
);
/** Namespace for the maintained receiver owned by one policy-partitioned coverage group. */
const COVERAGE_GROUP_SUBSCRIPTION_NAMESPACE = uuidFromString(
  "19fdc830-2dd8-5876-ae31-a8f526512ac5",
);

function encodePolicyBinding(
  w: Writer,
  [identity, claims]: [AuthorSubject, Map<string, Value>],
): void {
  encodePostcardValue(w, identity);
  w.varint(claims.size);
  for (const [key, value] of [...claims].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
    w.str(key);
    encodePostcardValue(w, value);
  }
}

export function relayUpstreamSubscriptionKey(
  connectionEpoch: number,
  downstream: SubscriptionKey,
  upstreamReadView: any,
  policyBinding: [AuthorSubject, Map<string, Value>],
): SubscriptionKey {
  // A direct downstream claim refresh must get a fresh opaque handle.
  const identity = encode(null, (w) => {
    w.varint(connectionEpoch);
    encodePostcardValue(w, downstream);
    encodePostcardValue(w, upstreamReadView);
    encodePolicyBinding(w, policyBinding);
  });
  return {
    shapeId: downstream.shapeId,
    bindingId: uuidV5(RELAY_UPSTREAM_SUBSCRIPTION_NAMESPACE, identity),
    readView: upstreamReadView,
  };
}

export function relayAuthorityCoverageKey(coverage: CoverageKey): CoverageKey {
  const opts = upstreamRegisterShapeOptions(
    coverage.opts.tier,
    coverage.opts.readView,
    DurabilityTier.Global,
  );
  opts.bindingSource = BindingSource.RelayAuthoritySession;
  return { ...coverage, opts };
}

export function coverageGroupSubscriptionKey(coverage: CoverageKey): SubscriptionKey {
  let bindingId = coverage.bindingId;
  if (coverage.policyBinding) {
    const identity = encode(null, (w) => {
      encodePostcardValue(w, coverage.bindingId);
      encodePostcardValue(w, coverage.policyBinding);
    });
    bindingId = uuidV5(COVERAGE_GROUP_SUBSCRIPTION_NAMESPACE, identity);
  }
  return { shapeId: coverage.shapeId, bindingId, readView: coverage.opts.readViewKey() };
}

export async function finishPeerPublicationOutcome<T>(
  node: SharedNodeState,
  rt: Node,
  progressWaker: Waker | undefined,
  outcome: any,
): Promise<[T, number]> {
  const [value, changed] = await finishPeerPublicationOutcomeWithRefresh<T>(
    node,
    rt,
    progressWaker,
    outcome,
    true,
  );
  return [value, changed];
}

export async function finishPeerPublicationOutcomeWithRefresh<T>(
  node: SharedNodeState,
  rt: Node,
  progressWaker: Waker | undefined,
  outcome: any,
  refresh: boolean,
): Promise<[T, number, boolean]> {
  const value: T = outcome.value;
  let publications: any[] = outcome.publications;
  const postSettlementWork: SyncMessage[] = [...outcome.postSettlementWork];
  let changed = 0;
  let publishedAny = false;
  for (;;) {
    if (publications.length) {
      publishedAny = true;
      const persisted: [TxId, any][] = [];
      for (const publication of publications)
        persisted.push([publication.txId(), await publication.persist()]);
      await node.with((state) => {
        for (const [txId, persistence] of persisted)
          state.settlePublishedTransaction(txId, persistence);
      });
      if (refresh) {
        try {
          changed += await refreshSubscriptionsIn(node, rt, progressWaker);
        } catch (error) {
          changed += routeSubscriptionRefreshFailure(rt.subscriptions, error);
        }
      }
    }
    const message = postSettlementWork.shift();
    if (!message) break;
    const next = await node.with((state) =>
      state.applySyncMessageWithIngestContext(message, {
        identity: SYSTEM,
        trust: CommitUnitTrust.TrustedBackend,
        admittedWriteAuthorization: false,
        versionReceiptsValidated: false,
      }),
    );
    publications = next.publications;
    postSettlementWork.push(...next.postSettlementWork);
  }
  return [value, changed, publishedAny];
}

const SYSTEM = "System" as AuthorSubject;

/** Dispatch one admitted subscriber message into node ingest. */
export async function dispatchAdmittedSubscriberMessage(
  node: SharedNodeState,
  peer: any,
  localReceiver: boolean,
  ingestContext: any,
  sessionClaimBinding: [AuthorSubject, Map<string, Value>],
  rt: Node,
  downstreamFates: any[],
  message: any,
): Promise<any> {
  const clientLink = peer.role().kind === PeerRole.ClientLink;
  if (message.kind === "CommitUnit" && clientLink)
    await node.with((n) => n.requireStagedLargeValuesForVersions(message.versions));
  if (message.kind === "CommitUnit" && localReceiver) {
    const txId = message.tx.txId;
    registerLocalFateRoute(rt.localFateRoutes, txId, downstreamFates);
    await node.with((state) =>
      state.ingestRelayCommitUnitWithEncoderTrust(
        message.tx,
        message.versions,
        CommitUnitTrust.isTrusted(ingestContext.trust) || ingestContext.versionReceiptsValidated,
      ),
    );
    return { value: [], publications: [], postSettlementWork: [] };
  }
  if (message.kind === "CommitUnit" && (clientLink || peer.role().kind === PeerRole.Relay)) {
    const { tx, versions } = message;
    // Terminal authorization belongs to the immutable session selected at admission.
    let permissionSubject: AuthorSubject;
    let mustBindProvenance: boolean;
    switch (ingestContext.trust) {
      case CommitUnitTrust.Session:
        permissionSubject = ingestContext.identity;
        mustBindProvenance = tx.madeBy?.kind === "SystemAt";
        break;
      case CommitUnitTrust.Relay:
        permissionSubject = sessionClaimBinding[0];
        mustBindProvenance = peer.admitsRelayBinding(sessionClaimBinding);
        break;
      case CommitUnitTrust.TrustedAdmin:
        permissionSubject = ingestContext.identity;
        mustBindProvenance = false;
        break;
      default:
        permissionSubject = tx.permissionSubject ?? tx.madeBy;
        mustBindProvenance = false;
    }
    if (mustBindProvenance && !admittedProvenanceMatches(permissionSubject, tx.madeBy)) {
      // A relay may replay a unit the authority already stored; it cannot introduce that origin.
      const knownReplay = await node.with((n) => n.knownUntrustedCommitUnitMatches(tx, versions));
      if (!knownReplay)
        throw new DbError(
          ErrorCode.Protocol,
          "commit provenance does not match the admitted session",
        );
    }
    // Only a relay's terminal ingest consumes this receipt.
    const admittedWriteAuthorization =
      ingestContext.trust === CommitUnitTrust.Relay
        ? await node.with((n) =>
            peer.proveTerminalCommitAuthorization(
              n,
              permissionSubject,
              sessionClaimBinding[1],
              versions,
              tx.txId,
            ),
          )
        : false;
    return node.with((n) =>
      n.applySyncMessageWithIngestContext(message, {
        ...ingestContext,
        admittedWriteAuthorization,
      }),
    );
  }
  return node.with((n) => n.applySyncMessageWithIngestContext(message, ingestContext));
}

/** An internal system capability may forward a node-specific durable origin. */
function admittedProvenanceMatches(admitted: AuthorSubject, madeBy: any): boolean {
  if (admitted === SYSTEM) return madeBy?.kind === "SystemAt";
  return dbg(madeBy) === dbg(admitted);
}

// ---------------------------------------------------------------------------
// Control responses retained until byte admission.

export type PendingSubscriberControlResponse =
  | { kind: "Direct"; message: SyncMessage }
  | { kind: "WithSyncContext"; message: SyncMessage }
  | { kind: "AuthorizationScopeSequence"; sequence: PendingAuthorizationScopeSequence };

/** Lazily emits one authorization-scope proof sequence. */
export class PendingAuthorizationScopeSequence {
  nextStep = 0;
  constructor(
    readonly requestId: string,
    readonly key: any,
    readonly hydration: any,
  ) {}
  nextMessage(): SyncMessage | undefined {
    const clauseCount = this.hydration.clauses.length;
    const proofSteps = clauseCount * 3;
    if (this.nextStep < proofSteps) {
      const clause = this.hydration.clauses[Math.floor(this.nextStep / 3)];
      switch (this.nextStep % 3) {
        case 0:
          return clause.register;
        case 1:
          return clause.subscribe;
        default: {
          const view = ViewUpdatePayload.fromViewUpdate(clause.view);
          if (!view) throw new Error("authority scope clauses are view updates");
          return {
            kind: "AuthorizationScopeView",
            requestId: this.requestId,
            key: this.key,
            clauseIndex: Math.floor(this.nextStep / 3),
            clauseCount,
            view,
          } as SyncMessage;
        }
      }
    }
    if (this.nextStep === proofSteps)
      return {
        kind: "AuthorizationScopeAggregateReceipt",
        requestId: this.requestId,
        receipt: this.hydration.receipt,
      } as SyncMessage;
    const clause = this.hydration.clauses[this.nextStep - proofSteps - 1];
    return clause && ({ kind: "Unsubscribe", subscription: clause.subscription } as SyncMessage);
  }
  advance(): void {
    this.nextStep++;
  }
}

export function queueDirectControl(
  pending: PendingSubscriberControlResponse[],
  message: SyncMessage,
): void {
  pending.push({ kind: "Direct", message });
}

export function queueSyncContextControl(
  pending: PendingSubscriberControlResponse[],
  message: SyncMessage,
): void {
  pending.push({ kind: "WithSyncContext", message });
}

export function queueAuthorizationScopeSequence(
  pending: PendingSubscriberControlResponse[],
  requestId: string,
  key: any,
  hydration: any,
): void {
  pending.push({
    kind: "AuthorizationScopeSequence",
    sequence: new PendingAuthorizationScopeSequence(requestId, key, hydration),
  });
}

// ---------------------------------------------------------------------------
// Link state.

export interface PendingLargeValueUpload {
  valueRef: any;
  requested: any[];
  /** Nodes sent in the current batch, retained until the receiver accepts them. */
  inFlight: any[];
  retryNotBeforeMs: number | undefined;
  started: boolean;
}
export type LargeValueUploadQueues = Map<TxId, PendingLargeValueUpload[]>;

export interface UpstreamConnectionState {
  localReceiver: boolean;
  pending: any[];
  announcedShapes: Set<string>;
  /** Exact admitted opens retained for a fresh snapshot after authority handoff. */
  sentSubscriptions: KeyedMap<SubscriptionKey, any>;
  awaitingSupportSnapshots: KeyedMap<SubscriptionKey, any>;
  sentSessionClaimRevisions: Map<AuthorSubject, number>;
  uploaded: Set<TxId>;
  largeValueUploads: LargeValueUploadQueues;
  awaitingLargeValueUploads: Map<TxId, any>;
  failedLargeValueUploads: Set<TxId>;
  missingBodyResends: KeyedMap<SubscriptionKey, true>;
  expectedScopeAuthority: AuthorityContext | undefined;
  scopeLeaseManager: any;
  readBarrier: any;
}

export interface SubscriberConnectionState {
  peer: any;
  ingestContext: any;
  sessionClaims: Map<string, Value>;
  sessionClaimRevision: number;
  localReceiver: boolean;
  served: KeyedMap<SubscriptionKey, CoverageKey>;
  coverageGroups: KeyedMap<CoverageKey, any>;
  shapeRegistrations: Map<string, any>;
  deferredSubscribeRejections: PendingSubscriberControlResponse[];
  pendingCatalogueSubscriptions: KeyedMap<SubscriptionKey, any>;
  authorityScopeHydrations: Map<string, any>;
  authorityScopeHydrationCount: number;
  serveDirty: boolean;
  readBarrier: any;
}

export type ConnectionLink =
  | { kind: "Upstream"; state: UpstreamConnectionState }
  | { kind: "Subscriber"; state: SubscriberConnectionState };

export const ConnectionLink = {
  upstream(init: {
    localReceiver: boolean;
    pending: any[];
    expectedScopeAuthority: AuthorityContext | undefined;
    largeValueUploads: LargeValueUploadQueues;
  }): ConnectionLink {
    return {
      kind: "Upstream",
      state: {
        ...init,
        announcedShapes: new Set(),
        sentSubscriptions: new KeyedMap(subscriptionKeyString),
        awaitingSupportSnapshots: new KeyedMap(subscriptionKeyString),
        sentSessionClaimRevisions: new Map(),
        uploaded: new Set(),
        awaitingLargeValueUploads: new Map(),
        failedLargeValueUploads: new Set(),
        missingBodyResends: new KeyedMap(subscriptionKeyString),
        scopeLeaseManager: new AuthorizationScopeLeaseManager(),
        readBarrier: new ReadBarrierSender(),
      },
    };
  },
  subscriber(init: {
    peer: any;
    ingestContext: any;
    sessionClaims: Map<string, Value>;
    sessionClaimRevision: number;
    localReceiver: boolean;
  }): ConnectionLink {
    return {
      kind: "Subscriber",
      state: {
        ...init,
        served: new KeyedMap(subscriptionKeyString),
        coverageGroups: new KeyedMap(coverageKeyString),
        shapeRegistrations: new Map(),
        deferredSubscribeRejections: [],
        pendingCatalogueSubscriptions: new KeyedMap(subscriptionKeyString),
        authorityScopeHydrations: new Map(),
        authorityScopeHydrationCount: 0,
        serveDirty: true,
        readBarrier: new ReadBarrierReceiver(),
      },
    };
  },
};

/** Transfer one detached upstream's resumable upload state. */
export function takeReconnectableLargeValueUploads(
  state: UpstreamConnectionState,
): LargeValueUploadQueues {
  const awaiting = state.awaitingLargeValueUploads;
  state.awaitingLargeValueUploads = new Map();
  const uploads = state.largeValueUploads;
  state.largeValueUploads = new Map();
  for (const [txId, valueRef] of awaiting) {
    const upload = uploads.get(txId)?.find((u) => dbg(u.valueRef) === dbg(valueRef));
    if (!upload) continue;
    if (!upload.inFlight.length) upload.started = false;
    else upload.requested.unshift(...upload.inFlight.splice(0));
  }
  return uploads;
}

/** Merge independently detached links without duplicating a logical upload. */
export function mergeReconnectableLargeValueUploads(
  destination: LargeValueUploadQueues,
  uploads: LargeValueUploadQueues,
): void {
  for (const [txId, list] of uploads) {
    let retained = destination.get(txId);
    if (!retained) destination.set(txId, (retained = []));
    for (const upload of list) {
      if (retained.some((existing) => dbg(existing.valueRef) === dbg(upload.valueRef))) continue;
      retained.push(upload);
    }
  }
}

export const RATE_LIMITED_UPLOAD_RETRY_DELAY_MS = 1_000;

function collectLargeValueRefs(value: any, refs: any[]): void {
  switch (value?.kind) {
    case "Large":
      if (!refs.some((r) => dbg(r) === dbg(value.ref))) refs.push(value.ref);
      break;
    case "Tuple":
    case "Array":
      for (const v of value.values) collectLargeValueRefs(v, refs);
      break;
    case "Nullable":
      if (value.value !== undefined) collectLargeValueRefs(value.value, refs);
      break;
    case "Record":
      try {
        for (const v of value.record.toValues()) collectLargeValueRefs(v, refs);
      } catch {
        // Malformed records carry no large values.
      }
      break;
    case "Enum":
      try {
        for (const v of value.value.record().toValues()) collectLargeValueRefs(v, refs);
      } catch {
        // Malformed records carry no large values.
      }
      break;
  }
}

export function commitUnitLargeValueRefs(unit: any): any[] {
  const refs: any[] = [];
  for (const version of unit.uploadedVersions())
    for (let position = 0; position < version.applicationCellCount(); position++) {
      const value = version.cellAt(position);
      if (value !== undefined) collectLargeValueRefs(value, refs);
    }
  return refs;
}

/**
 * Per-connection resume state for a served subscriber. Resume must never fall
 * back to identity-global claim state.
 */
export interface ResumeCursor {
  peer: any;
  ingestContext: any;
  sessionClaims: Map<string, Value>;
  sessionClaimRevision: number;
}

export function resumeCursorFullDiffFallbacks(cursor: ResumeCursor): any {
  return cursor.peer.maintainedSubscriptionViewMetrics().fullDiffFallbacks;
}

export function refreshScopeRelayAdmissionEpoch(cursor: ResumeCursor): boolean {
  return cursor.peer.refreshScopeRelayAdmissionEpoch();
}

// ---------------------------------------------------------------------------

/** A live link between this `Db` and one peer, owned by the `Db`. */
export class PeerConnection {
  readonly transport: Transport;
  stagedInbound: any[] = [];
  readonly downstreamFates: any[];
  observedSubscriberDirtyEpoch: number;
  /** Dirty epoch observed by replay recovery. */
  localReplayEpoch: number;
  observedSessionClaimRevision: number;
  inboundAuthorityReceiptQuarantine = false;
  /** Fresh non-resumable epoch binding authorization receipts to this link. */
  readonly connectionEpoch: number;
  startupError: DbError | undefined;
  /** Exact uploads whose applied fate made them globally settled or rejected. */
  releasedOutboxTxIds: TxId[] = [];
  /** One ordinary-wire chunk reply held at its semantic producer boundary. */
  pendingChunkResponse: any;
  /** Subscriber control/rejection replies retained until byte admission. */
  pendingControlResponses: PendingSubscriberControlResponse[] = [];
  link: ConnectionLink;
  lastResumeBytes: number | undefined;
  readonly auxiliaryPump: any;
  readonly upstreamUploadDestination: { remoteNode: any; linkIdentity: any } | undefined;

  constructor(
    readonly rt: Node,
    init: {
      transport: Transport;
      connectionEpoch: number;
      startupError: DbError | undefined;
      downstreamFates: any[];
      upstreamUploadDestination: { remoteNode: any; linkIdentity: any } | undefined;
      sessionClaimRevision: number;
      link: ConnectionLink;
      auxiliaryPump: any;
    },
  ) {
    this.transport = init.transport;
    this.connectionEpoch = init.connectionEpoch;
    this.startupError = init.startupError;
    this.downstreamFates = init.downstreamFates;
    this.upstreamUploadDestination = init.upstreamUploadDestination;
    this.observedSessionClaimRevision = init.sessionClaimRevision;
    this.link = init.link;
    this.auxiliaryPump = init.auxiliaryPump;
    this.observedSubscriberDirtyEpoch = rt.subscriberDirtyEpoch;
    this.localReplayEpoch = rt.subscriberDirtyEpoch;
  }

  get node(): SharedNodeState {
    return this.rt.node;
  }

  takeReleasedOutboxTxIds(): TxId[] {
    return this.releasedOutboxTxIds.splice(0);
  }

  forgetReleasedOutboxTxIds(released: Set<TxId>): void {
    if (this.link.kind !== "Upstream") return;
    for (const txId of released) this.link.state.uploaded.delete(txId);
  }

  /** Clone the binding-driven auxiliary I/O endpoint for this peer link. */
  ioPump(): any {
    this.auxiliaryPump.claimBindingOutput();
    return this.auxiliaryPump.clone();
  }

  /** Replace the claims authenticated by the host for this subscriber link. */
  updateAuthenticatedSessionClaims(claims: Map<string, Value>): void {
    if (this.link.kind !== "Subscriber") return;
    const state = this.link.state;
    // A scope relay receives a fresh immutable capability only through a new connection.
    if (state.peer.admittedScopeRelayBinding()) return;
    if (dbg(state.sessionClaims) === dbg(claims)) return;
    state.sessionClaims = claims;
    state.sessionClaimRevision++;
  }

  /** The claims admitted for this subscriber session. */
  subscriberSessionClaimBinding(): [AuthorSubject, Map<string, Value>] | undefined {
    if (this.link.kind !== "Subscriber") return undefined;
    const { peer, ingestContext, sessionClaims } = this.link.state;
    const binding = peer.admittedScopeRelayBinding();
    if (binding) return [binding.identity, binding.claims];
    return [ingestContext.identity, sessionClaims];
  }

  /** Keep the legacy admission map available to write-policy evaluation. */
  bindSubscriberSessionClaims(): void {
    if (this.link.kind === "Subscriber" && this.link.state.peer.admittedScopeRelayBinding()) return;
    const binding = this.subscriberSessionClaimBinding();
    if (binding) this.node.borrow().setSessionClaims(binding[0], binding[1]);
  }

  private subscriberSessionClaimRevision(): number {
    return this.link.kind === "Subscriber" ? this.link.state.sessionClaimRevision : 0;
  }

  /** Rebuild this subscriber's maintained views if its process-local claims changed. */
  async rebindSubscriberViewsAfterClaimChange(progressWaker: Waker | undefined): Promise<boolean> {
    const connectionEpoch = this.connectionEpoch;
    if (this.link.kind !== "Subscriber") return false;
    const binding = this.subscriberSessionClaimBinding()!;
    const currentRevision = this.subscriberSessionClaimRevision();
    if (this.observedSessionClaimRevision === currentRevision) return false;
    const state = this.link.state;
    const { peer, served, coverageGroups } = state;
    const owners = this.rt.relayUpstreamSubscriptionOwners;
    peer.advanceAuthorizationProgress();
    const refreshed: [AuthorSubject, Map<string, Value>] = [binding[0], binding[1]];
    // `CoverageKey` is also the maintained receiver's stable identity.
    const replacements: [CoverageKey, CoverageKey][] = [];
    for (const [coverage, group] of coverageGroups) {
      if (group.policyBindingOrigin !== CoveragePolicyBindingOrigin.DirectAdmitted) continue;
      const next = { ...coverage };
      if (next.policyBinding)
        next.policyBinding = {
          identity: refreshed[0],
          canonicalClaims: new CanonicalPolicyClaims(refreshed[1]),
        };
      if (coverageKeyString(next) !== coverageKeyString(coverage))
        replacements.push([coverage, next]);
    }
    const replacedByNew = new Map(
      replacements.map(([old, next]) => [coverageKeyString(next), old]),
    );
    const staleMaintained = replacements.map(([old]) => coverageGroupSubscriptionKey(old));
    for (const [old, next] of replacements) {
      if (coverageGroups.has(next))
        throw new DbError(
          ErrorCode.Protocol,
          "claim refresh would merge direct and existing relay coverage",
        );
      const group = coverageGroups.get(old);
      coverageGroups.delete(old);
      // The propagated usage's owner must follow the replacement coverage key.
      if (
        subscriptionKeyString(group.authorityResultSubscription) !==
        subscriptionKeyString(group.upstreamSubscription)
      ) {
        const owner = owners.get([
          group.upstreamSubscription,
          connectionEpoch,
          old.opts.readViewKey(),
        ]);
        if (owner) {
          owner.coverage = next;
          owner.policyBinding = refreshed;
        }
      }
      coverageGroups.set(next, group);
      for (const [subscription, coverage] of served)
        if (coverageKeyString(coverage) === coverageKeyString(old)) served.set(subscription, next);
    }
    // Retire the old maintained receiver key explicitly before scheduling a fresh group.
    for (const stale of staleMaintained) {
      peer.noteClaimRefreshFullDiff(stale);
      const node = this.node.borrow();
      node.applyUnsubscribe(stale);
      peer.forgetSubscriptionWithNode(node, stale);
    }
    // Retire a direct group's propagated usage and allocate a fresh opaque handle.
    const upstreamReplacements: any[] = [];
    const refreshedAuthoritySources = new Map<string, any>();
    const refreshedUpstreamHandles = new Map<string, SubscriptionKey>();
    const groups: any[] = [];
    for (const [coverage, group] of coverageGroups) {
      if (group.policyBindingOrigin === CoveragePolicyBindingOrigin.DirectAdmitted) {
        group.policyBinding = refreshed;
        const downstream = [...group.subscribers][0];
        if (
          subscriptionKeyString(group.authorityResultSubscription) ===
            subscriptionKeyString(group.upstreamSubscription) &&
          downstream
        ) {
          const oldUpstream = group.upstreamSubscription;
          const authorityCoverage = coverageKeyString(relayAuthorityCoverageKey(coverage));
          let fresh = refreshedUpstreamHandles.get(authorityCoverage);
          if (!fresh)
            for (const [[subscription], owner] of owners)
              if (
                coverageKeyString(relayAuthorityCoverageKey(owner.coverage)) ===
                  authorityCoverage &&
                dbg(owner.policyBinding) === dbg(refreshed)
              ) {
                fresh = subscription;
                break;
              }
          fresh ??= relayUpstreamSubscriptionKey(
            connectionEpoch,
            downstream,
            group.upstreamOpts.readViewKey(),
            refreshed,
          );
          refreshedUpstreamHandles.set(authorityCoverage, fresh);
          if (subscriptionKeyString(oldUpstream) !== subscriptionKeyString(fresh)) {
            group.upstreamSubscription = fresh;
            group.authorityResultSubscription = fresh;
            refreshedAuthoritySources.set(
              coverageKeyString(coverage),
              AuthorityResultKey.policyScoped(
                {
                  shapeId: group.shape.shapeId(),
                  bindingId: group.binding.bindingId(),
                  readView: group.upstreamOpts.readViewKey(),
                },
                PolicyBindingKey.fromCanonicalParts(refreshed[0], refreshed[1]),
              ),
            );
            group.awaitingUpstreamSettlement = true;
            // Once the replacement settles it must publish a fresh reset.
            group.initialized = false;
            group.pendingInitialSubscribers = new SortedSet(cmpSubscriptionKey, group.subscribers);
            group.pendingInitialUpdate = undefined;
            group.pendingIncrementalUpdates = [];
            upstreamReplacements.push({
              coverage,
              oldCoverage: replacedByNew.get(coverageKeyString(coverage)) ?? coverage,
              oldUpstream,
              fresh,
              shape: group.shape,
              binding: group.binding,
              opts: group.upstreamOpts,
              downstream: [...group.subscribers],
            });
          }
        }
      }
      groups.push({
        coverage,
        shape: group.shape,
        binding: group.binding,
        policyBinding: group.policyBinding,
        origin: group.policyBindingOrigin,
        subscribers: [...group.subscribers],
      });
    }
    const deferredRehydrates = new Set(
      upstreamReplacements.map((r) => coverageKeyString(r.coverage)),
    );
    for (const r of upstreamReplacements) {
      // Retire the group-owned receiver; it reopens only after the fresh U settles.
      const maintained = coverageGroupSubscriptionKey(r.coverage);
      const node = this.node.borrow();
      node.applyUnsubscribe(maintained);
      peer.forgetSubscriptionWithNode(node, maintained);
      const oldOwner = retireRelayUpstreamSubscription(
        owners,
        r.oldUpstream,
        connectionEpoch,
        r.oldCoverage,
      );
      const pending = this.rt.upstreamSubscriptions;
      if (oldOwner) {
        node.applyUnsubscribe(r.oldUpstream);
        const isOldOpen = (c: any) =>
          c.kind === "Subscribe" &&
          subscriptionKeyString(c.subscription.subscription) ===
            subscriptionKeyString(r.oldUpstream);
        const oldOpenWasPending = pending.some(isOldOpen);
        this.rt.upstreamSubscriptions = pending.filter((c) => !isOldOpen(c));
        if (!oldOpenWasPending)
          this.rt.upstreamSubscriptions.push({ kind: "Unsubscribe", subscription: r.oldUpstream });
      }
      const request = {
        subscription: r.fresh,
        shape: r.shape,
        binding: r.binding,
        opts: r.opts,
        identity: refreshed[0],
        policyBinding: refreshed,
      };
      owners.set([r.fresh, connectionEpoch, r.coverage.opts.readViewKey()], {
        request,
        scalarAuthorityRevision: 0,
        scalarReconciliation: { pending: [], retryDelayMs: 0 },
        downstreamConnectionEpoch: connectionEpoch,
        coverage: r.coverage,
        policyBinding: refreshed,
        downstreamSubscriptions: new SortedSet(cmpSubscriptionKey, r.downstream),
      });
      this.rt.upstreamSubscriptions.push({ kind: "Subscribe", subscription: { ...request } });
    }
    if (upstreamReplacements.length) {
      state.serveDirty = true;
      scheduleTickIn(this.rt.scheduler, TickUrgency.Immediate);
    }
    let rebindPending = false;
    for (const g of groups) {
      const maintained = coverageGroupSubscriptionKey(g.coverage);
      const retainedProgress = Math.max(
        0,
        ...g.subscribers.map((s: SubscriptionKey) => peer.authorizationProgressForSubscription(s)),
      );
      peer.setSubscriptionPolicyBinding(maintained, g.policyBinding);
      peer.retainAuthorizationProgressForSubscription(maintained, retainedProgress);
      if (g.origin === CoveragePolicyBindingOrigin.DirectAdmitted)
        for (const s of g.subscribers) peer.setSubscriptionPolicyBinding(s, refreshed);
      const key = coverageKeyString(g.coverage);
      if (deferredRehydrates.has(key)) {
        const source = refreshedAuthoritySources.get(key);
        if (!source) throw new Error("every deferred relay refresh selects a new exact U source");
        peer.setSubscriptionAuthorityResultSource(maintained, source);
        peer.setSubscriptionAwaitingSelectedAuthoritySource(maintained, true);
        for (const s of g.subscribers) peer.setSubscriptionAuthorityResultSource(s, source);
        // Tear down rather than rehydrate from the stale source.
        continue;
      }
      const group = coverageGroups.get(g.coverage);
      if (!group) throw new Error("claim-refresh coverage group remains registered");
      if (group.pendingClaimRefreshRevision !== currentRevision) {
        // A new admitted claim revision is a new opening transition for every live usage.
        group.initialized = false;
        group.pendingInitialSubscribers = new SortedSet(cmpSubscriptionKey, group.subscribers);
        group.pendingInitialUpdate = undefined;
        group.pendingIncrementalUpdates = [];
        group.pendingClaimRefreshRevision = currentRevision;
      }
      const refreshSubscribers: SubscriptionKey[] = [...group.pendingInitialSubscribers];
      const update = await this.node.with((n) =>
        peer.rehydrateQueryForSubscriptionWithOptsAndWaker(
          n.scopedActiveSessionClaims(binding[0], binding[1]),
          maintained,
          g.shape,
          g.binding,
          g.coverage.opts,
          progressWaker,
        ),
      );
      if (!update) {
        // Do not acknowledge the claim revision until every usage was replaced.
        rebindPending = true;
        continue;
      }
      const shared = { value: update as SyncMessage | undefined };
      refreshSubscribers.forEach((subscription, i) => {
        let next = retargetViewUpdate(
          takeLastOrClone(shared, i + 1 < refreshSubscribers.length),
          subscription,
        );
        next = stampViewUpdateAuthorizationProgressFrom(peer, maintained, next);
        sendSubscriberWithSyncContext(
          this.node,
          peer,
          this.transport,
          this.rt,
          this.downstreamFates,
          next,
        );
        group.pendingInitialSubscribers.delete(subscription);
      });
      group.initialized = group.pendingInitialSubscribers.size === 0;
    }
    if (rebindPending) {
      scheduleTickIn(this.rt.scheduler, TickUrgency.Immediate);
      return true;
    }
    this.observedSessionClaimRevision = currentRevision;
    return true;
  }

  remoteWireError(): any {
    return this.transport.remoteWireError();
  }

  /** Full-diff fallbacks of this connection's served maintained views. */
  fullDiffFallbacks(): any {
    return this.link.kind === "Subscriber"
      ? this.link.state.peer.maintainedSubscriptionViewMetrics().fullDiffFallbacks
      : { count: 0 };
  }

  /** Host-only capability; does not alter write or publication trust. */
  admitAuthorityQueryDelegate(): void {
    if (this.link.kind !== "Subscriber") return;
    const state = this.link.state;
    state.peer.authorityQueryDelegate =
      state.ingestContext.trust === CommitUnitTrust.TrustedAuthority &&
      state.ingestContext.identity === SYSTEM &&
      state.peer.role().kind === PeerRole.ClientLink;
  }

  /** Extract this subscriber connection's resume cursor for a reconnect. */
  takeResumeCursor(): ResumeCursor | undefined {
    if (this.link.kind !== "Subscriber") return undefined;
    const state = this.link.state;
    const role = state.peer.role();
    const replacement =
      role.kind === PeerRole.Relay ? PeerState.relay() : PeerState.clientLink(role.identity);
    const cursor = {
      peer: state.peer,
      ingestContext: state.ingestContext,
      sessionClaims: state.sessionClaims,
      sessionClaimRevision: state.sessionClaimRevision,
    };
    state.peer = replacement;
    state.sessionClaims = new Map();
    return cursor;
  }

  /** Rehydrate every view served on this subscriber link. */
  async rehydrateSubscriberViews(): Promise<void> {
    const progressWaker = makeQueryRuntimeWaker(
      this.rt.scheduler,
      this.rt.queryRuntimeWakePending,
      this.rt.queryRuntimeWakerCache,
    );
    const binding = this.subscriberSessionClaimBinding();
    if (this.link.kind !== "Subscriber") return;
    const state = this.link.state;
    const peer = state.peer;
    peer.advanceAuthorizationProgress();
    for (const [coverage, group] of [...state.coverageGroups]) {
      const groupSubscription = coverageGroupSubscriptionKey(coverage);
      const scopeRelay = this.node.borrow().clientRelayScope() !== undefined;
      peer.setSubscriptionPolicyBinding(groupSubscription, group.policyBinding);
      if (group.awaitingUpstreamSettlement) {
        let source: any;
        try {
          source = this.node
            .borrow()
            .authorityResultKeyForSubscription(group.authorityResultSubscription);
        } catch {
          source = undefined;
        }
        // A strict scope relay may not rehydrate from its overlay or guess a receipt.
        if (
          source === undefined ||
          (scopeRelay && !this.node.borrow().hasSettledAuthorityResult(source))
        ) {
          state.serveDirty = true;
          continue;
        }
        peer.setSubscriptionAuthorityResultSource(groupSubscription, source);
        peer.setSubscriptionAwaitingSelectedAuthoritySource(groupSubscription, scopeRelay);
      } else {
        peer.setSubscriptionAwaitingSelectedAuthoritySource(groupSubscription, false);
      }
      const update = await this.node.with((n) =>
        peer.rehydrateQueryForSubscriptionWithOptsAndWaker(
          n.scopedActiveSessionClaims(binding![0], binding![1]),
          groupSubscription,
          group.shape,
          group.binding,
          coverage.opts,
          progressWaker,
        ),
      );
      if (!update) {
        state.serveDirty = true;
        continue;
      }
      const subscribers = [...group.subscribers] as SubscriptionKey[];
      const shared = { value: update as SyncMessage | undefined };
      subscribers.forEach((subscription, i) => {
        let next = retargetViewUpdate(
          takeLastOrClone(shared, i + 1 < subscribers.length),
          subscription,
        );
        next = stampViewUpdateAuthorizationProgressFrom(peer, groupSubscription, next);
        this.lastResumeBytes = serializedSyncMessageLen(next);
        sendSubscriberWithSyncContext(
          this.node,
          peer,
          this.transport,
          this.rt,
          this.downstreamFates,
          next,
        );
      });
    }
    state.serveDirty = true;
  }

  /** Preserve staged frames across an authority handoff, ineligible as receipts. */
  stageInboundWithoutAuthorityReceipt(): void {
    for (const staged of this.stagedInbound) staged.authorityReceiptEligible = false;
    if (this.link.kind === "Upstream") {
      const { sentSubscriptions, awaitingSupportSnapshots, pending } = this.link.state;
      // Reissue admitted opens and ignore deltas until each snapshot arrives.
      for (const [key, request] of sentSubscriptions) {
        const k = subscriptionKeyString(key);
        if (
          pending.some(
            (c) => c.kind === "Unsubscribe" && subscriptionKeyString(c.subscription) === k,
          )
        )
          continue;
        if (!awaitingSupportSnapshots.has(key)) awaitingSupportSnapshots.set(key, { 0: 0 });
        if (
          !pending.some(
            (c) =>
              c.kind === "Subscribe" && subscriptionKeyString(c.subscription.subscription) === k,
          )
        )
          pending.push({ kind: "Subscribe", subscription: request });
      }
    }
    for (;;) {
      let message: any;
      try {
        message = this.transport.tryRecvOwnedResult();
      } catch (error) {
        if (handleTransportBackpressure(this.node, this.rt.scheduler, error)) {
          this.inboundAuthorityReceiptQuarantine = true;
          break;
        }
        this.startupError = transportError(error);
        break;
      }
      if (!message) {
        this.inboundAuthorityReceiptQuarantine = false;
        break;
      }
      this.stagedInbound.push({
        message: message.message,
        lease: message.lease,
        authorityReceiptEligible: false,
        receiptsValidated: message.receiptsValidated,
      });
    }
  }

  /** Service this connection once: drain inbound, apply, wake subscriptions, flush outbound. */
  async tick(): Promise<{ subscriptionEvents: number; remoteSyncApplied: number }> {
    try {
      const stats = await this.tickInner();
      const flush = this.transport.pollFlush();
      if (flush === "MoreReady") scheduleTickIn(this.rt.scheduler, TickUrgency.AfterCurrentTurn);
      const delay = this.transport.incompleteReceiveTimeoutMs();
      if (delay !== undefined) this.rt.scheduler.current?.scheduleTickAfter(delay);
      return stats;
    } catch (error) {
      const failure = error instanceof DbError ? error : transportError(error);
      if (this.link.kind === "Upstream")
        finishOpenSchemaConnection(this.rt.openSchemaAdmission, this.connectionEpoch, failure);
      throw failure;
    }
  }

  private async retryLocalReplayAfterProgress(): Promise<void> {
    const epoch = this.rt.subscriberDirtyEpoch;
    if (this.localReplayEpoch === epoch) return;
    this.localReplayEpoch = epoch;
    if (this.link.kind !== "Subscriber" || !this.link.state.localReceiver) return;
    const author = this.link.state.ingestContext.identity;
    const blocked = [...this.rt.localFateRoutes.valuesArray()].some((routes: any[]) =>
      routes.some((route) => !route.replayReady && route.replayAuthor === author),
    );
    if (blocked)
      await restoreLocalSubscriberReplay(
        this.node,
        this.rt.outbox,
        this.rt.localFateRoutes,
        author,
        this.downstreamFates,
      );
  }

  private async tickInner(): Promise<{ subscriptionEvents: number; remoteSyncApplied: number }> {
    if (this.startupError) {
      const error = this.startupError;
      this.startupError = undefined;
      throw error;
    }
    const stats = { subscriptionEvents: 0, remoteSyncApplied: 0 };
    // Finish draining the old authority's backlog before sending reissued opens.
    if (this.inboundAuthorityReceiptQuarantine) {
      this.stageInboundWithoutAuthorityReceipt();
      if (this.startupError) {
        const error = this.startupError;
        this.startupError = undefined;
        throw error;
      }
      if (this.inboundAuthorityReceiptQuarantine) return stats;
    }
    const progressWaker = makeQueryRuntimeWaker(
      this.rt.scheduler,
      this.rt.queryRuntimeWakePending,
      this.rt.queryRuntimeWakerCache,
    );
    // Only a host-admitted scope-isolated worker may forward one exact binding upstream.
    const permitsDelegatedSessions =
      this.transport.permitsDelegatedSessions() ||
      this.node.borrow().clientRelayScope() !== undefined;
    this.observeSharedSubscriberDirtyEpoch();
    await this.retryLocalReplayAfterProgress();
    const sessionClaimBinding = this.subscriberSessionClaimBinding();
    this.bindSubscriberSessionClaims();
    await this.rebindSubscriberViewsAfterClaimChange(progressWaker);
    this.pumpCurrentRows();
    const stop =
      this.link.kind === "Upstream"
        ? await tickUpstream(this, this.link.state, stats, progressWaker, permitsDelegatedSessions)
        : await tickSubscriber(this, this.link.state, stats, progressWaker, sessionClaimBinding!);
    if (stop) return stats;
    return stats;
  }

  /** Pull this tick's shared dirty epoch into a subscriber link. */
  observeSharedSubscriberDirtyEpoch(): void {
    const epoch = this.rt.subscriberDirtyEpoch;
    if (this.observedSubscriberDirtyEpoch === epoch) return;
    this.observedSubscriberDirtyEpoch = epoch;
    if (this.link.kind === "Subscriber") this.link.state.serveDirty = true;
  }

  /** Mark a subscriber link as needing another serve pass. */
  markSubscriberDirty(): boolean {
    if (this.link.kind !== "Subscriber") return false;
    this.link.state.serveDirty = true;
    this.observedSubscriberDirtyEpoch = this.rt.subscriberDirtyEpoch;
    return true;
  }

  /** Move routed current-rows traffic for this link (row_availability.rs). */
  pumpCurrentRows(): void {
    pumpCurrentRows(this);
  }
}

export type PeerConnectionHandle = LocalMutex<PeerConnection>;
export type { SortedMap };

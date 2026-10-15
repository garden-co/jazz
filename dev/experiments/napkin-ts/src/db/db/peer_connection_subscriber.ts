// Subscriber half of `PeerConnection::tick` (split out of peer_connection.rs).
import { SortedSet } from "../../support/collections.js";
import { uuidToString, uuidV4 } from "../../support/hash.js";
import {
  BindingSource,
  CatalogueBootstrapState,
  CommitUnitTrust,
  CoveragePolicyBindingOrigin,
  DbError,
  dbg,
  DurabilityTier,
  ErrorCode,
  FEATURE_AUTHORIZATION_SCOPE_VIEWS,
  MAX_SHAPE_REGISTRATION_BYTES,
  MAX_SHAPE_REGISTRATIONS_PER_PEER,
  PolicyBindingKey,
  AuthorityResultKey,
  SupportingRowsUpdate,
  TickUrgency,
  admittedRequestPolicyBinding,
  coverageKey,
  delegatedSessionCapability,
  ensureSupportedRegisterShapeOptions,
  isCatalogueMutation,
  isServerShapeValidationFailure,
  queueLocalAcknowledgements,
  queuePendingUploadIn,
  registerShapeRejectionSubscription,
  rowAvailabilityUnknownReceipt,
  rowAvailabilityValidRequest,
  serializedSize,
  serverSubscriptionFailureRejectionMessage,
  subscriberInboundMessageIsAuthorityOnly,
  subscriberPermissionSubject,
  subscriberPermissionsReady,
  unsupportedShapeCapabilityRejectionMessage,
  upstreamRegisterShapeOptions,
  validateShapeAstForRegistration,
  validateShapeRegistrationSize,
  type AuthorSubject,
  type CoverageKey,
  type SubscriptionKey,
  type SyncMessage,
  type Value,
} from "../../_extern/db-runtime.js";
import {
  cmpSubscriptionKey,
  refreshSubscriptionsIn,
  retireRelayUpstreamSubscription,
  subscriptionKeyString,
  type Waker,
} from "./node_runtime.js";
import {
  coverageGroupSubscriptionKey,
  dispatchAdmittedSubscriberMessage,
  finishPeerPublicationOutcomeWithRefresh,
  queueDirectControl,
  queueSyncContextControl,
  relayAuthorityCoverageKey,
  relayUpstreamSubscriptionKey,
  routeSubscriptionRefreshFailure,
  type PeerConnection,
  type SubscriberConnectionState,
} from "./peer_connection.js";
import {
  dropPeerRequest,
  fateUpdateSettlesUpload,
  flushDownstreamFates,
  flushPendingChunkResponse,
  flushPendingControlResponses,
  handleTransportBackpressure,
  handleWriteStateUpdate,
  retargetViewUpdate,
  rollbackRejectedSubscriberAdmission,
  sendCatalogueSnapshotIfNeeded,
  sendPreparedSubscriberWithSyncContext,
  sendSubscriberWithSyncContext,
  sendWithSyncContext,
  serializedSyncMessageLen,
  serveAuthorizationScopeIntent,
  stampSubscriberOpeningState,
  stampViewUpdateAuthorizationProgressFrom,
  takeLastOrClone,
  transportError,
  trySendPreparedSubscriberWithSyncContext,
  writeStateUpdateTxId,
  scheduleTickIn,
} from "./peer_connection_support.js";

type Stats = { subscriptionEvents: number; remoteSyncApplied: number };
type ClaimBinding = [AuthorSubject, Map<string, Value>];

interface TickFlags {
  appliedInbound: boolean;
  scheduledFollowUp: boolean;
  sentViewUpdate: boolean;
  needsSubscriptionRefresh: boolean;
}

const registrationKey = (shapeId: any, readView: any): string => `${dbg(shapeId)}|${dbg(readView)}`;

function flushControls(pc: PeerConnection, state: SubscriberConnectionState): boolean {
  return flushPendingControlResponses(
    pc.node,
    state.peer,
    pc.transport,
    pc.pendingControlResponses,
    pc.rt.scheduler,
  );
}

/** Queue a direct control reply and try to flush it; the caller continues either way. */
function rejectAndFlush(
  pc: PeerConnection,
  state: SubscriberConnectionState,
  message: SyncMessage,
): true {
  queueDirectControl(pc.pendingControlResponses, message);
  scheduleTickIn(pc.rt.scheduler, TickUrgency.Immediate);
  flushControls(pc, state);
  return true;
}

function scoped(node: any, binding: ClaimBinding): any {
  return node.scopedActiveSessionClaims(binding[0], binding[1]);
}

export async function tickSubscriber(
  pc: PeerConnection,
  state: SubscriberConnectionState,
  stats: Stats,
  progressWaker: Waker | undefined,
  sessionClaimBinding: ClaimBinding | undefined,
): Promise<boolean> {
  const rt = pc.rt;
  const { peer, ingestContext, readBarrier } = state;
  // Admitted app links get the catalogue ahead of any ViewUpdate (a reopening replica needs it to compile).
  const catalogueReady =
    pc.node.borrow().catalogueBootstrapState() === CatalogueBootstrapState.Ready;
  const ownerAdmitted =
    !rt.openSchemaAdmission.current ||
    rt.openSchemaAdmission.current.authoritativeCatalogueReceived;
  const catalogueEntitled =
    CommitUnitTrust.isTrusted(ingestContext.trust) ||
    (!peer.hasAnnouncedCatalogueSnapshot() &&
      (ingestContext.trust === CommitUnitTrust.Session ||
        (ingestContext.trust === CommitUnitTrust.Relay &&
          peer.admittedScopeRelayBinding() !== undefined)));
  if (catalogueReady && ownerAdmitted && catalogueEntitled) {
    try {
      sendCatalogueSnapshotIfNeeded(pc.node, peer, pc.transport);
    } catch (error: any) {
      if (error?.code !== ErrorCode.Backpressure) throw error;
      scheduleTickIn(rt.scheduler, TickUrgency.Deferred);
      return true;
    }
  }
  const flags: TickFlags = {
    appliedInbound: false,
    scheduledFollowUp: false,
    sentViewUpdate: false,
    needsSubscriptionRefresh: false,
  };
  const relayRejections = rt.pendingRelaySubscriptionRejections.get(pc.connectionEpoch) ?? [];
  rt.pendingRelaySubscriptionRejections.delete(pc.connectionEpoch);
  for (const rejection of relayRejections) {
    const group = state.coverageGroups.get(rejection.coverage);
    // Forward the authority's reason to every active usage site before retiring the group.
    if (group)
      for (const subscription of group.subscribers)
        if (rejection.downstreamSubscriptions.has(subscription))
          queueDirectControl(pc.pendingControlResponses, {
            kind: "SubscribeRejected",
            subscription,
            reason: rejection.reason,
          } as SyncMessage);
    if (!state.coverageGroups.delete(rejection.coverage) || !group) continue;
    const groupSubscription = coverageGroupSubscriptionKey(rejection.coverage);
    const node = pc.node.borrow();
    const binding = () =>
      PolicyBindingKey.fromCanonicalParts(rejection.policyBinding[0], rejection.policyBinding[1]);
    // Retire both the shared evaluator and each separately registered usage site.
    node.applyUnsubscribeWithAdmittedPolicyBinding(groupSubscription, binding());
    peer.forgetSubscriptionWithNode(node, groupSubscription);
    for (const subscription of group.subscribers) {
      node.applyUnsubscribeWithAdmittedPolicyBinding(subscription, binding());
      if (subscriptionKeyString(subscription) !== subscriptionKeyString(groupSubscription))
        peer.forgetSubscriptionWithNode(node, subscription);
      state.served.delete(subscription);
    }
  }
  if (
    !flushDownstreamFates(
      pc.node,
      peer,
      pc.transport,
      pc.downstreamFates,
      readBarrier,
      rt.scheduler,
    )
  )
    return true;
  if (!flushPendingChunkResponse(pc)) return true;
  if (!flushControls(pc, state)) return true;
  const aux = pc.auxiliaryPump.takeOutbound(64);
  if (aux) {
    try {
      pc.transport.send(aux);
    } catch (error) {
      pc.auxiliaryPump.restoreOutbound(aux);
      if (handleTransportBackpressure(pc.node, rt.scheduler, error)) return true;
      throw transportError(error);
    }
    pc.auxiliaryPump.acknowledgeOutbound(aux);
  }
  if (await subscriberInbound(pc, state, stats, progressWaker, sessionClaimBinding, flags))
    return true;
  return subscriberServe(pc, state, stats, progressWaker, sessionClaimBinding, flags);
}

/** Inbound dispatch; returns true when the tick must stop. */
async function subscriberInbound(
  pc: PeerConnection,
  state: SubscriberConnectionState,
  stats: Stats,
  progressWaker: Waker | undefined,
  sessionClaimBinding: ClaimBinding | undefined,
  flags: TickFlags,
): Promise<boolean> {
  const rt = pc.rt;
  const { peer, ingestContext, readBarrier } = state;
  const connectionEpoch = pc.connectionEpoch;
  const receivedLeases: any[] = [];
  for (;;) {
    // Drain new controls first, so cancellation retires parked requests before catalogue activation.
    const released = readBarrier.popReleased();
    const barrierPassed = released !== undefined;
    let next: { message: SyncMessage; lease: any; receiptsValidated: boolean };
    let parkedPolicyBinding: ClaimBinding | undefined;
    if (released) next = released;
    else if (pc.stagedInbound.length) next = pc.stagedInbound.shift();
    else {
      let received: any;
      try {
        received = pc.transport.tryRecvOwnedResult();
      } catch (error) {
        if (handleTransportBackpressure(pc.node, rt.scheduler, error)) return true;
        throw transportError(error);
      }
      if (received) next = received;
      else {
        // Input ran out with requests held: answer the writes ingested so far (SPEC 8 §8.2.1).
        if (readBarrier.isHolding()) {
          await queueLocalAcknowledgements(rt.localFateRoutes, pc.node);
          if (
            !flushDownstreamFates(
              pc.node,
              peer,
              pc.transport,
              pc.downstreamFates,
              readBarrier,
              rt.scheduler,
            )
          )
            return true;
          if (readBarrier.hasReleased()) continue;
        }
        const ready = [...state.pendingCatalogueSubscriptions].find(
          ([, pending]) =>
            pc.node.borrow().registeredShape(pending.subscribe.shapeId) !== undefined,
        );
        if (!ready) break;
        state.pendingCatalogueSubscriptions.delete(ready[0]);
        const pending = ready[1];
        next = {
          message: { kind: "Subscribe", ...pending.subscribe } as SyncMessage,
          lease: undefined,
          receiptsValidated: false,
        };
        parkedPolicyBinding = pending.policyBinding;
      }
    }
    // Hold requests behind an unanswered read barrier (SPEC 8 §8.2.1).
    if (!barrierPassed && !parkedPolicyBinding) {
      const admitted = readBarrier.admit(next);
      if (!admitted) continue;
      next = admitted;
    }
    const { message, lease, receiptsValidated } = next;
    if (lease) receivedLeases.push(lease);
    if (subscriberInboundMessageIsAuthorityOnly(message, ingestContext, peer)) {
      dropPeerRequest(pc.node);
      continue;
    }
    const previouslyAppliedInbound = flags.appliedInbound;
    flags.appliedInbound = true;
    const m: any = message;
    switch (m.kind) {
      case "ChunkRequestBatch": {
        const responses: any[] = [];
        for (const request of m.requests) {
          if (pc.auxiliaryPump.isDisconnected()) break;
          let local: any;
          try {
            local = {
              ok: await pc.node.with((n) => n.localChunk(request.locator, request.expectedHash)),
            };
          } catch (error: any) {
            local = { error };
          }
          if (pc.auxiliaryPump.isDisconnected()) break;
          if ("ok" in local)
            responses.push({
              requestId: request.requestId,
              result: { kind: "Found", bytes: local.ok },
            });
          else if (local.error?.kind === "Unavailable")
            pc.auxiliaryPump.resolver.enqueueRelay(connectionEpoch, request);
          else responses.push({ requestId: request.requestId, result: { kind: "Unavailable" } });
        }
        if (responses.length && !pc.auxiliaryPump.isDisconnected()) {
          pc.pendingChunkResponse = { responses };
          scheduleTickIn(rt.scheduler, TickUrgency.Immediate);
          return true;
        }
        continue;
      }
      case "ChunkResponseBatch":
      case "AuthorizationScopeView":
      case "AuthorizationScopeAggregateReceipt":
      case "AuthorizationScopeUnavailable":
      case "AuthorizationScopeDecision":
        dropPeerRequest(pc.node);
        continue;
      case "CurrentRowsCancel":
        rt.currentRows.cancelDownstream(connectionEpoch, m.requestId);
        continue;
      case "CurrentRowsRequest": {
        const request = m.request ?? m;
        const session = pc.transport.connectionSessionContext();
        const binding =
          session && rowAvailabilityValidRequest(request)
            ? admittedRequestPolicyBinding(
                ingestContext,
                peer,
                sessionClaimBinding,
                request.delegatedSession,
              )
            : undefined;
        if (!binding) {
          dropPeerRequest(pc.node);
          continue;
        }
        const [identity, claims] = binding;
        const context = PolicyBindingKey.fromCanonicalParts(identity, claims);
        if (pc.node.borrow().canMintCurrentRowReceipts()) {
          const progress = (rt.currentRows.progress.get(connectionEpoch) ?? 0) + 1;
          if (!Number.isSafeInteger(progress)) throw new Error("current row progress exhausted");
          rt.currentRows.progress.set(connectionEpoch, progress);
          const epoch = session!.local.epoch;
          const receipt = await pc.node.with((n) =>
            n
              .scopedActiveSessionClaims(identity, claims)
              .evaluateCurrentRows(request, context, epoch, progress),
          );
          queueSyncContextControl(pc.pendingControlResponses, {
            kind: "CurrentRowsReceipt",
            receipt,
          } as SyncMessage);
        } else {
          const forwarded = {
            ...request,
            requestId: uuidToString(uuidV4()),
            delegatedSession: undefined,
          };
          const route = {
            request: forwarded,
            context,
            upstream: undefined,
            downstream: [connectionEpoch, request.requestId],
            sender: undefined,
          };
          if (rt.admittedUpstreamAuthority === undefined || !rt.currentRows.admit(route))
            queueDirectControl(pc.pendingControlResponses, {
              kind: "CurrentRowsReceipt",
              receipt: rowAvailabilityUnknownReceipt(request, context),
            } as SyncMessage);
        }
        scheduleTickIn(rt.scheduler, TickUrgency.Immediate);
        if (!flushControls(pc, state)) return true;
        continue;
      }
      case "AuthorizationScopeIntent": {
        const context = pc.transport.connectionSessionContext();
        const admitted =
          context !== undefined &&
          (context.negotiatedFeatures & FEATURE_AUTHORIZATION_SCOPE_VIEWS) !== 0;
        const binding = admitted
          ? admittedRequestPolicyBinding(
              ingestContext,
              peer,
              sessionClaimBinding,
              m.delegatedSession,
            )
          : undefined;
        if (!binding) {
          dropPeerRequest(pc.node);
          continue;
        }
        await serveAuthorizationScopeIntent(
          pc.node,
          peer,
          pc.pendingControlResponses,
          binding[0],
          binding[1],
          connectionEpoch,
          m.requestId,
          m.action,
          ingestContext.trust,
          state,
        );
        if (pc.pendingControlResponses.length) {
          scheduleTickIn(rt.scheduler, TickUrgency.Immediate);
          if (!flushControls(pc, state)) return true;
        }
        continue;
      }
      case "RegisterShape":
        await admitRegisterShape(pc, state, sessionClaimBinding, m);
        continue;
      case "Subscribe":
        await admitSubscribe(
          pc,
          state,
          stats,
          progressWaker,
          sessionClaimBinding,
          parkedPolicyBinding,
          m,
          flags,
        );
        continue;
      case "Unsubscribe":
        handleUnsubscribe(pc, state, m.subscription);
        continue;
      default: {
        if (m.kind === "CommitUnit") readBarrier.commitUnitReceived(m.tx.txId);
        const outcome = await admitOther(
          pc,
          state,
          stats,
          progressWaker,
          sessionClaimBinding,
          message,
          lease,
          receiptsValidated,
          flags,
        );
        if (outcome === "catalogueDeferred") {
          flags.appliedInbound = previouslyAppliedInbound;
          return false;
        }
        continue;
      }
    }
  }
  return false;
}

async function admitRegisterShape(
  pc: PeerConnection,
  state: SubscriberConnectionState,
  sessionClaimBinding: ClaimBinding | undefined,
  m: any,
): Promise<void> {
  const { shapeId, opts, ast } = m;
  const { peer, ingestContext, shapeRegistrations } = state;
  // No stable subscription key exists before the read-view key is derived: fail the link.
  const sizeError = validateShapeRegistrationSize(ast, opts);
  if (sizeError !== undefined) throw new DbError(ErrorCode.Protocol, sizeError);
  const readViewKey = opts.readViewKey();
  const regKey = registrationKey(shapeId, readViewKey);
  const rejectionSubscription = () => registerShapeRejectionSubscription(shapeId, readViewKey);
  if (
    !shapeRegistrations.has(regKey) &&
    shapeRegistrations.size >= MAX_SHAPE_REGISTRATIONS_PER_PEER
  ) {
    const error = {
      kind: "UnsupportedSyncMessage",
      message: "peer shape registration limit exceeded",
    };
    rejectAndFlush(
      pc,
      state,
      serverSubscriptionFailureRejectionMessage(rejectionSubscription(), error),
    );
    return;
  }
  try {
    ensureSupportedRegisterShapeOptions(
      opts,
      state.localReceiver,
      peer.role(),
      delegatedSessionCapability(ingestContext, peer.role()),
    );
  } catch (error: any) {
    shapeRegistrations.set(regKey, {
      kind: "RejectedUnsupportedCapability",
      detail: error.message,
    });
    rejectAndFlush(
      pc,
      state,
      unsupportedShapeCapabilityRejectionMessage(rejectionSubscription(), error.message),
    );
    return;
  }
  let shape: any;
  try {
    shape = validateShapeAstForRegistration(pc.node.borrow(), shapeId, ast);
  } catch (error) {
    if (isServerShapeValidationFailure(error))
      rejectAndFlush(
        pc,
        state,
        serverSubscriptionFailureRejectionMessage(rejectionSubscription(), error),
      );
    else dropPeerRequest(pc.node);
    return;
  }
  // Branch compilation may install a sparse source; defer that preflight until Subscribe.
  const permissionSubject = shape ? subscriberPermissionSubject(ingestContext) : undefined;
  if (shape && shape.params().size === 0 && permissionSubject !== undefined) {
    let binding: any;
    try {
      binding = shape.bind(new Map());
    } catch {
      dropPeerRequest(pc.node);
      return;
    }
    const supported = await supportCheck(
      pc,
      sessionClaimBinding!,
      shape,
      binding,
      opts,
      permissionSubject,
    );
    if (supported) {
      const subscription = { shapeId, bindingId: binding.bindingId(), readView: readViewKey };
      if (supported.kind === "QueryCapability") {
        shapeRegistrations.set(regKey, {
          kind: "RejectedUnsupportedCapability",
          detail: supported.detail,
        });
        rejectAndFlush(
          pc,
          state,
          unsupportedShapeCapabilityRejectionMessage(subscription, supported.detail),
        );
      } else
        rejectAndFlush(
          pc,
          state,
          serverSubscriptionFailureRejectionMessage(subscription, supported),
        );
      return;
    }
  }
  const existing = shapeRegistrations.get(regKey);
  if (existing) {
    if (existing.kind === "RejectedUnsupportedCapability") {
      rejectAndFlush(
        pc,
        state,
        unsupportedShapeCapabilityRejectionMessage(rejectionSubscription(), existing.detail),
      );
      return;
    }
    if (dbg(existing.opts) !== dbg(opts)) {
      dropPeerRequest(pc.node);
      return;
    }
  }
  try {
    await pc.node.with((n) =>
      n.registerShapeForPeerWithOptions(pc.connectionEpoch, shapeId, ast, opts),
    );
  } catch (error) {
    rejectAndFlush(
      pc,
      state,
      serverSubscriptionFailureRejectionMessage(rejectionSubscription(), error),
    );
    return;
  }
  shapeRegistrations.set(regKey, {
    kind: shape ? "Registered" : "PendingCatalogueAdmission",
    opts,
    shapeId,
    readView: readViewKey,
  });
}

/** Returns the support error, or undefined when the maintained view is supported. */
async function supportCheck(
  pc: PeerConnection,
  binding: ClaimBinding,
  shape: any,
  shapeBinding: any,
  opts: any,
  subject: AuthorSubject,
): Promise<any> {
  try {
    await pc.node.with((owner) => {
      const node = scoped(owner, binding);
      return node.ensurePeerMaintainedSubscriptionViewSupported(
        shape,
        shapeBinding,
        opts.tier,
        subject,
        opts.readView,
        node.peerQueryAuthorizationMode(),
      );
    });
    return undefined;
  } catch (error) {
    return error;
  }
}

async function admitSubscribe(
  pc: PeerConnection,
  state: SubscriberConnectionState,
  stats: Stats,
  progressWaker: Waker | undefined,
  connectionBinding: ClaimBinding | undefined,
  parkedPolicyBinding: ClaimBinding | undefined,
  subscribe: any,
  flags: TickFlags,
): Promise<void> {
  const rt = pc.rt;
  const connectionEpoch = pc.connectionEpoch;
  const {
    peer,
    ingestContext,
    served,
    coverageGroups,
    shapeRegistrations,
    pendingCatalogueSubscriptions,
  } = state;
  const hasDelegatedSession = subscribe.delegatedSession !== undefined;
  // A relay transport is not a user: it may only carry the topology-assigned session snapshot.
  const sessionClaimBinding: ClaimBinding | undefined =
    parkedPolicyBinding ??
    admittedRequestPolicyBinding(
      ingestContext,
      peer,
      connectionBinding,
      subscribe.delegatedSession,
    );
  const { shapeId, subscription } = subscribe;
  if (!sessionClaimBinding || dbg(shapeId) !== dbg(subscription.shapeId)) {
    dropPeerRequest(pc.node);
    return;
  }
  // A repeat can race catalogue activation; validate it against the parked request first.
  const parked = pendingCatalogueSubscriptions.get(subscription);
  if (parked) {
    if (
      dbg(parked.subscribe) !== dbg(subscribe) ||
      dbg(parked.policyBinding) !== dbg(sessionClaimBinding)
    )
      throw new DbError(ErrorCode.Protocol, "conflicting pending catalogue subscription");
    pendingCatalogueSubscriptions.delete(subscription);
  }
  const { values, knownState } = subscribe;
  const regKey = registrationKey(shapeId, subscription.readView);
  const registration = shapeRegistrations.get(regKey);
  if (!registration) {
    dropPeerRequest(pc.node);
    return;
  }
  if (registration.kind === "RejectedUnsupportedCapability") {
    // Keep the permanent rejection, but let already-served views flush first.
    queueDirectControl(
      state.deferredSubscribeRejections,
      unsupportedShapeCapabilityRejectionMessage(subscription, registration.detail),
    );
    return;
  }
  const pendingCatalogueAdmission = registration.kind === "PendingCatalogueAdmission";
  const opts = registration.opts;
  const shape = pc.node.borrow().registeredShape(shapeId);
  if (!shape) {
    if (!pendingCatalogueAdmission) {
      dropPeerRequest(pc.node);
      return;
    }
    let encodedBytes: number;
    try {
      encodedBytes = serializedSize([subscribe, sessionClaimBinding]);
    } catch {
      throw new DbError(ErrorCode.Protocol, "cannot size pending catalogue subscription");
    }
    let retained = 0;
    for (const pending of pendingCatalogueSubscriptions.values()) retained += pending.encodedBytes;
    if (
      pendingCatalogueSubscriptions.size >= MAX_SHAPE_REGISTRATIONS_PER_PEER ||
      retained + encodedBytes > MAX_SHAPE_REGISTRATION_BYTES * MAX_SHAPE_REGISTRATIONS_PER_PEER
    )
      throw new DbError(ErrorCode.Protocol, "pending catalogue subscription limit exceeded");
    pendingCatalogueSubscriptions.set(subscription, {
      subscribe,
      policyBinding: sessionClaimBinding,
      encodedBytes,
    });
    return;
  }
  const params: string[] = [...shape.params().keys()];
  if (values.length !== params.length) {
    dropPeerRequest(pc.node);
    return;
  }
  let binding: any;
  try {
    binding = shape.bind(new Map(params.map((name, i) => [name, values[i]])));
    ensureSupportedRegisterShapeOptions(
      opts,
      state.localReceiver,
      peer.role(),
      delegatedSessionCapability(ingestContext, peer.role()),
    );
  } catch {
    dropPeerRequest(pc.node);
    return;
  }
  const policyBinding = sessionClaimBinding;
  // Every admitted reader gets its own immutable policy scope within the coverage evaluator.
  const coverage: CoverageKey = coverageKey(shape, binding, opts);
  coverage.policyBinding = PolicyBindingKey.fromCanonicalParts(policyBinding[0], policyBinding[1]);
  const existingCoverage = served.get(subscription);
  if (existingCoverage && dbg(existingCoverage) !== dbg(coverage)) {
    dropPeerRequest(pc.node);
    return;
  }
  if (pendingCatalogueAdmission)
    shapeRegistrations.set(regKey, { ...registration, kind: "Registered" });
  const supported = await supportCheck(
    pc,
    sessionClaimBinding,
    shape,
    binding,
    opts,
    policyBinding[0],
  );
  if (supported) {
    rejectAndFlush(
      pc,
      state,
      supported.kind === "QueryCapability"
        ? unsupportedShapeCapabilityRejectionMessage(subscription, supported.detail)
        : serverSubscriptionFailureRejectionMessage(subscription, supported),
    );
    return;
  }
  const groupSubscription = coverageGroupSubscriptionKey(coverage);
  const localSubscriber = state.localReceiver;
  const scopeRelay = pc.node.borrow().clientRelayScope() !== undefined;
  let upstreamOpts: any;
  if (localSubscriber || scopeRelay) {
    upstreamOpts = upstreamRegisterShapeOptions(opts.tier, opts.readView, DurabilityTier.Global);
    if (scopeRelay) upstreamOpts.bindingSource = BindingSource.RelayAuthoritySession;
  } else {
    // The incoming relay capability belongs to this hop; forward on our own trusted link.
    upstreamOpts = { ...opts, bindingSource: BindingSource.Ordinary };
  }
  // A scope relay has one authority stream per exact admitted coverage.
  const relayCoverage = scopeRelay ? dbg(relayAuthorityCoverageKey(coverage)) : undefined;
  const sharedUpstream = scopeRelay
    ? [...rt.relayUpstreamSubscriptionOwners].find(
        ([, owner]) =>
          dbg(relayAuthorityCoverageKey(owner.coverage)) === relayCoverage &&
          dbg(owner.policyBinding) === dbg(policyBinding),
      )?.[0][0]
    : undefined;
  const upstreamSubscription =
    sharedUpstream ??
    relayUpstreamSubscriptionKey(
      connectionEpoch,
      subscription,
      upstreamOpts.readViewKey(),
      policyBinding,
    );
  // Topology role, not transport direction: only a non-authoritative relay's U receives authority results.
  const propagates = localSubscriber || scopeRelay;
  const waitsForSelectedAuthority =
    propagates && DurabilityTier.cmp(opts.tier, DurabilityTier.Local) > 0;
  const authorityResultSubscription = propagates ? upstreamSubscription : groupSubscription;
  const existingGroup = coverageGroups.get(coverage);
  const firstSubscriber = !existingGroup || existingGroup.subscribers.size === 0;
  const permissionsReady = subscriberPermissionsReady(
    pc.node.borrow().permissionsReady(),
    ingestContext.trust,
  );
  const openingPending: any = permissionsReady
    ? undefined
    : {
        kind: "ViewUpdate",
        subscription,
        settledThrough: pc.node.borrow().committedGlobalTime(),
        versionCarriers: [],
        peerPayloadInventory: {
          completeTxPayloads: [],
          authorizationProgress: undefined,
          openingPending: true,
        },
        supportingRows: SupportingRowsUpdate.snapshot([]),
      };
  // Known state describes this concrete receiver, not the shared coverage evaluator.
  peer.declareKnownState(subscription, knownState);
  if (firstSubscriber && knownState?.kind === "Watermark")
    peer.declareKnownState(groupSubscription, knownState);
  peer.setSubscriptionPolicyBinding(subscription, policyBinding);
  peer.setSubscriptionPolicyBinding(groupSubscription, policyBinding);
  await pc.node.with((n) =>
    n.applySubscribeWithAdmittedPolicyBinding(
      { ...subscribe, subscription: groupSubscription },
      PolicyBindingKey.fromCanonicalParts(policyBinding[0], policyBinding[1]),
    ),
  );
  const outcome = { value: [], publications: [], postSettlementWork: [] };
  const upstreamBindingView = {
    shapeId: shape.shapeId(),
    bindingId: binding.bindingId(),
    readView: upstreamOpts.readViewKey(),
  };
  const selectedKey =
    scopeRelay && propagates
      ? AuthorityResultKey.policyScoped(
          upstreamBindingView,
          PolicyBindingKey.fromCanonicalParts(policyBinding[0], policyBinding[1]),
        )
      : undefined;
  // Record the exact source on D and the group owner before any publication path can emit.
  if (selectedKey) {
    for (const key of [groupSubscription, subscription]) {
      peer.setSubscriptionAuthorityResultSource(key, selectedKey);
      peer.setSubscriptionAwaitingSelectedAuthoritySource(key, waitsForSelectedAuthority);
    }
  }
  // An ownerless settled result survives reopen; don't let it settle a fresh Global usage early.
  if (localSubscriber && upstreamOpts.bindingSource === BindingSource.RelayAuthoritySession)
    pc.node.borrow().invalidateOwnerlessSettledResultView(upstreamBindingView);
  const [, changed, published] = await finishPeerPublicationOutcomeWithRefresh(
    pc.node,
    rt,
    progressWaker,
    outcome,
    false,
  );
  stats.subscriptionEvents += changed;
  flags.needsSubscriptionRefresh ||= published;
  // A strict child of a non-authoritative relay waits for its exact upstream receipt.
  let waitingForSettlement = false;
  if (waitsForSelectedAuthority) {
    const node = pc.node.borrow();
    let selected = selectedKey;
    if (!selected) {
      try {
        selected = node.authorityResultKeyForSubscription(authorityResultSubscription);
      } catch {
        selected = undefined;
      }
    }
    waitingForSettlement = !(selected && node.hasSettledAuthorityResult(selected));
  }
  let group = existingGroup;
  if (!group) {
    group = {
      shape,
      binding,
      policyBinding,
      policyBindingOrigin: hasDelegatedSession
        ? CoveragePolicyBindingOrigin.Delegated
        : CoveragePolicyBindingOrigin.DirectAdmitted,
      subscribers: new SortedSet(cmpSubscriptionKey),
      pendingInitialSubscribers: new SortedSet(cmpSubscriptionKey),
      pendingInitialUpdate: undefined,
      pendingIncrementalUpdates: [],
      publicationRuntimeToken: undefined,
      pendingClaimRefreshRevision: undefined,
      initialized: false,
      authorityResultSubscription,
      upstreamSubscription,
      upstreamOpts,
      awaitingUpstreamSettlement: waitingForSettlement,
    };
    coverageGroups.set(coverage, group);
  }
  group.subscribers.add(subscription);
  group.pendingInitialSubscribers.add(subscription);
  // Keep the policy-scoped U source selected at admission.
  if (selectedKey) peer.setSubscriptionAuthorityResultSource(groupSubscription, selectedKey);
  const ownerKey: [SubscriptionKey, number, any] = [
    group.upstreamSubscription,
    connectionEpoch,
    coverage.opts.readViewKey(),
  ];
  const existingOwner = rt.relayUpstreamSubscriptionOwners.get(ownerKey);
  if (existingOwner) existingOwner.downstreamSubscriptions.add(subscription);
  else
    rt.relayUpstreamSubscriptionOwners.set(ownerKey, {
      request: {
        subscription: group.upstreamSubscription,
        shape,
        binding,
        opts: upstreamOpts,
        identity: group.policyBinding[0],
        policyBinding: group.policyBinding,
      },
      scalarAuthorityRevision: 0,
      scalarReconciliation: {},
      downstreamConnectionEpoch: connectionEpoch,
      coverage,
      policyBinding: group.policyBinding,
      downstreamSubscriptions: new SortedSet(cmpSubscriptionKey, [subscription]),
    });
  served.set(subscription, coverage);
  if (openingPending) {
    const update = stampViewUpdateAuthorizationProgressFrom(
      peer,
      groupSubscription,
      openingPending,
    );
    pc.lastResumeBytes = serializedSyncMessageLen(update);
    sendSubscriberWithSyncContext(pc.node, peer, pc.transport, rt, pc.downstreamFates, update);
    flags.sentViewUpdate = true;
  }
  if (firstSubscriber)
    // The relay's transport identity is SYSTEM; each subscription carries the caller's admitted policy context.
    rt.upstreamSubscriptions.push({
      kind: "Subscribe",
      subscription: group.upstreamSubscription,
      shape,
      binding,
      opts: group.upstreamOpts,
      identity: group.policyBinding[0],
      policyBinding: group.policyBinding,
    });
  // Let local write receipts ingress before cold initial hydration.
  scheduleTickIn(rt.scheduler, TickUrgency.AfterCurrentTurn);
  flags.scheduledFollowUp = true;
}

function handleUnsubscribe(
  pc: PeerConnection,
  state: SubscriberConnectionState,
  subscription: SubscriptionKey,
): void {
  const rt = pc.rt;
  const { peer, served, coverageGroups, shapeRegistrations } = state;
  state.pendingCatalogueSubscriptions.delete(subscription);
  const admitted = peer.subscriptionPolicyBinding(subscription);
  const node = pc.node.borrow();
  if (admitted)
    node.applyUnsubscribeWithAdmittedPolicyBinding(
      subscription,
      PolicyBindingKey.fromCanonicalParts(admitted[0], admitted[1]),
    );
  else node.applyUnsubscribe(subscription);
  const key = subscriptionKeyString(subscription);
  const coverage = served.get(subscription);
  if (coverage) {
    served.delete(subscription);
    const group = coverageGroups.get(coverage);
    if (group) {
      group.subscribers.delete(subscription);
      group.pendingInitialSubscribers.delete(subscription);
      group.pendingIncrementalUpdates = group.pendingIncrementalUpdates.filter(
        ([recipient]: [SubscriptionKey]) => subscriptionKeyString(recipient) !== key,
      );
      if (
        group.pendingInitialUpdate &&
        subscriptionKeyString(group.pendingInitialUpdate[0]) === key
      )
        group.pendingInitialUpdate = undefined;
      const owner = rt.relayUpstreamSubscriptionOwners.get([
        group.upstreamSubscription,
        pc.connectionEpoch,
        coverage.opts.readViewKey(),
      ]);
      if (
        owner &&
        owner.downstreamConnectionEpoch === pc.connectionEpoch &&
        dbg(owner.coverage) === dbg(coverage)
      )
        owner.downstreamSubscriptions.delete(subscription);
      if (group.subscribers.size === 0) {
        const upstreamSubscription = group.upstreamSubscription;
        // Tear down the maintained receiver together with the peer cursor on the final usage site.
        peer.forgetSubscriptionWithNode(node, coverageGroupSubscriptionKey(coverage));
        coverageGroups.delete(coverage);
        if (
          retireRelayUpstreamSubscription(
            rt.relayUpstreamSubscriptionOwners,
            upstreamSubscription,
            pc.connectionEpoch,
            coverage,
          ) !== undefined
        ) {
          // The relay owns both the exact authority receipt and its wire usage site.
          node.applyUnsubscribe(upstreamSubscription);
          rt.upstreamSubscriptions.push({
            kind: "Unsubscribe",
            subscription: upstreamSubscription,
          });
        }
      }
    }
  }
  const regKey = registrationKey(subscription.shapeId, subscription.readView);
  const stillServed = [...served]
    .map(([active]) => active)
    .some(
      (active: SubscriptionKey) =>
        dbg(active.shapeId) === dbg(subscription.shapeId) &&
        dbg(active.readView) === dbg(subscription.readView),
    );
  if (stillServed) return;
  const removed = shapeRegistrations.get(regKey);
  shapeRegistrations.delete(regKey);
  if (
    removed &&
    removed.kind !== "RejectedUnsupportedCapability" &&
    ![...shapeRegistrations.values()].some(
      (r) =>
        r.kind !== "RejectedUnsupportedCapability" && dbg(r.shapeId) === dbg(subscription.shapeId),
    )
  )
    node.releaseShapeForPeer(pc.connectionEpoch, subscription.shapeId);
}

async function admitOther(
  pc: PeerConnection,
  state: SubscriberConnectionState,
  stats: Stats,
  progressWaker: Waker | undefined,
  sessionClaimBinding: ClaimBinding | undefined,
  other: SyncMessage,
  lease: any,
  receiptsValidated: boolean,
  flags: TickFlags,
): Promise<"catalogueDeferred" | "done"> {
  const rt = pc.rt;
  const { peer, ingestContext } = state;
  const o: any = other;
  // Claims are fixed at host admission; sessions and relays cannot mutate the node-wide cache.
  if (
    o.kind === "SessionClaims" &&
    (ingestContext.trust === CommitUnitTrust.Session ||
      ingestContext.trust === CommitUnitTrust.Relay)
  ) {
    dropPeerRequest(pc.node);
    return "done";
  }
  const localUpload = o.kind === "CommitUnit" ? o.tx.txId : undefined;
  if (
    localUpload !== undefined &&
    !state.localReceiver &&
    !subscriberPermissionsReady(pc.node.borrow().permissionsReady(), ingestContext.trust)
  ) {
    pc.downstreamFates.push({
      kind: "FateUpdate",
      txId: localUpload,
      fate: {
        kind: "Rejected",
        reason: {
          kind: "MalformedCommit",
          detail: "permissions_head_missing: no published permissions head",
        },
      },
      globalTime: undefined,
      durability: undefined,
    });
    return "done";
  }
  const writeStateTxId = writeStateUpdateTxId(other);
  let outcome: any;
  if (isCatalogueMutation(other)) {
    const owner = await pc.node.lock();
    try {
      if (owner.deferCatalogueForPersistence(progressWaker)) {
        pc.stagedInbound.unshift({
          message: other,
          lease,
          authorityReceiptEligible: false,
          receiptsValidated,
        });
        return "catalogueDeferred";
      }
      outcome = await owner.applySyncMessageWithIngestContext(other, ingestContext);
    } finally {
      pc.node.unlock();
    }
  } else {
    if (!sessionClaimBinding)
      throw new Error("subscriber dispatch has an admitted immutable session binding");
    outcome = await dispatchAdmittedSubscriberMessage(
      pc.node,
      peer,
      state.localReceiver,
      { ...ingestContext, versionReceiptsValidated: receiptsValidated },
      sessionClaimBinding,
      rt,
      pc.downstreamFates,
      other,
    );
  }
  const [responses, changed, published] = await finishPeerPublicationOutcomeWithRefresh<
    SyncMessage[]
  >(pc.node, rt, progressWaker, outcome, false);
  stats.subscriptionEvents += changed;
  flags.needsSubscriptionRefresh ||= published;
  if (writeStateTxId !== undefined) handleWriteStateUpdate(pc.node, rt, writeStateTxId);
  // A declared root with no upstream settles locally; anyone with an upstream must still relay.
  const settledHere =
    rt.outbox.settlesUploadsLocally() &&
    localUpload !== undefined &&
    responses.some((response) => fateUpdateSettlesUpload(response, localUpload));
  for (const response of responses) {
    if ((response as any).kind === "FateUpdate") pc.downstreamFates.push(response);
    else sendWithSyncContext(pc.node, peer, pc.transport, response);
  }
  if (localUpload !== undefined) {
    if (settledHere) {
      // A reconnect may have reconstructed this upload before the unit arrived.
      if (rt.outbox.contains(localUpload)) pc.releasedOutboxTxIds.push(localUpload);
    } else if (queuePendingUploadIn(rt.outbox, localUpload, other))
      scheduleTickIn(rt.scheduler, TickUrgency.Deferred);
  }
  return "done";
}

/** Post-ingress phase: acknowledgements, refresh, and maintained-view serving. */
async function subscriberServe(
  pc: PeerConnection,
  state: SubscriberConnectionState,
  stats: Stats,
  progressWaker: Waker | undefined,
  sessionClaimBinding: ClaimBinding | undefined,
  flags: TickFlags,
): Promise<boolean> {
  const rt = pc.rt;
  const { peer, ingestContext, readBarrier } = state;
  await queueLocalAcknowledgements(rt.localFateRoutes, pc.node);
  if (
    !flushDownstreamFates(
      pc.node,
      peer,
      pc.transport,
      pc.downstreamFates,
      readBarrier,
      rt.scheduler,
    )
  )
    return true;
  if (flags.needsSubscriptionRefresh) {
    try {
      stats.subscriptionEvents += await refreshSubscriptionsIn(pc.node, rt, progressWaker);
    } catch (error) {
      stats.subscriptionEvents += routeSubscriptionRefreshFailure(rt.subscriptions, error);
    }
  }
  if (flags.appliedInbound && !flags.scheduledFollowUp)
    scheduleTickIn(rt.scheduler, TickUrgency.AfterCurrentTurn);
  if (flags.appliedInbound) {
    const next = rt.subscriberDirtyEpoch + 1;
    rt.subscriberDirtyEpoch = next;
    pc.observedSubscriberDirtyEpoch = next;
    state.serveDirty = true;
    // Cold downstream view assembly is a separate turn so it can't withhold durability receipts.
    scheduleTickIn(rt.scheduler, TickUrgency.AfterCurrentTurn);
    return true;
  }
  if (
    state.serveDirty &&
    subscriberPermissionsReady(pc.node.borrow().permissionsReady(), ingestContext.trust)
  ) {
    const result = await serveCoverageGroups(pc, state, progressWaker, sessionClaimBinding!, flags);
    if (result === "stop") return true;
    state.serveDirty = result;
    if (result) scheduleTickIn(rt.scheduler, TickUrgency.Deferred);
  }
  if (flags.sentViewUpdate) {
    pc.pendingControlResponses.push(...state.deferredSubscribeRejections.splice(0));
    if (!flushControls(pc, state)) return true;
  }
  return false;
}

/** Send the group's queued incremental updates; returns false on backpressure. */
function drainIncremental(
  pc: PeerConnection,
  state: SubscriberConnectionState,
  group: any,
  flags: TickFlags,
): boolean {
  while (group.pendingIncrementalUpdates.length) {
    const [, update] = group.pendingIncrementalUpdates[0];
    try {
      sendPreparedSubscriberWithSyncContext(
        pc.node,
        state.peer,
        pc.transport,
        pc.rt,
        pc.downstreamFates,
        update,
      );
    } catch (error: any) {
      if (error?.code !== ErrorCode.Backpressure) throw error;
      scheduleTickIn(pc.rt.scheduler, TickUrgency.Deferred);
      return false;
    }
    group.pendingIncrementalUpdates.shift();
    flags.sentViewUpdate = true;
  }
  return true;
}

function rollbackAndReject(
  pc: PeerConnection,
  state: SubscriberConnectionState,
  subscription: SubscriptionKey,
  error: any,
): "stop" {
  rollbackRejectedSubscriberAdmission(
    pc.node,
    state.peer,
    state.served,
    state.coverageGroups,
    pc.rt.relayUpstreamSubscriptionOwners,
    pc.rt.upstreamSubscriptions,
    pc.connectionEpoch,
    subscription,
  );
  queueDirectControl(
    pc.pendingControlResponses,
    error?.kind === "QueryCapability"
      ? unsupportedShapeCapabilityRejectionMessage(subscription, error.detail)
      : serverSubscriptionFailureRejectionMessage(subscription, error),
  );
  scheduleTickIn(pc.rt.scheduler, TickUrgency.Immediate);
  return "stop";
}

/** Serve every coverage group; returns whether another pass is needed, or "stop". */
async function serveCoverageGroups(
  pc: PeerConnection,
  state: SubscriberConnectionState,
  progressWaker: Waker | undefined,
  binding: ClaimBinding,
  flags: TickFlags,
): Promise<boolean | "stop"> {
  const { peer } = state;
  let serveAgain = false;
  for (const [coverage, group] of [...state.coverageGroups]) {
    const runtimeToken = pc.node.borrow().grooveRuntimeToken();
    if (
      group.publicationRuntimeToken !== undefined &&
      group.publicationRuntimeToken !== runtimeToken &&
      (group.pendingInitialUpdate || group.pendingIncrementalUpdates.length)
    ) {
      // INV-SYNC-13/14: unaccepted envelopes must be reauthorized after a policy revision.
      group.pendingInitialUpdate = undefined;
      group.pendingIncrementalUpdates = [];
      group.pendingInitialSubscribers = new SortedSet(cmpSubscriptionKey, group.subscribers);
      group.initialized = false;
    }
    group.publicationRuntimeToken = runtimeToken;
    const groupSubscription = coverageGroupSubscriptionKey(coverage);
    peer.setSubscriptionPolicyBinding(groupSubscription, group.policyBinding);
    // Admission selected the canonical authority result; don't re-derive it from the wire handle.
    let authorityKey = peer.subscriptionAuthorityResultSource(groupSubscription);
    if (authorityKey === undefined) {
      try {
        authorityKey = pc.node
          .borrow()
          .authorityResultKeyForSubscription(group.authorityResultSubscription);
      } catch {
        authorityKey = undefined;
      }
    }
    const authoritySettled =
      authorityKey !== undefined && pc.node.borrow().hasSettledAuthorityResult(authorityKey);
    const settledHandoff = group.awaitingUpstreamSettlement && authoritySettled;
    if (group.awaitingUpstreamSettlement && !settledHandoff) continue;
    if (authorityKey !== undefined)
      peer.setSubscriptionAuthorityResultSource(groupSubscription, authorityKey);
    let pendingInitial = new SortedSet<SubscriptionKey>(cmpSubscriptionKey);
    if (group.pendingIncrementalUpdates.length === 0)
      pendingInitial = new SortedSet(cmpSubscriptionKey, group.pendingInitialSubscribers);
    // Finish an earlier publication before opening a new sibling.
    else serveAgain = true;
    if (pendingInitial.size) {
      const established = new SortedSet<SubscriptionKey>(
        cmpSubscriptionKey,
        [...group.subscribers].filter((s: SubscriptionKey) => !pendingInitial.has(s)),
      );
      const retainedKey = group.pendingInitialUpdate
        ? subscriptionKeyString(group.pendingInitialUpdate[0])
        : undefined;
      const isRetained = (s: SubscriptionKey) => subscriptionKeyString(s) === retainedKey;
      // A new sibling may sort before the stalled recipient; it cannot overtake that publication.
      const order = pendingInitial
        .toArray()
        .sort((a, b) => Number(!isRetained(a)) - Number(!isRetained(b)));
      for (const subscription of order) {
        let update: any;
        if (group.pendingInitialUpdate) {
          update = group.pendingInitialUpdate[1];
          group.pendingInitialUpdate = undefined;
          // Inbound writes may have arrived while this opening waited; publish their delta later.
          serveAgain = true;
        } else {
          const retry = () => {
            group.pendingInitialSubscribers.add(subscription);
            if (
              !peer.subscriptionAwaitsSelectedAuthoritySource(groupSubscription) &&
              !peer.hasPendingInitialMaintainedSubscription(groupSubscription)
            )
              serveAgain = true;
          };
          let reconciled: any;
          if (group.initialized || peer.hasMaintainedSubscription(groupSubscription)) {
            try {
              reconciled = await pc.node.with((owner) =>
                peer.reconcileMaintainedSubscriptionForClone(
                  scoped(owner, binding),
                  groupSubscription,
                  group.shape,
                  group.binding,
                  coverage.opts,
                  progressWaker,
                ),
              );
            } catch (error) {
              return rollbackAndReject(pc, state, subscription, error);
            }
            if (!reconciled) {
              retry();
              continue;
            }
            // Publish the canonical transition to established usages before any fallible reset.
            const canonical = reconciled.canonicalUpdate;
            if (canonical) {
              for (const sibling of established) {
                let siblingUpdate = retargetViewUpdate(structuredClone(canonical), sibling);
                siblingUpdate = stampViewUpdateAuthorizationProgressFrom(
                  peer,
                  groupSubscription,
                  siblingUpdate,
                );
                stampSubscriberOpeningState(pc.node, peer, siblingUpdate);
                group.pendingIncrementalUpdates.push([sibling, siblingUpdate]);
              }
              if (!drainIncremental(pc, state, group, flags)) return "stop";
            }
          }
          try {
            update = await pc.node.with(async (owner) => {
              const node = scoped(owner, binding);
              if (reconciled)
                return peer.rehydrateQueryForSubscriptionFromReconciledMaintainedSubscription(
                  node,
                  groupSubscription,
                  subscription,
                  group.shape,
                  reconciled,
                );
              const fresh = await peer.rehydrateQueryForSubscriptionWithOptsAndWaker(
                node,
                groupSubscription,
                group.shape,
                group.binding,
                coverage.opts,
                progressWaker,
              );
              return fresh && retargetViewUpdate(fresh, subscription);
            });
          } catch (error) {
            return rollbackAndReject(pc, state, subscription, error);
          }
          if (!update) {
            retry();
            continue;
          }
          group.initialized = true;
          update = stampViewUpdateAuthorizationProgressFrom(peer, groupSubscription, update);
          stampSubscriberOpeningState(pc.node, peer, update);
        }
        pc.lastResumeBytes = serializedSyncMessageLen(update);
        // Keep the exact opening until accepted: rehydrating on retry would advance its receipt.
        const returned = trySendPreparedSubscriberWithSyncContext(
          pc.node,
          peer,
          pc.transport,
          pc.rt,
          pc.downstreamFates,
          update,
        );
        if (returned) {
          group.pendingInitialUpdate = [subscription, returned];
          scheduleTickIn(pc.rt.scheduler, TickUrgency.Deferred);
          return "stop";
        }
        group.pendingInitialSubscribers.delete(subscription);
        flags.sentViewUpdate = true;
        established.add(subscription);
      }
      continue;
    }
    if (group.pendingIncrementalUpdates.length === 0) {
      let result: { ready: boolean; update?: any };
      try {
        result = await pc.node.with(async (owner) => {
          const node = scoped(owner, binding);
          const args = [
            node,
            groupSubscription,
            group.shape,
            group.binding,
            coverage.opts,
            progressWaker,
          ] as const;
          // A cold handoff resumes the just-opened view's initial delta instead of re-rehydrating it.
          if (!settledHandoff || peer.hasMaintainedSubscription(groupSubscription))
            return peer.queryUpdateForSubscriptionWithOptsAndWaker(...args);
          const update = await peer.rehydrateQueryForSubscriptionWithOptsAndWaker(...args);
          return update ? { ready: true, update } : { ready: false };
        });
      } catch (error) {
        for (const subscription of group.subscribers)
          queueDirectControl(
            pc.pendingControlResponses,
            serverSubscriptionFailureRejectionMessage(subscription, error),
          );
        scheduleTickIn(pc.rt.scheduler, TickUrgency.Immediate);
        return "stop";
      }
      if (!result.ready) {
        serveAgain = true;
        continue;
      }
      if (settledHandoff) {
        group.awaitingUpstreamSettlement = false;
        peer.setSubscriptionAwaitingSelectedAuthoritySource(groupSubscription, false);
      }
      // A settled unchanged view has no more work.
      if (!result.update) continue;
      const shared = { value: result.update };
      const recipients: SubscriptionKey[] = [...group.subscribers];
      recipients.forEach((subscription, i) => {
        let update = retargetViewUpdate(
          takeLastOrClone(shared, i < recipients.length - 1),
          subscription,
        );
        update = stampViewUpdateAuthorizationProgressFrom(peer, groupSubscription, update);
        stampSubscriberOpeningState(pc.node, peer, update);
        group.pendingIncrementalUpdates.push([subscription, update]);
      });
    }
    if (!drainIncremental(pc, state, group, flags)) return "stop";
  }
  return serveAgain;
}

export type { CoverageKey };

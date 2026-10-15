// Upstream half of `PeerConnection::tick` (split out of peer_connection.rs).
import { uuidToString, uuidV4 } from "../../support/hash.js";
import {
  AuthorizationScopeAcquisition,
  AuthorizationScopeInstall,
  AuthorizationScopeReadiness,
  DbError,
  dbg,
  DurabilityTier,
  ErrorCode,
  ShapeAst,
  TickUrgency,
  finishOpenSchemaConnection,
  isCatalogueMutation,
  pendingOpenSchemaError,
  receiveCurrentRows,
  routeLocalFate,
  type SubscriptionKey,
  type SyncMessage,
  type TxId,
} from "../../_extern/db-runtime.js";
import {
  routeUpstreamSubscriptionRejection,
  subscriptionKeyString,
  type Waker,
} from "./node_runtime.js";
import {
  commitUnitLargeValueRefs,
  finishPeerPublicationOutcome,
  queueRelaySubscriptionRejection,
  RATE_LIMITED_UPLOAD_RETRY_DELAY_MS,
  routeSubscriptionRefreshFailure,
  type PeerConnection,
  type UpstreamConnectionState,
} from "./peer_connection.js";
import {
  applyPendingAuthorityViewUpdates,
  authorizationScopeReceiptMatchesTransportContext,
  bindingValuesInParamOrder,
  dropPeerRequest,
  evaluateAuthoritativePermissionAdvice,
  fateUpdateSettlesUpload,
  handleDbBackpressure,
  handleTransportBackpressure,
  handleWriteStateUpdate,
  largeValueUploadChunkContext,
  pushViewUpdateMessageForReceiver,
  sendWithLocalSyncContext,
  stageInitialCoverageClearForUpdate,
  transportError,
  uploadAnswerReceiptEligible,
  writeStateUpdateTxId,
  type AuthorityViewBatch,
  scheduleTickIn,
} from "./peer_connection_support.js";
import { refreshSubscriptionsIn } from "./node_runtime.js";

type Stats = { subscriptionEvents: number; remoteSyncApplied: number };

/** Send `message`, returning true when backpressure asks the tick to stop. */
function sendOrStop(pc: PeerConnection, message: SyncMessage): boolean {
  try {
    pc.transport.send(message);
    return false;
  } catch (error) {
    if (handleTransportBackpressure(pc.node, pc.rt.scheduler, error)) return true;
    throw transportError(error);
  }
}

function claimsFor(pc: PeerConnection, identity: any): any {
  return (
    pc.node
      .borrow()
      .sessionClaimsWithRevisions()
      .find(([subject]: [any]) => subject === identity)?.[1] ?? new Map()
  );
}

function resolveWaiters(pc: PeerConnection, waiters: Iterable<string>, advice: any): void {
  for (const waiterId of waiters) {
    const waiter = pc.rt.permissionAdviceWaiters.get(waiterId);
    pc.rt.permissionAdviceWaiters.delete(waiterId);
    waiter?.(advice);
  }
}

/** Outbound half: auxiliary frames, claims, uploads, read barrier, then pending commands. */
async function upstreamOutbound(
  pc: PeerConnection,
  state: UpstreamConnectionState,
  stats: Stats,
  progressWaker: Waker | undefined,
  permitsDelegatedSessions: boolean,
): Promise<boolean> {
  const rt = pc.rt;
  const outboundAux = pc.auxiliaryPump.takeOutbound(64);
  if (outboundAux) {
    try {
      pc.transport.send(outboundAux);
    } catch (error) {
      pc.auxiliaryPump.restoreOutbound(outboundAux);
      if (handleTransportBackpressure(pc.node, rt.scheduler, error)) return true;
      throw transportError(error);
    }
    pc.auxiliaryPump.acknowledgeOutbound(outboundAux);
  }
  const { pending } = state;
  pending.push(...rt.upstreamSubscriptions.splice(0));
  if (permitsDelegatedSessions) {
    for (const [identity, claims, revision] of pc.node.borrow().sessionClaimsWithRevisions()) {
      const sent = state.sentSessionClaimRevisions.get(identity);
      if (sent !== undefined && sent >= revision) continue;
      if (sendOrStop(pc, { kind: "SessionClaims", identity, claims } as SyncMessage)) return true;
      state.sentSessionClaimRevisions.set(identity, revision);
    }
  }
  // Upload unshipped local commits before any request (SPEC 8 §8.2.1).
  const outbox: any[] = rt.outbox.entries();
  let toUpload: [TxId, SyncMessage | undefined][];
  const expectedMissing = outbox.length - state.uploaded.size;
  const suffix: [TxId, SyncMessage | undefined][] = [];
  for (let i = outbox.length - 1; i >= 0 && !state.uploaded.has(outbox[i].txId); i--)
    suffix.push([outbox[i].txId, outbox[i].unit]);
  if (expectedMissing === suffix.length) toUpload = suffix.reverse();
  else
    // Fate cleanup and handoff can leave holes; keep the general set difference.
    toUpload = outbox.filter((p) => !state.uploaded.has(p.txId)).map((p) => [p.txId, p.unit]);
  // A writer's writes go up in outbox order (SPEC 14).
  const blockedWriters = new Set<string>();
  uploads: for (const [txId, staged] of toUpload) {
    const writer = String(txId.node);
    if (blockedWriters.has(writer)) continue;
    if (
      state.failedLargeValueUploads.has(txId) ||
      state.awaitingLargeValueUploads.has(txId) ||
      state.awaitingLargeValueUploads.size > 0
    ) {
      blockedWriters.add(writer);
      continue;
    }
    const nowMs = rt.uploadRetryClock.nowMs();
    const deadline = rt.largeValueUploadRetryDeadlines.get(txId);
    if (deadline !== undefined && nowMs < deadline) {
      // A rate-limited large value holds back its writer's later writes.
      blockedWriters.add(writer);
      continue;
    }
    rt.largeValueUploadRetryDeadlines.delete(txId);
    const unit = staged ?? (await pc.node.with((n) => n.commitUnitFor(txId)));
    if (!state.largeValueUploads.has(txId))
      state.largeValueUploads.set(
        txId,
        commitUnitLargeValueRefs(unit).map((valueRef) => ({
          valueRef,
          requested: [],
          inFlight: [],
          retryNotBeforeMs: undefined,
          started: false,
        })),
      );
    const uploads = state.largeValueUploads.get(txId)!;
    const upload = uploads[0];
    if (upload) {
      const now = rt.uploadRetryClock.nowMs();
      if (upload.retryNotBeforeMs !== undefined && now < upload.retryNotBeforeMs) {
        blockedWriters.add(writer);
        continue;
      }
      upload.retryNotBeforeMs = undefined;
      const supplyingNodes = upload.started;
      let suppliedCount = 0;
      let message: SyncMessage;
      if (!upload.started) {
        message = { kind: "ChunkUploadStart", valueRef: upload.valueRef } as SyncMessage;
      } else {
        const requested = upload.requested.slice(0, 64);
        if (!requested.length) {
          blockedWriters.add(writer);
          continue;
        }
        const chunks: any[] = [];
        for (const nodeRef of requested) {
          const [replicaRole, sourceNode, encoded, failure] = await pc.node.with(async (node) => {
            const role =
              node.authoredCommitDurability() === DurabilityTier.None
                ? "non-durable-client"
                : "durable-relay";
            try {
              return [
                role,
                node.nodeUuid(),
                await node.localChunk(nodeRef.locator, nodeRef.objectHash),
                undefined,
              ];
            } catch (source) {
              return [role, node.nodeUuid(), undefined, source];
            }
          });
          if (failure)
            throw new DbError(
              ErrorCode.Internal,
              String(
                largeValueUploadChunkContext(
                  txId,
                  upload.valueRef,
                  nodeRef,
                  replicaRole,
                  sourceNode,
                ),
              ),
              failure,
            );
          chunks.push({ nodeRef, encoded: Uint8Array.from(encoded) });
        }
        suppliedCount = chunks.length;
        message = { kind: "ChunkUploadNodes", valueRef: upload.valueRef, chunks } as SyncMessage;
      }
      try {
        pc.transport.send(message);
      } catch (error) {
        // A full large-value channel holds back this commit and every later one.
        if (handleTransportBackpressure(pc.node, rt.scheduler, error)) break uploads;
        throw transportError(error);
      }
      if (supplyingNodes) upload.inFlight.push(...upload.requested.splice(0, suppliedCount));
      upload.started = true;
      rt.outbox.noteUploadProgress();
      state.awaitingLargeValueUploads.set(txId, upload.valueRef);
    }
    if (state.awaitingLargeValueUploads.has(txId) || uploads.length) {
      blockedWriters.add(writer);
      continue;
    }
    try {
      sendWithLocalSyncContext(pc.node, pc.transport, unit);
    } catch (error) {
      if (handleDbBackpressure(pc.node, rt.scheduler, error)) return true;
      throw error;
    }
    state.largeValueUploads.delete(txId);
    state.uploaded.add(txId);
    rt.outbox.markOnWire(txId);
    state.readBarrier.commitUnitSent();
  }
  if (pending.length) {
    const barrier = state.readBarrier.pendingBarrier();
    if (barrier) {
      if (sendOrStop(pc, barrier)) return true;
      state.readBarrier.barrierAccepted();
    }
  }
  const manager = state.scopeLeaseManager;
  while (pending.length) {
    const command = pending[0];
    switch (command.kind) {
      case "Subscribe": {
        const request = command.subscription;
        const { shape, binding } = request;
        const registrationKey = `${shape.shapeId()}|${dbg(request.opts.readViewKey())}`;
        if (!state.announcedShapes.has(registrationKey)) {
          state.announcedShapes.add(registrationKey);
          const register = {
            kind: "RegisterShape",
            shapeId: shape.shapeId(),
            ast: ShapeAst.fromValidated(shape),
            // Use the same read-view/compiler identity as the Subscribe below.
            opts: request.opts,
          } as SyncMessage;
          const outcome = await pc.node.with((n) => n.applySyncMessage(register));
          const [, changed] = await finishPeerPublicationOutcome(
            pc.node,
            rt,
            progressWaker,
            outcome,
          );
          stats.subscriptionEvents += changed;
          try {
            pc.transport.send(register);
          } catch (error) {
            state.announcedShapes.delete(registrationKey);
            if (handleTransportBackpressure(pc.node, rt.scheduler, error)) return true;
            throw transportError(error);
          }
        }
        const values = bindingValuesInParamOrder(shape, binding);
        const knownState = await pc.node.with((n) =>
          n.knownStateDeclarationForSubscription(
            shape,
            binding,
            request.subscription,
            values,
            request.identity,
            permitsDelegatedSessions ? request.policyBinding : undefined,
          ),
        );
        const delegated = permitsDelegatedSessions ? request.policyBinding : undefined;
        const subscribe = {
          shapeId: shape.shapeId(),
          subscription: request.subscription,
          values,
          knownState,
          delegatedSession: delegated && { identity: delegated[0], claims: delegated[1] },
        };
        const outcome = await pc.node.with((n) =>
          n.applySyncMessage({ kind: "Subscribe", subscribe } as SyncMessage),
        );
        const [, changed] = await finishPeerPublicationOutcome(pc.node, rt, progressWaker, outcome);
        stats.subscriptionEvents += changed;
        if (sendOrStop(pc, { kind: "Subscribe", subscribe } as SyncMessage)) return true;
        state.sentSubscriptions.set(request.subscription, request);
        break;
      }
      case "Unsubscribe": {
        const subscription: SubscriptionKey = command.subscription;
        state.sentSubscriptions.delete(subscription);
        state.awaitingSupportSnapshots.delete(subscription);
        rt.activeAuthorityViewReceipts?.subscriptions.delete(subscription);
        state.announcedShapes.delete(`${subscription.shapeId}|${dbg(subscription.readView)}`);
        // Reapplying a local retirement is idempotent.
        pc.node.borrow().applyUnsubscribe(subscription);
        if (sendOrStop(pc, { kind: "Unsubscribe", subscription } as SyncMessage)) return true;
        break;
      }
      case "AuthorizationScopeIntent": {
        const requestId: string = command.requestId;
        // An old or unauthenticated upstream must never receive a downgraded preflight.
        if (!state.expectedScopeAuthority) {
          const request = manager.requests.get(requestId);
          manager.requests.delete(requestId);
          if (request)
            for (const waiterId of request.waiters) rt.permissionAdviceWaiters.delete(waiterId);
          else rt.permissionAdviceWaiters.delete(requestId);
          break;
        }
        const existingRequest = manager.requests.get(requestId);
        const hasLiveWaiter = existingRequest
          ? [...existingRequest.waiters].some((id: string) => rt.permissionAdviceWaiters.has(id))
          : rt.permissionAdviceWaiters.has(requestId);
        if (!hasLiveWaiter) {
          manager.requests.delete(requestId);
          pending.shift();
          continue;
        }
        const expected = state.expectedScopeAuthority;
        const binding = command.sessionClaimBinding ??
          existingRequest?.sessionClaimBinding ?? [expected.link, claimsFor(pc, expected.link)];
        // Allocation establishes the immutable request binding before the first send.
        command.sessionClaimBinding = binding;
        const claimsStillBound = dbg(claimsFor(pc, binding[0])) === dbg(binding[1]);
        if (!command.delegatedSession && !claimsStillBound) {
          const request = manager.requests.get(requestId);
          manager.requests.delete(requestId);
          resolveWaiters(pc, request ? request.waiters : [requestId], "Unknown");
          pending.shift();
          continue;
        }
        let existing: [string, any] | undefined;
        for (const [wireId, request] of manager.requests)
          if (
            dbg(request.action) === dbg(command.action) &&
            dbg(request.sessionClaimBinding) === dbg(binding) &&
            dbg(request.delegatedSession) === dbg(command.delegatedSession)
          ) {
            existing = [wireId, request];
            break;
          }
        if (existing) {
          const [wireId, request] = existing;
          request.waiters.add(requestId);
          if (!request.intentSent) {
            const intent = {
              kind: "AuthorizationScopeIntent",
              requestId: wireId,
              action: request.action,
              delegatedSession: command.delegatedSession,
            } as SyncMessage;
            if (sendOrStop(pc, intent)) return true;
            request.intentSent = true;
          }
        } else {
          manager.requests.set(requestId, {
            action: command.action,
            sessionClaimBinding: binding,
            delegatedSession: command.delegatedSession,
            waiters: new Set([requestId]),
            intentSent: false,
            key: undefined,
            lease: undefined,
            owner: undefined,
            clauseCount: undefined,
            appliedClauses: new Map(),
          });
          const intent = {
            kind: "AuthorizationScopeIntent",
            requestId,
            action: command.action,
            delegatedSession: command.delegatedSession,
          } as SyncMessage;
          if (sendOrStop(pc, intent)) return true;
          manager.requests.get(requestId).intentSent = true;
        }
        break;
      }
    }
    pending.shift();
  }
  return false;
}

/** Upstream link turn: send outbound work, then drain and apply inbound frames. */
export async function tickUpstream(
  pc: PeerConnection,
  state: UpstreamConnectionState,
  stats: Stats,
  progressWaker: Waker | undefined,
  permitsDelegatedSessions: boolean,
): Promise<boolean> {
  const rt = pc.rt;
  if (await upstreamOutbound(pc, state, stats, progressWaker, permitsDelegatedSessions))
    return true;
  const manager = state.scopeLeaseManager;
  let applied = false;
  const publications: any[] = [];
  const batch: AuthorityViewBatch = {
    pendingViewUpdates: [],
    pendingInitialCoverageClears: new Set(),
  };
  const flushViews = async () => {
    if (batch.pendingViewUpdates.length)
      await applyPendingAuthorityViewUpdates(pc, batch, progressWaker);
  };
  const receivedLeases: any[] = [];
  let deferredStop = false;
  inbound: for (;;) {
    let next = pc.stagedInbound.shift();
    if (!next) {
      let message: any;
      try {
        message = pc.transport.tryRecvOwnedResult();
      } catch (error) {
        if (handleTransportBackpressure(pc.node, rt.scheduler, error)) {
          deferredStop = true;
          break inbound;
        }
        throw transportError(error);
      }
      if (!message) {
        pc.inboundAuthorityReceiptQuarantine = false;
        break inbound;
      }
      next = {
        message: message.message,
        lease: message.lease,
        authorityReceiptEligible: !pc.inboundAuthorityReceiptQuarantine,
        receiptsValidated: message.receiptsValidated,
      };
    }
    const { message, lease, authorityReceiptEligible, receiptsValidated } = next;
    if (lease) receivedLeases.push(lease);
    const writeStateTxId = writeStateUpdateTxId(message);
    switch (message.kind) {
      case "ChunkResponseBatch":
        for (const response of message.batch.responses)
          pc.auxiliaryPump.resolver.complete(response);
        continue;
      case "ChunkRequestBatch":
        dropPeerRequest(pc.node);
        continue;
      case "ChunkUploadResult": {
        const result = message.result;
        const rateLimited = result.status.kind === "RateLimited";
        let pendingTx: TxId | undefined;
        for (const [txId, valueRef] of state.awaitingLargeValueUploads)
          if (dbg(valueRef) === dbg(result.valueRef)) {
            pendingTx = txId;
            break;
          }
        if (pendingTx !== undefined) {
          const txId = pendingTx;
          const upload = state.largeValueUploads.get(txId)?.[0];
          switch (result.status.kind) {
            case "Need":
              rt.outbox.noteUploadProgress();
              state.awaitingLargeValueUploads.delete(txId);
              if (upload) {
                upload.inFlight = [];
                upload.requested.push(...result.status.nodes);
              }
              break;
            case "Staged":
              rt.outbox.noteUploadProgress();
              state.awaitingLargeValueUploads.delete(txId);
              if (upload) upload.inFlight = [];
              state.largeValueUploads.get(txId)?.shift();
              break;
            case "RateLimited":
              state.awaitingLargeValueUploads.delete(txId);
              if (upload) {
                upload.requested.unshift(...upload.inFlight.splice(0));
                const nowMs = rt.uploadRetryClock.nowMs();
                upload.retryNotBeforeMs = nowMs + RATE_LIMITED_UPLOAD_RETRY_DELAY_MS;
                rt.largeValueUploadRetryDeadlines.set(
                  txId,
                  nowMs + RATE_LIMITED_UPLOAD_RETRY_DELAY_MS,
                );
                rt.scheduler.current?.scheduleTickAfter(RATE_LIMITED_UPLOAD_RETRY_DELAY_MS);
              }
              break;
            case "Rejected":
              state.awaitingLargeValueUploads.delete(txId);
              state.failedLargeValueUploads.add(txId);
              state.largeValueUploads.delete(txId);
              rt.largeValueUploadRetryDeadlines.delete(txId);
              rt.outbox.retain((p: any) => p.txId !== txId);
              rt.outbox.markUploadFailed(txId, "its large value was not staged by the server");
              pc.stagedInbound.unshift({
                message: {
                  kind: "FateUpdate",
                  txId,
                  fate: {
                    kind: "Rejected",
                    reason: {
                      kind: "MalformedCommit",
                      detail: "large-value upload was not staged; upload again",
                    },
                  },
                  globalTime: undefined,
                  durability: undefined,
                },
                authorityReceiptEligible: false,
                lease: undefined,
                receiptsValidated: false,
              });
              break;
          }
        }
        if (!rateLimited) scheduleTickIn(rt.scheduler, TickUrgency.Immediate);
        continue;
      }
      case "CatalogueSnapshot": {
        await flushViews();
        const owner = await pc.node.lock();
        let outcome: any;
        try {
          if (!owner.deferCatalogueForPersistence(progressWaker))
            outcome = await owner.applyTrustedCatalogueSnapshot(message.snapshot);
        } finally {
          pc.node.unlock();
        }
        if (!outcome) {
          pc.stagedInbound.unshift({ message, lease, authorityReceiptEligible, receiptsValidated });
          break inbound;
        }
        const requested = rt.openSchemaAdmission.current?.schema;
        if (requested !== undefined) {
          const admitted = await pc.node.with((n) => n.catalogueSchemas().has(requested));
          const admission = rt.openSchemaAdmission.current;
          if (admission && admission.connectionEpoch === pc.connectionEpoch)
            admission.authoritativeCatalogueReceived = true;
          finishOpenSchemaConnection(
            rt.openSchemaAdmission,
            pc.connectionEpoch,
            admitted ? undefined : pendingOpenSchemaError(),
          );
        }
        publications.push(...outcome.publications);
        break;
      }
      case "CurrentRowsReceipt":
        await flushViews();
        await receiveCurrentRows(
          pc.node,
          rt.currentRows,
          rt.admittedUpstreamAuthority,
          state.expectedScopeAuthority,
          authorityReceiptEligible,
          message.receipt,
        );
        scheduleTickIn(rt.scheduler, TickUrgency.Immediate);
        applied = true;
        continue;
      case "ViewUpdate": {
        const view = message.view;
        const subscription: SubscriptionKey = view.subscription;
        const settledThrough = view.settledThrough;
        const minimumCut = state.awaitingSupportSnapshots.get(subscription);
        if (minimumCut !== undefined) {
          let cut = minimumCut;
          if (!authorityReceiptEligible && settledThrough > cut) {
            cut = settledThrough;
            state.awaitingSupportSnapshots.set(subscription, cut);
          }
          if (
            !authorityReceiptEligible ||
            !(view.supportingRows.isSnapshot() && !view.peerPayloadInventory.openingPending) ||
            settledThrough < cut
          ) {
            await pc.node.with((n) =>
              n.rememberDiscardedPendingViewTransactions(view.versionCarriers),
            );
            continue;
          }
          state.awaitingSupportSnapshots.delete(subscription);
        }
        const missing = await pc.node.with((n) => n.missingKnownStateRowVersionRefs(message));
        // A catch-up is a delta against its declared revision; re-ask when stale.
        if (view.supportingRows.kind === "CatchUp") {
          const stale =
            batch.pendingViewUpdates.some(
              (p) =>
                subscriptionKeyString(p.parts.subscription) === subscriptionKeyString(subscription),
            ) ||
            !(await pc.node.with((n) =>
              n.holdsSupportingRevision(subscription, view.supportingRows.predecessor),
            ));
          if (stale) {
            const request = state.sentSubscriptions.get(subscription);
            // A late catch-up for a view this link no longer serves has nothing to re-request.
            if (!request) {
              await pc.node.with((n) =>
                n.rememberDiscardedPendingViewTransactions(view.versionCarriers),
              );
              continue;
            }
            await pc.node.with((n) => n.forgetSupportingRevision(subscription));
            state.awaitingSupportSnapshots.set(subscription, settledThrough);
            state.pending.push({ kind: "Subscribe", subscription: request });
            await pc.node.with((n) =>
              n.rememberDiscardedPendingViewTransactions(view.versionCarriers),
            );
            scheduleTickIn(rt.scheduler, TickUrgency.Immediate);
            continue;
          }
        }
        if (!missing.length) {
          state.missingBodyResends.delete(subscription);
          stageInitialCoverageClearForUpdate(
            message,
            rt.latestCoverageSubscriptions,
            batch.pendingInitialCoverageClears,
          );
          pushViewUpdateMessageForReceiver(
            batch.pendingViewUpdates,
            message,
            authorityReceiptEligible,
          );
        } else {
          // A held row whose body is gone reopens the view without known state.
          await pc.node.with((n) =>
            n.rememberDiscardedPendingViewTransactions(view.versionCarriers),
          );
          const request = state.sentSubscriptions.get(subscription);
          if (!request) continue;
          if (state.missingBodyResends.has(subscription))
            throw new DbError(
              ErrorCode.Protocol,
              "a full resend omitted rows this receiver does not hold",
            );
          state.missingBodyResends.set(subscription, true);
          await pc.node.with((n) => n.forgetDeclaredKnownState(subscription));
          state.awaitingSupportSnapshots.set(subscription, settledThrough);
          state.pending.push({ kind: "Subscribe", subscription: request });
          scheduleTickIn(rt.scheduler, TickUrgency.Immediate);
          continue;
        }
        break;
      }
      case "SubscribeRejected": {
        const { subscription, reason } = message;
        if (
          !authorityReceiptEligible ||
          (rt.activeAuthorityViewReceipts &&
            rt.activeAuthorityViewReceipts.connectionEpoch !== pc.connectionEpoch)
        )
          continue;
        state.sentSubscriptions.delete(subscription);
        state.awaitingSupportSnapshots.delete(subscription);
        const delivered = queueRelaySubscriptionRejection(rt, subscription, reason);
        if (delivered > 0) {
          rt.subscriberDirtyEpoch++;
          scheduleTickIn(rt.scheduler, TickUrgency.Immediate);
        } else {
          stats.subscriptionEvents += routeUpstreamSubscriptionRejection(
            rt.subscriptions,
            rt.upstreamSubscriptionOwners,
            subscription,
            reason,
          );
        }
        break;
      }
      case "AuthorizationScopeView": {
        const { requestId, key, clauseIndex, clauseCount, view } = message;
        if (!state.expectedScopeAuthority || clauseCount === 0 || clauseIndex >= clauseCount) {
          dropPeerRequest(pc.node);
          continue;
        }
        const subscription = view.subscription;
        const settledThrough = view.settledThrough;
        const authorizationProgress = view.peerPayloadInventory.authorizationProgress ?? 0;
        const prior = manager.requests.get(requestId);
        // A cancelled intent cannot be revived by a late/replayed authority view.
        if (!prior) continue;
        const binding = prior.sessionClaimBinding;
        const keyMismatch =
          (prior.key !== undefined && dbg(prior.key) !== dbg(key)) ||
          (prior.clauseCount !== undefined && prior.clauseCount !== clauseCount);
        const needsAcquire = prior.lease === undefined;
        // The authority-selected key must name the exact immutable binding.
        if (key.subject !== binding[0]) {
          dropPeerRequest(pc.node);
          continue;
        }
        const claimsStillBound = dbg(claimsFor(pc, binding[0])) === dbg(binding[1]);
        if (!prior.delegatedSession && !claimsStillBound) {
          manager.requests.delete(requestId);
          resolveWaiters(pc, prior.waiters, "Unknown");
          continue;
        }
        if (keyMismatch) {
          dropPeerRequest(pc.node);
          continue;
        }
        // Acquire before the aggregate receipt so equal scopes share one lifecycle.
        const acquired = needsAcquire ? manager.registry.acquire(key) : undefined;
        const request = manager.requests.get(requestId);
        if (!request) continue;
        request.key = key;
        request.clauseCount = clauseCount;
        if (request.lease === undefined) {
          if (!acquired) {
            rt.permissionAdviceWaiters.delete(requestId);
            manager.requests.delete(requestId);
            continue;
          }
          const [leaseHandle, acquisition] = acquired;
          request.owner =
            acquisition.kind === AuthorizationScopeAcquisition.Owner
              ? acquisition.owner
              : undefined;
          request.lease = leaseHandle;
        }
        const priorClause = request.appliedClauses.get(clauseIndex);
        if (
          priorClause &&
          subscriptionKeyString(priorClause[0]) !== subscriptionKeyString(subscription)
        ) {
          dropPeerRequest(pc.node);
          continue;
        }
        // The envelope remains a normal ViewUpdate.
        pushViewUpdateMessageForReceiver(
          batch.pendingViewUpdates,
          view.intoViewUpdate(),
          authorityReceiptEligible,
        );
        request.appliedClauses.set(clauseIndex, [
          subscription,
          settledThrough,
          authorizationProgress,
        ]);
        break;
      }
      case "AuthorizationScopeAggregateReceipt": {
        const { requestId, receipt } = message;
        const current = manager.requests.get(requestId);
        // A B-scoped proof must never materialize support for an A request.
        const claimsStillBound =
          !current ||
          current.delegatedSession !== undefined ||
          dbg(claimsFor(pc, current.sessionClaimBinding[0])) ===
            dbg(current.sessionClaimBinding[1]);
        if ((!current || current.delegatedSession === undefined) && !claimsStillBound) {
          if (current) {
            manager.requests.delete(requestId);
            resolveWaiters(pc, current.waiters, "Unknown");
          }
          continue;
        }
        // Apply queued views now so receipt admission is never merely queued.
        await flushViews();
        const expected = state.expectedScopeAuthority;
        if (!expected) {
          dropPeerRequest(pc.node);
          continue;
        }
        const request = manager.requests.get(requestId);
        if (!request) continue;
        // An empty compiled support set is a valid public policy proof.
        const clauseCount = request.clauseCount ?? 0;
        const clauses = [...request.appliedClauses] as [number, [SubscriptionKey, any, number]][];
        const allCurrent =
          clauses.length === clauseCount &&
          clauses.every(([index, [, cut]]) => index < clauseCount && cut >= receipt.settledThrough);
        const appliedCut =
          clauseCount === 0
            ? receipt.settledThrough
            : clauses.length
              ? clauses.map(([, [, cut]]) => cut).reduce((a, b) => (a < b ? a : b))
              : undefined;
        const observed = pc.node.borrow();
        // Delegated subjects have independent claim counters.
        const observedClaims = request.delegatedSession
          ? 0
          : observed.sessionClaimRevision(expected.link);
        const observedPolicy = observed.activeCatalogueSeq();
        // Context components are monotonic per admitted connection.
        const appliedProgress = clauses.length ? Math.min(...clauses.map(([, [, , p]]) => p)) : 0;
        const receiptDecreased =
          receipt.claimsRevision < expected.claimsRevision ||
          receipt.policyEpoch < expected.policyEpoch ||
          receipt.authorizationProgress < expected.authorizationProgress ||
          receipt.settledThrough[0] < expected.settledThrough;
        if (observedClaims > expected.claimsRevision) expected.claimsRevision = observedClaims;
        else if (observedClaims === 0) expected.claimsRevision = receipt.claimsRevision;
        if (observedPolicy > expected.policyEpoch) expected.policyEpoch = observedPolicy;
        else if (observedPolicy === 0) expected.policyEpoch = receipt.policyEpoch;
        expected.authorizationProgress = Math.max(expected.authorizationProgress, appliedProgress);
        expected.settledThrough = Math.max(expected.settledThrough, receipt.settledThrough[0]);
        const receiptCurrent =
          request.key !== undefined &&
          dbg(request.key) === dbg(receipt.key) &&
          allCurrent &&
          !receiptDecreased &&
          authorizationScopeReceiptMatchesTransportContext(
            receipt,
            expected,
            request.sessionClaimBinding[0],
            appliedCut,
          );
        if (!receiptCurrent) {
          const stillBound =
            dbg(claimsFor(pc, request.sessionClaimBinding[0])) ===
            dbg(request.sessionClaimBinding[1]);
          if (!request.delegatedSession && !stillBound) {
            manager.requests.delete(requestId);
            resolveWaiters(pc, request.waiters, "Unknown");
            continue;
          }
          // Retire the lease and allocate a new opaque wire id; keep the caller waiters.
          const retryId = uuidToString(uuidV4());
          manager.requests.delete(requestId);
          manager.requests.set(retryId, {
            action: request.action,
            sessionClaimBinding: request.sessionClaimBinding,
            delegatedSession: request.delegatedSession,
            waiters: new Set(request.waiters),
            intentSent: false,
            key: undefined,
            lease: undefined,
            owner: undefined,
            clauseCount: undefined,
            appliedClauses: new Map(),
          });
          state.pending.push({
            kind: "AuthorizationScopeIntent",
            requestId: retryId,
            action: request.action,
            sessionClaimBinding: request.sessionClaimBinding,
            delegatedSession: request.delegatedSession,
          });
          dropPeerRequest(pc.node);
          continue;
        }
        // The support lease uses the current context under the immutable delegated subject.
        const scopeAuthority = { ...expected, link: request.sessionClaimBinding[0] };
        const owner = request.owner;
        request.owner = undefined;
        let admitted = false;
        if (request.lease !== undefined && owner !== undefined)
          admitted =
            manager.registry.install(request.lease, owner, scopeAuthority, receipt) ===
            AuthorizationScopeInstall.Installed;
        else if (request.lease !== undefined)
          admitted =
            manager.registry.receipt(
              request.lease,
              scopeAuthority,
              receipt.authorizationProgress,
              receipt.settledThrough[0],
            ).kind === AuthorizationScopeReadiness.Proven;
        if (!admitted) continue;
        manager.requests.delete(requestId);
        const advice = await pc.node.with((n) =>
          evaluateAuthoritativePermissionAdvice(
            n.scopedActiveSessionClaims(
              request.sessionClaimBinding[0],
              request.sessionClaimBinding[1],
            ),
            receipt.key.subject,
            request.action,
          ),
        );
        resolveWaiters(pc, request.waiters, advice);
        break;
      }
      case "AuthorizationScopeUnavailable": {
        const request = manager.requests.get(message.requestId);
        manager.requests.delete(message.requestId);
        if (request) resolveWaiters(pc, request.waiters, "Unknown");
        break;
      }
      case "AuthorizationScopeDecision": {
        if (!state.expectedScopeAuthority) {
          dropPeerRequest(pc.node);
          continue;
        }
        const request = manager.requests.get(message.requestId);
        manager.requests.delete(message.requestId);
        if (request) resolveWaiters(pc, request.waiters, message.advice);
        break;
      }
      default: {
        const releaseEligible = uploadAnswerReceiptEligible(
          authorityReceiptEligible,
          state.expectedScopeAuthority,
          rt.admittedUpstreamAuthority,
        );
        const routedFate = message.kind === "FateUpdate" ? message : undefined;
        const releasedTxId =
          message.kind === "FateUpdate" &&
          releaseEligible &&
          fateUpdateSettlesUpload(message, message.txId)
            ? message.txId
            : undefined;
        await flushViews();
        const owner = await pc.node.lock();
        let deferred = false;
        try {
          deferred =
            isCatalogueMutation(message) && owner.deferCatalogueForPersistence(progressWaker);
          if (deferred)
            pc.stagedInbound.unshift({
              message,
              lease,
              authorityReceiptEligible,
              receiptsValidated,
            });
          else if (state.localReceiver && message.kind === "CommitUnit")
            await owner.ingestRelayCommitUnitWithEncoderTrust(message.tx, message.versions, true);
          else
            publications.push(
              ...(await owner.applySyncMessageWithIngestContext(message, undefined)).publications,
            );
        } finally {
          pc.node.unlock();
        }
        if (deferred) break inbound;
        if (routedFate) routeLocalFate(rt.localFateRoutes, routedFate.txId, routedFate);
        if (releasedTxId !== undefined) pc.releasedOutboxTxIds.push(releasedTxId);
      }
    }
    if (writeStateTxId !== undefined) handleWriteStateUpdate(pc.node, rt, writeStateTxId);
    applied = true;
  }
  await flushViews();
  if (applied) {
    const persisted: [TxId, any][] = [];
    for (const publication of publications)
      persisted.push([publication.txId(), await publication.persist()]);
    await pc.node.with((node) => {
      for (const [txId, persistence] of persisted)
        node.settlePublishedTransaction(txId, persistence);
    });
    // A refresh failure belongs to the resident subscriptions, not this tick's receipt.
    try {
      stats.subscriptionEvents += await refreshSubscriptionsIn(pc.node, rt, progressWaker);
    } catch (error) {
      stats.subscriptionEvents += routeSubscriptionRefreshFailure(rt.subscriptions, error);
    }
    stats.remoteSyncApplied++;
    rt.subscriberDirtyEpoch++;
  }
  void receivedLeases;
  return deferredStop;
}

// Node-owned orchestration for upstream and subscriber connections.
import {
  cmpNum,
  cmpStr,
  cmpTuple,
  KeyedMap,
  SortedMap,
  SortedSet,
} from "../../support/collections.js";
import { uuidToString, uuidV4, uuidV5 } from "../../support/hash.js";
import { deferred, monotonicMs } from "../../support/runtime.js";
import {
  ConnectionLink,
  coverageGroupSubscriptionKey,
  mergeReconnectableLargeValueUploads,
  PeerConnection,
  takeReconnectableLargeValueUploads,
} from "./peer_connection.js";
import { scheduleTickIn } from "./peer_connection_support.js";
import {
  BindingId,
  CanonicalPolicyClaims,
  CommitUnitTrust,
  CurrentRowOutcome,
  DbError,
  dbg,
  DurabilityTier,
  ErrorCode,
  FEATURE_AUTHORIZATION_SCOPE_VIEWS,
  Fate,
  LocalAvailabilityWatermark,
  LocalRowAvailability,
  MAX_CURRENT_ROWS,
  MonotonicUploadRetryClock,
  MutationErrorWait,
  PeerChunkResolver,
  PeerIoPump,
  PeerIoPumpRole,
  PeerState,
  PolicyBindingKey,
  QUERY_NAMESPACE,
  QueryAuthorizationMode,
  ReadViewSpec,
  RegisterShapeOptions,
  RelationSnapshotIndex,
  RemoteLinkTracker,
  ReceivedSyncMessage,
  SubscriptionSnapshotSource,
  TickUrgency,
  TxKind,
  TxTime,
  UploadOutbox,
  CurrentRowsRouter,
  HeldRowChecks,
  WireFlushStatus,
  apply_maintained_update_to_snapshot,
  apply_terminal_operations_to_subscription_snapshot,
  beginOpenSchemaConnection,
  coveredInputTrace,
  demoteAuthorityReceiptSubscriptions,
  finishOpenSchemaAdmission,
  finishOpenSchemaConnection,
  maintainedSnapshotIndexOrRowIndex,
  materializeSubscriptionTerminalRecords,
  materializedSubscriptionSnapshot,
  mutationErrorEvent,
  queueMutationError,
  queuedMutationErrorEvent,
  queuePendingUploadIn,
  relationSnapshotIndexWithRootOccurrences,
  relationSnapshotWithDeltaSlack,
  requestCoverageKey,
  resetRemovedRoots,
  restoreLocalSubscriberReplay,
  simpleScalarExitQuery,
  snapshotsEqual,
  subscriptionDeltaEvent,
  subscriptionDeltaEventWithReset,
  subscriptionIsSettled,
  subscriptionOutputsWithOccurrenceSidecar,
  takePendingMutationErrorDelivery,
  transactionAbandoned,
  transactionSatisfiesWait,
  upstreamRegisterShapeOptions,
  writeRejected,
  DELETION_DISCOVERY_LIMIT_MS,
  type AuthorSubject,
  type AuthorityContext,
  type CoverageKey,
  type NodeState,
  type SubscriptionKey,
  type SyncMessage,
  type TickScheduler,
  type TransportError,
  type TxId,
  type Value,
  type WireAuthorityEndpoint,
  type WireError,
  type WireInboundContext,
} from "../../_extern/db-runtime.js";

// ---------------------------------------------------------------------------
// Async plumbing: futures::lock::Mutex and poll-once futures.

/** futures::lock::Mutex stand-in: FIFO async ownership of one value. */
export class LocalMutex<T> {
  private held = false;
  private readonly waiters: (() => void)[] = [];
  constructor(readonly value: T) {}
  lock(): Promise<T> {
    if (!this.held) {
      this.held = true;
      return Promise.resolve(this.value);
    }
    return new Promise((resolve) =>
      this.waiters.push(() => {
        this.held = true;
        resolve(this.value);
      }),
    );
  }
  tryLock(): T | undefined {
    if (this.held) return undefined;
    this.held = true;
    return this.value;
  }
  unlock(): void {
    this.held = false;
    this.waiters.shift()?.();
  }
  isLocked(): boolean {
    return this.held;
  }
  /** Synchronous borrow of an uncontended owner. */
  borrow(): T {
    if (this.held) throw new Error("synchronous node operation reentered a suspended operation");
    return this.value;
  }
  async with<R>(f: (value: T) => R | Promise<R>): Promise<R> {
    const value = await this.lock();
    try {
      return await f(value);
    } finally {
      this.unlock();
    }
  }
}

export type SharedNodeState = LocalMutex<NodeState>;
export type Waker = () => void;
export type PollResult<T> = { ok: true; value: T } | { ok: false; error: unknown };

/** A lazily started future polled by its owner, like a pinned Rust future. */
export class Polled<T> {
  private result: PollResult<T> | undefined;
  private started = false;
  private waker: Waker | undefined;
  constructor(private readonly make: () => Promise<T>) {}
  poll(waker?: Waker): PollResult<T> | undefined {
    this.waker = waker;
    if (!this.started) {
      this.started = true;
      this.make().then(
        (value) => this.finish({ ok: true, value }),
        (error) => this.finish({ ok: false, error }),
      );
    }
    return this.result;
  }
  private finish(result: PollResult<T>): void {
    this.result = result;
    this.waker?.();
  }
  async wait(): Promise<T> {
    for (;;) {
      const d = deferred<void>();
      const r = this.poll(() => d.resolve());
      if (r) {
        if (r.ok) return r.value;
        throw r.error;
      }
      await d.promise;
    }
  }
}

export const cmpSubscriptionKey = (a: SubscriptionKey, b: SubscriptionKey) =>
  cmpStr(subscriptionKeyString(a), subscriptionKeyString(b));
export const subscriptionKeyString = (k: SubscriptionKey): string =>
  `${k.shapeId}|${k.bindingId}|${k.readView}`;
const cmpTxId = (a: TxId, b: TxId) => cmpStr(String(a), String(b));
const refreshVisits = { count: 0 };

/** Wake observers after any transaction-state transition. */
export function notifyWriteStateWaiters(waiters: SortedMap<TxId, any[]>, txId: TxId): void {
  const list = waiters.delete(txId);
  if (!list) return;
  for (const waiter of list) waiter.notify();
}

/** Retain a FIFO owner operation while `Db::close` polls it. */
class QueuedMutationLease {
  private operation: any | undefined;
  constructor(
    private readonly node: Node,
    operation: any,
  ) {
    node.queuedMutationActiveLeases++;
    this.operation = operation;
  }
  get op(): any {
    if (!this.operation) throw new Error("queued mutation lease must retain its operation");
    return this.operation;
  }
  take(): any {
    const op = this.op;
    this.operation = undefined;
    return op;
  }
  release(): void {
    if (this.operation) this.node.queuedMutations.unshift(this.operation);
    this.operation = undefined;
    if (this.node.queuedMutationActiveLeases === 0)
      throw new Error("queued mutation lease count underflow");
    this.node.queuedMutationActiveLeases--;
  }
}

export type SchedulerSlot = { current: TickScheduler | undefined };

export function makeQueryRuntimeWaker(
  scheduler: SchedulerSlot,
  pending: { value: boolean },
  cache: { waker: Waker | undefined },
): Waker | undefined {
  if (cache.waker) return cache.waker;
  const schedulerWaker = scheduler.current?.queryRuntimeWaker?.();
  if (!schedulerWaker) return undefined;
  const waker = () => {
    pending.value = true;
    schedulerWaker();
  };
  cache.waker = waker;
  return waker;
}

type MutationOwnerLifecycle = "Open" | "Closing";

// ---------------------------------------------------------------------------

/** Node-owned participant surface for upstream and subscriber connections. */
export class Node {
  readonly node: SharedNodeState;
  private ownerReleaseWait: Polled<void> | undefined;
  private mutationOwnerLifecycle: MutationOwnerLifecycle = "Open";
  private readonly closeOwner = new LocalMutex<null>(null);
  private readonly txTimeReservationClock: { value: any };
  private readonly nodeUuid: any;
  private readonly receivesCommitsAsLocalFlag: boolean;
  subscriptions: WeakRef<any>[] = [];
  readonly outbox = new UploadOutbox();
  readonly pendingLocalPublications: any[] = [];
  queuedMutations: any[] = [];
  queuedMutationActiveLeases = 0;
  private transactionWaitObservers: Polled<void>[] = [];
  private readonly queuedMutationFailures = new SortedMap<TxId, DbError>(cmpTxId);
  private deferredRejectionDiscards = new SortedSet<TxId>(cmpTxId);
  private readonly queuedOpenTransactionFailures = new Map<number, DbError>();
  private readonly reservedMutations = new SortedSet<TxId>(cmpTxId);
  pendingTransactionAbandonments = new SortedSet<number>(cmpNum);
  private transactionAbandonmentsClosed = false;
  private transactionAbandonmentShutdownPending = false;
  upstreamSubscriptions: any[] = [];
  pendingSubscriptionFinalizations: any[] = [];
  private subscriptionFinalizationsClosed = false;
  private subscriptionRuntimeRetired = false;
  latestCoverageSubscriptions = new Map<string, SubscriptionKey>();
  readonly upstreamCoverageRefcounts = new Map<string, number>();
  readonly awaitingInitialAuthorityCoverage = new Set<string>();
  activeAuthorityViewReceipts: any | undefined;
  readonly coverageRefreshGenerations = new Map<string, number>();
  readonly queryCoverageRegistrations = new SortedMap<string, any>(cmpStr);
  readonly upstreamSubscriptionOwners = new SortedMap<SubscriptionKey, WeakRef<any>[]>(
    cmpSubscriptionKey,
  );
  readonly relayUpstreamSubscriptionOwners = new SortedMap<[SubscriptionKey, number, any], any>(
    cmpTuple(cmpSubscriptionKey, cmpNum, (a, b) => cmpStr(String(a), String(b))),
  );
  readonly pendingRelaySubscriptionRejections = new Map<number, any[]>();
  connections: LocalMutex<PeerConnection>[] = [];
  private pendingDetaches: LocalMutex<PeerConnection>[] = [];
  readonly scheduler: SchedulerSlot = { current: undefined };
  readonly remoteLink: any;
  readonly queryRuntimeWakePending = { value: false };
  readonly queryRuntimeWakerCache: { waker: Waker | undefined } = { waker: undefined };
  uploadRetryClock: any = new MonotonicUploadRetryClock();
  readonly detachedLargeValueUploads = new Map<string, Map<TxId, any[]>>();
  readonly largeValueUploadRetryDeadlines = new SortedMap<TxId, number>(cmpTxId);
  readonly writeStateWaiters = new SortedMap<TxId, any[]>(cmpTxId);
  readonly openSchemaAdmission: { current: any } = { current: undefined };
  readonly permissionAdviceWaiters = new Map<string, (advice: any) => void>();
  readonly currentRows = new CurrentRowsRouter();
  private readonly heldRowChecks = new HeldRowChecks();
  readonly localFateRoutes = new SortedMap<TxId, any>(cmpTxId);
  admittedUpstreamAuthorities: AuthorityContext[] = [];
  admittedUpstreamAuthority: AuthorityContext | undefined;
  readonly mutationErrors: any;
  readonly browserRelayRecoveredTxIds = new SortedSet<TxId>(cmpTxId);
  nextWriteStateWaiterId = 1;
  nextSubscriptionNonce = 1n;
  subscriberDirtyEpoch = 0;
  upstreamDurabilityFloor = DurabilityTier.Global;
  deferLocalPersistence = false;
  readonly chunkResolver = new PeerChunkResolver();
  private readonly detachCoveredChunkWaits: { value: boolean };
  readonly localChunkReader: any;
  observedChunkCompletionGeneration = 0;
  private localSubscriptionDirtyGeneration = 0;
  private observedLocalSubscriptionDirtyGeneration = 0;

  /** Wrap a node for serving subscriber links. */
  constructor(node: NodeState) {
    // History completeness is a structural property of the opened node.
    this.receivesCommitsAsLocalFlag = !node.isHistoryComplete();
    this.localChunkReader = node.localChunkReaderHandle();
    this.txTimeReservationClock = node.txTimeReservationClock();
    this.nodeUuid = node.nodeUuid();
    node.setMissingChunkResolver(this.chunkResolver);
    this.detachCoveredChunkWaits = node.detachCoveredChunkWaitsHandle();
    const pending = new SortedMap<TxId, any>(cmpTxId);
    for (const txId of node.rejectedTransactions()) {
      const rejected = node.rejectedTransaction(txId);
      if (rejected) pending.set(txId, mutationErrorEvent(rejected));
    }
    this.node = new LocalMutex(node);
    this.remoteLink = new RemoteLinkTracker(this.scheduler);
    this.mutationErrors = {
      callback: undefined,
      pending,
      applicationWaiters: new SortedMap(cmpTxId),
    };
  }

  /** Reserve a definitive local transaction identity without borrowing the node. */
  reserveTransactionId(nowMs: number): TxId {
    this.ensureMutationAdmissionOpen();
    const madeAt = TxTime.tick(this.txTimeReservationClock.value, nowMs);
    this.txTimeReservationClock.value = madeAt;
    return { madeAt, node: this.nodeUuid } as TxId;
  }

  beginMutationShutdown(): void {
    finishOpenSchemaAdmission(
      this.openSchemaAdmission,
      new DbError(ErrorCode.Protocol, "database closed before schema admission"),
    );
    this.mutationOwnerLifecycle = "Closing";
    // Closing is itself a terminal observation boundary: wake every waiter.
    const waiters = [...this.writeStateWaiters];
    this.writeStateWaiters.clear();
    for (const [, list] of waiters) for (const waiter of list) waiter.notify();
  }

  /** Serialize shutdown drains while keeping admission closure outside the await. */
  async lockCloseOwner(): Promise<() => void> {
    await this.closeOwner.lock();
    return () => this.closeOwner.unlock();
  }

  ensureMutationAdmissionOpen(): void {
    if (this.mutationOwnerLifecycle !== "Open")
      throw new DbError(ErrorCode.WriteRejected, "database mutation owner is closing");
  }

  ensureSubscriptionFinalizationOpen(): void {
    if (this.subscriptionFinalizationsClosed)
      throw new DbError(ErrorCode.Protocol, "database subscription admission is closed");
  }

  private pushOperation(op: {
    transaction?: [TxId, any];
    openTxId?: number;
    future: Polled<void>;
    status?: { value: any };
    completion?: (outcome: PollResult<void>) => void;
  }): void {
    this.queuedMutations.push(op);
    this.scheduleTick(TickUrgency.Immediate);
  }

  enqueueMutation(txId: TxId, future: Polled<void>): { value: any } {
    const status = { value: { kind: "Pending" } as any };
    this.reservedMutations.add(txId);
    this.pushOperation({ transaction: [txId, TxKind.Mergeable], future, status });
    return status;
  }

  /** Queue a write whose transaction identity was reserved just now. */
  enqueueReservedWrite(txId: TxId, future: Polled<void>): void {
    this.reservedMutations.add(txId);
    this.pushOperation({ transaction: [txId, TxKind.Mergeable], future });
  }

  enqueueTransactionOperation(openTxId: number, future: Polled<void>): void {
    this.ensureMutationAdmissionOpen();
    this.pushOperation({ openTxId, future });
  }

  /** Put a transaction-local read behind every already-admitted operation. */
  enqueueTransactionRead<T>(openTxId: number, read: () => Promise<T>): Promise<T> {
    return this.enqueueReadAfterMutations(openTxId, read);
  }

  queuedMutationCount(): number {
    return this.queuedMutations.length;
  }

  queuedMutationBarrier(): Promise<void> {
    if (this.ownerQueueIsQuiescent()) return Promise.resolve();
    return this.enqueueReadAfterMutations(undefined, async () => undefined);
  }

  private enqueueReadAfterMutations<T>(
    openTxId: number | undefined,
    read: () => Promise<T>,
  ): Promise<T> {
    const result = deferred<T>();
    let pending = true;
    this.pushOperation({
      openTxId,
      future: new Polled(async () => {
        // Cancelling an observation releases its read fence only.
        const value = await read();
        if (pending) {
          pending = false;
          result.resolve(value);
        }
      }),
      completion: (outcome) => {
        if (!outcome.ok && pending) {
          pending = false;
          result.reject(outcome.error);
        }
      },
    });
    return result.promise;
  }

  enqueueTransactionCommit(
    openTxId: number,
    txId: TxId,
    kind: any,
    future: Polled<void>,
  ): { value: any } {
    const status = { value: { kind: "Pending" } as any };
    this.reservedMutations.add(txId);
    this.pushOperation({ transaction: [txId, kind], openTxId, future, status });
    return status;
  }

  enqueueTransactionCleanup(future: Polled<void>): void {
    this.enqueueTransactionCleanupWithCompletion(future, undefined);
  }

  /** Order a non-transactional owner operation behind every operation already admitted. */
  enqueueOwnerOperation(future: Polled<void>): void {
    this.enqueueTransactionCleanupWithCompletion(future, undefined);
  }

  ownerQueueIsQuiescent(): boolean {
    return this.queuedMutations.length === 0 && this.queuedMutationActiveLeases === 0;
  }

  enqueueTransactionCleanupWithCompletion(
    future: Polled<void>,
    completion: ((outcome: PollResult<void>) => void) | undefined,
  ): void {
    this.pushOperation({ future, completion });
  }

  ownerIsAvailable(): boolean {
    return !this.node.isLocked();
  }

  /** Yield without losing the host wake needed after an external read finishes. */
  ownerIsAvailableOrWakeWhenReleased(): boolean {
    if (this.ownerIsAvailable()) {
      this.ownerReleaseWait = undefined;
      return true;
    }
    const waker = this.queryRuntimeWaker();
    if (!waker) return false;
    // Retain the lock future: dropping a pending mutex waiter cancels its wake.
    this.ownerReleaseWait ??= new Polled(async () => {
      await this.node.lock();
      this.node.unlock();
    });
    if (this.ownerReleaseWait.poll(waker)) {
      this.ownerReleaseWait = undefined;
      return true;
    }
    return false;
  }

  queuedTransactionError(id: number): DbError | undefined {
    return this.queuedOpenTransactionFailures.get(id);
  }

  retireQueuedTransactionError(id: number): void {
    this.queuedOpenTransactionFailures.delete(id);
  }

  /** Poll the next eligible owner operation, retaining pending read fences. */
  pollQueuedMutationOnce(): boolean {
    if (this.queuedMutations.length === 0) return false;
    const waker = this.queryRuntimeWaker();
    const deferredReads: any[] = [];
    let active: any;
    let pending: boolean;
    for (;;) {
      const operation = this.queuedMutations.shift();
      if (!operation) {
        pending = deferredReads.length > 0;
        break;
      }
      if (deferredReads.some((read) => read.openTxId === operation.openTxId)) {
        // Reads in one transaction retain their invocation order.
        if (operation.transaction || !operation.completion) {
          this.queuedMutations.unshift(operation);
          pending = true;
          break;
        }
        deferredReads.push(operation);
        continue;
      }
      const poisoned =
        operation.openTxId !== undefined
          ? this.queuedOpenTransactionFailures.get(operation.openTxId)
          : undefined;
      const outcome: PollResult<void> | undefined = poisoned
        ? { ok: false, error: poisoned }
        : operation.future.poll(waker);
      if (!outcome) {
        if (
          !operation.transaction &&
          operation.openTxId !== undefined &&
          operation.completion &&
          this.ownerIsAvailable()
        ) {
          // Remote coverage must not stop unrelated local work.
          deferredReads.push(operation);
          continue;
        }
        // Once started, this operation may own the lock needed by the deferred reads.
        active = operation;
        pending = true;
        break;
      }
      this.finishQueuedMutation(operation, outcome);
      pending = deferredReads.length > 0;
      break;
    }
    this.queuedMutations.unshift(...deferredReads);
    if (active) this.queuedMutations.unshift(active);
    return pending;
  }

  private finishQueuedMutation(operation: any, result: PollResult<void>): void {
    const completion = operation.completion;
    operation.completion = undefined;
    completion?.(result);
    if (operation.transaction) this.reservedMutations.delete(operation.transaction[0]);
    if (
      !result.ok &&
      operation.openTxId !== undefined &&
      !this.queuedOpenTransactionFailures.has(operation.openTxId)
    )
      this.queuedOpenTransactionFailures.set(operation.openTxId, result.error as DbError);
    if (operation.transaction && operation.status) {
      const [txId, kind] = operation.transaction;
      if (result.ok) {
        operation.status.value = { kind: "Published" };
      } else {
        const error = result.error as DbError;
        operation.status.value = { kind: "Failed", error };
        // Validation can fail before any durable transaction exists.
        queueMutationError(
          this.mutationErrors,
          this.scheduler,
          txId,
          queuedMutationErrorEvent(txId, kind, error),
        );
        this.queuedMutationFailures.set(txId, error);
      }
      notifyWriteStateWaiters(this.writeStateWaiters, txId);
      if (!result.ok && operation.openTxId !== undefined) {
        const openTxId = operation.openTxId;
        this.enqueueTransactionCleanup(
          new Polled(() =>
            this.node.with((node) => {
              try {
                node.abandonTx(openTxId);
              } catch (error: any) {
                if (error?.kind !== "MissingOpenBatch") throw error;
              }
            }),
          ),
        );
      }
    }
    if (operation.transaction && operation.openTxId !== undefined)
      this.queuedOpenTransactionFailures.delete(operation.openTxId);
    if (this.queuedMutations.length) this.scheduleTick(TickUrgency.Immediate);
  }

  async drainQueuedMutations(): Promise<void> {
    for (;;) {
      const operation = this.queuedMutations.shift();
      if (!operation) return;
      const lease = new QueuedMutationLease(this, operation);
      try {
        const poisoned =
          lease.op.openTxId !== undefined
            ? this.queuedOpenTransactionFailures.get(lease.op.openTxId)
            : undefined;
        let result: PollResult<void>;
        if (poisoned) result = { ok: false, error: poisoned };
        else {
          try {
            result = { ok: true, value: await lease.op.future.wait() };
          } catch (error) {
            result = { ok: false, error };
          }
        }
        this.finishQueuedMutation(lease.take(), result);
      } finally {
        lease.release();
      }
    }
  }

  upstreamRegisterShapeOptions(tier: any, readView: any): any {
    return upstreamRegisterShapeOptions(tier, readView, this.upstreamDurabilityFloor);
  }

  receivesCommitsAsLocal(): boolean {
    return this.receivesCommitsAsLocalFlag;
  }

  /** Declare this node the root authority for the uploads it accepts. */
  declareUploadRoot(): void {
    this.outbox.declareRoot();
  }

  enableAuthoritativeScalarExitRefresh(): void {
    this.node.borrow().enableAuthoritativeScalarExitRefresh();
  }

  setLargeValueStagingPolicy(policy: any): void {
    this.node.borrow().setLargeValueStagingPolicy(policy);
  }

  /** Run one host-driven staging-expiry maintenance pass. */
  evictExpiredStagedLargeValues(): Promise<number> {
    return this.node.borrow().evictExpiredStagedLargeValues();
  }

  setNonDurableClient(): void {
    this.node.borrow().setNonDurableClient();
    this.upstreamDurabilityFloor = DurabilityTier.Local;
  }

  configureScopeIsolatedClientRelay(scope: any): void {
    this.node.borrow().configureScopeIsolatedClientRelay(scope);
  }

  setDeferredLocalPersistence(deferredPersistence: boolean): void {
    this.deferLocalPersistence = deferredPersistence;
  }

  /** Change whether subscriber links may serve their registered views. */
  async setPermissionsReady(ready: boolean): Promise<void> {
    this.node.borrow().setPermissionsReady(ready);
    if (ready)
      for (const connection of this.connections)
        await connection.borrow().rehydrateSubscriberViews();
  }

  queuePendingUpload(txId: TxId, unit: SyncMessage | undefined): void {
    if (queuePendingUploadIn(this.outbox, txId, unit)) {
      this.markSubscriberConnectionsDirty();
      this.scheduleTick(TickUrgency.Deferred);
    }
  }

  queueLocalPublication(published: any, uploadUnit: SyncMessage | undefined): void {
    const settlement = new Polled(async () => {
      const txId = published.txId();
      const persistence = await published.persist();
      await this.node.with((node) => node.settlePublishedTransaction(txId, persistence));
      return txId;
    });
    this.pendingLocalPublications.push({ published, uploadUnit, settlement });
    this.scheduleTick(TickUrgency.Immediate);
  }

  hasPendingLocalPublications(): boolean {
    return this.pendingLocalPublications.length > 0;
  }

  /** Local commits still settling, with the storage tables each one changed. */
  pendingLocalPublicationTables(): [TxId, string[]][] {
    return this.pendingLocalPublications.map((p) => [
      p.published.txId(),
      [...p.published.changedTables()],
    ]);
  }

  isPendingLocalPublication(txId: TxId): boolean {
    return this.pendingLocalPublications.some((p) => p.published.txId() === txId);
  }

  private pollLocalPublicationSettlement(waker: Waker | undefined): PollResult<void> | undefined {
    for (;;) {
      const pending = this.pendingLocalPublications.shift();
      if (!pending) return { ok: true, value: undefined };
      const r = pending.settlement.poll(waker);
      if (!r) {
        this.pendingLocalPublications.unshift(pending);
        return undefined;
      }
      if (!r.ok) return r;
      // Persistence advanced durability from None to Local.
      notifyWriteStateWaiters(this.writeStateWaiters, r.value);
      this.queuePendingUpload(r.value, pending.uploadUnit);
    }
  }

  /** Advance ordered local durability without keeping the host tick pending. */
  pollLocalPublicationSettlementOnce(): boolean {
    const r = this.pollLocalPublicationSettlement(this.queryRuntimeWaker());
    if (!r) return true;
    if (!r.ok) throw r.error;
    return false;
  }

  /** Chunk staging must not retain the node while an earlier publication settles. */
  async lockForLargeValueStaging(): Promise<NodeState> {
    for (;;) {
      const node = await this.node.lock();
      if (!this.hasPendingLocalPublications()) return node;
      this.node.unlock();
      await this.settleLocalPublications();
    }
  }

  async settleLocalPublications(): Promise<void> {
    for (;;) {
      const d = deferred<void>();
      const r = this.pollLocalPublicationSettlement(() => d.resolve());
      if (r) {
        if (!r.ok) throw r.error;
        return;
      }
      await d.promise;
    }
  }

  /** Restore locally originated, unsettled durable writes after reopening storage. */
  async restorePendingUploads(identity: { node: any; author: AuthorSubject }): Promise<void> {
    const pending = await this.node.with((node) =>
      node.pendingTransactionIdsFor(identity.node, identity.author),
    );
    const restored = new Set<string>();
    for (const txId of pending) {
      if (restored.has(String(txId))) continue;
      restored.add(String(txId));
      this.queuePendingUpload(txId, undefined);
    }
  }

  async restoreBackendPendingUploads(nodeId: any): Promise<void> {
    const pending = await this.node.with((node) => node.synchronizingTransactionIdsForNode(nodeId));
    for (const txId of pending) this.queuePendingUpload(txId, undefined);
  }

  async restoreBrowserRelayPendingUploads(author: AuthorSubject): Promise<void> {
    const pending: TxId[] = await this.node.with((node) =>
      node.pendingTransactionIdsForAuthor(author),
    );
    for (const txId of pending) this.browserRelayRecoveredTxIds.add(txId);
    for (const txId of pending) this.queuePendingUpload(txId, undefined);
  }

  private restoreLocalSubscriber(author: AuthorSubject, downstreamFates: any[]): Promise<void> {
    return restoreLocalSubscriberReplay(
      this.node,
      this.outbox,
      this.localFateRoutes,
      author,
      downstreamFates,
    );
  }

  markSubscriberConnectionsDirty(): void {
    const next = this.subscriberDirtyEpoch + 1;
    this.subscriberDirtyEpoch = next;
    for (const handle of this.connections) {
      // A suspended peer observes the shared epoch on its next tick.
      const connection = handle.tryLock();
      if (!connection) continue;
      try {
        if (connection.link.kind === "Subscriber") {
          connection.link.state.serveDirty = true;
          connection.observedSubscriberDirtyEpoch = next;
        }
      } finally {
        handle.unlock();
      }
    }
  }

  markSubscriberConnectionsDirtyAfterQueryRuntimeWake(): void {
    if (this.queryRuntimeWakePending.value) {
      this.queryRuntimeWakePending.value = false;
      this.localSubscriptionDirtyGeneration++;
      this.markSubscriberConnectionsDirty();
    }
  }

  nextSubscriptionKey(shape: any, readView: any): SubscriptionKey {
    const nonce = this.nextSubscriptionNonce;
    this.nextSubscriptionNonce = nonce + 1n;
    const bytes = new Uint8Array(8);
    new DataView(bytes.buffer).setBigUint64(0, nonce);
    return { shapeId: shape.shapeId(), bindingId: uuidV5(QUERY_NAMESPACE, bytes), readView };
  }

  setScheduler(scheduler: TickScheduler | undefined): void {
    this.detachCoveredChunkWaits.value = scheduler?.dropsPendingTicks() ?? false;
    this.scheduler.current = scheduler;
    this.queryRuntimeWakerCache.waker = undefined;
  }

  scheduleTick(urgency: TickUrgency): void {
    scheduleTickIn(this.scheduler, urgency);
  }

  /** Obtain the host-owned waker that survives this short owner turn. */
  queryRuntimeWaker(): Waker | undefined {
    return makeQueryRuntimeWaker(
      this.scheduler,
      this.queryRuntimeWakePending,
      this.queryRuntimeWakerCache,
    );
  }

  /** Lock the node for opening a transaction while the database owns admission. */
  async lockForTransactionOpen(
    openTxId: number,
    ownerOperationAdmitted: boolean,
  ): Promise<NodeState> {
    if (!ownerOperationAdmitted) this.ensureTransactionAdmissionOpen();
    const node = await this.node.lock();
    try {
      if (!ownerOperationAdmitted && this.transactionAdmissionClosed()) {
        this.finishTransactionAbandonmentShutdownIn(node);
        this.ensureTransactionAdmissionOpen();
      }
      this.rejectTombstonedTransactionIn(node, openTxId);
      return node;
    } catch (error) {
      this.node.unlock();
      throw error;
    }
  }

  /** Lock the node for an operation on an existing transaction. */
  async lockForTransactionOperation(
    openTxId: number,
    ownerOperationAdmitted: boolean,
  ): Promise<NodeState> {
    const node = await this.node.lock();
    try {
      this.rejectTombstonedTransactionIn(node, openTxId);
      if (!ownerOperationAdmitted && this.transactionAdmissionClosed()) {
        this.finishTransactionAbandonmentShutdownIn(node);
        this.ensureTransactionAdmissionOpen();
      }
      return node;
    } catch (error) {
      this.node.unlock();
      throw error;
    }
  }

  private transactionAdmissionClosed(): boolean {
    return this.transactionAbandonmentsClosed;
  }

  private ensureTransactionAdmissionOpen(): void {
    if (this.transactionAbandonmentsClosed)
      throw new DbError(ErrorCode.Protocol, "database transaction admission is closed");
  }

  markTransactionAbandoned(openTxId: number): boolean {
    return this.pendingTransactionAbandonments.add(openTxId);
  }

  clearTransactionAbandonment(openTxId: number): void {
    this.pendingTransactionAbandonments.delete(openTxId);
  }

  private rejectTombstonedTransactionIn(node: NodeState, openTxId: number): void {
    if (!this.pendingTransactionAbandonments.has(openTxId)) return;
    Node.abandonTransactionForMaintenance(node, openTxId);
    this.clearTransactionAbandonment(openTxId);
    throw transactionAbandoned(openTxId);
  }

  /** Abandon an RAII-owned transaction now when uncontended, else leave a tombstone. */
  abandonOrEnqueueTransaction(openTxId: number): void {
    if (this.transactionAbandonmentsClosed) return;
    if (!this.markTransactionAbandoned(openTxId)) return;
    const node = this.node.tryLock();
    if (node) {
      try {
        try {
          Node.abandonTransactionForMaintenance(node, openTxId);
          this.clearTransactionAbandonment(openTxId);
          if (node.hasReleasedLargeValues()) this.scheduleTick(TickUrgency.Immediate);
        } catch {
          this.scheduleTick(TickUrgency.Immediate);
        }
      } finally {
        this.node.unlock();
      }
      return;
    }
    this.scheduleTick(TickUrgency.Immediate);
  }

  private static abandonTransactionForMaintenance(node: NodeState, openTxId: number): void {
    try {
      node.abandonTx(openTxId);
    } catch (error: any) {
      if (error?.kind !== "MissingOpenBatch") throw error;
    }
  }

  private drainTransactionAbandonmentsIn(node: NodeState): number {
    const abandonments = this.pendingTransactionAbandonments.toArray();
    this.pendingTransactionAbandonments = new SortedSet(cmpNum);
    let firstError: unknown;
    for (const openTxId of abandonments) {
      try {
        Node.abandonTransactionForMaintenance(node, openTxId);
      } catch (error) {
        this.pendingTransactionAbandonments.add(openTxId);
        firstError ??= error;
      }
    }
    if (firstError) throw firstError;
    return abandonments.length;
  }

  private finishTransactionAbandonmentShutdownIn(node: NodeState): number {
    let drained: number | undefined;
    let error: unknown;
    try {
      drained = this.drainTransactionAbandonmentsIn(node);
    } catch (e) {
      error = e;
    }
    // Keep the shutdown sweep pending until the FIFO owner has drained.
    if (this.transactionAbandonmentShutdownPending && this.ownerQueueIsQuiescent()) {
      this.transactionAbandonmentShutdownPending = false;
      node.abandonAllOpenTransactions();
    }
    if (error) throw error;
    return drained!;
  }

  private async drainTransactionAbandonments(): Promise<number> {
    return this.node.with(async (node) => {
      const drained = this.finishTransactionAbandonmentShutdownIn(node);
      // Groove defers eviction while a local publication is resident.
      if (node.hasReleasedLargeValues() && !this.hasPendingLocalPublications())
        await node.evictReleasedLargeValues();
      return drained;
    });
  }

  /** Close transaction admission and transfer the final sweep to the tick owner. */
  beginTransactionAbandonmentShutdown(): void {
    if (this.transactionAbandonmentsClosed) return;
    this.transactionAbandonmentsClosed = true;
    this.transactionAbandonmentShutdownPending = true;
    this.scheduleTick(TickUrgency.Immediate);
  }

  async finishTransactionAbandonmentShutdown(): Promise<number> {
    let drained: number | undefined;
    let error: unknown;
    await this.node.with((node) => {
      try {
        drained = this.finishTransactionAbandonmentShutdownIn(node);
      } catch (e) {
        error = e;
      }
    });
    // No later owner turn runs after close.
    await this.evictReleasedLargeValues();
    if (error) throw error;
    return drained!;
  }

  /** Best-effort eviction of Groove roots staged by abandoned transactions. */
  async evictReleasedLargeValues(): Promise<void> {
    if (!(await this.node.with((node) => node.hasReleasedLargeValues()))) return;
    let node: NodeState;
    try {
      node = await this.lockForLargeValueStaging();
    } catch {
      return;
    }
    try {
      await node.evictReleasedLargeValues();
    } finally {
      this.node.unlock();
    }
  }

  /** Enqueue a stream-finalization command without touching the node mutex. */
  enqueueSubscriptionFinalization(command: any): void {
    if (this.subscriptionFinalizationsClosed && this.subscriptionRuntimeRetired) {
      // Terminal retirement already drained every maintained view.
      command.acknowledgement?.();
      command.acknowledgement = undefined;
      return;
    }
    this.pendingSubscriptionFinalizations.push(command);
    this.scheduleTick(TickUrgency.Immediate);
  }

  /** Drain queued stream cleanup under the ordinary async node owner. */
  async drainSubscriptionFinalizations(): Promise<number> {
    const node = await this.node.lock();
    const commands = this.pendingSubscriptionFinalizations;
    this.pendingSubscriptionFinalizations = [];
    let drained = 0;
    let changedUpstream = false;
    try {
      for (const command of commands) {
        let local: [number, any] | undefined;
        let upstream: any[];
        let owner: any;
        if (command.state) {
          const state = command.state;
          command.state = undefined;
          owner = state;
          state.scalarReconciliation = { pending: [], retryDelayMs: 0 };
          // A closing stream stops withholding, never reconciling (INV-SYNC-48).
          const reconciliation = state.deletionReconciliation;
          state.deletionReconciliation = { kind: "Disabled" };
          switch (reconciliation.kind) {
            case "Pending":
              this.heldRowChecks.enqueue(reconciliation.rows, reconciliation.context);
              this.scheduleTick(TickUrgency.Immediate);
              break;
            case "Deferred":
            case "Discovering": {
              const redundant = this.heldRowChecks.adoptDiscovery(reconciliation);
              releaseRedundantDiscovery(node, redundant);
              this.scheduleTick(TickUrgency.Immediate);
              break;
            }
            default:
              state.deletionReconciliation =
                reconciliation.kind === "Disabled" ? reconciliation : { kind: "Disabled" };
          }
          upstream = [...state.upstreamSubscriptionHandles, ...state.authorityWitness];
          state.upstreamSubscriptionHandles = [];
          state.authorityWitness = [];
          local = state.localSubscriptionCleanup.value;
          state.localSubscriptionCleanup.value = undefined;
        } else {
          local = command.openingLocal;
          command.openingLocal = undefined;
          upstream = command.openingUpstream;
          command.openingUpstream = [];
          owner = undefined;
        }
        if (local && node.grooveRuntimeToken() === local[0])
          node.unsubscribeGrooveSubscription(local[1]);
        for (const handle of upstream) {
          unregisterUpstreamSubscriptionOwner(
            this.upstreamSubscriptionOwners,
            handle.subscription,
            owner,
          );
          const key = coverageKeyString(handle.coverage);
          const count = this.upstreamCoverageRefcounts.get(key);
          if (count === undefined) continue;
          if (count > 1) {
            this.upstreamCoverageRefcounts.set(key, count - 1);
            continue;
          }
          this.upstreamCoverageRefcounts.delete(key);
          this.awaitingInitialAuthorityCoverage.delete(key);
          node.applyUnsubscribe(handle.subscription);
          this.coverageRefreshGenerations.delete(key);
          this.activeAuthorityViewReceipts?.subscriptions.delete(handle.subscription);
          for (const [coverage, subscription] of [...this.latestCoverageSubscriptions])
            if (
              coverage === key ||
              subscriptionKeyString(subscription) === subscriptionKeyString(handle.subscription)
            )
              this.latestCoverageSubscriptions.delete(coverage);
          this.upstreamSubscriptions.push({
            kind: "Unsubscribe",
            subscription: handle.subscription,
          });
          changedUpstream = true;
        }
        command.acknowledgement?.();
        command.acknowledgement = undefined;
        drained++;
      }
    } finally {
      this.node.unlock();
    }
    if (changedUpstream) this.scheduleTick(TickUrgency.Immediate);
    return drained;
  }

  /** Atomically close the admission gate and enqueue every live stream for retirement. */
  beginSubscriptionFinalizationShutdown(): void {
    if (this.subscriptionFinalizationsClosed) return;
    this.subscriptionFinalizationsClosed = true;
    for (const weak of this.subscriptions) {
      const state = weak.deref();
      if (!state) continue;
      state.closed.value = true;
      this.pendingSubscriptionFinalizations.push({
        state,
        openingUpstream: [],
        openingLocal: undefined,
        acknowledgement: undefined,
      });
    }
    this.scheduleTick(TickUrgency.Immediate);
  }

  /** Release all connection and subscription bookkeeping after storage closed. */
  retireSubscriptionRuntimeAfterClose(): void {
    this.subscriptionRuntimeRetired = true;
    this.subscriptions = [];
    this.connections = [];
    this.pendingDetaches = [];
    this.upstreamSubscriptions.length = 0;
    this.pendingSubscriptionFinalizations = [];
    this.latestCoverageSubscriptions.clear();
    this.upstreamCoverageRefcounts.clear();
    this.awaitingInitialAuthorityCoverage.clear();
    this.queryCoverageRegistrations.clear();
    this.upstreamSubscriptionOwners.clear();
    this.relayUpstreamSubscriptionOwners.clear();
    this.pendingRelaySubscriptionRejections.clear();
  }

  setMutationErrorCallback(callback: ((event: any) => void) | undefined): void {
    this.mutationErrors.callback = callback;
    if (callback && this.mutationErrors.pending.size) this.scheduleTick(TickUrgency.Immediate);
  }

  private consumeMutationError(txId: TxId): void {
    this.mutationErrors.pending.delete(txId);
    if (this.node.borrow().rejectedTransaction(txId)) {
      this.deferredRejectionDiscards.add(txId);
      this.scheduleTick(TickUrgency.Immediate);
    }
  }

  /** Finish rejection acknowledgement after every woken wait observer had a turn. */
  async flushDeferredRejectionDiscards(): Promise<void> {
    const txIds = this.deferredRejectionDiscards.toArray();
    this.deferredRejectionDiscards = new SortedSet(cmpTxId);
    let firstError: any;
    for (const txId of txIds) {
      try {
        await this.node.with((node) => node.discardRejection(txId));
      } catch (error) {
        this.deferredRejectionDiscards.add(txId);
        firstError ??= error;
      }
    }
    if (firstError) {
      // A failed atomic Groove commit poisons this opened database.
      if (firstError.kind === "Groove" && firstError.inner?.kind === "DatabasePoisoned") {
        console.error(
          `${firstError} deferred mutation-error acknowledgement requires reopening the database`,
        );
      } else {
        console.warn(
          `${firstError} deferred mutation-error acknowledgement retained for an explicit later retry`,
        );
      }
      throw DbError.from(firstError);
    }
  }

  async transactionWaitOutcome(txId: TxId, tier: any): Promise<PollResult<TxId> | undefined> {
    const failure = this.queuedMutationFailures.get(txId);
    if (failure) return { ok: false, error: failure };
    if (this.reservedMutations.has(txId)) return undefined;
    const state = await this.node.with((node) => node.transactionState(txId));
    if (!state)
      return {
        ok: false,
        error: new DbError(ErrorCode.NotObserved, `transaction ${dbg(txId)} is not known locally`),
      };
    return waitOutcomeFromState(state, txId, txId, tier);
  }

  /** Read a transaction's wait predicate without claiming its rejection. */
  private async completionTargetWaitOutcome(
    publicTxId: TxId,
    targetTxId: TxId,
    tier: any,
  ): Promise<PollResult<TxId> | undefined> {
    const state = await this.node.with((node) => node.transactionState(targetTxId));
    if (!state)
      return {
        ok: false,
        error: new DbError(
          ErrorCode.NotObserved,
          `completion target for transaction ${dbg(publicTxId)} is not known locally`,
        ),
      };
    return waitOutcomeFromState(state, publicTxId, publicTxId, tier);
  }

  queuedMutationWriteState(txId: TxId): PollResult<any> | undefined {
    const failure = this.queuedMutationFailures.get(txId);
    if (failure) return { ok: false, error: failure };
    return this.reservedMutations.has(txId)
      ? {
          ok: true,
          value: { fate: Fate.Pending, globalTime: undefined, durability: DurabilityTier.None },
        }
      : undefined;
  }

  takeQueuedMutationFailure(txId: TxId): DbError | undefined {
    const error = this.queuedMutationFailures.delete(txId);
    if (!error) return undefined;
    // A binding returning the failure synchronously has already reported it.
    this.consumeMutationError(txId);
    return error;
  }

  waitForTransactionWith(txId: TxId, options: any, callback: (r: PollResult<TxId>) => void): void {
    this.waitForWriteWith(txId, undefined, options, callback);
  }

  /** Callback wait for a binding-owned write handle. */
  waitForWriteWith(
    txId: TxId,
    alias: { value: TxId | undefined } | undefined,
    options: any,
    callback: (r: PollResult<TxId>) => void,
  ): void {
    if (this.mutationOwnerLifecycle === "Closing") {
      callback({
        ok: false,
        error: new DbError(
          ErrorCode.NotObserved,
          `database is closed; transaction ${dbg(txId)} cannot be observed`,
        ),
      });
      return;
    }
    const errorWait = this.registerMutationErrorWait(txId, options);
    this.transactionWaitObservers.push(
      new Polled(async () => {
        try {
          callback({ ok: true, value: await this.waitForWrite(txId, alias, options, errorWait) });
        } catch (error) {
          callback({ ok: false, error });
        }
      }),
    );
    this.scheduleTick(TickUrgency.Immediate);
  }

  private registerMutationErrorWait(txId: TxId, options: any): any {
    return options.observeOnly
      ? undefined
      : new MutationErrorWait(this.mutationErrors, this.scheduler, txId);
  }

  private async waitForWrite(
    txId: TxId,
    alias: { value: TxId | undefined } | undefined,
    options: any,
    errorWait: any,
  ): Promise<TxId> {
    try {
      return await this.observeWrite(txId, alias, options.tier);
    } catch (error) {
      if (!options.observeOnly) this.consumeMutationError(txId);
      throw error;
    } finally {
      errorWait?.drop();
    }
  }

  private closedBefore(txId: TxId, tier: any): DbError {
    return new DbError(
      ErrorCode.NotObserved,
      `database closed before transaction ${dbg(txId)} reached ${dbg(tier)}`,
    );
  }

  private async observeWrite(
    txId: TxId,
    alias: { value: TxId | undefined } | undefined,
    tier: any,
  ): Promise<TxId> {
    if (!alias) return this.observeTransaction(txId, tier);
    for (;;) {
      if (alias.value !== undefined) return this.waitForCompletionTarget(txId, alias.value, tier);
      const outcome = await this.transactionWaitOutcome(txId, tier);
      if (outcome) return unwrap(outcome);
      if (this.mutationOwnerLifecycle === "Closing" && !this.reservedMutations.has(txId))
        throw this.closedBefore(txId, tier);
      const stateChange = this.registerWriteStateWaiter(txId);
      if (alias.value !== undefined) {
        stateChange.drop();
        return this.waitForCompletionTarget(txId, alias.value, tier);
      }
      const again = await this.transactionWaitOutcome(txId, tier);
      if (again) {
        stateChange.drop();
        return unwrap(again);
      }
      await stateChange.promise;
    }
  }

  private async waitForCompletionTarget(
    publicTxId: TxId,
    targetTxId: TxId,
    tier: any,
  ): Promise<TxId> {
    for (;;) {
      const outcome = await this.completionTargetWaitOutcome(publicTxId, targetTxId, tier);
      if (outcome) return unwrap(outcome);
      const stateChange = this.registerWriteStateWaiter(targetTxId);
      const again = await this.completionTargetWaitOutcome(publicTxId, targetTxId, tier);
      if (again) {
        stateChange.drop();
        return unwrap(again);
      }
      await stateChange.promise;
    }
  }

  async waitForTransaction(txId: TxId, options: any): Promise<TxId> {
    const errorWait = this.registerMutationErrorWait(txId, options);
    return this.waitForWrite(txId, undefined, options, errorWait);
  }

  private async observeTransaction(txId: TxId, tier: any): Promise<TxId> {
    for (;;) {
      const outcome = await this.transactionWaitOutcome(txId, tier);
      if (outcome) return unwrap(outcome);
      if (this.mutationOwnerLifecycle === "Closing" && !this.reservedMutations.has(txId))
        throw this.closedBefore(txId, tier);
      const stateChange = this.registerWriteStateWaiter(txId);
      const again = await this.transactionWaitOutcome(txId, tier);
      if (again) {
        stateChange.drop();
        return unwrap(again);
      }
      if (this.mutationOwnerLifecycle === "Closing" && !this.reservedMutations.has(txId)) {
        stateChange.drop();
        throw this.closedBefore(txId, tier);
      }
      await stateChange.promise;
    }
  }

  pollTransactionWaitObservers(): void {
    const waker = this.queryRuntimeWaker();
    const observers = this.transactionWaitObservers.filter((observer) => !observer.poll(waker));
    this.transactionWaitObservers = [...observers, ...this.transactionWaitObservers];
  }

  async drainTransactionWaitObservers(): Promise<void> {
    for (;;) {
      const d = deferred<void>();
      const observers = this.transactionWaitObservers;
      this.transactionWaitObservers = [];
      const remaining = observers.filter((observer) => !observer.poll(() => d.resolve()));
      this.transactionWaitObservers = [...remaining, ...this.transactionWaitObservers];
      if (!remaining.length) return;
      await d.promise;
    }
  }

  /** Deliver only at the start of the outer Db turn. */
  deliverPendingMutationErrors(): void {
    const delivery = takePendingMutationErrorDelivery(this.mutationErrors);
    if (!delivery) return;
    const [callback, events] = delivery;
    for (const [txId, event] of events) {
      this.consumeMutationError(txId);
      callback(event);
    }
  }

  requestPermissionAdvice(action: any): any {
    return this.queuePermissionAdvice(action, undefined, undefined);
  }

  requestPermissionAdviceWithDelegatedSession(action: any, session: any): any {
    return this.queuePermissionAdvice(action, [session.identity, session.claims], session);
  }

  private queuePermissionAdvice(action: any, sessionClaimBinding: any, delegatedSession: any): any {
    const requestId = uuidToString(uuidV4());
    const advice = deferred<any>();
    this.permissionAdviceWaiters.set(requestId, advice.resolve);
    this.upstreamSubscriptions.push({
      kind: "AuthorizationScopeIntent",
      requestId,
      action,
      sessionClaimBinding,
      delegatedSession,
    });
    this.scheduleTick(TickUrgency.Immediate);
    return {
      requestId,
      promise: advice.promise,
      cancel: () => this.cancelPermissionAdviceRequest(requestId),
    };
  }

  cancelPermissionAdviceRequest(requestId: string): void {
    const key = requestId;
    this.permissionAdviceWaiters.delete(key);
    for (const handle of this.connections) {
      const connection = handle.borrow();
      if (connection.link.kind !== "Upstream") continue;
      const manager = connection.link.state.scopeLeaseManager;
      let empty = false;
      for (const request of manager.requests.values())
        if (request.waiters.delete(key)) empty ||= request.waiters.size === 0;
      if (empty)
        for (const [k, request] of [...manager.requests])
          if (!request.waiters.size) manager.requests.delete(k);
    }
  }

  registerWriteStateWaiter(txId: TxId): { promise: Promise<void>; drop(): void } {
    const waiterId = this.nextWriteStateWaiterId;
    this.nextWriteStateWaiterId = Math.max(waiterId + 1, 1);
    const changed = deferred<void>();
    this.writeStateWaiters
      .getOrInsert(txId, () => [])
      .push({ id: waiterId, notify: () => changed.resolve() });
    return {
      promise: changed.promise,
      drop: () => {
        const list = this.writeStateWaiters.get(txId);
        if (!list) return;
        const rest = list.filter((w) => w.id !== waiterId);
        if (rest.length) this.writeStateWaiters.set(txId, rest);
        else this.writeStateWaiters.delete(txId);
      },
    };
  }

  refreshSubscriptions(): Promise<number> {
    return this.refreshSubscriptionsWithTables(undefined);
  }

  refreshSubscriptionsWithTables(changedTables: Set<string> | undefined): Promise<number> {
    return refreshSubscriptionsInWithTables(
      this.node,
      this,
      changedTables,
      this.queryRuntimeWaker(),
    );
  }

  /** Retire authority settlement synchronously without re-evaluating data. */
  private demoteAuthorityReceiptSubscriptions(): void {
    demoteAuthorityReceiptSubscriptions(this.subscriptions, new Set());
  }

  /** Attach an upstream after acquiring a stable runtime and peer inventory. */
  connectUpstream(transport: Transport): Promise<LocalMutex<PeerConnection>> {
    return this.connectUpstreamInner(transport);
  }

  tryConnectUpstream(transport: Transport): Promise<LocalMutex<PeerConnection>> {
    return this.connectUpstreamInner(transport);
  }

  private async connectUpstreamInner(transport: Transport): Promise<LocalMutex<PeerConnection>> {
    transport.setTrustedEncoder(true);
    this.outbox.markUpstreamAttached();
    for (;;) {
      const peers = [...this.connections];
      const peerGuards = await Node.acquirePeerInventory(peers);
      const node = await this.node.lock();
      if (!this.peerInventoryMatches(peers)) {
        this.node.unlock();
        releaseGuards(peerGuards);
        continue;
      }
      const sessionContext = transport.connectionSessionContext();
      const localReceiver = !node.isHistoryComplete();
      const confirmationFloor = node.committedGlobalTime();
      this.node.unlock();
      try {
        const wireInboundContext = transport.wireInboundContext();
        const sharedAuxiliaryEndpoint = transport.sharedAuxiliaryEndpoint();
        const destination =
          sessionContext?.remote &&
          ({
            remoteNode: sessionContext.remote.node,
            linkIdentity: sessionContext.linkIdentity,
          } as const);
        const destinationKey = destination && uploadDestinationKey(destination);
        const transferred =
          (destinationKey && this.detachedLargeValueUploads.get(destinationKey)) || new Map();
        if (destinationKey) this.detachedLargeValueUploads.delete(destinationKey);
        const connectionEpoch = sessionContext?.local.epoch ?? randomEpoch();
        beginOpenSchemaConnection(this.openSchemaAdmission, connectionEpoch);
        // A new upstream owns no settlement receipts until it sends a fresh ViewUpdate.
        this.activeAuthorityViewReceipts = {
          connectionEpoch,
          confirmationFloor,
          subscriptions: new SortedSet(cmpSubscriptionKey),
          bindingViews: new Set(),
        };
        this.demoteAuthorityReceiptSubscriptions();
        const expectedScopeAuthority: AuthorityContext | undefined =
          sessionContext &&
          (sessionContext.negotiatedFeatures & FEATURE_AUTHORIZATION_SCOPE_VIEWS) !== 0 &&
          sessionContext.remote
            ? {
                authority: sessionContext.remote.node,
                link: sessionContext.linkIdentity,
                connectionId: connectionEpoch,
                connectionEpoch: sessionContext.remote.epoch,
                claimsRevision: 0,
                policyEpoch: 0,
                authorizationProgress: 0,
                settledThrough: 0,
              }
            : undefined;
        // Keep every admitted link eligible, but bind each route to one stable owner.
        if (expectedScopeAuthority) {
          if (
            !this.admittedUpstreamAuthorities.some((c) =>
              authorityEquals(c, expectedScopeAuthority),
            )
          )
            this.admittedUpstreamAuthorities.push(expectedScopeAuthority);
          this.admittedUpstreamAuthority ??= expectedScopeAuthority;
        }
        // Carry queued and already-registered subscriptions upstream immediately.
        const pending = this.upstreamSubscriptions.splice(0);
        const pendingSubscriptions = new Set<string>(
          pending
            .filter((c) => c.kind === "Subscribe")
            .map((c) => subscriptionKeyString(c.subscription.subscription)),
        );
        const addSubscribe = (subscription: any) => {
          const key = subscriptionKeyString(subscription.subscription);
          if (pendingSubscriptions.has(key)) return;
          pendingSubscriptions.add(key);
          pending.push({ kind: "Subscribe", subscription });
        };
        for (const registration of this.queryCoverageRegistrations.valuesArray())
          addSubscribe({ ...registration.subscription });
        for (const weak of this.subscriptions) {
          const state = weak.deref();
          if (!state || !state.propagatesUpstream) continue;
          const { shape, binding } = state.kind;
          const opts = this.upstreamRegisterShapeOptions(state.readTier, state.readView);
          const coverage = requestCoverageKey(shape, binding, opts, state.requestIdentityClaims);
          let subscription: SubscriptionKey | undefined;
          for (const [key, owners] of this.upstreamSubscriptionOwners)
            if (owners.some((owner) => owner.deref() === state)) {
              subscription = key;
              break;
            }
          subscription ??= this.nextSubscriptionKey(shape, opts.readViewKey());
          this.latestCoverageSubscriptions.set(coverageKeyString(coverage), subscription);
          addSubscribe({
            subscription,
            shape,
            binding,
            opts,
            identity: state.author,
            policyBinding: state.requestIdentityClaims,
          });
        }
        // Replay every live relay owner onto the successor authority.
        for (const handle of this.connections) {
          const connection = peerGuards.get(handle);
          if (!connection) throw new Error("stable upstream peer inventory");
          if (connection.link.kind !== "Subscriber") continue;
          for (const [coverage, group] of connection.link.state.coverageGroups) {
            const owner = this.relayUpstreamSubscriptionOwners.get([
              group.upstreamSubscription,
              connection.connectionEpoch,
              coverage.opts.readViewKey(),
            ]);
            if (
              !owner ||
              owner.downstreamConnectionEpoch !== connection.connectionEpoch ||
              coverageKeyString(owner.coverage) !== coverageKeyString(coverage)
            )
              continue;
            // Forwarded under the subscription's topology-admitted policy binding.
            addSubscribe({
              subscription: group.upstreamSubscription,
              shape: group.shape,
              binding: group.binding,
              opts: group.upstreamOpts,
              identity: group.policyBinding[0],
              policyBinding: group.policyBinding,
            });
          }
        }
        const connection = new LocalMutex(
          new PeerConnection(this, {
            transport,
            connectionEpoch,
            startupError: undefined,
            downstreamFates: [],
            upstreamUploadDestination: destination,
            sessionClaimRevision: 0,
            link: ConnectionLink.upstream({
              localReceiver,
              pending,
              expectedScopeAuthority,
              largeValueUploads: transferred,
            }),
            auxiliaryPump: new PeerIoPump(
              this.chunkResolver,
              this.localChunkReader,
              connectionEpoch,
              PeerIoPumpRole.Upstream,
              wireInboundContext,
            ).withSharedAuxiliaryEndpoint(sharedAuxiliaryEndpoint),
          }),
        );
        this.connections.push(connection);
        this.remoteLink.upstreamAttached();
        this.scheduleTick(TickUrgency.Immediate);
        return connection;
      } finally {
        releaseGuards(peerGuards);
      }
    }
  }

  /** Accept a subscriber connection served under `identity`. */
  acceptSubscriber(
    transport: Transport,
    identity: AuthorSubject,
  ): Promise<LocalMutex<PeerConnection>> {
    return this.acceptSubscriberWithTrust(transport, identity, CommitUnitTrust.Session);
  }

  acceptSubscriberWithClaims(
    transport: Transport,
    identity: AuthorSubject,
    claims: Map<string, Value>,
  ): Promise<LocalMutex<PeerConnection>> {
    return this.acceptSubscriberWithClaimsAndTrust(
      transport,
      identity,
      claims,
      CommitUnitTrust.Session,
    );
  }

  acceptSubscriberWithTrust(
    transport: Transport,
    identity: AuthorSubject,
    trust: any,
  ): Promise<LocalMutex<PeerConnection>> {
    return this.acceptSubscriberWithResumeAndTrust(
      transport,
      identity,
      trust,
      new Map(),
      undefined,
    );
  }

  acceptRelaySubscriber(transport: Transport): Promise<LocalMutex<PeerConnection>> {
    return this.acceptSubscriberWithPeer(
      transport,
      AuthorSubject_SYSTEM,
      CommitUnitTrust.Relay,
      new Map(),
      undefined,
      PeerState.relay(),
    );
  }

  /** Admit the one immutable session selected during a scope-isolated relay handshake. */
  acceptScopeIsolatedRelaySubscriber(
    transport: Transport,
    identity: AuthorSubject,
    claims: Map<string, Value>,
    admissionEpoch: number,
  ): Promise<LocalMutex<PeerConnection>> {
    return this.acceptSubscriberWithPeer(
      transport,
      identity,
      CommitUnitTrust.Relay,
      new Map(),
      undefined,
      PeerState.scopeIsolatedRelay(identity, claims, admissionEpoch),
    );
  }

  acceptSubscriberWithClaimsAndTrust(
    transport: Transport,
    identity: AuthorSubject,
    claims: Map<string, Value>,
    trust: any,
  ): Promise<LocalMutex<PeerConnection>> {
    return this.acceptSubscriberWithResumeAndTrust(transport, identity, trust, claims, undefined);
  }

  /** Accept a reconnecting subscriber, resuming from a previous cursor. */
  acceptSubscriberWithResume(
    transport: Transport,
    identity: AuthorSubject,
    cursor: any,
  ): Promise<LocalMutex<PeerConnection>> {
    return this.acceptSubscriberWithResumeAndTrust(
      transport,
      identity,
      CommitUnitTrust.Session,
      new Map(),
      cursor,
    );
  }

  private acceptSubscriberWithResumeAndTrust(
    transport: Transport,
    identity: AuthorSubject,
    trust: any,
    claims: Map<string, Value>,
    cursor: any,
  ): Promise<LocalMutex<PeerConnection>> {
    // Local/pending ingestion describes durability, not authorization.
    let peer: any;
    switch (trust) {
      case CommitUnitTrust.TrustedBackend:
      case CommitUnitTrust.TrustedAuthority:
      case CommitUnitTrust.TrustedAdmin:
        peer = PeerState.clientLinkWithPermissionIdentity(identity, AuthorSubject_SYSTEM);
        break;
      case CommitUnitTrust.Session:
        peer = PeerState.clientLink(identity);
        break;
      default:
        peer = PeerState.relay();
    }
    return this.acceptSubscriberWithPeer(transport, identity, trust, claims, cursor, peer);
  }

  private async acceptSubscriberWithPeer(
    transport: Transport,
    identity: AuthorSubject,
    trust: any,
    claims: Map<string, Value>,
    cursor: any,
    peer: any,
  ): Promise<LocalMutex<PeerConnection>> {
    if (cursor && cursor.ingestContext.identity !== identity)
      throw new Error("a resume cursor may only be used by its authenticated identity");
    const [downstreamFates, startupError] = await this.subscriberStartup(identity, trust);
    return this.acceptSubscriberWithPeerAndStartup(
      transport,
      identity,
      trust,
      claims,
      cursor,
      peer,
      downstreamFates,
      startupError,
    );
  }

  /** Admit a normal session subscriber without waiting on its semantic owner. */
  async acceptSubscriberWithClaimsAsync(
    transport: Transport,
    identity: AuthorSubject,
    claims: Map<string, Value>,
  ): Promise<LocalMutex<PeerConnection>> {
    const [downstreamFates, startupError] = await this.subscriberStartup(
      identity,
      CommitUnitTrust.Session,
    );
    if (startupError) throw startupError;
    return this.acceptSubscriberWithPeerAndStartup(
      transport,
      identity,
      CommitUnitTrust.Session,
      claims,
      undefined,
      PeerState.clientLink(identity),
      downstreamFates,
      undefined,
    );
  }

  private async subscriberStartup(
    identity: AuthorSubject,
    trust: any,
  ): Promise<[any[], DbError | undefined]> {
    const downstreamFates: any[] = [];
    const scope = await this.node.with((node) => node.clientRelayScope());
    const scopeMismatch =
      scope && trust === CommitUnitTrust.Session && !scope.admitsSession(identity)
        ? new DbError(
            ErrorCode.Protocol,
            "foreground session is outside this scope-isolated relay ownership scope",
          )
        : undefined;
    let startupError = scopeMismatch;
    if (!scopeMismatch && this.receivesCommitsAsLocal()) {
      try {
        await this.restoreLocalSubscriber(identity, downstreamFates);
      } catch (error) {
        startupError = error as DbError;
      }
    }
    return [downstreamFates, startupError];
  }

  private acceptSubscriberWithPeerAndStartup(
    transport: Transport,
    identity: AuthorSubject,
    trust: any,
    claims: Map<string, Value>,
    cursor: any,
    peer: any,
    downstreamFates: any[],
    startupError: DbError | undefined,
  ): LocalMutex<PeerConnection> {
    const localReceiver = this.receivesCommitsAsLocal();
    let ingestContext: any;
    let sessionClaims = claims;
    let sessionClaimRevision = 0;
    if (cursor) {
      if (cursor.ingestContext.identity !== identity)
        throw new Error("a resume cursor may only be used by its authenticated identity");
      // Catalogue receipt is per physical connection.
      cursor.peer.resetCatalogueSnapshotAnnouncement();
      peer = cursor.peer;
      ingestContext = cursor.ingestContext;
      sessionClaims = cursor.sessionClaims;
      sessionClaimRevision = cursor.sessionClaimRevision;
    } else {
      ingestContext = {
        identity,
        trust,
        admittedWriteAuthorization: false,
        versionReceiptsValidated: false,
      };
    }
    const connectionEpoch = transport.connectionSessionContext()?.local.epoch ?? randomEpoch();
    transport.setTrustedEncoder(CommitUnitTrust.isTrusted(ingestContext.trust));
    const wireInboundContext = transport.wireInboundContext();
    const sharedAuxiliaryEndpoint = transport.sharedAuxiliaryEndpoint();
    const connection = new LocalMutex(
      new PeerConnection(this, {
        transport,
        connectionEpoch,
        startupError,
        downstreamFates,
        upstreamUploadDestination: undefined,
        sessionClaimRevision,
        link: ConnectionLink.subscriber({
          peer,
          ingestContext,
          sessionClaims,
          sessionClaimRevision,
          localReceiver,
        }),
        auxiliaryPump: new PeerIoPump(
          this.chunkResolver,
          this.localChunkReader,
          connectionEpoch,
          PeerIoPumpRole.Subscriber,
          wireInboundContext,
        ).withSharedAuxiliaryEndpoint(sharedAuxiliaryEndpoint),
      }),
    );
    this.connections.push(connection);
    this.scheduleTick(TickUrgency.Immediate);
    return connection;
  }

  /** Detach a previously attached peer connection from this node. */
  detachConnection(connection: LocalMutex<PeerConnection>): boolean {
    if (this.pendingDetaches.includes(connection)) return false;
    const detached = this.tryDetachConnectionNow(connection);
    if (detached !== undefined) return detached;
    this.pendingDetaches.push(connection);
    this.scheduleTick(TickUrgency.Immediate);
    return true;
  }

  /** Detach with every owner taken by `tryLock`, or undefined when one is suspended. */
  private tryDetachConnectionNow(connection: LocalMutex<PeerConnection>): boolean | undefined {
    const peers = [...this.connections];
    if (!peers.includes(connection)) return false;
    const guards = new Map<LocalMutex<PeerConnection>, PeerConnection>();
    for (const peer of peers) {
      const guard = peer.tryLock();
      if (!guard) {
        releaseGuards(guards);
        return undefined;
      }
      guards.set(peer, guard);
    }
    const node = this.node.tryLock();
    if (!node) {
      releaseGuards(guards);
      return undefined;
    }
    return this.detachConnectionWithGuards(connection, node, guards);
  }

  /** Complete queued synchronous detaches without awaiting an owner. */
  private drainPendingDetaches(): void {
    if (!this.pendingDetaches.length) return;
    const finished = this.pendingDetaches.filter(
      (c) => this.tryDetachConnectionNow(c) !== undefined,
    );
    this.pendingDetaches = this.pendingDetaches.filter((c) => !finished.includes(c));
    if (this.pendingDetaches.length) this.scheduleTick(TickUrgency.Deferred);
  }

  private static async acquirePeerInventory(
    peers: LocalMutex<PeerConnection>[],
  ): Promise<Map<LocalMutex<PeerConnection>, PeerConnection>> {
    const guards = new Map<LocalMutex<PeerConnection>, PeerConnection>();
    for (const peer of peers) guards.set(peer, await peer.lock());
    return guards;
  }

  private peerInventoryMatches(peers: LocalMutex<PeerConnection>[]): boolean {
    return (
      this.connections.length === peers.length && this.connections.every((c, i) => c === peers[i])
    );
  }

  /** Await a stable peer inventory before detach mutation. */
  async detachConnectionAsync(connection: LocalMutex<PeerConnection>): Promise<boolean> {
    for (;;) {
      const peers = [...this.connections];
      if (!peers.includes(connection)) return false;
      // Every concurrent detach uses registry order, never target-first.
      const guards = await Node.acquirePeerInventory(peers);
      const node = await this.node.lock();
      if (!this.peerInventoryMatches(peers)) {
        this.node.unlock();
        releaseGuards(guards);
        continue;
      }
      return this.detachConnectionWithGuards(connection, node, guards);
    }
  }

  private detachConnectionWithGuards(
    connection: LocalMutex<PeerConnection>,
    node: NodeState,
    guards: Map<LocalMutex<PeerConnection>, PeerConnection>,
  ): boolean {
    const target = guards.get(connection)!;
    guards.delete(connection);
    let nodeHeld = true;
    try {
      if (!this.connections.includes(connection)) return false;
      const connectionEpoch = target.connectionEpoch;
      if (target.link.kind === "Upstream")
        finishOpenSchemaConnection(
          this.openSchemaAdmission,
          connectionEpoch,
          new DbError(ErrorCode.Protocol, "upstream disconnected before schema admission"),
        );
      target.stagedInbound.length = 0;
      if (target.link.kind === "Upstream")
        target.link.state.missingBodyResends = new KeyedMap(subscriptionKeyString);
      this.currentRows.disconnect(connectionEpoch);
      const destination = target.upstreamUploadDestination;
      const reconnectAdvice: any[] = [];
      const terminalAdvice: any[] = [];
      const currentSessionClaims: [AuthorSubject, any, number][] =
        node.sessionClaimsWithRevisions();
      const claimsFor = (identity: AuthorSubject) =>
        currentSessionClaims.find(([current]) => current === identity)?.[1] ?? new Map();
      let authority: AuthorityContext | undefined;
      let upstreamEpoch: number | undefined;
      let transferable: Map<TxId, any[]> | undefined;
      let retiredRelay: [SubscriptionKey, any][] = [];
      if (target.link.kind === "Upstream") {
        const state = target.link.state;
        // Permission-advice futures outlive one transport; rebuild their bookkeeping.
        const queued = new Set<string>();
        for (const request of state.scopeLeaseManager.requests.values())
          for (const requestId of request.waiters) {
            if (!this.permissionAdviceWaiters.has(requestId) || queued.has(requestId)) continue;
            queued.add(requestId);
            const [identity, claims] = request.sessionClaimBinding;
            if (request.delegatedSession || claimsEqual(claimsFor(identity), claims))
              reconnectAdvice.push({
                kind: "AuthorizationScopeIntent",
                requestId,
                action: request.action,
                sessionClaimBinding: request.sessionClaimBinding,
                delegatedSession: request.delegatedSession,
              });
            // A successor can only prove its currently admitted session.
            else terminalAdvice.push(requestId);
          }
        for (const command of state.pending) {
          if (command.kind !== "AuthorizationScopeIntent") continue;
          const key: string = command.requestId;
          if (!this.permissionAdviceWaiters.has(key) || queued.has(key)) continue;
          queued.add(key);
          const binding = command.sessionClaimBinding;
          if (
            command.delegatedSession ||
            !binding ||
            claimsEqual(claimsFor(binding[0]), binding[1])
          )
            reconnectAdvice.push({ ...command });
          else terminalAdvice.push(key);
        }
        authority = state.expectedScopeAuthority;
        upstreamEpoch = connectionEpoch;
        transferable = takeReconnectableLargeValueUploads(state);
      } else {
        const state = target.link.state;
        retiredRelay = retireRelayUpstreamSubscriptionsForConnection(
          this.relayUpstreamSubscriptionOwners,
          connectionEpoch,
        );
        // A detached subscriber cannot later send a normal Unsubscribe.
        const groups = state.coverageGroups;
        state.coverageGroups = new KeyedMap(coverageKeyString);
        for (const [coverage, group] of groups) {
          for (const subscription of group.subscribers) {
            node.applyUnsubscribe(subscription);
            state.served.delete(subscription);
          }
          state.peer.forgetSubscriptionWithNode(node, coverageGroupSubscriptionKey(coverage));
        }
        node.releaseShapesForPeer(connectionEpoch);
        state.shapeRegistrations.clear();
        state.authorityScopeHydrations.clear();
      }
      this.node.unlock();
      nodeHeld = false;
      // Retire the auxiliary lane before releasing this connection.
      target.auxiliaryPump.disconnect();
      connection.unlock();
      this.pendingRelaySubscriptionRejections.delete(connectionEpoch);
      this.connections = this.connections.filter((c) => c !== connection);
      if (upstreamEpoch !== undefined) {
        this.remoteLink.upstreamDetached();
        this.outbox.forgetOnWire();
      }
      for (const requestId of terminalAdvice) {
        const waiter = this.permissionAdviceWaiters.get(requestId);
        this.permissionAdviceWaiters.delete(requestId);
        waiter?.("Unknown");
      }
      if (retiredRelay.length) {
        for (const [subscription] of retiredRelay)
          this.upstreamSubscriptions.push({ kind: "Unsubscribe", subscription });
        this.scheduleTick(TickUrgency.Immediate);
      }
      if (reconnectAdvice.length) {
        this.upstreamSubscriptions.push(...reconnectAdvice);
        this.scheduleTick(TickUrgency.Immediate);
      }
      if (destination && transferable && transferable.size) {
        const key = uploadDestinationKey(destination);
        let uploads = this.detachedLargeValueUploads.get(key);
        if (!uploads) this.detachedLargeValueUploads.set(key, (uploads = new Map()));
        mergeReconnectableLargeValueUploads(uploads, transferable);
      }
      if (
        upstreamEpoch !== undefined &&
        this.activeAuthorityViewReceipts?.connectionEpoch === upstreamEpoch
      ) {
        // A surviving parallel upstream must confirm the view again before settling.
        const retired = [...this.activeAuthorityViewReceipts.subscriptions];
        const owner = this.node.borrow();
        for (const subscription of retired) {
          try {
            owner.invalidateAuthorityResultSettlement(
              owner.authorityResultKeyForSubscription(subscription),
            );
          } catch {
            // Unknown subscriptions have nothing to invalidate.
          }
        }
        this.activeAuthorityViewReceipts = undefined;
        const fallback = [...this.connections]
          .reverse()
          .find((c) => guards.get(c)?.link.kind === "Upstream");
        const fallbackPeer = fallback && guards.get(fallback)!;
        fallbackPeer?.stageInboundWithoutAuthorityReceipt();
        this.activeAuthorityViewReceipts = fallbackPeer && {
          connectionEpoch: fallbackPeer.connectionEpoch,
          confirmationFloor: owner.committedGlobalTime(),
          subscriptions: new SortedSet(cmpSubscriptionKey),
          bindingViews: new Set(),
        };
        this.demoteAuthorityReceiptSubscriptions();
      }
      if (authority) {
        this.admittedUpstreamAuthorities = this.admittedUpstreamAuthorities.filter(
          (c) => !authorityEquals(c, authority!),
        );
        if (
          this.admittedUpstreamAuthority &&
          authorityEquals(this.admittedUpstreamAuthority, authority)
        ) {
          this.admittedUpstreamAuthority = this.admittedUpstreamAuthorities[0];
          this.scheduleTick(TickUrgency.Immediate);
        }
      }
      return true;
    } finally {
      if (nodeHeld) {
        this.node.unlock();
        connection.unlock();
      }
      releaseGuards(guards);
    }
  }

  private async tickConnection(
    handle: LocalMutex<PeerConnection>,
    stats: any,
    released: Set<TxId>,
    retired: LocalMutex<PeerConnection>[],
  ): Promise<boolean> {
    const connection = await handle.lock();
    try {
      let next: any;
      try {
        next = await connection.tick();
      } catch (error) {
        if (connection.link.kind === "Subscriber" && connection.transport.hasTerminalFailure()) {
          connection.auxiliaryPump.disconnect();
          retired.push(handle);
          return false;
        }
        throw error;
      }
      for (const txId of connection.takeReleasedOutboxTxIds()) released.add(txId);
      stats.subscriptionEvents += next.subscriptionEvents;
      stats.remoteSyncApplied += next.remoteSyncApplied;
      return next.remoteSyncApplied > 0;
    } finally {
      handle.unlock();
    }
  }

  /** Service every accepted subscriber connection once. */
  async tick(): Promise<any> {
    // Consume the cross-thread marker only at this owner boundary.
    this.markSubscriberConnectionsDirtyAfterQueryRuntimeWake();
    this.remoteLink.onTick();
    this.drainPendingDetaches();
    await this.drainTransactionAbandonments();
    await this.drainSubscriptionFinalizations();
    const stats = { subscriptionEvents: 0, remoteSyncApplied: 0 };
    const progressWaker = this.queryRuntimeWaker();
    const chunkCompletionGeneration = this.chunkResolver.completionGeneration();
    // A queued terminal still needs to be folded into the application stream.
    const localDirtyGeneration = this.localSubscriptionDirtyGeneration;
    if (
      localDirtyGeneration !== this.observedLocalSubscriptionDirtyGeneration ||
      this.chunkResolver.hasPendingLocalDemand() ||
      chunkCompletionGeneration !== this.observedChunkCompletionGeneration ||
      (await this.node.with((node) => node.hasPendingQueryRuntime()))
    ) {
      stats.subscriptionEvents += await refreshSubscriptionsIn(this.node, this, progressWaker);
      // Acknowledge only completed delivery.
      this.observedLocalSubscriptionDirtyGeneration = localDirtyGeneration;
      this.observedChunkCompletionGeneration = chunkCompletionGeneration;
    }
    let remoteSyncApplied = false;
    const released = new Set<TxId>();
    // A later connection can mutate Core state after an earlier subscriber's turn.
    const dirtyEpochBefore = this.subscriberDirtyEpoch;
    let connections = [...this.connections];
    let retired: LocalMutex<PeerConnection>[] = [];
    for (const handle of connections)
      if (await this.tickConnection(handle, stats, released, retired)) remoteSyncApplied = true;
    for (const connection of retired) await this.detachConnectionAsync(connection);
    connections = connections.filter((c) => !retired.includes(c));
    retired = [];
    const subscriberStateChanged = this.subscriberDirtyEpoch !== dirtyEpochBefore;
    if (remoteSyncApplied || subscriberStateChanged) {
      // A bare `Db` without a scheduler keeps the bounded second serve pass.
      if (this.scheduler.current) {
        for (const connection of connections) await connection.with((c) => c.markSubscriberDirty());
        this.scheduleTick(TickUrgency.AfterCurrentTurn);
      } else {
        for (const connection of connections) {
          const shouldTick =
            (await connection.with((c) => c.markSubscriberDirty())) || subscriberStateChanged;
          if (shouldTick) await this.tickConnection(connection, stats, released, retired);
        }
      }
    }
    for (const connection of retired) await this.detachConnectionAsync(connection);
    await this.advanceDeletionReconciliations();
    await this.reconcileScalarQueryInputs();
    if (released.size) this.releaseOutboxUploads(released);
    await this.resolveAuthorityWitnesses();
    return stats;
  }

  /** Settle first-load authority witnesses and retire released gates' coverage. */
  private async resolveAuthorityWitnesses(): Promise<void> {
    if (!this.remoteLink.hasWitnesses()) return;
    const retired = await this.node.with((owner) =>
      this.remoteLink.resolveWitnesses(
        (handles: any[]) =>
          handles.length > 0 &&
          handles.every((handle) => {
            try {
              const key = owner.authorityResultKeyForSubscription(handle.subscription);
              return (
                owner.hasSettledAuthorityResult(key) && !owner.openingPendingForAuthorityResult(key)
              );
            } catch {
              return false;
            }
          }),
      ),
    );
    if (retired.length)
      this.enqueueSubscriptionFinalization({
        state: undefined,
        openingUpstream: retired,
        openingLocal: undefined,
        acknowledgement: undefined,
      });
  }

  /** Public local-first queries and relay-owned scopes refresh extra scalar inputs. */
  private async reconcileScalarQueryInputs(): Promise<void> {
    for (const weak of [...this.subscriptions]) {
      const owner = weak.deref();
      if (!owner || owner.closed.value || !owner.scalarReconciliationEnabled) continue;
      const handle = owner.upstreamSubscriptionHandles[0];
      if (!handle) continue;
      const { shape, binding } = owner.kind;
      const request = {
        subscription: handle.subscription,
        shape,
        binding,
        opts: handle.coverage.opts,
        identity: owner.author,
        policyBinding: owner.requestIdentityClaims ?? [
          owner.author,
          this.node.borrow().sessionClaimsFor(owner.author),
        ],
      };
      const reconciliation = owner.scalarReconciliation;
      owner.scalarReconciliation = { pending: [], retryDelayMs: 0 };
      await this.advanceScalarReconciliation(
        request,
        owner.scalarAuthorityRevision,
        reconciliation,
        owner,
      );
      if (!owner.closed.value) owner.scalarReconciliation = reconciliation;
    }
    for (const [key] of [...this.relayUpstreamSubscriptionOwners]) {
      const owner = this.relayUpstreamSubscriptionOwners.get(key);
      if (!owner) continue;
      const reconciliation = owner.scalarReconciliation;
      owner.scalarReconciliation = { pending: [], retryDelayMs: 0 };
      await this.advanceScalarReconciliation(
        owner.request,
        owner.scalarAuthorityRevision,
        reconciliation,
        undefined,
      );
      const current = this.relayUpstreamSubscriptionOwners.get(key);
      if (current) current.scalarReconciliation = reconciliation;
    }
  }

  /** Re-run local subscription refresh on the next owner turn. */
  private markLocalSubscriptionsDirty(): void {
    this.subscriberDirtyEpoch++;
    this.localSubscriptionDirtyGeneration++;
    this.scheduleTick(TickUrgency.Immediate);
  }

  /** Drive deletion reconciliation; nothing is abandoned on a timer (INV-SYNC-48). */
  private async advanceDeletionReconciliations(): Promise<void> {
    for (const weak of [...this.subscriptions]) {
      const owner = weak.deref();
      if (!owner || owner.closed.value) continue;
      const since = deletionWaitingSince(owner.deletionReconciliation);
      if (since === undefined) continue;
      const elapsed = monotonicMs() - since;
      const expired = elapsed >= DELETION_DISCOVERY_LIMIT_MS;
      const reconciliation = owner.deletionReconciliation;
      owner.deletionReconciliation = { kind: "Disabled" };
      let next: any = reconciliation;
      switch (reconciliation.kind) {
        case "Pending":
          next = {
            kind: "Probing",
            rows: this.heldRowChecks.enqueue(reconciliation.rows, reconciliation.context),
            since: reconciliation.since,
          };
          break;
        case "Probing":
          // Resolved, or settlement stops waiting; unresolved rows keep being checked.
          if (expired || reconciliation.rows.every((row: any) => !this.heldRowChecks.rows.has(row)))
            next = { kind: "Done" };
          break;
        case "Deferred":
        case "Discovering":
          if (expired) {
            const redundant = this.heldRowChecks.adoptDiscovery(reconciliation);
            if (redundant)
              await this.node.with((node) => releaseRedundantDiscovery(node, redundant));
            next = { kind: "Done" };
          }
          break;
      }
      const done = next.kind === "Done";
      const deferredStep = next.kind === "Deferred";
      const discovering = deferredStep || next.kind === "Discovering";
      owner.deletionReconciliation = next;
      if (done) {
        this.markLocalSubscriptionsDirty();
        continue;
      }
      // Re-arm discovery on a short cadence within the bound.
      if (
        discovering &&
        (!deferredStep || !(await this.node.with((node) => node.hasPendingQueryRuntime())))
      )
        this.localSubscriptionDirtyGeneration++;
      const scheduler = this.scheduler.current;
      if (scheduler) {
        const remaining = Math.max(1, Math.max(0, DELETION_DISCOVERY_LIMIT_MS - elapsed));
        scheduler.scheduleTickAfter(discovering ? Math.min(remaining, 50) : remaining);
      }
    }
    await this.advanceRuntimeDeletionDiscoveries();
    const heldBefore = this.heldRowChecks.rows.size;
    await this.advanceHeldRowChecks();
    // Release waiting streams in this same turn.
    const routable = this.admittedUpstreamAuthority !== undefined;
    const heldAfter = this.heldRowChecks.rows.size;
    if (heldAfter < heldBefore || (!routable && heldAfter > 0))
      this.releaseProbingStreams(routable);
  }

  /** Mark `Probing` streams as `Done` so they refresh and settle. */
  private releaseProbingStreams(routable: boolean): void {
    let released = false;
    for (const weak of [...this.subscriptions]) {
      const state = weak.deref();
      if (!state || state.closed.value) continue;
      const reconciliation = state.deletionReconciliation;
      if (reconciliation.kind !== "Probing") continue;
      const resolved =
        !routable || reconciliation.rows.every((row: any) => !this.heldRowChecks.rows.has(row));
      if (resolved) {
        state.deletionReconciliation = { kind: "Done" };
        released = true;
      }
    }
    if (released) this.markLocalSubscriptionsDirty();
  }

  /** Finish discoveries whose stream closed or stopped waiting. */
  private async advanceRuntimeDeletionDiscoveries(): Promise<void> {
    if (!this.heldRowChecks.discoveries.length) return;
    const discoveries = this.heldRowChecks.discoveries;
    this.heldRowChecks.discoveries = [];
    const waker = this.queryRuntimeWaker();
    const remaining: any[] = [];
    const found: [any[], any][] = [];
    let failure: unknown;
    await this.node.with(async (node) => {
      const runtimeToken = node.grooveRuntimeToken();
      for (const discovery of discoveries) {
        if (failure) {
          remaining.push(discovery);
          continue;
        }
        if (discovery.kind !== "Discovering" && discovery.kind !== "Deferred") continue;
        // A runtime rebuild retired the old graph with its IDs.
        const maintained =
          discovery.kind === "Discovering" && discovery.runtimeToken === runtimeToken
            ? discovery.maintained
            : undefined;
        const spec = discovery.spec;
        const since = monotonicMs();
        try {
          const step = maintained
            ? await drainDeletionDiscovery(node, maintained, waker)
            : await openDeletionDiscovery(node, spec, waker, false);
          if (step.kind === "Cold") {
            remaining.push({
              kind: "Discovering",
              maintained: step.maintained,
              runtimeToken,
              since,
              spec,
            });
            continue;
          }
          try {
            found.push([await deletionCandidates(node, step.snapshot, spec), spec.context]);
          } catch (error) {
            remaining.push({ kind: "Deferred", since, spec });
            failure = error;
          }
        } catch (error) {
          remaining.push({ kind: "Deferred", since, spec });
          failure = error;
        }
      }
    });
    for (const [rows, context] of found) this.heldRowChecks.enqueue(rows, context);
    const pending = remaining.length > 0;
    this.heldRowChecks.discoveries = [...remaining, ...this.heldRowChecks.discoveries];
    if (pending) this.scheduler.current?.scheduleTickAfter(100);
    if (failure) throw failure;
  }

  /** Probe held rows in `MAX_CURRENT_ROWS` batches through the current-rows path. */
  private async advanceHeldRowChecks(): Promise<void> {
    const upstream = this.admittedUpstreamAuthority?.connectionId;
    const now = monotonicMs();
    const checks = this.heldRowChecks;
    if (!checks.rows.size) {
      checks.active = undefined;
      checks.upstreamConnection = upstream;
      return;
    }
    if (upstream !== undefined && checks.upstreamConnection !== upstream) {
      // A new upstream link: retry every held row immediately.
      for (const check of checks.rows.values()) {
        check.attempts = 0;
        check.retryAt = undefined;
      }
    }
    checks.upstreamConnection = upstream;
    let changed = false;
    if (checks.active) {
      const probe = checks.active;
      const polled = probe.future.poll(this.queryRuntimeWaker());
      const result = polled?.ok
        ? polled.value
        : polled
          ? { kind: "Unknown" }
          : now >= probe.deadline
            ? { kind: "Unknown" }
            : undefined;
      if (result) {
        checks.active = undefined;
        const answered = new Set<string>();
        if (result.kind === "Applied") {
          const receipt = result.receipt;
          receipt.rows.forEach((row: any, i: number) => {
            if (receipt.outcomes[i] !== CurrentRowOutcome.Unknown)
              answered.add(heldRowKey([row.table, row.row]));
          });
        }
        changed = answered.size > 0;
        for (const key of probe.rows) {
          if (answered.has(heldRowKey(key))) checks.rows.delete(key);
          else {
            const check = checks.rows.get(key);
            if (check) {
              check.retryAt = now + HeldRowChecks.retryDelay(check.attempts);
              check.attempts++;
            }
          }
        }
      }
    }
    if (!checks.active && upstream !== undefined) {
      const held = [...checks.rows].map(([key, check]: [any, any]) => [key, check.coordinate]);
      // Cancel rows whose local version changed since discovery.
      const changedRows = await this.node.with((node) =>
        held
          .filter(([, coordinate]) => {
            try {
              return !coordinatesEqual(
                node.currentRowCoordinate(coordinate.table, coordinate.row),
                coordinate,
              );
            } catch {
              return true;
            }
          })
          .map(([key]) => key),
      );
      for (const key of changedRows) checks.rows.delete(key);
      const ready = [...checks.rows].filter(
        ([, check]: [any, any]) => check.retryAt === undefined || check.retryAt <= now,
      );
      if (ready.length) {
        const context = ready[0][1].context;
        const keys = ready
          .filter(([, check]: [any, any]) => policyBindingEquals(check.context, context))
          .map(([key]) => key)
          .slice(0, MAX_CURRENT_ROWS);
        const batch = keys.map((key) => checks.rows.get(key).coordinate);
        checks.active = {
          rows: keys,
          deadline: now + 10_000,
          future: new Polled(() => this.requestCurrentRows(batch, context)),
        };
        this.scheduler.current?.scheduleTickAfter(10_000);
        this.scheduleTick(TickUrgency.Immediate);
      }
    }
    // Wake for the earliest backoff.
    let nextRetry: number | undefined;
    for (const check of checks.rows.values())
      if (check.retryAt !== undefined && check.retryAt > now)
        nextRetry = Math.min(nextRetry ?? Infinity, check.retryAt);
    if (
      !checks.active &&
      upstream !== undefined &&
      nextRetry !== undefined &&
      this.scheduler.current
    )
      this.scheduler.current.scheduleTickAfter(Math.max(1, nextRetry - now));
    if (changed) this.markLocalSubscriptionsDirty();
  }

  requestCurrentRows(rows: any[], context: any): Promise<any> {
    return this.currentRows.request(this, rows, context);
  }

  private async advanceScalarReconciliation(
    request: any,
    revision: number,
    state: any,
    localOwner: any | undefined,
  ): Promise<void> {
    if (!request.opts.readView.isDefault() || !simpleScalarExitQuery(request.shape.query())) return;
    const receipts = this.activeAuthorityViewReceipts;
    if (!receipts?.subscriptions.has(request.subscription)) {
      resetScalar(state);
      return;
    }
    const generation: [number, SubscriptionKey, number] = [
      receipts.connectionEpoch,
      request.subscription,
      revision,
    ];
    // An authority connection replacement cancels immediately.
    if (
      state.generation &&
      (state.generation[0] !== generation[0] ||
        subscriptionKeyString(state.generation[1]) !== subscriptionKeyString(generation[1]))
    )
      resetScalar(state);
    if (state.active) {
      const polled = state.active.future.poll(this.queryRuntimeWaker());
      let result: any;
      if (polled) result = polled.ok ? polled.value : { kind: "Unknown" };
      else if (monotonicMs() >= state.active.deadline) result = { kind: "Unknown" };
      else return;
      const retryRows: any[] =
        result.kind === "Unknown"
          ? [...state.active.rows]
          : result.receipt.rows
              .filter(
                (_: any, i: number) => result.receipt.outcomes[i] === CurrentRowOutcome.Unknown,
              )
              .map((row: any) => row.row);
      state.active = undefined;
      if (result.kind === "Applied") {
        const receipt = result.receipt;
        // This live original owner additionally gates marker application.
        const [identity, claims] = request.policyBinding ?? [request.identity, new Map()];
        if (
          !policyBindingEquals(
            receipt.context,
            PolicyBindingKey.fromCanonicalParts(identity, claims),
          )
        ) {
          resetScalar(state);
          return;
        }
        const owner = await this.node.lock();
        try {
          const ownerGone = localOwner
            ? localOwner.closed.value ||
              !localOwner.upstreamSubscriptionHandles.some(
                (h: any) =>
                  subscriptionKeyString(h.subscription) ===
                  subscriptionKeyString(request.subscription),
              )
            : ![...this.relayUpstreamSubscriptionOwners.valuesArray()].some(
                (o) =>
                  subscriptionKeyString(o.request.subscription) ===
                    subscriptionKeyString(request.subscription) &&
                  policyBindingTupleEquals(o.request.policyBinding, request.policyBinding),
              );
          const active = this.activeAuthorityViewReceipts;
          if (
            ownerGone ||
            !(
              active &&
              active.connectionEpoch === generation[0] &&
              active.subscriptions.has(request.subscription)
            )
          ) {
            resetScalar(state);
            return;
          }
          const node = owner.scopedActiveSessionClaims(
            receipt.context.identity,
            receipt.context.claims(),
          );
          const scope = node.localReadPolicyBinding(receipt.context.identity);
          if (scope) {
            const outcomes: [any, any, any][] = [];
            receipt.rows.forEach((row: any, i: number) => {
              const outcome = receipt.outcomes[i];
              // Availability constrains settled input; Unknown is skipped.
              if (outcome === CurrentRowOutcome.Readable)
                outcomes.push([row.physicalTable, row.row, LocalRowAvailability.Readable]);
              else if (outcome === CurrentRowOutcome.CurrentUnavailable)
                outcomes.push([
                  row.physicalTable,
                  row.row,
                  LocalRowAvailability.CurrentUnavailable,
                ]);
            });
            if (outcomes.length) {
              node.activateLocalAvailabilityAuthority(scope, receipt.core, receipt.coreEpoch);
              const watermark: LocalAvailabilityWatermark = {
                core: receipt.core,
                coreEpoch: receipt.coreEpoch,
                claimsRevision: receipt.claimsRevision,
                policyEpoch: receipt.policyEpoch,
                settledThrough: receipt.settledThrough,
                authorizationProgress: receipt.authorizationProgress,
              };
              if (await node.applyVerifiedLocalRowAvailability(scope, watermark, outcomes)) {
                this.subscriberDirtyEpoch++;
                this.localSubscriptionDirtyGeneration++;
                this.scheduleTick(TickUrgency.Immediate);
              }
            }
          }
        } finally {
          this.node.unlock();
        }
      }
      if (!retryRows.length) {
        if (!state.pending.length) state.retryDelayMs = 0;
      } else {
        state.pending.push(...retryRows);
        state.retryDelayMs =
          state.retryDelayMs === 0 ? 100 : Math.min(state.retryDelayMs * 2, 2_000);
        state.retryAt = monotonicMs() + state.retryDelayMs;
        this.scheduler.current?.scheduleTickAfter(state.retryDelayMs);
      }
    }
    if (state.retryAt !== undefined) {
      if (monotonicMs() < state.retryAt) return;
      state.retryAt = undefined;
    }
    const sameGeneration =
      state.generation &&
      state.generation[0] === generation[0] &&
      subscriptionKeyString(state.generation[1]) === subscriptionKeyString(generation[1]) &&
      state.generation[2] === generation[2];
    if (!sameGeneration && !state.pending.length) {
      const stop = await this.node.with(async (owner) => {
        const table = request.shape.query().table;
        const key = owner.authorityResultKeyForSubscription(request.subscription);
        if (
          !owner.hasSettledAuthorityResult(key) ||
          owner.openingPendingForAuthorityResult(key) ||
          owner.publicationDeferredForAuthorityResult(key) ||
          owner.authoritySourceClosureGeneration(key) === undefined
        )
          return true;
        const authoritative: Set<string> = owner.scalarAuthorityInputRows(key, table);
        const node = owner.scopedOptionalSessionClaims(
          request.identity,
          request.policyBinding?.[1],
        );
        // Public local-first output is the candidate inventory.
        const local: any[] = localOwner
          ? localOwner.snapshot.rows
              .slice(0, localOwner.snapshot.rootCount)
              .map((row: any) => row.rowUuid())
          : (
              await node.queryRowsForClient(
                request.shape,
                request.binding,
                DurabilityTier.Local,
                request.identity,
              )
            ).map((row: any) => row.rowUuid());
        const candidates = new SortedSet<any>(
          cmpStr,
          local.filter((row) => !authoritative.has(row)),
        );
        const scope = node.localReadPolicyBinding(request.identity);
        if (scope) {
          try {
            const tableId = node.localAvailabilityTableId(request.shape.schemaVersion(), table);
            // A fresh inclusion must revalidate a hidden row as well.
            for (const row of authoritative)
              if (node.isLocalRowUnavailable(scope, tableId, row)) candidates.add(row);
          } catch {
            // Tables without local availability state contribute nothing.
          }
        }
        state.pending = candidates.toArray();
        state.generation = generation;
        return false;
      });
      if (stop) return;
    }
    const batch = state.pending.splice(0, Math.min(state.pending.length, 64));
    if (!batch.length) return;
    const candidates = await this.node.with(async (node) => {
      const table = request.shape.query().table;
      const out: any[] = [];
      for (const row of batch) {
        const tx = await node.localContentWinnerTxId(table, row);
        if (tx === undefined) continue;
        const record = await node.transactionRecord(tx);
        if (
          record &&
          record.fate.kind === "Accepted" &&
          record.durability >= DurabilityTier.Global
        ) {
          try {
            out.push(node.currentRowCoordinate(table, row));
          } catch {
            // Rows without a current coordinate are not probed.
          }
        }
      }
      return out;
    });
    if (candidates.length) {
      const [identity, claims] = request.policyBinding ?? [request.identity, new Map()];
      const context = { identity, canonicalClaims: new CanonicalPolicyClaims(claims) };
      state.active = {
        deadline: monotonicMs() + 5_000,
        rows: candidates.map((row) => row.row),
        future: new Polled(() => this.requestCurrentRows(candidates, context)),
      };
      this.scheduler.current?.scheduleTickAfter(5_000);
      this.scheduleTick(TickUrgency.Immediate);
    } else if (state.pending.length) {
      this.scheduleTick(TickUrgency.AfterCurrentTurn);
    }
  }

  private releaseOutboxUploads(releasedTxIds: Set<TxId>): void {
    const released: Set<TxId> = this.outbox.removeReleased(new Set(releasedTxIds));
    for (const connection of this.connections)
      connection.borrow().forgetReleasedOutboxTxIds(released);
    this.largeValueUploadRetryDeadlines.retain((txId) => !released.has(txId));
    for (const [key, uploads] of [...this.detachedLargeValueUploads]) {
      for (const txId of [...uploads.keys()]) if (released.has(txId)) uploads.delete(txId);
      if (!uploads.size) this.detachedLargeValueUploads.delete(key);
    }
  }
}

// ---------------------------------------------------------------------------
// Free helpers.

const AuthorSubject_SYSTEM = "System" as AuthorSubject;

function unwrap<T>(r: PollResult<T>): T {
  if (r.ok) return r.value;
  throw r.error;
}

function waitOutcomeFromState(
  [fate, globalTime, durability]: [any, any, any],
  txId: TxId,
  publicTxId: TxId,
  tier: any,
): PollResult<TxId> | undefined {
  const satisfied = transactionSatisfiesWait(fate, globalTime, durability, tier);
  if (fate.kind === "Rejected") return { ok: false, error: writeRejected(publicTxId, fate.reason) };
  return satisfied ? { ok: true, value: txId } : undefined;
}

function releaseGuards(guards: Map<LocalMutex<PeerConnection>, PeerConnection>): void {
  for (const mutex of guards.keys()) mutex.unlock();
  guards.clear();
}

const randomEpoch = (): number => new DataView(uuidV4().buffer).getUint32(0) * 2 ** 21;
const uploadDestinationKey = (d: { remoteNode: any; linkIdentity: any }) =>
  `${String(d.remoteNode)}|${dbg(d.linkIdentity)}`;
export const coverageKeyString = (coverage: CoverageKey): string => dbg(coverage);
const heldRowKey = ([table, row]: [string, any]) => `${table}|${String(row)}`;
const authorityEquals = (a: AuthorityContext, b: AuthorityContext) => dbg(a) === dbg(b);
const claimsEqual = (a: any, b: any) => dbg(a) === dbg(b);
const coordinatesEqual = (a: any, b: any) => dbg(a) === dbg(b);
const policyBindingEquals = (a: any, b: any) => dbg(a) === dbg(b);
const policyBindingTupleEquals = (a: any, b: any) => dbg(a) === dbg(b);

function resetScalar(state: any): void {
  state.pending = [];
  state.active = undefined;
  state.generation = undefined;
  state.retryAt = undefined;
  state.retryDelayMs = 0;
}

function deletionWaitingSince(r: any): number | undefined {
  switch (r.kind) {
    case "Deferred":
    case "Discovering":
    case "Pending":
    case "Probing":
      return r.since;
    default:
      return undefined;
  }
}

/** Resolve a public stream's authority lifecycle from its own registered usage site. */
function authorityResultKeyForStream(
  node: NodeState,
  handles: any[],
  bindingView: any,
): any | undefined {
  let key: any;
  for (const handle of handles) {
    let candidate: any;
    try {
      candidate = node.authorityResultKeyForSubscription(handle.subscription);
    } catch {
      continue;
    }
    if (dbg(candidate.bindingView) !== dbg(bindingView)) continue;
    if (key === undefined) key = candidate;
    else if (dbg(candidate) !== dbg(key)) return undefined;
  }
  return key;
}

function subscriptionNeedsTargetedRefresh(state: any, changedLogicalTables: Set<string>): boolean {
  const maintained = state.kind.maintainedSubscription;
  return !maintained || maintained.usesLogicalTables(changedLogicalTables);
}

/** The answer of `checkDeletionReconciliation` when it needs no node work. */
function deletionReconciliationKnown(state: any): boolean | undefined {
  switch (state.deletionReconciliation.kind) {
    case "Disabled":
    case "Done":
      return false;
    case "Pending":
    case "Probing":
      return true;
    default:
      return state.upstreamSubscriptionHandles.length === 0 ? false : undefined;
  }
}

/** Compare a fresh strict stream's first settled authority answer with held rows. */
async function checkDeletionReconciliation(
  node: SharedNodeState,
  state: any,
  streamGraph: any | undefined,
  progressWaker: Waker | undefined,
): Promise<boolean> {
  const known = deletionReconciliationKnown(state);
  if (known !== undefined) return known;
  const subscription = state.upstreamSubscriptionHandles[0].subscription;
  let previous = state.deletionReconciliation;
  state.deletionReconciliation = { kind: "Unchecked" };
  const { shape, binding } = state.kind;
  const author = state.author;
  const requestClaims = state.requestIdentityClaims;
  const restore = (r: any) => (state.deletionReconciliation = r);
  const owner = await node.lock();
  try {
    const runtimeToken = owner.grooveRuntimeToken();
    // A runtime rebuild retired the old discovery graph with its IDs.
    if (previous.kind === "Discovering" && previous.runtimeToken !== runtimeToken)
      previous = { kind: "Deferred", since: previous.since, spec: previous.spec };
    let key: any;
    try {
      key = owner.authorityResultKeyForSubscription(subscription);
    } catch {
      key = undefined;
    }
    if (
      key !== undefined &&
      !(
        owner.hasSettledAuthorityResult(key) &&
        !owner.openingPendingForAuthorityResult(key) &&
        !owner.publicationDeferredForAuthorityResult(key) &&
        owner.authoritySourceClosureGeneration(key) !== undefined
      )
    )
      key = undefined;
    if (key === undefined) {
      // Nothing can claim settlement yet; compare once an answer settles.
      restore(previous);
      return previous.kind !== "Unchecked";
    }
    const table = shape.query().table;
    const authoritative = owner.scalarAuthorityInputRows(key, table);
    let since: number;
    let spec: any;
    let maintained: any;
    if (previous.kind === "Discovering") ({ since, spec, maintained } = previous);
    else if (previous.kind === "Deferred") ({ since, spec } = previous);
    else {
      const [identity, claims] = requestClaims ?? [author, owner.sessionClaimsFor(author)];
      since = monotonicMs();
      spec = {
        shape,
        binding,
        author,
        context: { identity, canonicalClaims: new CanonicalPolicyClaims(claims) },
        claims,
        authoritative: new Set(),
      };
    }
    // The freshest settled answer decides which held rows are omitted.
    spec.authoritative = authoritative;
    let step: DiscoveryStep;
    if (maintained) step = await drainDeletionDiscovery(owner, maintained, progressWaker);
    else {
      // The stream's own receiver graph must have installed and evaluated this closure first.
      const generation = owner.authoritySourceClosureGeneration(key);
      const ready =
        streamGraph &&
        streamGraph.hasCoveredInputSources() &&
        generation !== undefined &&
        streamGraph.hasInstalledCoveredClosure(key, generation) &&
        !owner.subscriptionHasPendingQueryRuntime(streamGraph.subscriptionId());
      if (!ready) {
        restore({ kind: "Deferred", since, spec });
        return true;
      }
      const drive = !owner.hasPendingQueryRuntime();
      step = await openDeletionDiscovery(owner, spec, progressWaker, drive);
    }
    if (step.kind === "Cold") {
      restore({ kind: "Discovering", maintained: step.maintained, runtimeToken, since, spec });
      return true;
    }
    const rows = await deletionCandidates(owner, step.snapshot, spec);
    if (!rows.length) {
      restore({ kind: "Done" });
      return false;
    }
    restore({ kind: "Pending", rows, context: spec.context, since });
    return true;
  } finally {
    node.unlock();
  }
}

type DiscoveryStep = { kind: "Cold"; maintained: any } | { kind: "Snapshot"; snapshot: any };

/** Open the Local-tier discovery graph of `spec`'s query. */
async function openDeletionDiscovery(
  owner: NodeState,
  spec: any,
  progressWaker: Waker | undefined,
  drive: boolean,
): Promise<DiscoveryStep> {
  const scoped = owner.scopedOptionalSessionClaims(spec.author, spec.claims);
  const [maintained, snapshot] =
    await scoped.openMaintainedViewSubscriptionInAuthorizationModeWithWaker(
      spec.shape,
      spec.binding,
      spec.author,
      DurabilityTier.Local,
      ReadViewSpec.default(),
      undefined,
      QueryAuthorizationMode.ClientLocal,
      false,
      progressWaker,
    );
  if (maintained.initialSnapshotReceived()) {
    owner.unsubscribeGrooveSubscription(maintained.subscriptionId());
    return { kind: "Snapshot", snapshot };
  }
  if (!drive) return { kind: "Cold", maintained };
  // A cold opening needs only local storage work here.
  await owner.driveQueryRuntime();
  return drainDeletionDiscovery(owner, maintained, progressWaker);
}

/** Drain a cold discovery graph without blocking on other query work. */
async function drainDeletionDiscovery(
  owner: NodeState,
  maintained: any,
  progressWaker: Waker | undefined,
): Promise<DiscoveryStep> {
  try {
    await owner.driveReadyQueryRuntimeWithWaker(progressWaker);
    await owner.drainLocalMaintainedViewSubscriptionPreservingRowsWithWaker(
      maintained,
      undefined,
      new Set(),
      progressWaker,
    );
  } catch (error) {
    owner.unsubscribeGrooveSubscription(maintained.subscriptionId());
    throw error;
  }
  if (!maintained.initialSnapshotReceived()) return { kind: "Cold", maintained };
  try {
    const materialized =
      await owner.materializeLocalMaintainedRelationSnapshotWithOccurrences(maintained);
    return { kind: "Snapshot", snapshot: materialized.snapshot };
  } finally {
    owner.unsubscribeGrooveSubscription(maintained.subscriptionId());
  }
}

/** Held rows the authority omitted whose local winner is an accepted Global transaction. */
async function deletionCandidates(owner: NodeState, snapshot: any, spec: any): Promise<any[]> {
  const table = spec.shape.query().table;
  const rows: any[] = [];
  for (const entry of snapshot.rows.slice(0, snapshot.rootCount)) {
    const row = entry.rowUuid();
    if (spec.authoritative.has(row)) continue;
    const tx = await owner.localContentWinnerTxId(table, row);
    if (tx === undefined) continue;
    const record = await owner.transactionRecord(tx);
    if (!record || record.fate.kind !== "Accepted" || record.durability < DurabilityTier.Global)
      continue;
    try {
      rows.push(owner.currentRowCoordinate(table, row));
    } catch {
      // No current coordinate: nothing to probe.
    }
  }
  return rows;
}

/** Release the discovery graph of a discovery superseded by an equivalent one. */
function releaseRedundantDiscovery(owner: NodeState, redundant: any | undefined): void {
  if (redundant?.kind === "Discovering" && owner.grooveRuntimeToken() === redundant.runtimeToken)
    owner.unsubscribeGrooveSubscription(redundant.maintained.subscriptionId());
}

/** The runtime-owned fields refresh needs; `Node` satisfies it. */
export type RefreshRuntime = { subscriptions: WeakRef<any>[]; activeAuthorityViewReceipts: any };

export function refreshSubscriptionsIn(
  node: SharedNodeState,
  rt: RefreshRuntime,
  progressWaker: Waker | undefined,
): Promise<number> {
  return refreshSubscriptionsInWithTables(node, rt, undefined, progressWaker);
}

function bindingView(shape: any, binding: any, tier: any, readView: any): any {
  return {
    shapeId: shape.shapeId(),
    bindingId: binding.bindingId(),
    readView: RegisterShapeOptions.with({ tier, readView }).readViewKey(),
  };
}

/** Swap the maintained handle out of the stream while refresh awaits, restoring it after. */
class DetachedSubscriptionRefresh {
  maintained: any;
  snapshot: any;
  snapshotIndex: any;
  snapshotSource: any;
  settled: boolean;
  readonly sender: any;
  readonly localSubscriptionCleanup: { value: [number, any] | undefined };
  constructor(private readonly state: any) {
    this.maintained = state.kind.maintainedSubscription;
    state.kind.maintainedSubscription = undefined;
    this.snapshot = state.snapshot;
    this.snapshotIndex = state.snapshotIndex;
    state.snapshot = undefined;
    state.snapshotIndex = new RelationSnapshotIndex();
    this.snapshotSource = state.snapshotSource;
    this.settled = state.settled;
    this.sender = state.sender;
    this.localSubscriptionCleanup = state.localSubscriptionCleanup;
  }
  restore(): void {
    this.state.kind.maintainedSubscription = this.maintained;
    this.maintained = undefined;
    this.state.snapshot = this.snapshot;
    this.state.snapshotIndex = this.snapshotIndex;
    this.state.snapshotSource = this.snapshotSource;
    this.state.settled = this.settled;
  }
}

async function publishEvent(
  node: SharedNodeState,
  sender: any,
  shape: any,
  event: any,
  before: any,
  snapshot: any,
  index: any,
): Promise<boolean> {
  const materialized = sender.materialized(node.borrow(), shape, event);
  return sender.publish(event, before, snapshot, index, materialized);
}

export async function refreshSubscriptionsInWithTables(
  node: SharedNodeState,
  rt: RefreshRuntime,
  changedTables: Set<string> | undefined,
  progressWaker: Waker | undefined,
): Promise<number> {
  const retained: WeakRef<any>[] = [];
  let changed = 0;
  const pendingResets: SortedMap<any, number> = await node.with((n) =>
    n.snapshotPendingAuthoritativeResets(),
  );
  const reconciledResets = new Map<string, [any, number]>();
  const noteReset = (key: any) => {
    const generation = pendingResets.get(key);
    if (generation !== undefined) reconciledResets.set(dbg(key), [key, generation]);
  };
  await node.with((n) => n.driveReadyQueryRuntimeWithWaker(progressWaker));
  let changedLogicalTables: Set<string> | undefined;
  if (changedTables) {
    const logical: Set<string> = await node.with((n) =>
      n.logicalTableNamesForStorageTables(changedTables),
    );
    if (logical.size) changedLogicalTables = logical;
  }
  const settledFor = (
    shape: any,
    binding: any,
    tier: any,
    readView: any,
    requiresReceipt: boolean,
    key: any,
  ) =>
    subscriptionIsSettled(
      node.borrow(),
      rt.activeAuthorityViewReceipts,
      shape,
      binding,
      tier,
      readView,
      requiresReceipt,
      key,
    );
  for (const weak of [...rt.subscriptions]) {
    const state = weak.deref();
    if (!state) continue;
    // Never rehydrate a stream whose cleanup is merely waiting for this tick.
    if (state.closed.value) continue;
    if (changedLogicalTables && !subscriptionNeedsTargetedRefresh(state, changedLogicalTables)) {
      retained.push(weak);
      continue;
    }
    refreshVisits.count++;
    const {
      readTier,
      pendingOverlay,
      remoteReadTier,
      requiresAuthorityReceipt,
      readView,
      snapshotSource: previousSource,
      settled: previousSettled,
      author,
      authorizationMode,
      terminalRows,
    } = state;
    const handles = [...state.upstreamSubscriptionHandles];
    const awaitingInitialOwnerResult: boolean = state.pendingInitialOwnerResult;
    if (awaitingInitialOwnerResult) {
      const ready = await node.with(
        (owner) =>
          handles.length > 0 &&
          handles.every((handle) => {
            try {
              const key = owner.authorityResultKeyForSubscription(handle.subscription);
              return (
                owner.hasSettledAuthorityResult(key) && !owner.openingPendingForAuthorityResult(key)
              );
            } catch {
              return false;
            }
          }),
      );
      if (!ready) {
        retained.push(weak);
        continue;
      }
    }
    // A settled strict answer may omit rows this client still holds live.
    let deletionWithheld =
      deletionReconciliationKnown(state) ??
      (await checkDeletionReconciliation(node, state, undefined, progressWaker));
    const requestClaims = state.requestIdentityClaims?.[1];
    const grooveRuntimeToken: number = await node.with((n) => n.grooveRuntimeToken());
    const settledTier = remoteReadTier ?? readTier;
    if (state.grooveRuntimeToken !== grooveRuntimeToken) {
      // Recompile only after real runtime/plan invalidation.
      if (coveredInputTrace())
        console.error(
          `JAZZ_COVERED_INPUT_TRACE stage=reopen_runtime stale=${state.grooveRuntimeToken} current=${grooveRuntimeToken}`,
        );
      const staleId = state.kind.maintainedSubscription?.subscriptionId();
      if (staleId !== undefined) await node.with((n) => n.unsubscribeGrooveSubscription(staleId));
      const [shape, binding] = await node.with((owner) =>
        owner
          .scopedOptionalSessionClaims(author, requestClaims)
          .queryBindingForLink(state.kind.shape, state.kind.binding),
      );
      const previousSnapshot = materializedSubscriptionSnapshot(
        state.snapshot,
        state.snapshotIndex,
      );
      const previousSnapshotIndex = state.snapshotIndex.clone();
      let [maintained, snapshot] = await node.with((owner) =>
        owner
          .scopedOptionalSessionClaims(author, requestClaims)
          .openMaintainedViewSubscriptionInAuthorizationModeWithWaker(
            shape,
            binding,
            author,
            readTier,
            readView,
            undefined,
            authorizationMode,
            pendingOverlay,
            progressWaker,
          ),
      );
      const replacementIsCold = !maintained.initialSnapshotReceived();
      if (state.closed.value) {
        await node.with((n) => n.unsubscribeGrooveSubscription(maintained.subscriptionId()));
        continue;
      }
      const deliveredView = bindingView(shape, binding, readTier, readView);
      // From here cleanup must target the replacement runtime subscription.
      state.kind.maintainedSubscription = maintained;
      state.localSubscriptionCleanup.value = [grooveRuntimeToken, maintained.subscriptionId()];
      state.pendingInitialLocalSnapshot = replacementIsCold;
      state.pendingInitialOwnerResult = false;
      if (replacementIsCold) {
        // Own the replacement before yielding its cold initial batch.
        state.grooveRuntimeToken = grooveRuntimeToken;
        retained.push(weak);
        continue;
      }
      const snapshotIndex = relationSnapshotIndexWithRootOccurrences(
        snapshot,
        maintained.rootOccurrenceIds(),
      );
      snapshotIndex.terminalRecords = maintained.decodedTerminalRecords();
      const settledView = bindingView(shape, binding, settledTier, readView);
      const delivered = authorityResultKeyForStream(node.borrow(), handles, deliveredView);
      const settledKey = authorityResultKeyForStream(node.borrow(), handles, settledView);
      const pendingKey =
        (delivered !== undefined && pendingResets.has(delivered) ? delivered : undefined) ??
        (settledKey !== undefined && pendingResets.has(settledKey) ? settledKey : undefined);
      if (pendingKey !== undefined) {
        // A client receiver only advances from the exact CoveredInput closure.
        maintained = state.kind.maintainedSubscription;
        state.kind.maintainedSubscription = undefined;
        const nodeRef = await node.lock();
        if (state.closed.value) {
          node.unlock();
          state.kind.maintainedSubscription = maintained;
          continue;
        }
        let update: any;
        try {
          [update] = await nodeRef.drainLocalMaintainedViewSubscriptionPreservingRowsWithWaker(
            maintained,
            pendingKey,
            new Set(),
            progressWaker,
          );
        } finally {
          state.kind.maintainedSubscription = maintained;
          node.unlock();
        }
        if (update) {
          apply_maintained_update_to_snapshot(
            snapshot,
            snapshotIndex,
            update,
            shape.query().table,
            readTier,
            previousSettled,
            maintained.terminalRootLayout(),
          );
          materializeSubscriptionTerminalRecords(snapshot, snapshotIndex);
          noteReset(pendingKey);
        }
      }
      // Preserve the graph's complete tuple identities and current positions.
      const rootOccurrenceIds = snapshotIndex.roots.filter((r: any) => r !== undefined);
      const settled =
        !deletionWithheld &&
        settledFor(shape, binding, settledTier, readView, requiresAuthorityReceipt, settledKey);
      const event = subscriptionDeltaEventWithReset(
        readTier,
        settled,
        previousSnapshot,
        snapshot,
        false,
        terminalRows,
      );
      if (event.kind === "Delta") {
        event.reset = true;
        event.added = subscriptionOutputsWithOccurrenceSidecar(snapshot, rootOccurrenceIds);
        event.updated = [];
        event.removed = resetRemovedRoots(
          previousSnapshot,
          previousSnapshotIndex,
          rootOccurrenceIds,
        );
        event.publishable = settled || event.added.length > 0 || event.removed.length > 0;
      }
      const before = state.sender.checkpoint(
        readTier,
        settled,
        state.snapshot,
        state.snapshotIndex,
      );
      state.grooveRuntimeToken = grooveRuntimeToken;
      state.snapshot = relationSnapshotWithDeltaSlack(snapshot);
      state.snapshotIndex = RelationSnapshotIndex.withRootOccurrences(rootOccurrenceIds);
      state.snapshotIndex.terminalRecords =
        state.kind.maintainedSubscription?.decodedTerminalRecords() ?? [];
      state.snapshotSource = SubscriptionSnapshotSource.LocalMaintained;
      state.settled = settled;
      // A strict remote opening is intentionally absent until its closure arrives.
      if (
        await publishEvent(
          node,
          state.sender,
          shape,
          event,
          before,
          state.snapshot,
          state.snapshotIndex,
        )
      )
        changed++;
      retained.push(weak);
      continue;
    }
    const initialLocalSnapshotPending: boolean = state.pendingInitialLocalSnapshot;
    const refresh = new DetachedSubscriptionRefresh(state);
    let snapshot: any;
    let snapshotSource: any;
    let settled: boolean;
    let snapshotTier: any;
    let forceResetEvent: boolean;
    try {
      const { shape, binding } = state.kind;
      const hasMaintainedSubscription = refresh.maintained !== undefined;
      const delivered = authorityResultKeyForStream(
        node.borrow(),
        handles,
        bindingView(shape, binding, readTier, readView),
      );
      const settledKey = authorityResultKeyForStream(
        node.borrow(),
        handles,
        bindingView(shape, binding, settledTier, readView),
      );
      const remoteSettledTier =
        remoteReadTier !== undefined &&
        settledKey !== undefined &&
        node.borrow().hasSettledAuthorityResult(settledKey)
          ? remoteReadTier
          : undefined;
      const resetKey =
        (delivered !== undefined && pendingResets.has(delivered) ? delivered : undefined) ??
        (settledKey !== undefined && pendingResets.has(settledKey) ? settledKey : undefined);
      // Observation tier decides whether the remote closure is this stream's input frontier.
      const authorityScoped = readTier >= DurabilityTier.Global;
      const authoritativeReset = authorityScoped && resetKey !== undefined;
      if (
        settledKey !== undefined &&
        node.borrow().publicationDeferredForAuthorityResult(settledKey)
      ) {
        retained.push(weak);
        continue;
      }
      snapshotTier = remoteSettledTier ?? readTier;
      const isSettled = () =>
        !deletionWithheld &&
        settledFor(shape, binding, settledTier, readView, requiresAuthorityReceipt, settledKey);
      if (coveredInputTrace())
        console.error(
          `JAZZ_COVERED_INPUT_TRACE stage=refresh terminal_rows=${terminalRows} covered=${!!refresh.maintained?.hasCoveredInputSources()} reset_pending=${authoritativeReset} authoritative_reset=${authoritativeReset} delivered=${dbg(delivered)} settled=${dbg(settledKey)}`,
        );
      if (
        authoritativeReset &&
        !initialLocalSnapshotPending &&
        terminalRows &&
        !refresh.maintained?.hasCoveredInputSources()
      ) {
        // A client receiver may only reset after installing the exact closure.
        if (
          authorizationMode === QueryAuthorizationMode.ClientLocal &&
          remoteReadTier !== undefined
        ) {
          retained.push(weak);
          continue;
        }
        if (!refresh.maintained)
          throw new DbError(ErrorCode.Protocol, "structured subscription lost its Groove terminal");
        const staleId = refresh.maintained.subscriptionId();
        // Re-open the Groove terminal at an authoritative boundary.
        const [replacement, replacementSnapshot] = await node.with((n) =>
          n.openMaintainedViewSubscriptionInAuthorizationModeWithWaker(
            shape,
            binding,
            author,
            readTier,
            readView,
            undefined,
            authorizationMode,
            pendingOverlay,
            progressWaker,
          ),
        );
        await node.with((n) => n.unsubscribeGrooveSubscription(staleId));
        refresh.maintained = replacement;
        refresh.localSubscriptionCleanup.value = [grooveRuntimeToken, replacement.subscriptionId()];
        if (!replacement.initialSnapshotReceived()) {
          state.pendingInitialLocalSnapshot = true;
          retained.push(weak);
          continue;
        }
        snapshot = replacementSnapshot;
        snapshotSource = SubscriptionSnapshotSource.LocalMaintained;
        settled = isSettled();
        forceResetEvent = true;
      } else {
        let maintainedUpdate: any;
        let suppressedAuthoritativeChange = false;
        if (refresh.maintained) {
          const nodeRef = await node.lock();
          try {
            // Do not drain the detached receiver after its stream closed.
            if (state.closed.value) continue;
            // Only a scope-bound source consumes authority inputs.
            const authorityKey =
              authorizationMode === QueryAuthorizationMode.ClientLocal &&
              refresh.maintained.hasCoveredInputSources()
                ? settledKey
                : undefined;
            if (coveredInputTrace())
              console.error(
                `JAZZ_COVERED_INPUT_TRACE stage=runtime_drain sources=${refresh.maintained.hasCoveredInputSources()} authority=${dbg(authorityKey)}`,
              );
            try {
              const [update, suppressed] =
                await nodeRef.drainLocalMaintainedViewSubscriptionPreservingRowsWithWaker(
                  refresh.maintained,
                  authorityKey,
                  new Set(),
                  progressWaker,
                );
              // The receiver's evaluation is still waiting (#3349).
              if (
                !update &&
                authorityKey !== undefined &&
                nodeRef.coveredReceiverEvaluationPending(refresh.maintained)
              ) {
                retained.push(weak);
                continue;
              }
              maintainedUpdate = update;
              suppressedAuthoritativeChange = suppressed;
            } catch (error: any) {
              if (error?.kind !== "MissingTransaction") throw DbError.from(error);
              nodeRef.recordAuthoritativeResetMissingPayloadFallback();
              retained.push(weak);
              continue;
            }
          } finally {
            node.unlock();
          }
        }
        if (deletionWithheld)
          deletionWithheld = await checkDeletionReconciliation(
            node,
            state,
            refresh.maintained,
            progressWaker,
          );
        // A reset is publishable only after the receiver installed that exact closure.
        if (
          authoritativeReset &&
          authorizationMode === QueryAuthorizationMode.ClientLocal &&
          remoteReadTier !== undefined &&
          resetKey !== undefined
        ) {
          const generation = node.borrow().authoritySourceClosureGeneration(resetKey);
          const closureInstalled =
            !!refresh.maintained?.hasCoveredInputSources() &&
            generation !== undefined &&
            refresh.maintained.hasInstalledCoveredClosure(resetKey, generation);
          if (!closureInstalled) {
            retained.push(weak);
            continue;
          }
        }
        if (resetKey !== undefined) noteReset(resetKey);
        if (initialLocalSnapshotPending) {
          const replacementReady =
            !!refresh.maintained?.initialSnapshotReceived() &&
            (!awaitingInitialOwnerResult ||
              !node
                .borrow()
                .subscriptionHasPendingQueryRuntime(refresh.maintained.subscriptionId()));
          if (!replacementReady) {
            retained.push(weak);
            continue;
          }
          const previous = materializedSubscriptionSnapshot(
            refresh.snapshot,
            refresh.snapshotIndex,
          );
          const previousSnapshotIndex = refresh.snapshotIndex.clone();
          const materialized = await node.with((n) =>
            n.materializeLocalMaintainedRelationSnapshotWithOccurrences(refresh.maintained),
          );
          const before = refresh.sender.checkpoint(
            readTier,
            false,
            refresh.snapshot,
            refresh.snapshotIndex,
          );
          const replacement = materialized.snapshot;
          refresh.snapshot = relationSnapshotWithDeltaSlack(replacement);
          refresh.snapshotIndex = relationSnapshotIndexWithRootOccurrences(
            refresh.snapshot,
            materialized.rootOccurrenceIds,
          );
          refresh.snapshotIndex.terminalRecords = refresh.maintained.decodedTerminalRecords();
          const isNowSettled = isSettled();
          refresh.snapshotSource = SubscriptionSnapshotSource.LocalMaintained;
          refresh.settled = isNowSettled;
          state.pendingInitialLocalSnapshot = false;
          state.pendingInitialOwnerResult = false;
          const event = subscriptionDeltaEventWithReset(
            snapshotTier,
            isNowSettled,
            previous,
            replacement,
            true,
            terminalRows,
          );
          if (event.kind === "Delta") {
            event.added = subscriptionOutputsWithOccurrenceSidecar(
              replacement,
              materialized.rootOccurrenceIds,
            );
            event.updated = [];
            event.removed = resetRemovedRoots(
              previous,
              previousSnapshotIndex,
              materialized.rootOccurrenceIds,
            );
          }
          retained.push(weak);
          if (
            await publishEvent(
              node,
              refresh.sender,
              shape,
              event,
              before,
              refresh.snapshot,
              refresh.snapshotIndex,
            )
          )
            changed++;
          continue;
        }
        if (maintainedUpdate) {
          if (maintainedUpdate.kind === "Structured") {
            const ops = maintainedUpdate.terminalOperations;
            if (ops.length) {
              const isNowSettled = isSettled();
              const layout = refresh.maintained?.terminalRootLayout();
              if (!layout)
                throw new DbError(
                  ErrorCode.Protocol,
                  "terminal operation arrived without a prepared root layout",
                );
              const count = ops.length;
              if (coveredInputTrace())
                console.error(
                  `JAZZ_COVERED_INPUT_TRACE stage=terminal_ops count=${count} reset=${authoritativeReset}`,
                );
              const previousSnapshot = authoritativeReset
                ? materializedSubscriptionSnapshot(refresh.snapshot, refresh.snapshotIndex)
                : undefined;
              const before = refresh.sender.checkpoint(
                readTier,
                isNowSettled,
                refresh.snapshot,
                refresh.snapshotIndex,
              );
              let event = apply_terminal_operations_to_subscription_snapshot(
                refresh.snapshot,
                refresh.snapshotIndex,
                ops,
                undefined,
                layout,
                shape.query().table,
                snapshotTier,
                isNowSettled,
              );
              // A newly received authority closure is a reset boundary.
              if (authoritativeReset) {
                materializeSubscriptionTerminalRecords(refresh.snapshot, refresh.snapshotIndex);
                event = subscriptionDeltaEventWithReset(
                  snapshotTier,
                  isNowSettled,
                  previousSnapshot,
                  refresh.snapshot,
                  true,
                  terminalRows,
                );
              }
              if (coveredInputTrace())
                console.error(
                  `JAZZ_COVERED_INPUT_TRACE stage=publish_terminal ops=${count} roots=${refresh.snapshot.rootCount}`,
                );
              refresh.settled = isNowSettled;
              retained.push(weak);
              if (
                await publishEvent(
                  node,
                  refresh.sender,
                  shape,
                  event,
                  before,
                  refresh.snapshot,
                  refresh.snapshotIndex,
                )
              )
                changed++;
              continue;
            }
            // No terminal delta: the receiver-local collector state did not change.
          } else {
            const before = refresh.sender.checkpoint(
              readTier,
              false,
              refresh.snapshot,
              refresh.snapshotIndex,
            );
            const previousSnapshot = materializedSubscriptionSnapshot(
              refresh.snapshot,
              refresh.snapshotIndex,
            );
            let event = apply_maintained_update_to_snapshot(
              refresh.snapshot,
              refresh.snapshotIndex,
              maintainedUpdate,
              shape.query().table,
              snapshotTier,
              previousSettled,
              undefined,
            );
            refresh.snapshotSource = SubscriptionSnapshotSource.LocalMaintained;
            const isNowSettled =
              isSettled() &&
              node.borrow().relationSnapshotHasMaterializedRequiredCells(shape, refresh.snapshot);
            if (authoritativeReset)
              event = subscriptionDeltaEventWithReset(
                snapshotTier,
                isNowSettled,
                previousSnapshot,
                refresh.snapshot,
                true,
                terminalRows,
              );
            if (event.kind === "Delta") {
              event.publishable =
                previousSettled !== isNowSettled ||
                event.reset ||
                event.added.length > 0 ||
                event.updated.length > 0 ||
                event.removed.length > 0;
              event.settled = isNowSettled;
            }
            refresh.settled = isNowSettled;
            retained.push(weak);
            if (
              await publishEvent(
                node,
                refresh.sender,
                shape,
                event,
                before,
                refresh.snapshot,
                refresh.snapshotIndex,
              )
            )
              changed++;
            continue;
          }
        }
        if (authoritativeReset && refresh.maintained?.hasCoveredInputSources()) {
          const before = refresh.sender.checkpoint(
            readTier,
            false,
            refresh.snapshot,
            refresh.snapshotIndex,
          );
          // A no-op source handoff is not a user-visible reset.
          if (refresh.settled) {
            refresh.snapshotSource = SubscriptionSnapshotSource.LocalMaintained;
            retained.push(weak);
            continue;
          }
          // Aggregate fact terminals: their self-contained payload is the local terminal state.
          if (shape.query().aggregate !== undefined) {
            const materialized = await node.with((n) =>
              n.materializeLocalMaintainedRelationSnapshotWithOccurrences(refresh.maintained),
            );
            refresh.snapshot = materialized.snapshot;
          }
          const isNowSettled = isSettled();
          // A complete closure can be observably unchanged; still publish its reset.
          const current = materializedSubscriptionSnapshot(refresh.snapshot, refresh.snapshotIndex);
          const event = subscriptionDeltaEventWithReset(
            snapshotTier,
            isNowSettled,
            current,
            current,
            true,
            terminalRows,
          );
          if (coveredInputTrace())
            console.error(
              `JAZZ_COVERED_INPUT_TRACE stage=publish_covered_reset roots=${current.rootCount} settled=${isNowSettled}`,
            );
          refresh.snapshotSource = SubscriptionSnapshotSource.LocalMaintained;
          refresh.settled = isNowSettled;
          retained.push(weak);
          const deliveredReset = await publishEvent(
            node,
            refresh.sender,
            shape,
            event,
            before,
            refresh.snapshot,
            refresh.snapshotIndex,
          );
          if (coveredInputTrace())
            console.error(
              `JAZZ_COVERED_INPUT_TRACE stage=covered_reset_delivery delivered=${deliveredReset}`,
            );
          if (deliveredReset) changed++;
          continue;
        }
        const preserveLocalOverlay = suppressedAuthoritativeChange;
        if (terminalRows || remoteSettledTier !== undefined) {
          // Never rebuild a relation facade from authority output.
          snapshot = materializedSubscriptionSnapshot(refresh.snapshot, refresh.snapshotIndex);
          snapshotSource = SubscriptionSnapshotSource.LocalMaintained;
        } else if (preserveLocalOverlay || hasMaintainedSubscription) {
          snapshot = materializedSubscriptionSnapshot(refresh.snapshot, refresh.snapshotIndex);
          snapshotSource = previousSource;
        } else {
          snapshot = await node.with((n) =>
            n.subscriptionSnapshotInAuthorizationMode(
              shape,
              binding,
              readTier,
              author,
              readView,
              authorizationMode,
            ),
          );
          snapshotSource = SubscriptionSnapshotSource.LinkSnapshot;
        }
        settled = isSettled();
        forceResetEvent = preserveLocalOverlay;
      }
    } finally {
      refresh.restore();
    }
    const previous = materializedSubscriptionSnapshot(state.snapshot, state.snapshotIndex);
    const prevSource = state.snapshotSource;
    const hasMaintainedFlatTuple =
      state.kind.maintainedSubscription !== undefined &&
      state.kind.shape.query().flatJoin !== undefined;
    // Replace an unaddressable facade snapshot from the maintained terminal.
    if (
      !forceResetEvent &&
      hasMaintainedFlatTuple &&
      prevSource === SubscriptionSnapshotSource.LocalMaintained &&
      snapshotSource === SubscriptionSnapshotSource.LinkSnapshot
    ) {
      const maintained = state.kind.maintainedSubscription;
      const materialized = await node.with((n) =>
        n.materializeLocalMaintainedRelationSnapshotWithOccurrences(maintained),
      );
      snapshot = materialized.snapshot;
      snapshotSource = SubscriptionSnapshotSource.LocalMaintained;
    }
    if (forceResetEvent || !snapshotsEqual(snapshot, previous) || settled !== previousSettled) {
      const before = state.sender.checkpoint(
        readTier,
        settled,
        state.snapshot,
        state.snapshotIndex,
      );
      const event = forceResetEvent
        ? subscriptionDeltaEventWithReset(
            snapshotTier,
            settled,
            previous,
            snapshot,
            true,
            terminalRows,
          )
        : subscriptionDeltaEvent(snapshotTier, settled, previous, snapshot, terminalRows);
      state.snapshot = relationSnapshotWithDeltaSlack(snapshot);
      state.snapshotIndex = maintainedSnapshotIndexOrRowIndex(
        node.borrow(),
        state.kind,
        state.snapshot,
      );
      state.snapshotIndex.terminalRecords =
        state.kind.maintainedSubscription?.decodedTerminalRecords() ?? [];
      state.snapshotSource = snapshotSource;
      state.settled = settled;
      if (
        await publishEvent(
          node,
          state.sender,
          state.kind.shape,
          event,
          before,
          state.snapshot,
          state.snapshotIndex,
        )
      )
        changed++;
    }
    retained.push(weak);
  }
  rt.subscriptions = retained;
  for (const [key, generation] of reconciledResets.values())
    node.borrow().acknowledgeAuthoritativeReset(key, generation);
  return changed;
}

export function registerUpstreamSubscriptionOwner(
  owners: SortedMap<SubscriptionKey, WeakRef<any>[]>,
  handles: any[],
  state: any,
): void {
  const weak = new WeakRef(state);
  for (const handle of handles) owners.getOrInsert(handle.subscription, () => []).push(weak);
}

export function unregisterUpstreamSubscriptionOwner(
  owners: SortedMap<SubscriptionKey, WeakRef<any>[]>,
  subscription: SubscriptionKey,
  state: any,
): void {
  const entries = owners.get(subscription);
  if (!entries) return;
  const rest = entries.filter((entry) => {
    const live = entry.deref();
    return live !== undefined && live !== state;
  });
  if (rest.length) owners.set(subscription, rest);
  else owners.delete(subscription);
}

/** Release the matching connection pin; return the owner only for the final pin. */
export function retireRelayUpstreamSubscription(
  owners: SortedMap<[SubscriptionKey, number, any], any>,
  subscription: SubscriptionKey,
  downstreamConnectionEpoch: number,
  coverage: CoverageKey,
): any | undefined {
  const key: [SubscriptionKey, number, any] = [
    subscription,
    downstreamConnectionEpoch,
    coverage.opts.readViewKey(),
  ];
  const owner = owners.get(key);
  if (
    !owner ||
    owner.downstreamConnectionEpoch !== downstreamConnectionEpoch ||
    coverageKeyString(owner.coverage) !== coverageKeyString(coverage)
  )
    return undefined;
  const removed = owners.delete(key);
  // Only the last pin authorizes wire/local retirement.
  const target = subscriptionKeyString(subscription);
  return owners.keysArray().some(([candidate]) => subscriptionKeyString(candidate) === target)
    ? undefined
    : removed;
}

/** Retire a relay owner after its authority has terminally rejected the wire subscription. */
export function takeRelayUpstreamSubscriptionOwner(
  owners: SortedMap<[SubscriptionKey, number, any], any>,
  subscription: SubscriptionKey,
): [SubscriptionKey, any][] {
  const nil = isNilBinding(subscription.bindingId);
  const target = subscriptionKeyString(subscription);
  const keys = owners
    .keysArray()
    .filter(
      ([candidate]) =>
        subscriptionKeyString(candidate) === target ||
        (nil &&
          candidate.shapeId === subscription.shapeId &&
          dbg(candidate.readView) === dbg(subscription.readView)),
    );
  const out: [SubscriptionKey, any][] = [];
  for (const key of keys) {
    const owner = owners.delete(key);
    if (owner) out.push([key[0], owner]);
  }
  return out;
}

/** Take every propagated upstream owner for a disconnected downstream link. */
export function retireRelayUpstreamSubscriptionsForConnection(
  owners: SortedMap<[SubscriptionKey, number, any], any>,
  downstreamConnectionEpoch: number,
): [SubscriptionKey, any][] {
  const keys = [...owners].filter(
    ([, owner]) => owner.downstreamConnectionEpoch === downstreamConnectionEpoch,
  );
  const out: [SubscriptionKey, any][] = [];
  for (const [key] of keys) {
    const owner = owners.delete(key);
    if (!owner) continue;
    const target = subscriptionKeyString(key[0]);
    if (!owners.keysArray().some(([candidate]) => subscriptionKeyString(candidate) === target))
      out.push([key[0], owner]);
  }
  return out;
}

export function registerShapeRejectionMatches(
  subscription: SubscriptionKey,
  shape: any,
  opts: any,
): boolean {
  return (
    shape.shapeId() === subscription.shapeId &&
    dbg(opts.readViewKey()) === dbg(subscription.readView)
  );
}

const isNilBinding = (bindingId: any): boolean => dbg(bindingId) === dbg(BindingId.nil());

export function routeUpstreamSubscriptionRejection(
  subscriptions: WeakRef<any>[],
  owners: SortedMap<SubscriptionKey, WeakRef<any>[]>,
  subscription: SubscriptionKey,
  reason: any,
): number {
  let delivered = 0;
  const entries = owners.get(subscription);
  if (entries) {
    const live = entries.filter((entry) => entry.deref() !== undefined);
    owners.set(subscription, live);
    for (const entry of live)
      if (entry.deref().sendRejection({ kind: "Rejected", reason })) delivered++;
    if (delivered > 0) return delivered;
  }
  for (const weak of subscriptions) {
    const state = weak.deref();
    if (!state?.propagatesUpstream) continue;
    const opts = RegisterShapeOptions.with({
      tier: state.remoteReadTier ?? state.readTier,
      readView: state.readView,
    });
    if (!registerShapeRejectionMatches(subscription, state.kind.shape, opts)) continue;
    if (
      !isNilBinding(subscription.bindingId) &&
      dbg(state.kind.binding.bindingId()) !== dbg(subscription.bindingId)
    )
      continue;
    if (state.sendRejection({ kind: "Rejected", reason })) delivered++;
  }
  return delivered;
}

/**
 * Binding-supplied transport for one peer link. Both methods are non-blocking;
 * `tryRecv` returning undefined means "nothing staged right now", not "closed".
 */
export abstract class Transport {
  abstract send(message: SyncMessage): void;
  /** Backpressure hands the unsent message back instead of throwing. */
  trySend(message: SyncMessage): SyncMessage | undefined {
    try {
      this.send(message);
      return undefined;
    } catch (error: any) {
      if (error?.kind === "Backpressure") return message;
      throw error;
    }
  }
  abstract tryRecv(): SyncMessage | undefined;
  tryRecvResult(): SyncMessage | undefined {
    return this.tryRecv();
  }
  tryRecvOwnedResult(): any | undefined {
    const message = this.tryRecvResult();
    return message && ReceivedSyncMessage.unleased(message);
  }
  pollFlush(): any {
    return WireFlushStatus.Idle;
  }
  hasTerminalFailure(): boolean {
    return false;
  }
  remoteWireError(): WireError | undefined {
    return undefined;
  }
  incompleteReceiveTimeoutMs(): number | undefined {
    return undefined;
  }
  sharedAuxiliaryEndpoint(): any | undefined {
    return undefined;
  }
  setTrustedEncoder(_trusted: boolean): void {}
  wireInboundContext(): WireInboundContext | undefined {
    return undefined;
  }
  connectionSessionContext(): ConnectionSessionContext | undefined {
    return undefined;
  }
  permitsDelegatedSessions(): boolean {
    return false;
  }
}

export type TransportFailure = TransportError;

/** Handshake/session facts for one accepted connection. */
export interface ConnectionSessionContext {
  local: WireAuthorityEndpoint;
  remote?: WireAuthorityEndpoint;
  linkIdentity: AuthorSubject;
  negotiatedFeatures: number;
}

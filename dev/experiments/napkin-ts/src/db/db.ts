// Port of crates/jazz/layers/db/src/db.rs (core facade). Chunk routing lives in
// ../db_peer_io.ts and subscription output reduction in ./db_subscription_output.ts.
import * as X from "../_extern/db-core.js";
import { SortedMap, SortedSet, blake3Keyed, deferred, toHex, utf8 } from "../support/index.js";
import type {
  SyncMessage,
  TxId,
  AuthorSubject,
  JazzSchema,
  Value,
  CurrentRow,
} from "../_extern/db-core.js";
import { PositionalOrder } from "./positional_order.js";
import type { ResultNode, ResultRelation } from "./result_tree.js";
import { ResultTree } from "./result_tree.js";
import { PeerChunkResolver, PeerIoPump } from "./db_peer_io.js";
export { PeerIoPump, PeerChunkResolver } from "./db_peer_io.js";
export type { PeerIoTraceEntry } from "./db_peer_io.js";
export * from "./db_subscription_output.js";
import {
  PreparedQuery,
  ScalarReconciliation,
  SubscriptionSender,
  SubscriptionStream,
  type SubscriptionState,
  subscriptionRowOccurrenceId,
} from "./db_subscription_output.js";
import { mutationErrorDetails } from "./db/mutation_errors.js";

export const Error_ = X.ApiError;
export type DbError = X.ApiError;
export const dbError = (code: string, message: string): X.ApiError => new X.ApiError(code, message);
export const cmpTx = X.cmpTxId;

// ---------------------------------------------------------------- open schema admission

export class PendingOpenSchema {
  connectionEpoch: number | undefined;
  authoritativeCatalogueReceived = false;
  result: { ok: true } | { ok: false; error: DbError } | undefined;
  sender: ((r: { ok: true } | { ok: false; error: DbError }) => void) | undefined;
  wait: Promise<void>;
  constructor(readonly schema: X.SchemaVersionId) {
    const d = deferred<{ ok: true } | { ok: false; error: DbError }>();
    this.sender = d.resolve;
    this.wait = d.promise.then((r) => {
      if (!r.ok) throw r.error;
    });
  }
  /** Called when the owner drops without delivering a result. */
  closeUnsent(): void {
    this.sender?.({
      ok: false,
      error: dbError("Protocol", "database closed before schema admission"),
    });
    this.sender = undefined;
  }
}
export type OpenSchemaAdmission = { v: PendingOpenSchema | undefined };

export function finishOpenSchemaAdmission(
  state: OpenSchemaAdmission,
  result: { ok: true } | { ok: false; error: DbError },
): void {
  const pending = state.v;
  if (!pending || pending.result?.ok) return;
  pending.result = result;
  const sender = pending.sender;
  pending.sender = undefined;
  sender?.(result);
}

export function finishOpenSchemaConnection(
  state: OpenSchemaAdmission,
  connectionEpoch: number,
  result: { ok: true } | { ok: false; error: DbError },
): void {
  if (state.v?.connectionEpoch === connectionEpoch) finishOpenSchemaAdmission(state, result);
}

export function beginOpenSchemaConnection(
  state: OpenSchemaAdmission,
  connectionEpoch: number,
): void {
  const p = state.v;
  const replace = !!p && p.connectionEpoch !== undefined && !p.result?.ok;
  if (replace)
    finishOpenSchemaAdmission(state, {
      ok: false,
      error: dbError("Protocol", "upstream replaced before schema admission"),
    });
  const pending = state.v;
  if (!pending) return;
  if (pending.result?.ok) return;
  if (replace) state.v = new PendingOpenSchema(pending.schema);
  state.v!.connectionEpoch = connectionEpoch;
}

export const pendingOpenSchemaError = (): DbError =>
  dbError(
    "Schema",
    "opened schema is awaiting published catalogue admission; connect to the authority",
  );

/** LocalMutexBorrow: synchronous access to the node state when no suspended operation holds it. */
export function borrowNode<T>(mutex: X.LocalMutex<T>): T {
  const v = mutex.tryLock();
  if (v === undefined)
    throw new Error("synchronous node operation reentered a suspended operation");
  return v;
}

// ---------------------------------------------------------------- scheduling and errors

export type TickUrgency = "Immediate" | "Deferred" | "AfterCurrentTurn";

export interface TickScheduler {
  scheduleTick(urgency: TickUrgency): void;
  scheduleTickAfter(delayMs: number): void;
  queryRuntimeWaker?(): (() => void) | undefined;
  dropsPendingTicks?(): boolean;
}

export type TransactionKind = "mergeable" | "exclusive";
export const transactionKindFrom = (kind: X.TxKind): TransactionKind =>
  kind === "Mergeable" ? "mergeable" : "exclusive";

export type TransactionFate = {
  kind: "rejected";
  transactionId: X.TransactionId;
  code: string;
  reason: string;
};

export interface LocalTransactionRecord {
  transactionId: X.TransactionId;
  kind: TransactionKind;
  sealed: boolean;
  latestSettlement: TransactionFate;
}

export interface MutationErrorEvent {
  code: string;
  reason: string;
  transaction: LocalTransactionRecord;
}

export type MutationErrorCallback = (event: MutationErrorEvent) => void;

export const syncAutopsy = {
  enable(): void {},
  clear(): void {},
  dump(): string {
    return "";
  },
};

export const blockOn = X.blockOn;

/** StackSafeFuture: JS async frames are heap-allocated; nothing to grow. */
export const stackSafe = <T>(p: Promise<T>): Promise<T> => p;

// ---------------------------------------------------------------- schema views

export class SchemaViewId {
  constructor(readonly bytes: Uint8Array) {}
  asBytes(): Uint8Array {
    return this.bytes;
  }
  static forSchema(schema: JazzSchema): SchemaViewId {
    const bytes = utf8(JSON.stringify(schema.publicSchema()));
    return new SchemaViewId(blake3Keyed(X.deriveKey("jazz typed schema view id v1"), bytes));
  }
  key(): string {
    return toHex(this.bytes);
  }
}

export type MutationOwnerLifecycle = "Open" | "Closing";

export type QueuedMutationStatus =
  | { kind: "Pending" }
  | { kind: "Published" }
  | { kind: "Failed"; error: DbError };

export interface QueuedMutationOperation {
  transaction: [TxId, X.TxKind] | undefined;
  openTxId: X.OpenTransactionId | undefined;
  future: () => Promise<void>;
  status: { v: QueuedMutationStatus } | undefined;
  completion: ((r: DbError | undefined) => void) | undefined;
}

export class StreamingValueUploadCleanupTicket {
  constructor(readonly cell: { v: { ok: boolean; error?: DbError } | undefined }) {}
  result() {
    return this.cell.v;
  }
}

export interface UpstreamUploadDestination {
  remoteNode: Uint8Array;
  linkIdentity: AuthorSubject;
}

export class WriteClock {
  nextCounterMs = 1;
  wall = false;
  nextNowMs(): number {
    if (this.wall) return Math.min(Math.max(Date.now(), 0), X.HLC_MAX_PHYSICAL_MS);
    return this.nextCounterMs++;
  }
}

export interface UploadRetryClock {
  nowMs(): number;
}

export class MonotonicUploadRetryClock implements UploadRetryClock {
  private readonly started = performance.now();
  nowMs(): number {
    return Math.floor(performance.now() - this.started);
  }
}

export interface PendingLocalPublication {
  published: X.PublishedTransaction;
  uploadUnit: SyncMessage | undefined;
  settlement: Promise<TxId>;
}

export class AuthorityViewReceipts {
  connectionEpoch = 0;
  confirmationFloor: X.GlobalTime = X.GlobalTimeZero;
  subscriptions = new SortedSet<X.SubscriptionKey>(X.cmpSubscriptionKey);
  bindingViews = new SortedSet<X.BindingViewKey>(X.cmpBindingViewKey);
}

export interface StagedInboundMessage {
  message: SyncMessage;
  lease: X.BufferLease | undefined;
  authorityReceiptEligible: boolean;
  receiptsValidated: boolean;
}

export interface PendingAuthorityViewUpdate {
  parts: X.ViewUpdateParts;
  authorityReceiptEligible: boolean;
}

// ---------------------------------------------------------------- local fate routes

export type FateQueue = { items: SyncMessage[]; alive: boolean };

export interface LocalFateRoute {
  queue: FateQueue;
  localAcknowledged: boolean;
  replayReady: boolean;
  replayAuthor: AuthorSubject | undefined;
  replayUnit: SyncMessage | undefined;
  heldFate: SyncMessage | undefined;
}
export type LocalFateRoutes = SortedMap<TxId, LocalFateRoute[]>;

const authorEq = (a: AuthorSubject | undefined, b: AuthorSubject | undefined) =>
  a === b || (a !== undefined && b !== undefined && X.authorSubjectEq(a, b));

export const registerLocalFateRoute = (routes: LocalFateRoutes, txId: TxId, queue: FateQueue) =>
  registerLocalFateRouteState(routes, txId, queue, false, true, undefined, undefined);
export const registerLocalFateObserver = (routes: LocalFateRoutes, txId: TxId, queue: FateQueue) =>
  registerLocalFateRouteState(routes, txId, queue, true, true, undefined, undefined);
export const registerLocalFateRouteWithAcknowledgement = (
  routes: LocalFateRoutes,
  txId: TxId,
  queue: FateQueue,
  ack: boolean,
) => registerLocalFateRouteState(routes, txId, queue, ack, true, undefined, undefined);
export const registerLocalReplayRoute = (
  routes: LocalFateRoutes,
  txId: TxId,
  queue: FateQueue,
  author: AuthorSubject,
  unit: SyncMessage | undefined,
) => registerLocalFateRouteState(routes, txId, queue, false, false, author, unit);

function registerLocalFateRouteState(
  routes: LocalFateRoutes,
  txId: TxId,
  queue: FateQueue,
  localAcknowledged: boolean,
  replayReady: boolean,
  replayAuthor: AuthorSubject | undefined,
  replayUnit: SyncMessage | undefined,
): void {
  routes.retain((_, pending) => {
    const kept = pending.filter((c) => !c.replayReady || c.queue.alive);
    pending.splice(0, pending.length, ...kept);
    return pending.length > 0;
  });
  const pending = routes.get(txId);
  const existing = pending?.find((r) => r.queue.alive && r.queue === queue);
  if (existing) {
    existing.localAcknowledged ||= localAcknowledged;
    if (replayReady) {
      existing.replayReady = true;
      existing.replayUnit = undefined;
    } else {
      existing.replayAuthor = replayAuthor ?? existing.replayAuthor;
      if (replayUnit) existing.replayUnit = replayUnit;
    }
    return;
  }
  if (!replayReady) {
    const dead = pending?.find(
      (r) => !r.replayReady && !r.queue.alive && authorEq(r.replayAuthor, replayAuthor),
    );
    if (dead) {
      dead.queue = queue;
      dead.localAcknowledged = localAcknowledged;
      if (!dead.replayUnit) dead.replayUnit = replayUnit;
      return;
    }
  }
  let heldFate: SyncMessage | undefined;
  if (!replayReady && pending) {
    for (const c of pending) {
      if (
        !c.replayReady &&
        authorEq(c.replayAuthor, replayAuthor) &&
        c.queue.alive &&
        c.heldFate &&
        localFateIsTerminal(c.heldFate)
      ) {
        heldFate = c.heldFate;
        break;
      }
    }
  }
  routes
    .getOrInsert(txId, () => [])
    .push({ queue, localAcknowledged, replayReady, replayAuthor, replayUnit, heldFate });
}

export async function queueLocalAcknowledgements(
  routes: LocalFateRoutes,
  node: X.SharedNodeState,
): Promise<void> {
  const txIds: TxId[] = [];
  for (const [txId, pending] of routes)
    if (pending.some((r) => r.replayReady && !r.localAcknowledged && r.queue.alive))
      txIds.push(txId);
  const durable = new SortedSet<TxId>(cmpTx);
  const state = await node.lock();
  try {
    for (const txId of txIds) {
      const s = await state.transactionState(txId);
      if (s && X.durabilityRank(s[2]) >= X.durabilityRank("Local")) durable.add(txId);
    }
  } finally {
    node.unlock();
  }
  routes.retain((txId, pending) => {
    const locallyDurable = durable.has(txId);
    const kept = pending.filter((route) => {
      if (!route.queue.alive) return !route.replayReady;
      if (route.replayReady && locallyDurable && !route.localAcknowledged) {
        route.queue.items.push({
          kind: "FateUpdate",
          txId,
          fate: { kind: "Pending" },
          globalTime: undefined,
          durability: "Local",
        });
        route.localAcknowledged = true;
      }
      return true;
    });
    pending.splice(0, pending.length, ...kept);
    return pending.length > 0;
  });
}

export function localFateIsTerminal(fate: SyncMessage): boolean {
  return (
    fate.kind === "FateUpdate" && (fate.fate.kind === "Rejected" || fate.durability === "Global")
  );
}

export function releaseLocalReplayFates(routes: LocalFateRoutes): void {
  routes.retain((_, pending) => {
    const kept = pending.filter((route) => {
      if (!route.replayReady) return true;
      if (!route.queue.alive) return false;
      const fate = route.heldFate;
      if (!fate) return true;
      route.heldFate = undefined;
      route.queue.items.push(fate);
      return !localFateIsTerminal(fate);
    });
    pending.splice(0, pending.length, ...kept);
    return pending.length > 0;
  });
}

export function routeLocalFate(routes: LocalFateRoutes, txId: TxId, fate: SyncMessage): void {
  const terminal = localFateIsTerminal(fate);
  const pending = routes.get(txId);
  if (!pending) return;
  const kept = pending.filter((c) => {
    if (!c.replayReady) {
      const replace = c.heldFate && localFateIsTerminal(c.heldFate) ? terminal : true;
      if (replace) c.heldFate = fate;
      return true;
    }
    if (!c.queue.alive) return false;
    c.queue.items.push(fate);
    return !terminal;
  });
  if (kept.length) routes.set(txId, kept);
  else routes.delete(txId);
}

type LocalReplayStatus = "Visiting" | "Complete" | "Blocked";

export function localReplayUnitIsComplete(unit: SyncMessage): boolean {
  return unit.kind === "CommitUnit" && unit.tx.nTotalWrites === unit.versions.length;
}

type LocalReplayFrame = { enter: boolean; txId: TxId };

async function planLocalReplayCommitUnits(
  node: X.NodeState,
  roots: SortedSet<TxId>,
  retained: SortedMap<TxId, SyncMessage>,
): Promise<
  [SortedMap<TxId, LocalReplayStatus>, SortedMap<TxId, SyncMessage>, [TxId, SyncMessage][]]
> {
  const statuses = new SortedMap<TxId, LocalReplayStatus>(cmpTx);
  const units = new SortedMap<TxId, SyncMessage>(cmpTx);
  const overrides = retained.clone();
  const parentsByTx = new SortedMap<TxId, TxId[]>(cmpTx);
  const complete: [TxId, SyncMessage][] = [];
  for (const root of roots) {
    const frames: LocalReplayFrame[] = [{ enter: true, txId: root }];
    while (frames.length) {
      const frame = frames.pop()!;
      const txId = frame.txId;
      if (frame.enter) {
        const status = statuses.get(txId);
        if (status === "Complete" || status === "Blocked") continue;
        if (status === "Visiting") {
          statuses.set(txId, "Blocked");
          continue;
        }
        let unit = overrides.delete(txId);
        if (!unit) {
          try {
            unit = await node.commitUnitFor(txId);
          } catch (error: any) {
            if (error?.kind === "MissingTransaction") {
              statuses.set(txId, "Blocked");
              continue;
            }
            throw error;
          }
        }
        if (unit!.kind !== "CommitUnit")
          X.unreachable("commit_unit_for always returns a commit unit");
        const parents: TxId[] = [];
        units.set(txId, unit!);
        if (unit!.tx.nTotalWrites !== unit!.versions.length) {
          statuses.set(txId, "Blocked");
          continue;
        }
        parentsByTx.set(txId, parents);
        statuses.set(txId, "Visiting");
        frames.push({ enter: false, txId });
        for (const p of [...parents].reverse()) frames.push({ enter: true, txId: p });
      } else {
        if (statuses.get(txId) !== "Visiting") continue;
        const ok = (parentsByTx.get(txId) ?? []).every((p) => statuses.get(p) === "Complete");
        if (ok) {
          statuses.set(txId, "Complete");
          const unit = units.delete(txId);
          if (!unit) throw new Error("visiting replay node retains its unit");
          complete.push([txId, unit]);
        } else statuses.set(txId, "Blocked");
      }
    }
  }
  return [statuses, units, complete];
}

function localReplayUploadOrder(
  pending: SortedSet<TxId>,
  complete: [TxId, SyncMessage][],
  roots: SortedSet<TxId>,
  statuses: SortedMap<TxId, LocalReplayStatus>,
  blocked: SortedMap<TxId, SyncMessage>,
  retained: SortedMap<TxId, SyncMessage>,
): SortedMap<TxId, SyncMessage> {
  const uploads = new SortedMap<TxId, SyncMessage>(cmpTx);
  for (const [txId, unit] of complete) if (pending.has(txId)) uploads.getOrInsert(txId, () => unit);
  for (const txId of roots) {
    if (!pending.has(txId) || statuses.get(txId) === "Complete") continue;
    const unit = blocked.get(txId) ?? retained.get(txId);
    if (unit) uploads.getOrInsert(txId, () => unit);
  }
  return uploads;
}

export async function restoreLocalSubscriberReplay(
  node: X.SharedNodeState,
  outbox: UploadOutbox,
  routes: LocalFateRoutes,
  author: AuthorSubject,
  downstreamFates: FateQueue,
): Promise<void> {
  const retainedRoots = new SortedSet<TxId>(cmpTx);
  const retainedUnits = new SortedMap<TxId, SyncMessage>(cmpTx);
  for (const [txId, rs] of routes) {
    let matching = false;
    for (const route of rs) {
      if (route.replayReady || !authorEq(route.replayAuthor, author)) continue;
      matching = true;
      if (route.replayUnit && localReplayUnitIsComplete(route.replayUnit))
        retainedUnits.getOrInsert(txId, () => route.replayUnit!);
    }
    if (matching) retainedRoots.add(txId);
  }
  const state = await node.lock();
  let plan: Awaited<ReturnType<typeof planLocalReplayCommitUnits>>;
  let pendingSet: SortedSet<TxId>;
  let roots: SortedSet<TxId>;
  try {
    const pending: TxId[] = await state.pendingTransactionIdsForAuthor(author);
    pendingSet = new SortedSet(cmpTx, pending);
    roots = new SortedSet(cmpTx, [...pendingSet, ...retainedRoots]);
    plan = await planLocalReplayCommitUnits(state, roots, retainedUnits);
  } finally {
    node.unlock();
  }
  const [statuses, blockedUnits, replayUnits] = plan;
  const outboxUnits = new SortedSet<TxId>(
    cmpTx,
    outbox.pending.filter((p) => p.unit).map((p) => p.txId),
  );
  const uploads = localReplayUploadOrder(
    pendingSet,
    replayUnits,
    roots,
    statuses,
    blockedUnits,
    retainedUnits,
  );
  for (const [, unit] of replayUnits) downstreamFates.items.push(unit);
  for (const [txId, unit] of uploads)
    if (outboxUnits.add(txId)) queuePendingUploadIn(outbox, txId, unit);
  for (const txId of roots) {
    if (statuses.get(txId) === "Complete") {
      registerLocalReplayRoute(routes, txId, downstreamFates, author, undefined);
      registerLocalFateRoute(routes, txId, downstreamFates);
    }
  }
  for (const txId of roots) {
    if (statuses.get(txId) === "Complete") continue;
    registerLocalReplayRoute(
      routes,
      txId,
      downstreamFates,
      author,
      blockedUnits.get(txId) ?? retainedUnits.get(txId),
    );
  }
  await queueLocalAcknowledgements(routes, node);
  releaseLocalReplayFates(routes);
}

// ---------------------------------------------------------------- shape registrations & schema views

export type SubscriberShapeRegistration =
  | { kind: "Registered"; options: X.RegisterShapeOptions }
  | { kind: "PendingCatalogueAdmission"; options: X.RegisterShapeOptions }
  | { kind: "RejectedUnsupportedCapability"; message: string };
export const ownsNodeShape = (r: SubscriberShapeRegistration) =>
  r.kind !== "RejectedUnsupportedCapability";

export function defaultCellForColumnType(columnType: X.GrooveColumnType, def: Value): Value {
  if (columnType.kind === "Nullable" && def.kind !== "Nullable")
    return { kind: "Nullable", value: def };
  return def;
}

export function schemaViewColumnDefault(column: X.ColumnSchema): Value {
  if (column.default !== undefined) return column.default;
  if (column.columnType.kind === "Nullable") return { kind: "Nullable", value: undefined };
  throw dbError("Schema", `schema view column ${column.name} requires a migration default`);
}

export function directSchemaViewLens(
  source: JazzSchema,
  target: JazzSchema,
): [X.MigrationLens, string[], string[]] {
  const tableLenses: any[] = [];
  for (const sourceTable of source.tables) {
    const targetTable = target.tables.find((t: any) => t.name === sourceTable.name);
    if (!targetTable) continue;
    const name = targetTable.name;
    if (!X.deepEq(sourceTable.references, targetTable.references))
      throw dbError("Schema", `schema view changes references on ${name} without an explicit lens`);
    if (!X.deepEq(sourceTable.mergeStrategies, targetTable.mergeStrategies))
      throw dbError(
        "Schema",
        `schema view changes merge strategies on ${name} without an explicit lens`,
      );
    if (!X.deepEq(sourceTable.indexedColumns, targetTable.indexedColumns))
      throw dbError(
        "Schema",
        `schema view changes indices on ${name} without explicit index admission`,
      );
    if (!X.deepEq(sourceTable.compositeIndexes, targetTable.compositeIndexes))
      throw dbError(
        "Schema",
        `schema view changes composite indexes on ${name} without explicit index admission`,
      );
    const ops: X.LensOp[] = [];
    for (const targetColumn of targetTable.columns) {
      const sourceColumn = sourceTable.columns.find((c: any) => c.name === targetColumn.name);
      if (!sourceColumn)
        ops.push({
          kind: "AddColumn",
          column: targetColumn.name,
          default: schemaViewColumnDefault(targetColumn),
        });
      else if (!X.deepEq(sourceColumn.columnType, targetColumn.columnType))
        throw dbError(
          "Schema",
          `schema view changes type of ${name}.${targetColumn.name} without an explicit lens`,
        );
    }
    for (const sourceColumn of sourceTable.columns)
      if (targetTable.columns.every((c: any) => c.name !== sourceColumn.name))
        ops.push({
          kind: "DropColumn",
          column: sourceColumn.name,
          backwardsDefault: schemaViewColumnDefault(sourceColumn),
        });
    tableLenses.push({ sourceTable: sourceTable.name, targetTable: name, ops });
  }
  const newTables = target.tables
    .filter((t: any) => source.tables.every((s: any) => s.name !== t.name))
    .map((t: any) => t.name);
  const droppedTables = source.tables
    .filter((t: any) => target.tables.every((s: any) => s.name !== t.name))
    .map((t: any) => t.name);
  return [
    X.MigrationLens.new(source.versionId(), target.versionId(), tableLenses),
    newTables,
    droppedTables,
  ];
}

// ---------------------------------------------------------------- write waiters, upstream bookkeeping

export interface WriteStateWaiter {
  id: number;
  notify: () => void;
}

export class MutationErrorState {
  callback: MutationErrorCallback | undefined;
  pending = new SortedMap<TxId, MutationErrorEvent>(cmpTx);
  applicationWaiters = new SortedMap<TxId, number>(cmpTx);
}

export interface WriteWaitOptions {
  tier: X.DurabilityTier;
  observeOnly: boolean;
}
export const writeWaitOptions = (tier: X.DurabilityTier): WriteWaitOptions => ({
  tier,
  observeOnly: false,
});

export type PolicyBinding = [AuthorSubject, Map<string, Value>];

export type PendingUpstreamCommand =
  | { kind: "Subscribe"; sub: PendingUpstreamSubscription }
  | { kind: "Unsubscribe"; key: X.SubscriptionKey }
  | {
      kind: "AuthorizationScopeIntent";
      requestId: X.PermissionAdviceRequestId;
      action: X.PermissionAdviceAction;
      sessionClaimBinding: PolicyBinding | undefined;
      delegatedSession: X.DelegatedSessionBinding | undefined;
    };

export interface PendingUpstreamSubscription {
  subscription: X.SubscriptionKey;
  shape: X.ValidatedQuery;
  binding: X.Binding;
  opts: X.RegisterShapeOptions;
  identity: AuthorSubject;
  policyBinding: PolicyBinding | undefined;
}

export interface QueryCoverageRegistration {
  coverage: X.CoverageKey;
  subscription: PendingUpstreamSubscription;
  refCount: number;
}

export interface UpstreamCoverageHandle {
  coverage: X.CoverageKey;
  subscription: X.SubscriptionKey;
}

export interface PendingSubscriptionFinalization {
  state: SubscriptionState | undefined;
  openingUpstream: UpstreamCoverageHandle[];
  openingLocal: [number, X.GrooveSubscriptionId] | undefined;
  acknowledgement: (() => void) | undefined;
}

export interface OpenedUpstreamCoverage {
  handles: UpstreamCoverageHandle[];
  awaitsInitialAuthorityResponse: boolean;
}

export type CoveragePolicyBindingOrigin = "DirectAdmitted" | "Delegated";

export interface CoverageGroup {
  shape: X.ValidatedQuery;
  binding: X.Binding;
  policyBinding: PolicyBinding;
  policyBindingOrigin: CoveragePolicyBindingOrigin;
  subscribers: SortedSet<X.SubscriptionKey>;
  pendingInitialSubscribers: SortedSet<X.SubscriptionKey>;
  pendingInitialUpdate: [X.SubscriptionKey, SyncMessage] | undefined;
  pendingIncrementalUpdates: [X.SubscriptionKey, SyncMessage][];
  publicationRuntimeToken: number | undefined;
  pendingClaimRefreshRevision: number | undefined;
  initialized: boolean;
  authorityResultSubscription: X.SubscriptionKey;
  upstreamSubscription: X.SubscriptionKey;
  upstreamOpts: X.RegisterShapeOptions;
  awaitingUpstreamSettlement: boolean;
}

export interface RelayUpstreamSubscriptionOwner {
  request: PendingUpstreamSubscription;
  scalarAuthorityRevision: number;
  scalarReconciliation: ScalarReconciliation;
  downstreamConnectionEpoch: number;
  coverage: X.CoverageKey;
  policyBinding: PolicyBinding;
  downstreamSubscriptions: SortedSet<X.SubscriptionKey>;
}

export interface RelaySubscriptionRejection {
  coverage: X.CoverageKey;
  policyBinding: PolicyBinding;
  downstreamSubscriptions: SortedSet<X.SubscriptionKey>;
  reason: X.SubscribeRejectReason;
}

export interface AuthorizationScopeLeaseRequest {
  action: X.PermissionAdviceAction;
  sessionClaimBinding: PolicyBinding;
  delegatedSession: X.DelegatedSessionBinding | undefined;
  waiters: SortedSet<X.PermissionAdviceRequestId>;
  intentSent: boolean;
  key: X.AuthorizationSupportScopeKey | undefined;
  lease: X.AuthorizationScopeLease | undefined;
  owner: X.AuthorizationScopeOwnerToken | undefined;
  clauseCount: number | undefined;
  appliedClauses: Map<number, [X.SubscriptionKey, X.GlobalTime, number]>;
}

export class AuthorizationScopeLeaseManager {
  registry = new X.AuthorizationScopeRegistry();
  requests = new Map<X.PermissionAdviceRequestId, AuthorizationScopeLeaseRequest>();
}

export interface ServedAuthorizationScopeClause {
  subscription: X.SubscriptionKey;
  register: SyncMessage;
  subscribe: SyncMessage;
  view: SyncMessage;
}
export interface ServedAuthorizationScopeHydration {
  clauses: ServedAuthorizationScopeClause[];
  receipt: X.AuthorizationScopeReceipt;
}

// ---------------------------------------------------------------- upload outbox

const RECENT_UPLOAD_FAILURES = 64;

export interface PendingUpload {
  txId: TxId;
  unit: SyncMessage | undefined;
}

export class UploadOutbox {
  pending: PendingUpload[] = [];
  txIds = new Set<string>();
  declaredRoot = false;
  upstreamAttached = false;
  onWire = new Set<string>();
  uploadFailures: [TxId, string][] = [];
  progress = 0;

  settlesUploadsLocally(): boolean {
    return this.declaredRoot && !this.upstreamAttached;
  }
  declareRoot(): void {
    this.declaredRoot = true;
  }
  markUpstreamAttached(): void {
    this.upstreamAttached = true;
  }
  push(p: PendingUpload): boolean {
    const k = X.txIdKey(p.txId);
    if (this.txIds.has(k)) return false;
    this.txIds.add(k);
    this.pending.push(p);
    return true;
  }
  len(): number {
    return this.pending.length;
  }
  contains(txId: TxId): boolean {
    return this.txIds.has(X.txIdKey(txId));
  }
  retain(keep: (p: PendingUpload) => boolean): void {
    this.pending = this.pending.filter(keep);
    this.txIds = new Set(this.pending.map((p) => X.txIdKey(p.txId)));
    for (const k of [...this.onWire]) if (!this.txIds.has(k)) this.onWire.delete(k);
  }
  markOnWire(txId: TxId): void {
    const k = X.txIdKey(txId);
    if (this.txIds.has(k)) this.onWire.add(k);
    this.noteUploadProgress();
  }
  noteUploadProgress(): void {
    this.progress = (this.progress + 1) % Number.MAX_SAFE_INTEGER;
  }
  uploadProgress(): number {
    return this.progress;
  }
  forgetOnWire(): void {
    this.onWire.clear();
  }
  awaitsWire(txId: TxId): boolean {
    const k = X.txIdKey(txId);
    return this.txIds.has(k) && !this.onWire.has(k);
  }
  markUploadFailed(txId: TxId, reason: string): void {
    if (this.uploadFailures.length >= RECENT_UPLOAD_FAILURES) this.uploadFailures.shift();
    this.uploadFailures.push([txId, reason]);
  }
  uploadFailure(txId: TxId): string | undefined {
    return this.uploadFailures.find(([t]) => cmpTx(t, txId) === 0)?.[1];
  }
  removeReleased(released: Set<string>): Set<string> {
    const completed = new Set(released);
    while (this.pending.length && released.delete(X.txIdKey(this.pending[0].txId))) {
      const p = this.pending.shift()!;
      const k = X.txIdKey(p.txId);
      this.txIds.delete(k);
      this.onWire.delete(k);
    }
    if (released.size) this.retain((p) => !released.has(X.txIdKey(p.txId)));
    return completed;
  }
}

export function queuePendingUploadIn(
  outbox: UploadOutbox,
  txId: TxId,
  unit: SyncMessage | undefined,
): boolean {
  if (outbox.contains(txId)) {
    const pending = outbox.pending.find((p) => cmpTx(p.txId, txId) === 0);
    if (pending) {
      if (!unit) return false;
      if (pending.unit && X.syncMessageEq(pending.unit, unit)) return false;
      pending.unit = unit;
      return true;
    }
  }
  outbox.push({ txId, unit });
  return true;
}

export function transactionSatisfiesWait(
  fate: X.Fate,
  globalTime: X.GlobalTime | undefined,
  durability: X.DurabilityTier,
  tier: X.DurabilityTier,
): boolean {
  const r = X.durabilityRank;
  return (
    r(durability) >= r(tier) &&
    (r(tier) <= r("Local") || fate.kind === "Accepted") &&
    (r(tier) < r("Global") || globalTime !== undefined)
  );
}

export interface WriteState {
  fate: X.Fate;
  globalTime: X.GlobalTime | undefined;
  durability: X.DurabilityTier;
}

export class InitialSyncFlushCadence {
  static readonly DEFAULT = new InitialSyncFlushCadence(512);
  private constructor(private readonly n: number) {}
  static every(writes: number): InitialSyncFlushCadence {
    if (writes <= 0) throw new Error("non-zero");
    return new InitialSyncFlushCadence(writes);
  }
  writes(): number {
    return this.n;
  }
}

export class QueryAttachment {
  constructor(
    public subscriptions: X.SubscriptionKey[],
    public requiredAfter: [X.BindingViewKey, number][],
    public requiresDeliveryReceipt: boolean,
    public requiresCurrentAuthorityReceipt: boolean,
    public registrations: X.SubscriptionKey[],
    public refreshes: [X.CoverageKey, number][],
  ) {}
  subscription(): X.SubscriptionKey {
    return this.subscriptions[0];
  }
}

export type WriteStateWaiters = SortedMap<TxId, WriteStateWaiter[]>;

/** WriteStateChange: a cancellable wake for one transaction's write state. */
export function writeStateChange(
  waiters: WriteStateWaiters,
  txId: TxId,
  waiterId: number,
): Promise<void> & { cancel(): void } {
  const d = deferred<void>();
  waiters.getOrInsert(txId, () => []).push({ id: waiterId, notify: () => d.resolve() });
  const p = d.promise as Promise<void> & { cancel(): void };
  p.cancel = () => {
    const list = waiters.get(txId);
    if (!list) return;
    const kept = list.filter((w) => w.id !== waiterId);
    if (kept.length) waiters.set(txId, kept);
    else waiters.delete(txId);
  };
  return p;
}

export type PermissionAdviceWaiters = Map<
  X.PermissionAdviceRequestId,
  (a: X.PermissionAdvice) => void
>;

export class PermissionAdviceFuture {
  readonly promise: Promise<X.PermissionAdvice>;
  constructor(
    readonly waiters: PermissionAdviceWaiters,
    readonly requestId: X.PermissionAdviceRequestId,
  ) {
    const d = deferred<X.PermissionAdvice>();
    waiters.set(requestId, d.resolve);
    this.promise = d.promise;
  }
  cancel(): void {
    const w = this.waiters.get(this.requestId);
    this.waiters.delete(this.requestId);
    w?.({ kind: "Unknown" } as X.PermissionAdvice);
  }
}

export interface DbTickStats {
  subscriptionEvents: number;
  remoteSyncApplied: number;
}

// ---------------------------------------------------------------- read options and error helpers

export type ReadTier = "LocalFirst" | "Remote" | "LocalOnly";

export interface ReadOpts {
  tier: ReadTier;
  includeDeleted: boolean;
  readView: X.ReadViewSpec;
  firstLoad: X.FirstLoad;
}

export const defaultReadOpts = (): ReadOpts => ({
  tier: "LocalFirst",
  includeDeleted: false,
  readView: X.ReadViewSpec.default(),
  firstLoad: "Deliver",
});
export const readOptsForTier = (tier: ReadTier): ReadOpts => ({ ...defaultReadOpts(), tier });
export const readOptsBranchView = (
  opts: ReadOpts,
  head: X.BranchSelector,
  base?: X.BranchViewBase,
): ReadOpts => ({
  ...opts,
  readView: X.ReadViewSpec.branchView(head, base),
});

export const transactionAbandoned = (openTxId: X.OpenTransactionId) =>
  dbError("Protocol", `transaction handle was abandoned: ${openTxId}`);
export const rowAlreadyDeleted = (row: X.RowUuid) =>
  dbError("WriteRejected", `row already deleted: ${X.rowUuidString(row)}`);
export const rowAlreadyExists = (table: string, row: X.RowUuid) =>
  dbError("WriteRejected", `row already exists in table ${table}: ${X.rowUuidString(row)}`);
export const readForWriteDenied = (operation: string, table: string) =>
  dbError(
    "WriteRejected",
    `read policy denied ${operation} on table ${table}: the operation requires read permission on the target row`,
  );
export const updateTargetNotLoaded = (operation: string, table: string, row: X.RowUuid) =>
  dbError(
    "NotObserved",
    `${operation} on table ${table} needs the current row, but row ${X.rowUuidString(row)} is not loaded locally; read or subscribe to the row before updating it`,
  );

export const readCoverageTier = (opts: ReadOpts): X.DurabilityTier =>
  opts.tier === "Remote" ? "Global" : "Local";

export function remoteSubscriptionTier(
  tier: X.DurabilityTier,
  floor: X.DurabilityTier,
): X.DurabilityTier {
  return X.durabilityRank(tier) >= X.durabilityRank(floor) ? tier : floor;
}

export function upstreamRegisterShapeOptions(
  tier: X.DurabilityTier,
  readView: X.ReadViewSpec,
  floor: X.DurabilityTier,
): X.RegisterShapeOptions {
  return {
    ...X.RegisterShapeOptions.default(),
    tier: remoteSubscriptionTier(tier, floor),
    readView,
  };
}

export function ensureDefaultReadView(opts: ReadOpts): void {
  if (opts.readView.isDefault()) return;
  throw dbError(
    "Query",
    "non-default read_view is not supported yet; reads currently execute against the current/default view",
  );
}

export function ensureSupportedReadView(opts: ReadOpts): void {
  const k = opts.readView.source.kind;
  if (k === "Current" || k === "BranchView" || k === "Snapshot") return;
  throw dbError("Query", "this read_view combination is not supported yet");
}

export function ensureSupportedSubscriptionReadOpts(opts: ReadOpts): void {
  if (opts.includeDeleted)
    throw dbError("Query", "live subscriptions do not support include_deleted yet");
  ensureSupportedReadView(opts);
}

export function ensureSupportedRegisterShapeReadView(opts: X.RegisterShapeOptions): void {
  ensureSupportedReadView({ ...defaultReadOpts(), readView: opts.readView });
}

export function ensureSupportedRegisterShapeOptions(
  opts: X.RegisterShapeOptions,
  localReceiver: boolean,
  peerRole: X.PeerRole,
  delegatedCapability: boolean,
): void {
  ensureSupportedRegisterShapeReadView(opts);
  if (opts.bindingSource === "RelayAuthoritySession" && !delegatedCapability)
    throw dbError(
      "Query",
      "relay authority-session bindings require a live server-admitted scope-isolated relay capability",
    );
  const r = X.durabilityRank;
  const supported =
    localReceiver || peerRole.kind === "Relay"
      ? r(opts.tier) >= r("Local")
      : opts.tier === "Global";
  if (!supported) {
    const message = localReceiver
      ? "Local sync subscription serving requires at least local-tier registration"
      : peerRole.kind === "Relay"
        ? "relay sync subscription serving requires at least local-tier registration"
        : "sync subscription serving supports only global-tier registration";
    throw dbError("Query", message);
  }
}

export const validateShapeAstForRegistration = (
  node: X.NodeState,
  shapeId: X.ShapeId,
  ast: X.ShapeAst,
) => node.validateShapeAstForRegistration(shapeId, ast);

export const unsupportedShapeCapabilityRejectionMessage = (
  subscription: X.SubscriptionKey,
  detail: string,
) => subscriptionRejectionMessage(subscription, { kind: "UnsupportedShapeCapability", detail });

export function serverSubscriptionFailureRejectionMessage(
  subscription: X.SubscriptionKey,
  error: X.NodeError,
): SyncMessage {
  console.error(
    `jazz subscription rejected: shape=${subscription.shapeId} binding=${subscription.bindingId} read_view=${subscription.readView.id} server_error=${error}`,
  );
  return subscriptionRejectionMessage(subscription, {
    kind: "ServerFailure",
    code: serverFailureCode(error),
  });
}

export const subscriptionRejectionMessage = (
  subscription: X.SubscriptionKey,
  reason: X.SubscribeRejectReason,
): SyncMessage => ({
  kind: "SubscribeRejected",
  subscription,
  reason,
});

export function serverFailureCode(error: X.NodeError): X.SubscribeServerFailureCode {
  switch (error.kind) {
    case "TableNotFound":
      return "TableNotFound";
    case "Query":
      return error.error.kind === "UnknownTable" ? "TableNotFound" : "QueryValidation";
    case "QueryLowering":
      return "QueryLowering";
    case "AuthorizationDenied":
      return "PolicyEvaluation";
    case "InvalidStoredValue":
    case "InvalidCatalogueUpdate":
      return "SchemaResolution";
    default:
      return "Internal";
  }
}

export const isServerShapeValidationFailure = (error: X.NodeError) =>
  error.kind === "TableNotFound" || error.kind === "Query";

export const registerShapeRejectionSubscription = (
  shapeId: X.ShapeId,
  readView: X.ReadViewKey,
): X.SubscriptionKey => ({
  shapeId,
  bindingId: X.NIL_BINDING_ID,
  readView,
});

export const coverageKey = (
  shape: X.ValidatedQuery,
  binding: X.Binding,
  opts: X.RegisterShapeOptions,
): X.CoverageKey => ({
  shapeId: shape.shapeId(),
  bindingId: binding.bindingId(),
  opts,
  policyBinding: undefined,
});

export function requestCoverageKey(
  shape: X.ValidatedQuery,
  binding: X.Binding,
  opts: X.RegisterShapeOptions,
  policyBinding: PolicyBinding | undefined,
): X.CoverageKey {
  const key = coverageKey(shape, binding, opts);
  key.policyBinding =
    policyBinding && X.PolicyBindingKey.fromCanonicalParts(policyBinding[0], policyBinding[1]);
  return key;
}

export const subscriberPermissionsReady = (ready: boolean, trust: X.CommitUnitTrust) =>
  X.trustIsTrusted(trust) || ready;

const AUTHORITY_ONLY = new Set([
  "FateUpdate",
  "SubscribeRejected",
  "CatalogueAck",
  "ViewUpdate",
  "CatalogueSnapshot",
  "AuthorizationScopeView",
  "AuthorizationScopeAggregateReceipt",
  "AuthorizationScopeUnavailable",
  "AuthorizationScopeDecision",
]);

export function subscriberInboundMessageIsAuthorityOnly(
  message: SyncMessage,
  ingest: X.CommitUnitIngestContext,
  peer: X.PeerState,
): boolean {
  if (AUTHORITY_ONLY.has(message.kind)) return true;
  return (
    message.kind === "SessionClaims" &&
    (peer.rejectsRawSessionClaims() ||
      (!X.trustIsTrusted(ingest.trust) && !delegatedSessionCapability(ingest, peer.role())))
  );
}

export const delegatedSessionCapability = (ingest: X.CommitUnitIngestContext, role: X.PeerRole) =>
  ingest.trust === "Relay" && role.kind === "Relay";

export function admittedRequestPolicyBinding(
  ingest: X.CommitUnitIngestContext,
  peer: X.PeerState,
  direct: PolicyBinding | undefined,
  delegated: X.DelegatedSessionBinding | undefined,
): PolicyBinding | undefined {
  const role = peer.role();
  if (!delegated) return role.kind === "Relay" ? undefined : direct;
  if (delegatedSessionCapability(ingest, role)) {
    const binding: PolicyBinding = [delegated.identity, delegated.claims];
    return peer.admitsRelayBinding(binding) ? binding : undefined;
  }
  if (ingest.trust === "TrustedBackend" && role.kind === "ClientLink")
    return [delegated.identity, delegated.claims];
  if (
    peer.authorityQueryDelegate &&
    ingest.trust === "TrustedAuthority" &&
    X.authorSubjectEq(ingest.identity, X.AuthorSubjectT.SYSTEM) &&
    role.kind === "ClientLink"
  )
    return [delegated.identity, delegated.claims];
  return undefined;
}

export function subscriberPermissionSubject(
  ingest: X.CommitUnitIngestContext,
): AuthorSubject | undefined {
  switch (ingest.trust) {
    case "Session":
      return ingest.identity;
    case "Relay":
      return undefined;
    default:
      return X.AuthorSubjectT.SYSTEM;
  }
}

export type RowCells = Map<string, Value>;

// ---------------------------------------------------------------- write identities, options, tx handles

export type WriteIdentity =
  | { kind: "Database" }
  | { kind: "Session"; author: AuthorSubject }
  | { kind: "Attribution"; author: AuthorSubject };

export interface ResolvedWriteIdentity {
  madeBy: AuthorSubject;
  permissionSubject: AuthorSubject | undefined;
}

export type ExactWriteTarget = { kind: "Root" } | { kind: "Branch"; branch: X.BranchSelector };
export const exactTargetBranch = (t: ExactWriteTarget): X.BranchSelector =>
  t.kind === "Root" ? X.BranchSelector.default() : t.branch;

export type WriteTarget =
  | { kind: "Root" }
  | { kind: "BranchView"; head: X.BranchSelector; base: X.BranchViewBase | undefined };

export interface InsertOptions {
  rowId?: X.RowUuid;
  identity?: WriteIdentity;
  target?: ExactWriteTarget;
  updatedAtMs?: number;
}
export interface UpdateOptions {
  identity?: WriteIdentity;
  target?: WriteTarget;
  updatedAtMs?: number;
}
export type UpsertOptions = UpdateOptions;
export type DeleteOptions = UpdateOptions;
export interface RestoreOptions {
  identity?: WriteIdentity;
  target?: ExactWriteTarget;
  updatedAtMs?: number;
}

export function ensureTransactionIdentity(identity: WriteIdentity | undefined): void {
  if (!identity || identity.kind === "Database") return;
  throw dbError("Schema", "transaction identity is selected when the transaction is opened");
}
export function ensureExclusiveTarget(target: ExactWriteTarget | undefined): void {
  if (target && target.kind !== "Root")
    throw dbError("Schema", "exclusive transactions do not support branch writes");
}
export function ensureExclusiveViewTarget(target: WriteTarget | undefined): void {
  if (target && target.kind !== "Root")
    throw dbError("Schema", "exclusive transactions do not support branch writes");
}

abstract class TxReads {
  constructor(
    readonly db: Db,
    readonly txId: X.OpenTransactionId,
  ) {}
  read(table: string, row: X.RowUuid): Promise<RowCells | undefined> {
    return this.db.transactionRead(this.txId, table, row);
  }
  allPrepared(prepared: PreparedQuery, opts: ReadOpts = defaultReadOpts()): Promise<CurrentRow[]> {
    return this.db.transactionAll(this.txId, prepared, opts);
  }
  allPreparedForIdentity(
    prepared: PreparedQuery,
    author: AuthorSubject,
    opts: ReadOpts = defaultReadOpts(),
  ): Promise<CurrentRow[]> {
    return this.db.transactionAllForIdentity(this.txId, prepared, author, opts);
  }
  relationSnapshotPreparedWithOpts(
    prepared: PreparedQuery,
    opts: ReadOpts,
  ): Promise<X.RelationSnapshot> {
    return this.db.transactionRelationSnapshot(this.txId, prepared, opts);
  }
  relationSnapshotPreparedForIdentityWithOpts(
    prepared: PreparedQuery,
    author: AuthorSubject,
    opts: ReadOpts,
  ): Promise<X.RelationSnapshot> {
    return this.db.transactionRelationSnapshotForIdentity(this.txId, prepared, author, opts);
  }
}

export class MergeableTxRef extends TxReads {
  async insert(table: string, cells: RowCells, options: InsertOptions = {}): Promise<X.RowUuid> {
    ensureTransactionIdentity(options.identity);
    const knownFresh = options.rowId === undefined && this.db.rowIdSourceGuaranteesFresh;
    const row = options.rowId ?? this.db.rowIdSource.nextRowId();
    const target = options.target ?? { kind: "Root" };
    if (target.kind === "Root")
      await this.db.stageMergeableInsert(
        this.txId,
        table,
        row,
        cells,
        options.updatedAtMs,
        knownFresh,
      );
    else
      await this.db.stageMergeableInsertInBranch(
        this.txId,
        table,
        target.branch,
        row,
        cells,
        options.updatedAtMs,
        knownFresh,
      );
    return row;
  }
  async update(
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    options: UpdateOptions = {},
  ): Promise<void> {
    ensureTransactionIdentity(options.identity);
    const target = options.target ?? { kind: "Root" };
    if (target.kind === "Root")
      return this.db.stageMergeableUpdate(this.txId, table, row, patch, options.updatedAtMs);
    return this.db.stageMergeableUpdateInBranchView(
      this.txId,
      table,
      target.head,
      target.base,
      row,
      patch,
      options.updatedAtMs,
    );
  }
  upsert(
    table: string,
    row: X.RowUuid,
    cells: RowCells,
    options: UpsertOptions = {},
  ): Promise<void> {
    return this.db.stageMergeableUpsert(this.txId, table, row, cells, options, false);
  }
  async delete(table: string, row: X.RowUuid, options: DeleteOptions = {}): Promise<void> {
    ensureTransactionIdentity(options.identity);
    const target = options.target ?? { kind: "Root" };
    if (target.kind === "Root")
      return this.db.stageMergeableDelete(this.txId, table, row, options.updatedAtMs);
    return this.db.stageMergeableDeleteInBranchView(
      this.txId,
      table,
      target.head,
      target.base,
      row,
      options.updatedAtMs,
    );
  }
  async restore(
    table: string,
    row: X.RowUuid,
    cells: RowCells | undefined,
    options: RestoreOptions = {},
  ): Promise<void> {
    ensureTransactionIdentity(options.identity);
    if (!cells) throw dbError("Schema", "transaction restores currently require replacement cells");
    const target = options.target ?? { kind: "Root" };
    if (target.kind === "Root")
      return this.db.stageMergeableRestore(this.txId, table, row, cells, options.updatedAtMs);
    return this.db.stageMergeableRestoreInBranch(
      this.txId,
      table,
      target.branch,
      row,
      cells,
      options.updatedAtMs,
    );
  }
  async moveBetweenBranches(
    table: string,
    source: X.BranchSelector,
    target: X.BranchSelector,
    row: X.RowUuid,
  ): Promise<void> {
    if (X.deepEq(source, target))
      throw dbError("Schema", "branch move requires distinct source and target selectors");
    const node = await this.db.node.node.lock();
    let cells: RowCells | undefined;
    try {
      cells = await node.visibleCurrentCellsInBranch(table, source, row);
    } finally {
      this.db.node.node.unlock();
    }
    if (!cells)
      throw dbError(
        "NotObserved",
        `source branch-local row is not visible: ${X.rowUuidString(row)}`,
      );
    const tableSchema = this.db.schema.tables.find((t: any) => t.name === table);
    if (!tableSchema) throw dbError("Schema", `unknown table ${table}`);
    let targetCells: RowCells;
    try {
      targetCells = this.db.schema.projectBranchSelector(tableSchema, target)[1];
    } catch (message) {
      throw dbError("Schema", String(message));
    }
    for (const [k, v] of targetCells) cells.set(k, v);
    await this.restore(table, row, cells, { target: { kind: "Branch", branch: target } });
    await this.delete(table, row, {
      target: { kind: "BranchView", head: source, base: undefined },
    });
  }
}

export class MergeableTx extends MergeableTxRef {
  private committed = false;
  async commit(): Promise<TxId> {
    const tx = await this.db.commitMergeableHandle(this.txId);
    this.committed = true;
    return tx;
  }
  drop(): void {
    if (!this.committed) this.db.node.abandonOrEnqueueTransaction(this.txId);
  }
}

export class ExclusiveTxRef extends TxReads {
  all(table: string): Promise<CurrentRow[]> {
    return this.db.transactionCurrentRows(this.txId, table);
  }
  async insert(table: string, cells: RowCells, options: InsertOptions = {}): Promise<X.RowUuid> {
    ensureTransactionIdentity(options.identity);
    ensureExclusiveTarget(options.target);
    const row = options.rowId ?? this.db.rowIdSource.nextRowId();
    await this.db.stageExclusiveInsert(
      this.txId,
      table,
      row,
      cells,
      options.updatedAtMs,
      options.rowId !== undefined,
    );
    return row;
  }
  update(
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    options: UpdateOptions = {},
  ): Promise<void> {
    ensureTransactionIdentity(options.identity);
    ensureExclusiveViewTarget(options.target);
    return this.db.stageExclusiveUpdate(this.txId, table, row, patch, options.updatedAtMs);
  }
  upsert(
    table: string,
    row: X.RowUuid,
    cells: RowCells,
    options: UpsertOptions = {},
  ): Promise<void> {
    ensureTransactionIdentity(options.identity);
    ensureExclusiveViewTarget(options.target);
    return this.db.stageExclusiveUpsert(this.txId, table, row, cells, options.updatedAtMs, false);
  }
  delete(table: string, row: X.RowUuid, options: DeleteOptions = {}): Promise<void> {
    ensureTransactionIdentity(options.identity);
    ensureExclusiveViewTarget(options.target);
    return this.db.stageExclusiveDelete(this.txId, table, row, options.updatedAtMs);
  }
  restore(
    table: string,
    row: X.RowUuid,
    cells: RowCells | undefined,
    options: RestoreOptions = {},
  ): Promise<void> {
    ensureTransactionIdentity(options.identity);
    ensureExclusiveTarget(options.target);
    if (!cells) throw dbError("Schema", "exclusive transaction restores require replacement cells");
    return this.db.stageExclusiveRestore(this.txId, table, row, cells, options.updatedAtMs);
  }
}

export class ExclusiveTx extends ExclusiveTxRef {
  private committed = false;
  async commit(): Promise<TxId> {
    const tx = await this.db.commitExclusiveHandle(this.txId);
    this.committed = true;
    return tx;
  }
  drop(): void {
    if (!this.committed) this.db.node.abandonOrEnqueueTransaction(this.txId);
  }
}

// ---------------------------------------------------------------- write handle

export class WriteHandle {
  constructor(
    readonly node: { deref(): X.SharedNodeState | undefined },
    readonly rowUuidValue: X.RowUuid,
    readonly txId: TxId,
    readonly localTier: X.DurabilityTier,
    readonly queuedStatus: { v: QueuedMutationStatus } | undefined,
    readonly queuedAlias: { v: TxId | undefined } | undefined,
  ) {}

  rowUuid(): X.RowUuid {
    return this.rowUuidValue;
  }
  mergeableTxId(): TxId {
    return this.txId;
  }

  async wait(tier: X.DurabilityTier): Promise<TxId> {
    const r = X.durabilityRank;
    const reservationPending = this.queuedStatus?.v.kind === "Pending";
    if (r(tier) <= r(this.localTier) && !reservationPending) return this.txId;
    const state = await this.writeState();
    if (state.fate.kind === "Rejected") throw writeRejected(this.txId, state.fate.reason);
    if (state.fate.kind === "Pending" && r(tier) >= r("Global"))
      throw dbError("NotObserved", `write has not been accepted at requested tier ${tier}`);
    if (!transactionSatisfiesWait(state.fate, state.globalTime, state.durability, tier))
      throw dbError("NotObserved", `write has not reached requested tier ${tier}`);
    return this.txId;
  }

  async writeState(): Promise<WriteState> {
    const status = this.queuedStatus?.v;
    if (status?.kind === "Pending")
      return { fate: { kind: "Pending" }, globalTime: undefined, durability: "None" };
    if (status?.kind === "Failed") throw status.error;
    const node = this.node.deref();
    if (!node) throw dbError("NotObserved", "database handle was dropped");
    const resolved = this.queuedAlias?.v ?? this.txId;
    const state = await node.lock();
    let s: any;
    try {
      s = await state.transactionState(resolved);
    } finally {
      node.unlock();
    }
    if (!s)
      throw dbError("NotObserved", `transaction ${X.txIdDebug(this.txId)} is not known locally`);
    return { fate: s[0], globalTime: s[1], durability: s[2] };
  }
}

export function writeRejected(transactionId: unknown, reason: X.RejectionReason): DbError {
  const [, readable] = mutationErrorDetails(reason);
  return dbError(
    "WriteRejected",
    `transaction ${X.debugFmt(transactionId)} was rejected: ${X.debugFmt(reason)} (reason: ${readable})`,
  );
}

// ---------------------------------------------------------------- structured result trees

export function materializeResultTree(query: X.Query, snapshot: X.RelationSnapshot): ResultTree {
  const nodeFromRecord = (
    table: string,
    record: X.OwnedRecord,
    arrays: X.ArraySubquery[],
  ): ResultNode => {
    const descriptor = record.descriptor();
    let values: Value[];
    try {
      values = record.toValues();
    } catch (error) {
      throw dbError("Protocol", `invalid Groove terminal record: ${error}`);
    }
    const arrayByName = new Map(arrays.map((a) => [a.columnName, a]));
    const scalarFields: [string, X.ValueType][] = [];
    const scalarValues: Value[] = [];
    const relations = new Map<string, ResultRelation>();
    const fields = descriptor.fields();
    for (let i = 0; i < fields.length; i++) {
      const name = fields[i].name;
      const value = values[i];
      if (name === undefined) throw dbError("Protocol", "Groove terminal emitted an unnamed field");
      const array = arrayByName.get(name);
      if (!array) {
        scalarFields.push([name, fields[i].valueType]);
        scalarValues.push(value);
        continue;
      }
      if (value.kind !== "Array")
        throw dbError("Protocol", `Groove terminal relation ${name} was not an array`);
      const children = value.items.map((child: Value) => {
        if (child.kind !== "Record")
          throw dbError("Protocol", `Groove terminal relation ${name} contained a non-record`);
        return nodeFromRecord(array.table, child.record, array.nestedArrays);
      });
      relations.set(name, { kind: "Array", nodes: children });
    }
    const scalarDescriptor = X.RecordDescriptorT.new(scalarFields);
    let raw: Uint8Array;
    try {
      raw = scalarDescriptor.create(scalarValues);
    } catch (error) {
      throw dbError("Protocol", `invalid scalar projection from Groove terminal: ${error}`);
    }
    const row = X.CurrentRowT.new(table, X.OwnedRecordT.new(raw, scalarDescriptor));
    const occurrence = X.OutputOccurrenceId.singleSource(X.ObjectId.fromUuid(row.rowUuid()));
    return { occurrence, row, relations };
  };
  if (snapshot.edges.length)
    throw dbError("Protocol", "structured result unexpectedly contained relation-fact edges");
  const tree = new ResultTree();
  tree.roots = snapshot.rows.slice(0, snapshot.rootCount).map((row: CurrentRow) => {
    const [descriptor, raw] = row.encodedRecord();
    return nodeFromRecord(
      row.table(),
      X.OwnedRecordT.new(raw.slice(), descriptor),
      query.arraySubqueries,
    );
  });
  return tree;
}

// ---------------------------------------------------------------- the Db facade

export interface DbIdentity {
  node: X.NodeUuid;
  author: AuthorSubject;
}

/** Thread-affine high-level database handle. Method groups are installed from db/*.ts. */
export class Db {
  [method: string]: any;
  schema!: JazzSchema;
  schemaVersionId!: X.SchemaVersionId;
  schemaViewIsFixed = false;
  requiresOpenSchemaAdmission = false;
  schemaViews = new Map<string, JazzSchema>();
  identity!: DbIdentity;
  node!: X.Node;
  rowIdSource!: X.RowIdSource;
  rowIdSourceGuaranteesFresh = false;
  nextNowMs = new WriteClock();
  reservedTxId: TxId | undefined;
  ownerOperationAdmitted = false;
  backendAttribution = false;

  async reserveMintedTxTimeAfter(highWater: X.TxTime): Promise<void> {
    const node = this.node.node;
    (await node.lock()).reserveTxTimeAfter(highWater);
    node.unlock();
  }

  async mintedTxTimeHighWater(): Promise<X.TxTime> {
    const node = this.node.node;
    const v = (await node.lock()).txTimeHighWater();
    node.unlock();
    return v;
  }
}

import { catalogueMethods } from "./db/catalogue.js";
import { mutationMethods } from "./db/mutations.js";
import { readMethods } from "./db/reads.js";
import { transactionMethods } from "./db/transactions.js";
Object.assign(Db.prototype, catalogueMethods, mutationMethods, readMethods, transactionMethods);

export { ProductionRowIdSource, SeededRowIdSource } from "./db/config.js";
export type { RowIdSource, DbConfig } from "./db/config.js";

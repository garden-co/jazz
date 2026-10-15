// Split from db.rs: subscription state, publication boundary, stream and
// snapshot reduction helpers.
import * as X from "../_extern/db-core.js";
import { Channel, SortedMap, SortedSet, deferred } from "../support/index.js";
import type { AuthorSubject, CurrentRow, SyncMessage, Value } from "../_extern/db-core.js";
import { PositionalOrder } from "./positional_order.js";
import { dbError, type DbError, type PolicyBinding, type UpstreamCoverageHandle } from "./db.js";

type Occ = X.OutputOccurrenceId;
const cmpOcc = X.cmpOccurrence;
type Snapshot = X.RelationSnapshot;

// ---------------------------------------------------------------- reconciliation state

export interface ScalarProbe {
  deadline: number;
  rows: X.RowUuid[];
  future: Promise<X.CurrentRowsResult>;
}

export class ScalarReconciliation {
  generation: [number, X.SubscriptionKey, number] | undefined;
  pending: X.RowUuid[] = [];
  active: ScalarProbe | undefined;
  retryAt: number | undefined;
  retryDelayMs = 0;
}

export const DELETION_DISCOVERY_LIMIT_MS = 5000;

export interface DeletionDiscoverySpec {
  shape: X.ValidatedQuery;
  binding: X.Binding;
  author: AuthorSubject;
  claims: Map<string, Value>;
  context: X.PolicyBindingKey;
  authoritative: Set<string>;
}

export const sameDiscovery = (a: DeletionDiscoverySpec, b: DeletionDiscoverySpec) =>
  X.deepEq(a.shape, b.shape) &&
  X.deepEq(a.binding, b.binding) &&
  X.authorSubjectEq(a.author, b.author) &&
  X.deepEq(a.context, b.context);

export type DeletionReconciliation =
  | { kind: "Disabled" }
  | { kind: "Unchecked" }
  | { kind: "Deferred"; since: number; spec: DeletionDiscoverySpec }
  | {
      kind: "Discovering";
      maintained: X.LocalMaintainedViewSubscription;
      runtimeToken: number;
      since: number;
      spec: DeletionDiscoverySpec;
    }
  | { kind: "Pending"; rows: X.CurrentRowCoordinate[]; context: X.PolicyBindingKey; since: number }
  | { kind: "Probing"; rows: [string, X.RowUuid][]; since: number }
  | { kind: "Done" };

export function waitingSince(r: DeletionReconciliation): number | undefined {
  return "since" in r ? r.since : undefined;
}

export interface HeldRowCheck {
  coordinate: X.CurrentRowCoordinate;
  context: X.PolicyBindingKey;
  attempts: number;
  retryAt: number | undefined;
}

export interface HeldRowProbe {
  rows: [string, X.RowUuid][];
  deadline: number;
  future: Promise<X.CurrentRowsResult>;
}

const heldKey = (table: string, row: X.RowUuid) => `${table}\u0000${X.rowUuidString(row)}`;

export class HeldRowChecks {
  rows = new Map<string, HeldRowCheck>();
  active: HeldRowProbe | undefined;
  discoveries: DeletionReconciliation[] = [];
  upstreamConnection: number | undefined;

  enqueue(rows: X.CurrentRowCoordinate[], context: X.PolicyBindingKey): [string, X.RowUuid][] {
    const keys: [string, X.RowUuid][] = [];
    for (const coordinate of rows) {
      const k = heldKey(coordinate.table, coordinate.row);
      const existing = this.rows.get(k);
      if (!existing || !X.deepEq(existing.coordinate, coordinate))
        this.rows.set(k, { coordinate, context, attempts: 0, retryAt: undefined });
      keys.push([coordinate.table, coordinate.row]);
    }
    return keys;
  }

  adoptDiscovery(reconciliation: DeletionReconciliation): DeletionReconciliation | undefined {
    if (reconciliation.kind !== "Deferred" && reconciliation.kind !== "Discovering")
      return undefined;
    const spec = reconciliation.spec;
    for (const existing of this.discoveries) {
      if (
        (existing.kind === "Deferred" || existing.kind === "Discovering") &&
        sameDiscovery(existing.spec, spec)
      ) {
        existing.spec.authoritative = new Set(spec.authoritative);
        return reconciliation;
      }
    }
    this.discoveries.push(reconciliation);
    return undefined;
  }

  static retryDelayMs(attempts: number): number {
    return Math.min(250 * 2 ** Math.min(attempts, 7), 30000);
  }
}

// ---------------------------------------------------------------- events and rows

export interface RemovedRow {
  table: string;
  rowUuid: X.RowUuid;
  occurrenceId: Occ;
  index: number;
}
export const removedRowFromResultKey = (
  table: string,
  rowUuid: X.RowUuid,
  key: X.ResultKey,
): RemovedRow => ({
  table,
  rowUuid,
  occurrenceId: key.asOccurrence(),
  index: 0,
});

export interface SubscriptionOutputRow {
  occurrenceId: Occ;
  row: CurrentRow;
  previousIndex: number | undefined;
  index: number;
}

export type SubscriptionDelta = {
  kind: "Delta";
  reset: boolean;
  publishable: boolean;
  added: SubscriptionOutputRow[];
  updated: SubscriptionOutputRow[];
  removed: RemovedRow[];
  terminalOperations: X.TerminalOperation[];
  settled: boolean;
  tier: X.DurabilityTier;
};
export type SubscriptionEvent =
  | SubscriptionDelta
  | { kind: "Rejected"; reason: X.SubscribeRejectReason }
  | { kind: "Closed" };

const emptyDelta = (tier: X.DurabilityTier, settled: boolean): SubscriptionDelta => ({
  kind: "Delta",
  reset: false,
  publishable: true,
  added: [],
  updated: [],
  removed: [],
  terminalOperations: [],
  settled,
  tier,
});

// ---------------------------------------------------------------- snapshot index

export class RelationSnapshotIndex {
  constructor(
    public roots: PositionalOrder<Occ> = new PositionalOrder(cmpOcc),
    public terminalRecords = new SortedMap<Occ, X.TerminalRecordState>(cmpOcc),
  ) {}

  static fromSnapshot(snapshot: Snapshot): RelationSnapshotIndex {
    return RelationSnapshotIndex.withRootOccurrences(
      snapshot.rows.slice(0, snapshot.rootCount).map(subscriptionRowOccurrenceId),
    );
  }

  static withRootOccurrences(occurrences: Iterable<Occ>): RelationSnapshotIndex {
    const roots = new PositionalOrder<Occ>(cmpOcc);
    for (const o of occurrences) {
      roots.anonymize(o);
      roots.insert(roots.len(), o);
    }
    return new RelationSnapshotIndex(roots);
  }

  clone(): RelationSnapshotIndex {
    const roots = PositionalOrder.fromOrdered<Occ>(cmpOcc, []);
    Object.assign(roots, structuredClone(this.roots));
    return new RelationSnapshotIndex(roots, this.terminalRecords.clone());
  }
}

export type SubscriptionSnapshotSource = "LocalMaintained" | "LinkSnapshot";

export type SubscriptionKind = {
  kind: "Prepared";
  shape: X.ValidatedQuery;
  binding: X.Binding;
  maintainedSubscription: X.LocalMaintainedViewSubscription | undefined;
};

// ---------------------------------------------------------------- publication boundary

export class SubscriptionPublicationSnapshot {
  constructor(
    readonly snapshot: Snapshot,
    readonly occurrences: Occ[],
  ) {}
  static capture(
    snapshot: Snapshot,
    index: RelationSnapshotIndex,
  ): SubscriptionPublicationSnapshot {
    return new SubscriptionPublicationSnapshot(
      materializedSubscriptionSnapshot(snapshot, index),
      snapshotRootOccurrences(snapshot, index),
    );
  }
}

export class SubscriptionPublication {
  opened = false;
  deferred: SubscriptionPublicationSnapshot | undefined;
  reset = false;
  unresolved = new SortedSet<Occ>(cmpOcc);
  openingGate: X.OpeningGate | undefined;
  remoteWindow = false;
  windowFellBack = false;
}

export class SubscriptionSender {
  constructor(
    readonly sender: Channel<SubscriptionEvent>,
    readonly publication: SubscriptionPublication,
    readonly requestedTier: X.DurabilityTier,
  ) {}

  materialized(node: X.NodeState, shape: X.ValidatedQuery, event: SubscriptionEvent): boolean {
    const p = this.publication;
    if (event.kind === "Delta") {
      if (event.reset) p.unresolved = new SortedSet(cmpOcc);
      for (const row of event.removed) p.unresolved.delete(row.occurrenceId);
      for (const row of [...event.added, ...event.updated]) {
        const changed = { rootCount: 1, rows: [row.row], edges: [] } as unknown as Snapshot;
        if (node.relationSnapshotHasMaterializedRequiredCells(shape, changed))
          p.unresolved.delete(row.occurrenceId);
        else p.unresolved.add(row.occurrenceId);
      }
    }
    return p.unresolved.size === 0;
  }

  checkpoint(
    tier: X.DurabilityTier,
    settled: boolean,
    snapshot: Snapshot,
    index: RelationSnapshotIndex,
  ): SubscriptionPublicationSnapshot | undefined {
    const p = this.publication;
    if (X.durabilityRank(tier) >= X.durabilityRank("Global") && !settled && p.opened && !p.deferred)
      return SubscriptionPublicationSnapshot.capture(snapshot, index);
    return undefined;
  }

  publish(
    event: SubscriptionEvent,
    before: SubscriptionPublicationSnapshot | undefined,
    snapshot: Snapshot,
    index: RelationSnapshotIndex,
    materialized: boolean,
  ): boolean {
    if (event.kind !== "Delta") return this.unboundedSend(event);
    const { reset, publishable, settled, tier } = event;
    if (
      !publishable &&
      !reset &&
      !event.added.length &&
      !event.updated.length &&
      !event.removed.length &&
      !event.terminalOperations.length
    )
      return false;
    if (settled && !materialized)
      throw dbError("Protocol", "settled subscription retained unresolved required cells");
    const p = this.publication;
    const global = X.durabilityRank(this.requestedTier) >= X.durabilityRank("Global");
    if (!publishable || !materialized || (global && !settled)) {
      if (publishable && materialized && !p.opened && p.openingGate) p.openingGate.withheld = true;
      if (p.opened && !p.deferred && global) {
        if (!before)
          throw dbError("Protocol", "withheld subscription change has no publication baseline");
        p.deferred = before;
      }
      p.reset ||= reset || !global;
      return false;
    }
    if (p.openingGate) {
      if (!p.opened && p.openingGate.awaitsAnswer(settled)) {
        p.openingGate.withheld = true;
        return false;
      }
      p.openingGate = undefined;
    }
    let out: SubscriptionEvent = event;
    if (!p.opened || p.reset || (reset && p.deferred)) {
      const current = SubscriptionPublicationSnapshot.capture(snapshot, index);
      out = {
        ...emptyDelta(tier, settled),
        reset: true,
        added: subscriptionOutputsWithOccurrenceSidecar(current.snapshot, current.occurrences),
      };
    } else if (p.deferred) {
      const current = SubscriptionPublicationSnapshot.capture(snapshot, index);
      out = subscriptionTerminalDeltaEvent(
        tier,
        settled,
        p.deferred.snapshot,
        p.deferred.occurrences,
        current.snapshot,
        current.occurrences,
      );
    }
    p.opened = true;
    p.deferred = undefined;
    p.reset = false;
    return this.sender.send(out);
  }

  unboundedSend(event: SubscriptionEvent): boolean {
    const terminal =
      event.kind === "Closed" ||
      (event.kind === "Rejected" &&
        event.reason.kind !== "ShapeRegistrationPendingCatalogueAdmission");
    const p = this.publication;
    if (terminal) {
      p.deferred = undefined;
      p.openingGate = undefined;
    } else if (event.kind === "Delta" && p.openingGate && !p.opened) return true;
    return this.sender.send(event);
  }
}

// ---------------------------------------------------------------- subscription state

export interface SubscriptionState {
  closed: { v: boolean };
  terminalRows: boolean;
  deletionReconciliation: DeletionReconciliation;
  scalarReconciliationEnabled: boolean;
  scalarAuthorityRevision: number;
  scalarReconciliation: ScalarReconciliation;
  kind: SubscriptionKind;
  grooveRuntimeToken: number;
  localSubscriptionCleanup: { v: [number, X.GrooveSubscriptionId] | undefined };
  upstreamSubscriptionHandles: UpstreamCoverageHandle[];
  propagatesUpstream: boolean;
  requestIdentityClaims: PolicyBinding | undefined;
  author: AuthorSubject;
  authorizationMode: X.QueryAuthorizationMode;
  readTier: X.DurabilityTier;
  pendingOverlay: boolean;
  remoteReadTier: X.DurabilityTier | undefined;
  requiresAuthorityReceipt: boolean;
  readView: X.ReadViewSpec;
  snapshot: Snapshot;
  snapshotIndex: RelationSnapshotIndex;
  snapshotSource: SubscriptionSnapshotSource;
  settled: boolean;
  pendingInitialLocalSnapshot: boolean;
  pendingInitialOwnerResult: boolean;
  authorityWitness: UpstreamCoverageHandle[];
  sender: SubscriptionSender;
  /** WeakRef-style liveness for the shared subscription list. */
  dropped?: boolean;
}
export type SubscriptionList = { items: SubscriptionState[] };

// ---------------------------------------------------------------- stream

type SubscriptionCleanup = (ack: (() => void) | undefined) => Promise<void> | undefined;
type SubscriptionFinalization =
  | { kind: "Pending"; completion: Promise<void> }
  | { kind: "Failed"; code: string; message: string };

export class CleanupGuard {
  constructor(private cleanup: (() => void) | undefined) {}
  take(): () => void {
    const c = this.cleanup;
    if (!c) throw new Error("cleanup guard must only be disarmed once");
    this.cleanup = undefined;
    return c;
  }
  drop(): void {
    const c = this.cleanup;
    this.cleanup = undefined;
    c?.();
  }
}

export const subscriptionEventIsPublishable = (e: SubscriptionEvent) =>
  !(e.kind === "Delta" && !e.publishable);

export class SubscriptionStream {
  private finalization: SubscriptionFinalization | undefined;
  private terminated = false;
  private servingFallback = false;

  constructor(
    readonly receiver: Channel<SubscriptionEvent>,
    readonly state: SubscriptionState,
    private cleanup: SubscriptionCleanup | undefined,
    public windowFallback: SubscriptionStream | undefined = undefined,
  ) {}

  async close(): Promise<void> {
    await this.closeOwn();
    if (this.windowFallback) await this.windowFallback.close();
  }

  private async closeOwn(): Promise<void> {
    if (!this.finalization) {
      const cleanup = this.cleanup;
      if (!cleanup) return;
      this.cleanup = undefined;
      this.terminated = true;
      this.state.sender.publication.deferred = undefined;
      this.receiver.close();
      const ack = deferred<void>();
      let acked = false;
      const drain = cleanup(() => {
        acked = true;
        ack.resolve();
      });
      this.finalization = {
        kind: "Pending",
        completion: (async () => {
          if (drain) await drain;
          if (!acked) {
            const timeout = await Promise.race([
              ack.promise.then(() => false),
              Promise.resolve(true),
            ]);
            if (timeout && !acked) await ack.promise;
          }
        })(),
      };
    }
    const f = this.finalization!;
    if (f.kind === "Failed") throw dbError(f.code, f.message);
    try {
      await f.completion;
      this.finalization = undefined;
    } catch (error: any) {
      this.finalization = { kind: "Failed", code: error.code, message: error.message };
      throw error;
    }
  }

  isRemoteWindow(): boolean {
    return this.state.sender.publication.remoteWindow;
  }

  private syncWindowFallback(): void {
    if (this.servingFallback || !this.windowFallback) return;
    const p = this.state.sender.publication;
    if (p.windowFellBack) this.servingFallback = true;
    else if (p.opened || !p.openingGate) this.windowFallback = undefined;
  }

  private pollWindowFallback(): SubscriptionEvent | undefined | "pending" {
    for (;;) {
      const event = this.receiver.tryRecv();
      if (!event) break;
      if (event.kind === "Delta") {
        if (event.reset && event.publishable) {
          this.servingFallback = false;
          this.windowFallback = undefined;
          return event;
        }
        continue;
      }
      if (event.kind === "Rejected") return event;
      break;
    }
    if (!this.windowFallback) return undefined;
    return this.windowFallback.tryNextEvent() ?? "pending";
  }

  settledReceiverLocalSnapshot(): Snapshot {
    const s = this.state;
    if (!s.settled)
      throw dbError(
        "Protocol",
        "remote one-shot attempted to materialize before subscription settlement",
      );
    if (s.snapshotSource !== "LocalMaintained")
      throw dbError(
        "Protocol",
        "remote one-shot attempted to materialize a non-local maintained snapshot",
      );
    return materializedSubscriptionSnapshot(s.snapshot, s.snapshotIndex);
  }

  tryNextEvent(): SubscriptionEvent | undefined {
    if (this.terminated) return undefined;
    for (;;) {
      this.syncWindowFallback();
      if (this.servingFallback) {
        const r = this.pollWindowFallback();
        return r === "pending" ? undefined : r;
      }
      const event = this.receiver.tryRecv();
      if (!event) return undefined;
      if (subscriptionEventIsPublishable(event)) return event;
    }
  }

  async nextEvent(): Promise<SubscriptionEvent | undefined> {
    if (this.terminated) return undefined;
    for (;;) {
      this.syncWindowFallback();
      if (this.servingFallback) {
        const r = this.pollWindowFallback();
        if (r !== "pending") return r;
        const next = await Promise.race([this.receiver.recv(), this.windowFallback!.nextEvent()]);
        if (next) return next;
        continue;
      }
      const event = await this.receiver.recv();
      if (!event) return undefined;
      if (subscriptionEventIsPublishable(event)) return event;
    }
  }

  async *[Symbol.asyncIterator](): AsyncIterableIterator<SubscriptionEvent> {
    for (;;) {
      const e = await this.nextEvent();
      if (!e) return;
      yield e;
    }
  }

  drop(): void {
    const cleanup = this.cleanup;
    this.cleanup = undefined;
    cleanup?.(undefined);
  }
}

// ---------------------------------------------------------------- prepared queries

export type SerializedReadResult =
  | { kind: "Rows"; rows: CurrentRow[] }
  | { kind: "Relation"; snapshot: Snapshot };
export type SerializedSubscriptionAuthorization =
  | { kind: "ClientLocal" }
  | { kind: "TrustedServing"; author: AuthorSubject }
  | { kind: "TrustedClient"; author: AuthorSubject };

export class PreparedQuery {
  constructor(
    public requestIdentityClaims: PolicyBinding | undefined,
    public shapeValue: X.ValidatedQuery,
    public bindingValue: X.Binding,
    public localPlan: X.PreparedQueryPlanHandle | undefined,
    public globalPlan: X.PreparedQueryPlanHandle | undefined,
    public grooveRuntimeToken: number,
  ) {}

  requestIdentity(): AuthorSubject | undefined {
    return this.requestIdentityClaims?.[0];
  }

  withIdentityClaims(author: AuthorSubject, claims: Map<string, Value>): PreparedQuery {
    return new PreparedQuery(
      [author, claims],
      this.shapeValue,
      this.bindingValue,
      undefined,
      undefined,
      this.grooveRuntimeToken,
    );
  }

  requestPolicyBinding(author: AuthorSubject): PolicyBinding | undefined {
    if (this.requestIdentityClaims && !X.authorSubjectEq(this.requestIdentityClaims[0], author))
      throw dbError("Protocol", "prepared request identity does not match read identity");
    return this.requestIdentityClaims;
  }

  scopedNode(node: X.NodeState, author: AuthorSubject): X.ActiveSessionClaimsScope {
    this.requestPolicyBinding(author);
    return node.scopedOptionalSessionClaims(author, this.requestIdentityClaims?.[1]);
  }

  shape(): X.ValidatedQuery {
    return this.shapeValue;
  }
  binding(): X.Binding {
    return this.bindingValue;
  }

  planForTier(tier: X.DurabilityTier, token: number): X.PreparedQueryPlanHandle | undefined {
    if (this.grooveRuntimeToken !== token) return undefined;
    return tier === "Local" ? this.localPlan : tier === "Global" ? this.globalPlan : undefined;
  }
}

export const shouldInstallPreparedPlan = (shape: X.ValidatedQuery) =>
  shape.query().joins.length > 0 || shape.query().reachable.length > 0;

// ---------------------------------------------------------------- delta construction

export const subscriptionDeltaEvent = (
  tier: X.DurabilityTier,
  settled: boolean,
  previous: Snapshot,
  current: Snapshot,
  terminalRows: boolean,
) => subscriptionDeltaEventWithReset(tier, settled, previous, current, false, terminalRows);

export function demoteAuthorityReceiptSubscriptions(
  subscriptions: SubscriptionList,
  publishing: SortedSet<X.SubscriptionKey>,
): void {
  const retained: SubscriptionState[] = [];
  for (const state of subscriptions.items) {
    if (state.dropped) continue;
    if (state.propagatesUpstream) {
      state.requiresAuthorityReceipt = true;
      if (state.settled) {
        state.settled = false;
        const frameWillPublish = state.upstreamSubscriptionHandles.some((h) =>
          publishing.has(h.subscription),
        );
        if (!frameWillPublish && X.durabilityRank(state.readTier) < X.durabilityRank("Global"))
          state.sender.unboundedSend(
            subscriptionDeltaEvent(
              state.readTier,
              false,
              state.snapshot,
              state.snapshot,
              state.terminalRows,
            ),
          );
      }
    }
    retained.push(state);
  }
  subscriptions.items = retained;
}

export function subscriptionTerminalDeltaEvent(
  tier: X.DurabilityTier,
  settled: boolean,
  previous: Snapshot,
  previousOccurrences: Occ[],
  current: Snapshot,
  currentOccurrences: Occ[],
): SubscriptionEvent & { kind: "Delta" } {
  const previousRoots = previous.rows.slice(0, previous.rootCount);
  const currentRoots = current.rows.slice(0, current.rootCount);
  if (
    previousRoots.length !== previousOccurrences.length ||
    currentRoots.length !== currentOccurrences.length
  )
    throw dbError(
      "Protocol",
      "maintained terminal occurrence sidecar length does not match root rows",
    );
  let prefix = 0;
  const n = Math.min(previousRoots.length, currentRoots.length);
  while (prefix < n && cmpOcc(previousOccurrences[prefix], currentOccurrences[prefix]) === 0)
    prefix++;
  const updated: SubscriptionOutputRow[] = [];
  for (let i = 0; i < prefix; i++)
    if (!previousRoots[i].subscriptionEquivalent(currentRoots[i]))
      updated.push({
        occurrenceId: currentOccurrences[i],
        row: currentRoots[i],
        previousIndex: i,
        index: i,
      });
  const previousSuffix = new SortedMap<Occ, number>(cmpOcc);
  for (let i = prefix; i < previousOccurrences.length; i++)
    previousSuffix.set(previousOccurrences[i], i);
  const currentSuffix = new SortedSet<Occ>(cmpOcc, currentOccurrences.slice(prefix));
  const removed: RemovedRow[] = [];
  for (let i = prefix; i < previousRoots.length; i++) {
    if (currentSuffix.has(previousOccurrences[i])) continue;
    const row = previousRoots[i];
    removed.push({
      table: row.table(),
      rowUuid: row.rowUuid(),
      occurrenceId: previousOccurrences[i],
      index: i,
    });
  }
  const added: SubscriptionOutputRow[] = [];
  for (let i = prefix; i < currentRoots.length; i++) {
    const occurrenceId = currentOccurrences[i];
    const previousIndex = previousSuffix.get(occurrenceId);
    const out = { occurrenceId, row: currentRoots[i], previousIndex, index: i };
    if (previousIndex !== undefined) updated.push(out);
    else added.push(out);
  }
  return { ...emptyDelta(tier, settled), added, updated, removed };
}

export function subscriptionDeltaEventWithReset(
  tier: X.DurabilityTier,
  settled: boolean,
  previous: Snapshot,
  current: Snapshot,
  reset: boolean,
  _terminalRows: boolean,
): SubscriptionEvent {
  if (reset)
    return {
      ...emptyDelta(tier, settled),
      reset: true,
      added: current.rows.map((row: CurrentRow, i: number) =>
        subscriptionOutputRow(row, undefined, i),
      ),
    };
  const byId = (s: Snapshot) => {
    const m = new SortedMap<Occ, [number, CurrentRow]>(cmpOcc);
    s.rows.forEach((row: CurrentRow, i: number) => m.set(subscriptionRowKey(row), [i, row]));
    return m;
  };
  const previousById = byId(previous);
  const currentById = byId(current);
  const added: SubscriptionOutputRow[] = [];
  const updated: SubscriptionOutputRow[] = [];
  const removed: RemovedRow[] = [];
  for (const [key, [index, row]] of currentById) {
    const prev = previousById.get(key);
    if (!prev) added.push(subscriptionOutputRow(row, undefined, index));
    else if (!prev[1].subscriptionEquivalent(row) || prev[0] !== index)
      updated.push(subscriptionOutputRow(row, prev[0], index));
  }
  for (const [key, [index, row]] of previousById)
    if (!currentById.has(key))
      removed.push({ table: row.table(), rowUuid: row.rowUuid(), occurrenceId: key, index });
  return { ...emptyDelta(tier, settled), added, updated, removed };
}

export function applyMaintainedUpdateToSnapshot(
  holder: { snapshot: Snapshot; snapshotIndex: RelationSnapshotIndex },
  update: X.LocalMaintainedViewSubscriptionUpdate,
  table: string,
  tier: X.DurabilityTier,
  settled: boolean,
  terminalLayout: X.TerminalRootLayout | undefined,
): SubscriptionEvent {
  if (X.coveredInputTrace()) {
    const kind =
      update.kind === "Structured"
        ? `structured:${update.terminalOperations.length}`
        : update.kind === "Flat"
          ? `flat:add=${update.added.length} remove=${update.removed.length}`
          : `ordered-window:roots=${update.snapshot.rootCount} occurrences=${update.occurrenceIds.length}`;
    console.error(
      `JAZZ_COVERED_INPUT_TRACE stage=apply_maintained_snapshot roots=${holder.snapshot.rootCount} update=${kind}`,
    );
  }
  switch (update.kind) {
    case "OrderedWindow": {
      const currentIndex = relationSnapshotIndexWithRootOccurrences(
        update.snapshot,
        update.occurrenceIds,
      );
      const event = subscriptionTerminalDeltaEvent(
        tier,
        settled,
        holder.snapshot,
        snapshotRootOccurrences(holder.snapshot, holder.snapshotIndex),
        update.snapshot,
        update.occurrenceIds,
      );
      holder.snapshot = update.snapshot;
      holder.snapshotIndex = currentIndex;
      return event;
    }
    case "Flat": {
      const event = applyMaintainedMembershipUpdateToSnapshot(
        holder.snapshot,
        holder.snapshotIndex,
        update.added,
        update.removed,
        tier,
        settled,
      );
      for (const output of [...event.added, ...event.updated]) {
        const index = holder.snapshotIndex.roots.position(output.occurrenceId);
        if (index === undefined) continue;
        output.row = holder.snapshot.rows[index];
        output.index = index;
      }
      return event;
    }
    case "Structured": {
      const ops = update.terminalOperations;
      if (!ops.length) return emptyDelta(tier, settled);
      if (!terminalLayout)
        throw dbError(
          "Protocol",
          "structured terminal operation arrived without a prepared root layout",
        );
      const overrides = new Map<string, Occ>();
      for (const op of ops) {
        if (op.path.length) continue;
        try {
          X.terminalRootOccurrenceIdWithRootUnion(op.rootKey, terminalLayout.rootUnionArm);
          continue;
        } catch {}
        const rootBytes = op.rootKey[0] === 10 ? op.rootKey.subarray(1, 17) : undefined;
        const candidates = holder.snapshotIndex.roots
          .keysUnordered()
          .filter((c) => rootBytes && X.bytesEq(c.canonicalBytes().subarray(0, 16), rootBytes));
        if (candidates.length === 1) overrides.set(X.bytesKey(op.rootKey), candidates[0]);
        else if (candidates.length === 0)
          X.terminalRootOccurrenceIdWithRootUnion(op.rootKey, terminalLayout.rootUnionArm);
        else
          throw dbError(
            "Protocol",
            "terminal root key is ambiguous after hiding internal join identities",
          );
      }
      return applyTerminalOperationsToSubscriptionSnapshot(
        holder.snapshot,
        holder.snapshotIndex,
        ops,
        overrides,
        terminalLayout,
        table,
        tier,
        settled,
      );
    }
  }
  throw new Error("unreachable");
}

export function applyMaintainedMembershipUpdateToSnapshot(
  snapshot: Snapshot,
  index: RelationSnapshotIndex,
  updateAdded: [Occ, CurrentRow][],
  updateRemoved: Occ[],
  tier: X.DurabilityTier,
  settled: boolean,
): SubscriptionDelta {
  if (
    !snapshot.rows.length &&
    !snapshot.edges.length &&
    snapshot.rootCount === 0 &&
    !updateRemoved.length
  ) {
    snapshot.rootCount = updateAdded.length;
    snapshot.rows.push(...updateAdded.map(([, r]) => r));
    index.roots = RelationSnapshotIndex.withRootOccurrences(updateAdded.map(([o]) => o)).roots;
    return {
      ...emptyDelta(tier, settled),
      added: updateAdded.map(([occurrenceId, row], i) => ({
        occurrenceId,
        row,
        previousIndex: undefined,
        index: i,
      })),
    };
  }
  const added: SubscriptionOutputRow[] = [];
  const updated: SubscriptionOutputRow[] = [];
  const removed: RemovedRow[] = [];
  for (const [key, row] of updateAdded) {
    const position = index.roots.position(key);
    if (position !== undefined) {
      const equivalent = snapshot.rows[position].subscriptionEquivalent(row);
      if (X.coveredInputTrace())
        console.error(
          `JAZZ_COVERED_INPUT_TRACE stage=flat_snapshot_replace occurrence=${X.debugFmt(key)} position=${position} equivalent=${equivalent} old=${X.debugFmt(snapshot.rows[position])} new=${X.debugFmt(row)}`,
        );
      if (!equivalent) {
        snapshot.rows[position] = row;
        updated.push({ occurrenceId: key, row, previousIndex: position, index: position });
      }
    } else {
      const i = snapshot.rootCount;
      snapshot.rows.splice(i, 0, row);
      index.roots.insert(i, key);
      snapshot.rootCount++;
      added.push({ occurrenceId: key, row, previousIndex: undefined, index: i });
    }
  }
  if (updateRemoved.length) {
    const replaced = new SortedSet<Occ>(
      cmpOcc,
      updateAdded.map(([o]) => o),
    );
    const requested = new SortedSet<Occ>(cmpOcc, updateRemoved);
    const removals: [Occ, number][] = [];
    for (const o of requested) {
      if (replaced.has(o)) continue;
      const p = index.roots.position(o);
      if (p !== undefined) removals.push([o, p]);
    }
    removals.sort((a, b) => a[1] - b[1]);
    if (removals.length) {
      const positions = new Set(removals.map(([, p]) => p));
      for (const [occurrenceId, i] of removals) {
        const row = snapshot.rows[i];
        removed.push({ table: row.table(), rowUuid: row.rowUuid(), occurrenceId, index: i });
      }
      const rootCount = snapshot.rootCount;
      snapshot.rows = snapshot.rows.filter(
        (_: unknown, p: number) => p >= rootCount || !positions.has(p),
      );
      snapshot.rootCount -= positions.size;
      for (const [o] of removals) index.roots.remove(o);
    }
  }
  return { ...emptyDelta(tier, settled), added, updated, removed };
}

export function applyTerminalOperationsToSubscriptionSnapshot(
  snapshot: Snapshot,
  index: RelationSnapshotIndex,
  operations: X.TerminalOperation[],
  overrides: Map<string, Occ> | undefined,
  layout: X.TerminalRootLayout,
  table: string,
  tier: X.DurabilityTier,
  settled: boolean,
): SubscriptionEvent {
  const addressed: [Occ | undefined, X.TerminalOperation][] = [];
  for (const op of operations) {
    if (!X.deepEq(op.rootDescriptor, layout.rootDescriptor))
      throw dbError(
        "Protocol",
        "terminal operation descriptor disagrees with its prepared root layout",
      );
    if (op.path.length) {
      addressed.push([undefined, op]);
      continue;
    }
    if (!X.bytesEq(op.edit.key, op.rootKey))
      throw dbError("Protocol", "terminal root edit key does not match its addressed root key");
    const occurrence =
      overrides?.get(X.bytesKey(op.rootKey)) ??
      X.terminalRootOccurrenceIdWithRootUnion(op.rootKey, layout.rootUnionArm);
    addressed.push([occurrence, op]);
  }
  if (index.roots.len() !== snapshot.rootCount || index.roots.anonymousLen() !== 0)
    throw dbError("Protocol", "maintained snapshot root is missing an occurrence identity");
  const affected = new SortedSet<Occ>(
    cmpOcc,
    addressed.flatMap(([o]) => (o ? [o] : [])),
  );
  for (const o of affected) materializeSubscriptionTerminalRecord(snapshot, index, o);
  const before = new SortedMap<Occ, [number, CurrentRow]>(cmpOcc);
  for (const o of affected) {
    const i = index.roots.position(o);
    if (i !== undefined) before.set(o, [i, snapshot.rows[i]]);
  }
  const descendant: X.TerminalOperation[] = [];
  const removedRoots = new SortedSet<Occ>(cmpOcc);
  for (const [occurrence, op] of addressed) {
    if (!occurrence) {
      applyDescendantTerminalOperationsToSnapshot(
        snapshot,
        index,
        removedRoots,
        [op],
        layout.rootUnionArm,
      );
      descendant.push(op);
      continue;
    }
    const edit = op.edit;
    if (edit.kind === "Update") {
      if (!index.terminalRecords.has(occurrence)) {
        const position = index.roots.position(occurrence);
        if (position === undefined)
          throw dbError("Protocol", "terminal root update addressed a missing result");
        const [descriptor, raw] = snapshot.rows[position].encodedRecord();
        index.terminalRecords.set(
          occurrence,
          X.TerminalRecordState.new(X.OwnedRecordT.new(raw.slice(), descriptor)),
        );
      }
      index.terminalRecords
        .get(occurrence)!
        .updateRecord(X.OwnedRecordT.new(edit.value.slice(), op.rootDescriptor));
    } else if (edit.kind !== "Move") index.terminalRecords.delete(occurrence);
    switch (edit.kind) {
      case "Insert": {
        removedRoots.delete(occurrence);
        const existing = index.roots.remove(occurrence);
        if (existing !== undefined) {
          snapshot.rows.splice(existing, 1);
          snapshot.rootCount--;
        }
        const i = Math.min(edit.index, snapshot.rootCount);
        const row = terminalSubscriptionOutputRow(
          table,
          occurrence,
          edit.value,
          layout,
          undefined,
          i,
        ).row;
        index.roots.insert(i, occurrence);
        snapshot.rows.splice(i, 0, row);
        snapshot.rootCount++;
        break;
      }
      case "Update": {
        const i = index.roots.position(occurrence);
        if (i === undefined)
          throw dbError("Protocol", "terminal root update addressed a missing result");
        snapshot.rows[i] = terminalSubscriptionOutputRow(
          table,
          occurrence,
          edit.value,
          layout,
          i,
          i,
        ).row;
        break;
      }
      case "Remove": {
        removedRoots.add(occurrence);
        const i = index.roots.remove(occurrence);
        if (i === undefined)
          throw dbError("Protocol", "terminal root removal addressed a missing result");
        snapshot.rows.splice(i, 1);
        snapshot.rootCount--;
        break;
      }
      case "Move": {
        const prev = index.roots.remove(occurrence);
        if (prev === undefined)
          throw dbError("Protocol", "terminal root move addressed a missing result");
        const [row] = snapshot.rows.splice(prev, 1);
        const i = Math.min(edit.index, Math.max(snapshot.rootCount - 1, 0));
        index.roots.insert(i, occurrence);
        snapshot.rows.splice(i, 0, row);
        break;
      }
    }
  }
  const keptDescendants = descendant.filter((op) => {
    try {
      return !affected.has(
        X.terminalRootOccurrenceIdWithRootUnion(op.rootKey, layout.rootUnionArm),
      );
    } catch {
      return true;
    }
  });
  for (const o of affected) materializeSubscriptionTerminalRecord(snapshot, index, o);
  const added: SubscriptionOutputRow[] = [];
  const updated: SubscriptionOutputRow[] = [];
  const removed: RemovedRow[] = [];
  for (const occurrenceId of affected) {
    const previous = before.get(occurrenceId);
    const i = index.roots.position(occurrenceId);
    const current = i === undefined ? undefined : snapshot.rows[i];
    if (!previous && current)
      added.push({ occurrenceId, row: current, previousIndex: undefined, index: i! });
    else if (previous && !current)
      removed.push({
        table: previous[1].table(),
        rowUuid: previous[1].rowUuid(),
        occurrenceId,
        index: previous[0],
      });
    else if (previous && current && (!X.deepEq(previous[1], current) || previous[0] !== i))
      updated.push({ occurrenceId, row: current, previousIndex: previous[0], index: i! });
  }
  return {
    ...emptyDelta(tier, settled),
    added,
    updated,
    removed,
    terminalOperations: keptDescendants,
  };
}

export function applyDescendantTerminalOperationsToSnapshot(
  snapshot: Snapshot,
  index: RelationSnapshotIndex,
  removedRoots: SortedSet<Occ>,
  operations: X.TerminalOperation[],
  rootUnionArm: boolean,
): void {
  for (const op of operations) {
    if (!op.path.length) continue;
    const occurrence = X.terminalRootOccurrenceIdWithRootUnion(op.rootKey, rootUnionArm);
    const rootIndex = index.roots.position(occurrence);
    if (rootIndex === undefined) {
      if (removedRoots.has(occurrence)) continue;
      throw dbError("Protocol", "terminal descendant operation addressed a missing root");
    }
    const root = snapshot.rows[rootIndex];
    if (!root) throw dbError("Protocol", "terminal root position is outside the receiver snapshot");
    if (!index.terminalRecords.has(occurrence)) {
      const [descriptor, raw] = root.encodedRecord();
      index.terminalRecords.set(
        occurrence,
        X.TerminalRecordState.new(X.OwnedRecordT.new(raw.slice(), descriptor)),
      );
    }
    index.terminalRecords.get(occurrence)!.apply(op);
  }
}

export function materializedSubscriptionSnapshot(
  snapshot: Snapshot,
  index: RelationSnapshotIndex,
): Snapshot {
  const out = {
    ...snapshot,
    rows: snapshot.rows.slice(),
    edges: snapshot.edges.slice(),
  } as Snapshot;
  materializeSubscriptionTerminalRecords(out, index);
  return out;
}

export function materializeSubscriptionTerminalRecords(
  snapshot: Snapshot,
  index: RelationSnapshotIndex,
): void {
  for (const [o] of index.terminalRecords)
    materializeSubscriptionTerminalRecord(snapshot, index, o);
}

export function materializeSubscriptionTerminalRecord(
  snapshot: Snapshot,
  index: RelationSnapshotIndex,
  occurrence: Occ,
): void {
  const record = index.terminalRecords.get(occurrence);
  if (!record) return;
  const position = index.roots.position(occurrence);
  if (position === undefined)
    throw dbError("Protocol", "retained terminal record has no root occurrence");
  const root = snapshot.rows[position];
  if (!root) throw dbError("Protocol", "retained terminal root position is outside snapshot");
  snapshot.rows[position] = X.CurrentRowT.newWithPublicationFields(
    root.table(),
    record.record(),
    root.publicationFields().slice(),
  );
}

export function terminalSubscriptionOutputRow(
  table: string,
  occurrenceId: Occ,
  raw: Uint8Array,
  layout: X.TerminalRootLayout,
  previousIndex: number | undefined,
  index: number,
): SubscriptionOutputRow {
  const fields = layout.rootDescriptor.fields();
  const rootField = fields[layout.rootKeySlot];
  if (!rootField) throw dbError("Protocol", "terminal root layout key slot is out of bounds");
  if (rootField.name !== layout.rootKeyFieldName)
    throw dbError("Protocol", "terminal root layout key slot does not match its descriptor");
  if (layout.rootKeySlot !== 0)
    throw dbError("Protocol", "terminal root layout cannot be represented as a current row");
  const occupied = new Set([layout.rootKeySlot]);
  for (const field of layout.publicFields) {
    const d = fields[field.slot];
    if (!d) throw dbError("Protocol", `terminal root layout field ${field.name} is out of bounds`);
    if (d.name !== field.descriptorFieldName || occupied.has(field.slot))
      throw dbError(
        "Protocol",
        `terminal root layout field ${field.name} does not match its descriptor`,
      );
    occupied.add(field.slot);
  }
  const borrowed = X.BorrowedRecord.new(raw, layout.rootDescriptor);
  try {
    borrowed.toValues();
  } catch (error) {
    throw dbError("Protocol", `invalid terminal root payload: ${error}`);
  }
  let rowUuid: Uint8Array;
  try {
    rowUuid = borrowed.getUuid(layout.rootKeySlot);
  } catch (error) {
    throw dbError("Protocol", `invalid terminal root key: ${error}`);
  }
  if (!X.bytesEq(occurrenceId.canonicalBytes().subarray(0, 16), rowUuid))
    throw dbError("Protocol", "terminal root payload key does not match its addressed result");
  return {
    occurrenceId,
    row: X.CurrentRowT.newWithPublicationFields(
      table,
      X.OwnedRecordT.new(raw.slice(), layout.rootDescriptor),
      X.terminalRootPublicationFields(layout),
    ),
    previousIndex,
    index,
  };
}

export function snapshotRootOccurrences(snapshot: Snapshot, index: RelationSnapshotIndex): Occ[] {
  if (index.roots.len() > snapshot.rootCount)
    throw dbError("Protocol", "maintained root occurrence index exceeds snapshot roots");
  if (index.roots.len() !== snapshot.rootCount || index.roots.anonymousLen() !== 0)
    throw dbError("Protocol", "maintained snapshot root is missing an occurrence identity");
  return [...index.roots.iter()].filter((o): o is Occ => o !== undefined);
}

export function relationSnapshotIndexWithRootOccurrences(
  snapshot: Snapshot,
  occurrences: Occ[],
): RelationSnapshotIndex {
  if (snapshot.rootCount !== occurrences.length)
    throw dbError(
      "Protocol",
      "maintained terminal occurrence sidecar length does not match root rows",
    );
  try {
    return new RelationSnapshotIndex(PositionalOrder.fromOrdered(cmpOcc, occurrences));
  } catch {
    throw dbError("Protocol", "maintained terminal occurrence sidecar contains duplicate identity");
  }
}

export async function maintainedSnapshotIndexOrRowIndex(
  node: X.NodeState,
  kind: SubscriptionKind,
  snapshot: Snapshot,
): Promise<RelationSnapshotIndex> {
  if (!kind.maintainedSubscription || !kind.shape.query().flatJoin)
    return RelationSnapshotIndex.fromSnapshot(snapshot);
  const materialized = await node.materializeLocalMaintainedRelationSnapshotWithOccurrences(
    kind.maintainedSubscription,
  );
  if (materialized.snapshot.rootCount === snapshot.rootCount)
    return relationSnapshotIndexWithRootOccurrences(snapshot, materialized.rootOccurrenceIds);
  return RelationSnapshotIndex.fromSnapshot(snapshot);
}

export const relationSnapshotWithDeltaSlack = (snapshot: Snapshot): Snapshot =>
  ({ ...snapshot, rows: snapshot.rows.slice(), edges: snapshot.edges.slice() }) as Snapshot;

export function subscriptionIsSettled(
  node: X.NodeState,
  receipts: { v: { bindingViews: SortedSet<X.BindingViewKey> } | undefined },
  shape: X.ValidatedQuery,
  binding: X.Binding,
  tier: X.DurabilityTier,
  readView: X.ReadViewSpec,
  requiresAuthorityReceipt: boolean,
  authorityResultKey: X.AuthorityResultKey | undefined,
): boolean {
  if (X.durabilityRank(tier) <= X.durabilityRank("Local")) return true;
  const bindingViewKey: X.BindingViewKey = {
    shapeId: shape.shapeId(),
    bindingId: binding.bindingId(),
    readView: X.RegisterShapeOptions.readViewKey({
      ...X.RegisterShapeOptions.default(),
      tier,
      readView,
    }),
  };
  const key = authorityResultKey ?? X.AuthorityResultKey.unscoped(bindingViewKey);
  return (
    node.hasSettledAuthorityResult(key) &&
    !node.openingPendingForAuthorityResult(key) &&
    (!requiresAuthorityReceipt || !!receipts.v?.bindingViews.has(bindingViewKey))
  );
}

export function subscriptionRowOccurrenceId(row: CurrentRow): Occ {
  const root = X.ObjectId.fromUuid(row.rowUuid());
  const joined: X.ObjectId[] = [];
  for (let position = 1; ; position++) {
    const v = row.rawField(`__flat_join_row_${position}`);
    if (!v || v.kind !== "Uuid") break;
    joined.push(X.ObjectId.fromUuid(v.value));
  }
  return X.OutputOccurrenceId.new(root, joined);
}

export function subscriptionOutputsWithOccurrenceSidecar(
  snapshot: Snapshot,
  occurrenceIds: Occ[],
): SubscriptionOutputRow[] {
  if (occurrenceIds.length !== snapshot.rootCount)
    throw dbError("Protocol", "maintained root occurrence sidecar length does not match root rows");
  const unique = new SortedSet<Occ>(cmpOcc);
  return snapshot.rows.slice(0, snapshot.rootCount).map((row: CurrentRow, index: number) => {
    const occurrenceId = occurrenceIds[index];
    if (!X.bytesEq(occurrenceId.canonicalBytes().subarray(0, 16), X.rowUuidBytes(row.rowUuid())))
      throw dbError("Protocol", "maintained root occurrence sidecar root does not match row");
    if (!unique.add(occurrenceId))
      throw dbError("Protocol", "maintained root occurrence sidecar contains duplicate identity");
    return { occurrenceId, row, previousIndex: undefined, index };
  });
}

export function resetRemovedRoots(
  previous: Snapshot,
  previousIndex: RelationSnapshotIndex,
  currentOccurrences: Occ[],
): RemovedRow[] {
  const current = new SortedSet<Occ>(cmpOcc, currentOccurrences);
  return previousIndex.roots
    .keysUnordered()
    .filter((o) => !current.has(o))
    .map((occurrenceId) => {
      const position = previousIndex.roots.position(occurrenceId);
      if (position === undefined) throw new Error("indexed root occurrence has a position");
      const row = previous.rows[position];
      return { table: row.table(), rowUuid: row.rowUuid(), occurrenceId, index: position };
    });
}

export const subscriptionOutputRow = (
  row: CurrentRow,
  previousIndex: number | undefined,
  index: number,
): SubscriptionOutputRow => ({
  occurrenceId: subscriptionRowOccurrenceId(row),
  row,
  previousIndex,
  index,
});
export const subscriptionRowKey = subscriptionRowOccurrenceId;

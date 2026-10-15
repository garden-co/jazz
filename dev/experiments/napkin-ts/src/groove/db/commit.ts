import { equalBytes, toHex } from "../../support/bytes.js";
import { SortedMap, SortedSet, cmpStr } from "../../support/collections.js";
import {
  type NodeRef,
  type RecordDescriptor,
  type TableSchema,
  type TickMetrics,
  cmpNodeRef,
  encodeVariantRecord,
  PublicationInstallFailures,
} from "../../_extern/groove-db-storage.js";
import {
  type WriteOperation,
  OwnedStorage,
  StagedWriteOverlay,
  StagedWriteState,
} from "../storage/index.js";
import {
  type Database,
  type PersistedBatch,
  AppliedBatch,
  DbError,
  LARGE_VALUE_METADATA_CF,
  MetadataChunkInstallObserver,
  decodeLargeValueRootReferences,
  decodeStagedLargeValueAtKey,
  emptyRootReferences,
  encodeLargeValueRootReferences,
  largeValueNodeKey,
  largeValueNodeTransitionOperations,
  largeValueRootKey,
  lvMeta,
  wakePublicationOwner,
  stagedLargeValueKey as stagedLargeValueKeyOf,
  type StagedLargeValue,
} from "./index.js";
import {
  type BatchOperation,
  type DatabaseBatch,
  type RawRecordInput,
  type RecordInput,
  primaryKeyValueBytes,
} from "./batch.js";
import {
  primaryKeyBytes,
  resolveRawRecordInput,
  resolveRecordInput,
  validateRawPrimaryKey,
} from "./encoding.js";
import {
  type LockRelease,
  type PendingTableWrite,
  type StorageReadMetrics,
  MeteredStorage,
  computeTableDeltas,
  emptyReadMetrics,
  storedRecord,
} from "./storage_helpers.js";
import { completedLargeValueCleanupOperations } from "./facade.js";
import { RecordStore } from "../storage/index.js";

const wrapIvm = async <T>(db: Database, p: Promise<T>): Promise<T> => {
  try {
    return await p;
  } catch (error) {
    db.poisoned = true;
    throw new DbError("IvmRuntime", error);
  }
};

export class CommitMethods {
  /** Run one IVM tick without base-table writes. */
  async flush(this: Database): Promise<void> {
    this.ensureNotPoisoned();
    await wrapIvm(this, this.ivmRuntime.drivePendingIncremental());
    this.refreshResidentWrites();
    const resident = new StagedWriteOverlay(this.storage, this.residentWrites);
    const storage = new MeteredStorage(resident, this.storageReadMetrics);
    this.lastTickMetrics = await wrapIvm(this, this.ivmRuntime.tick([], storage));
  }

  getLastCommitMetrics(this: Database) {
    return this.lastCommitMetrics;
  }
  getLastTickMetrics(this: Database): TickMetrics | undefined {
    return this.lastTickMetrics;
  }
  getStorageReadMetrics(this: Database): StorageReadMetrics {
    return structuredClone(this.storageReadMetrics.metrics);
  }
  resetStorageReadMetrics(this: Database) {
    this.storageReadMetrics.metrics = emptyReadMetrics();
  }
  takeStorageReadMetrics(this: Database): StorageReadMetrics {
    const metrics = this.storageReadMetrics.metrics;
    this.resetStorageReadMetrics();
    return metrics;
  }

  /**
   * Apply resident rows and unblocked terminal deltas before ordered
   * persistence. The returned handle owns the pending persistence work.
   */
  async applyBatch(this: Database, batch: DatabaseBatch): Promise<AppliedBatch> {
    batch.checkExactBase(this);
    this.ensureNotPoisoned();
    const acceptedLargeValues = batch.acceptedLargeValues.slice();
    const deferNotifications = batch.notificationTiming === "AfterPersistence";
    // Later ordinary writes must not invalidate an ensure_exact result.
    const exactKeys = batch.exactKeys;
    batch.exactKeys = new Map();
    const pendingWrites = this.pendingWritesFromBatch(batch);
    for (const write of pendingWrites) {
      const expected = exactKeys.get(`${write.table}\0${toHex(write.key)}`);
      if (expected) {
        const stored = storedRecord(write);
        if (!stored || !equalBytes(stored, expected[2]))
          throw new DbError("ImmutableBatchConflict");
      }
    }
    const resident = () => this.residentStorage();
    const acceptedStaging: StagedLargeValue[] = [];
    for (const id of acceptedLargeValues) {
      const key = stagedLargeValueKeyOf(id);
      // An earlier pending batch may already have consumed this id.
      const encoded = await dbTry(resident().get(LARGE_VALUE_METADATA_CF, key));
      if (!encoded) throw lvMeta("accepted staging id is missing or already consumed");
      acceptedStaging.push(decodeStagedLargeValueAtKey(key, encoded));
    }
    for (const staged of acceptedStaging) {
      let found = false;
      for (const write of pendingWrites) {
        if (write.kind !== "Set") continue;
        found = dbSync(() =>
          write.descriptor.visitLargeValueRefs(write.record, (r: any) => r.equals(staged.valueRef)),
        );
        if (found) break;
      }
      if (!found)
        throw lvMeta("accepted staging root is not referenced by this physical-record batch");
    }
    const overlay = new StagedWriteOverlay(this.storage, this.residentWrites);
    const stores = pendingWrites.map((w) => new RecordStore(overlay, w.table, w.descriptor));
    const tableDeltas = await computeTableDeltas(pendingWrites, stores, this.ivmRuntime.schema());
    const changedTables = [...new Set<string>(tableDeltas.map((d) => d.table))].sort(cmpStr);
    const durableRootDeltas = new SortedMap<NodeRef, number>(cmpNodeRef);
    for (const td of tableDeltas)
      for (const delta of td.deltas)
        dbSync(() =>
          td.descriptor.visitLargeValueRefs(delta.record, (r: any) => {
            durableRootDeltas.set(r.root, (durableRootDeltas.get(r.root) ?? 0) + delta.weight);
            return false;
          }),
        );
    const stagedOperations: WriteOperation[] = pendingWrites.map((w) =>
      w.kind === "Set"
        ? {
            kind: "Set",
            cf: w.table,
            key: w.key,
            value: encodeVariantRecord(w.variantTag, w.record),
          }
        : { kind: "Delete", cf: w.table, key: w.key },
    );
    for (const id of acceptedLargeValues) {
      const key = stagedLargeValueKeyOf(id);
      if (!(await dbTry(resident().get(LARGE_VALUE_METADATA_CF, key))))
        throw lvMeta("accepted staging id is missing or already consumed");
      stagedOperations.push({ kind: "Delete", cf: LARGE_VALUE_METADATA_CF, key });
      stagedOperations.push(...(await completedLargeValueCleanupOperations(resident(), id)));
    }
    const acceptedRoots = new SortedMap<NodeRef, number>(cmpNodeRef);
    for (const staged of acceptedStaging)
      acceptedRoots.set(staged.valueRef.root, (acceptedRoots.get(staged.valueRef.root) ?? 0) + 1);
    const roots = new SortedSet<NodeRef>(cmpNodeRef, [
      ...durableRootDeltas.keysArray(),
      ...acceptedRoots.keysArray(),
    ]);
    // Newly allocated index ids become durable atomically with this publication.
    const indexIdRegistrations = this.ivmRuntime.indexIds().value.pending();
    stagedOperations.push(...indexIdRegistrations.operations());
    const residentOverlay = new StagedWriteOverlay(this.storage, this.residentWrites);
    const stagedState = StagedWriteState.from(stagedOperations);
    const storage = new StagedWriteOverlay(residentOverlay, stagedState);
    const residentInstallDurable = { value: false };
    const installFailures = new PublicationInstallFailures();
    const observer = new MetadataChunkInstallObserver(
      new WeakRef(this.storage),
      new WeakRef(this.largeValueLifecycle),
      {
        storage: new OwnedStorage(storage),
        staged: stagedState,
        lifecycleHeld: this.largeValueLifecycleHeld,
        durable: residentInstallDurable,
        installFailures,
      },
    );
    const tickStart = performance.now();
    const residentTick: any = await wrapIvm(
      this,
      this.ivmRuntime.tickResidentStaged(
        tableDeltas,
        new OwnedStorage(storage),
        deferNotifications,
        [observer, installFailures],
      ),
    );
    // Durable node writes belong to this publication's atomic batch.
    stagedState.extend(residentTick.takeDurableWrites());
    // Finish CPU-only continuations this tick scheduled before returning.
    this.drainSelfScheduledResidentProgress();
    const ivmTickTime = performance.now() - tickStart;
    let lifecycleGuard: LockRelease | undefined;
    if (roots.size && !this.largeValuePublicationLifecycleGuard)
      lifecycleGuard = await this.lockLargeValueLifecycle();
    if (roots.size) {
      const nodeTransitions: [NodeRef, number][] = [];
      const lifecycleOperations: WriteOperation[] = [];
      for (const root of roots) {
        const key = largeValueRootKey(root);
        const encoded = await dbTry(storage.get(LARGE_VALUE_METADATA_CF, key));
        const refs = encoded ? decodeLargeValueRootReferences(encoded) : emptyRootReferences();
        const previousTotal = refs.durable + refs.staged;
        refs.durable += durableRootDeltas.get(root) ?? 0;
        if (refs.durable < 0 || !Number.isSafeInteger(refs.durable))
          throw lvMeta("durable root count overflow/underflow");
        refs.staged -= acceptedRoots.get(root) ?? 0;
        if (refs.staged < 0) throw lvMeta("staged root count underflow");
        const nextTotal = refs.durable + refs.staged;
        if (previousTotal === 0 && nextTotal > 0 && !refs.nodeActive) {
          if (await dbTry(storage.get(LARGE_VALUE_METADATA_CF, largeValueNodeKey(root)))) {
            refs.nodeActive = true;
            nodeTransitions.push([root, 1]);
          }
        } else if (previousTotal > 0 && nextTotal === 0 && refs.nodeActive) {
          refs.nodeActive = false;
          nodeTransitions.push([root, -1]);
        }
        lifecycleOperations.push({
          kind: "Set",
          cf: LARGE_VALUE_METADATA_CF,
          key,
          value: encodeLargeValueRootReferences(refs),
        });
      }
      lifecycleOperations.push(
        ...(await largeValueNodeTransitionOperations(
          storage,
          new SortedMap(cmpNodeRef),
          nodeTransitions,
          false,
        )),
      );
      stagedState.extend(lifecycleOperations);
    }
    // Every fallible operation is complete: allocate the id without an intervening await.
    const publication = this.nextPublicationId++;
    const tick = this.ivmRuntime.assignResidentPublication(residentTick, publication);
    this.residentWrites.extendShared(stagedState.snapshot());
    this.residentPublications.set(publication, stagedState);
    this.ivmRuntime.indexIds().value.markPersisted(indexIdRegistrations);
    if (roots.size) {
      if (lifecycleGuard) {
        this.largeValuePublicationLifecycleGuard = lifecycleGuard;
        this.largeValueLifecycleHeld.value = true;
      }
      this.largeValueLifecyclePublications.add(publication);
    }
    return new AppliedBatch(
      publication,
      { value: this.storage },
      stagedState,
      residentInstallDurable,
      this.publicationPersistence,
      ivmTickTime,
      tick,
      changedTables,
      deferNotifications,
      { value: "Applied" },
      this.abandonedApplication,
      this.ivmRuntime.indexIds(),
    );
  }

  /** Persist a resident publication while this caller keeps driving suspended query work. */
  async persistWithProgress(this: Database, applied: AppliedBatch): Promise<PersistedBatch> {
    const persist = applied.persist();
    let settled = false;
    persist.finally(() => (settled = true)).catch(() => {});
    // A progress failure poisons the database; the publication still settles.
    while (!settled && this.hasPendingProgress())
      await Promise.race([persist, this.driveReadyProgress().catch(() => {})]);
    return persist;
  }

  /** Take the lifecycle mutex, driving suspended evaluations that may hold it. */
  async lockLargeValueLifecycle(this: Database): Promise<LockRelease> {
    for (;;) {
      const guard = this.largeValueLifecycle.tryLock();
      if (guard) return guard;
      if (!this.hasPendingProgress()) return this.largeValueLifecycle.lock();
      await Promise.race([
        this.largeValueLifecycle.lock().then((g) => g()),
        this.driveReadyProgress(),
      ]);
    }
  }

  /** Install one persistence result and advance the contiguous durable frontier. */
  finishPersistence(this: Database, persistence: PersistedBatch): number {
    try {
      const result = this.finishPersistenceInner(persistence);
      if (!this.residentPublications.size) wakePublicationOwner(this.publicationPersistence);
      return result;
    } catch (error) {
      wakePublicationOwner(this.publicationPersistence);
      throw error;
    }
  }

  finishPersistenceInner(this: Database, persistence: PersistedBatch): number {
    if (this.publicationPersistence !== persistence.receipt.order)
      throw new DbError("PublicationNotFound", persistence.publication);
    persistence.receipt.finish();
    this.lastTickMetrics = persistence.metrics.tick;
    this.lastCommitMetrics = persistence.metrics;
    if (persistence.result) {
      if (persistence.notificationsDeferred)
        this.ivmRuntime.discardDeferredNotifications(persistence.publication);
      this.poisoned = true;
      throw DbError.from(persistence.result);
    }
    if (!this.residentPublications.has(persistence.publication))
      throw new DbError("PublicationNotFound", persistence.publication);
    this.persistedPublications.add(persistence.publication);
    let frontier = (this.durablePublicationFrontier ?? 0) + 1;
    while (this.persistedPublications.delete(frontier)) {
      this.residentPublications.delete(frontier);
      this.largeValueLifecyclePublications.delete(frontier);
      this.durablePublicationFrontier = frontier++;
    }
    if (!this.largeValueLifecyclePublications.size) {
      const guard = this.largeValuePublicationLifecycleGuard;
      this.largeValuePublicationLifecycleGuard = undefined;
      this.largeValueLifecycleHeld.value = false;
      guard?.();
    }
    this.refreshResidentWrites();
    if (persistence.notificationsDeferred)
      this.ivmRuntime.settleDeferredNotifications(persistence.publication);
    return persistence.publication;
  }

  pendingWritesFromBatch(this: Database, batch: DatabaseBatch): PendingTableWrite[] {
    const prepared = batch.prepared;
    const writes = prepared.owner === this.batchPreparationOwner ? prepared.writes.slice() : [];
    for (const op of batch.operations.slice(writes.length))
      writes.push(this.pendingWriteFromOperation(op));
    return writes;
  }

  refreshResidentWrites(this: Database) {
    const resident = new StagedWriteState();
    for (const ops of this.residentPublications.valuesArray())
      resident.extendShared(ops.snapshot());
    this.residentWrites = resident;
  }

  ensureBatchStorageTxn(this: Database, batch: DatabaseBatch) {
    if (batch.prepared.owner !== this.batchPreparationOwner) {
      batch.prepared = { owner: this.batchPreparationOwner, writes: [] };
      batch.txnOperations = new StagedWriteState();
      batch.txnIndexedOperations = 0;
    }
    while (batch.txnIndexedOperations < batch.operations.length) {
      const pending = this.pendingWriteFromOperation(batch.operations[batch.txnIndexedOperations]);
      batch.txnOperations.stage(this.ownedStorageOperationForPending(pending));
      batch.prepared.writes.push(pending);
      batch.txnIndexedOperations++;
    }
  }

  ownedStorageOperationForPending(this: Database, pending: PendingTableWrite): WriteOperation {
    return pending.kind === "Set"
      ? { kind: "Set", cf: pending.table, key: pending.key, value: storedRecord(pending)! }
      : { kind: "Delete", cf: pending.table, key: pending.key };
  }

  pendingWriteFromOperation(this: Database, operation: BatchOperation): PendingTableWrite {
    const table = this.table(operation.table);
    switch (operation.kind) {
      case "Insert":
      case "Update": {
        const descriptor = this.recordDescriptorForInput(operation.table, operation.record);
        const [variantTag, d, record] = resolveRecordInput(table, operation.record, descriptor);
        const key = primaryKeyBytes(table, variantTag, d, record);
        return {
          kind: "Set",
          mode: operation.kind,
          table: operation.table,
          key,
          variantTag,
          descriptor: d,
          record,
        };
      }
      case "InsertRaw":
      case "InsertRawFresh":
      case "UpdateRaw": {
        const descriptor = this.recordDescriptorForRawInput(operation.table, operation.record);
        const [variantTag, d, record] = resolveRawRecordInput(table, operation.record, descriptor);
        const key = primaryKeyValueBytes(operation.key);
        validateRawPrimaryKey(table, variantTag, d, record, key);
        const mode =
          operation.kind === "InsertRaw"
            ? "Insert"
            : operation.kind === "UpdateRaw"
              ? "Update"
              : "InsertFresh";
        return {
          kind: "Set",
          mode,
          table: operation.table,
          key,
          variantTag,
          descriptor: d,
          record,
        };
      }
      case "Delete":
        return {
          kind: "Delete",
          table: operation.table,
          key: primaryKeyValueBytes(operation.key),
          descriptor: table.recordSchema(),
        };
    }
  }

  recordDescriptorForInput(this: Database, table: string, record: RecordInput): RecordDescriptor {
    return this.recordDescriptor(table, record.kind === "Values" ? 0 : record.record.variantTag());
  }

  recordDescriptorForRawInput(
    this: Database,
    table: string,
    record: RawRecordInput,
  ): RecordDescriptor {
    return this.recordDescriptor(table, record.kind === "Payload" ? 0 : record.record.variantTag());
  }

  recordDescriptor(this: Database, table: string, variantTag: number): RecordDescriptor {
    const cached = this.ivmRuntime.recordDescriptor(table, variantTag);
    if (cached) return cached;
    const descriptor = this.table(table).recordSchemaForVariant(variantTag);
    if (!descriptor) throw new DbError("UnknownTableVariant", { table, version: variantTag });
    return descriptor;
  }

  tableStorageDescriptor(this: Database, table: string): RecordDescriptor {
    const descriptor = this.ivmRuntime.tableStorageDescriptor(table);
    if (!descriptor) throw new DbError("TableNotFound", table);
    return descriptor;
  }

  table(this: Database, table: string): TableSchema {
    this.ensureNotPoisoned();
    const schema = this.ivmRuntime.table(table);
    if (!schema) throw new DbError("TableNotFound", table);
    return schema;
  }

  ensureNotPoisoned(this: Database) {
    if (this.poisoned || this.abandonedApplication.value) throw new DbError("DatabasePoisoned");
  }
}

export async function dbTry<T>(p: Promise<T>): Promise<T> {
  try {
    return await p;
  } catch (error) {
    throw DbError.from(error);
  }
}
export function dbSync<T>(f: () => T): T {
  try {
    return f();
  } catch (error) {
    throw DbError.from(error);
  }
}

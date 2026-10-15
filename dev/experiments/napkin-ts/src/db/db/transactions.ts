// Mergeable and exclusive transaction handles and staging.
import * as X from "../../_extern/db-core.js";
import type { AuthorSubject, CurrentRow, SyncMessage, TxId, Value } from "../../_extern/db-core.js";
import type {
  Db,
  DeleteOptions,
  InsertOptions,
  PreparedQuery,
  ReadOpts,
  RestoreOptions,
  RowCells,
  UpdateOptions,
  UpsertOptions,
  WriteHandle,
  WriteIdentity,
} from "../db.js";
import {
  ExclusiveTx,
  ExclusiveTxRef,
  MergeableTx,
  MergeableTxRef,
  dbError,
  ensureDefaultReadView,
  ensureExclusiveTarget,
  ensureExclusiveViewTarget,
  ensureTransactionIdentity,
  readForWriteDenied,
  rowAlreadyDeleted,
  rowAlreadyExists,
} from "../db.js";
import { validateUpdatedAtMs, type LargeValueUpdate } from "./mutations.js";

type TxId_ = X.OpenTransactionId;

const extend = (into: RowCells, from: RowCells): RowCells => {
  for (const [k, v] of from) into.set(k, v);
  return into;
};

const notVisibleInBranchView = (row: X.RowUuid) =>
  dbError("NotObserved", `row is not visible in branch view: ${X.rowUuidString(row)}`);

/** Run `f` with the node guard admitted for this open transaction; always releases it. */
async function withTx<T>(
  db: Db,
  txId: TxId_,
  f: (node: X.NodeState) => Promise<T> | T,
): Promise<T> {
  const node = await db.node.lockForTransactionOperation(txId, db.ownerOperationAdmitted);
  try {
    return await f(node);
  } finally {
    db.node.node.unlock();
  }
}

async function withTxOpen<T>(
  db: Db,
  txId: TxId_,
  f: (node: X.NodeState) => Promise<T> | T,
): Promise<T> {
  const node = await db.node.lockForTransactionOpen(txId, db.ownerOperationAdmitted);
  try {
    return await f(node);
  } finally {
    db.node.node.unlock();
  }
}

async function withTxCommit<T>(
  db: Db,
  txId: TxId_,
  f: (node: X.NodeState) => Promise<T> | T,
): Promise<T> {
  for (;;) {
    const node = await db.node.lockForTransactionOperation(txId, db.ownerOperationAdmitted);
    let ready: boolean;
    try {
      ready =
        !db.node.hasPendingLocalPublications() || !node.transactionNeedsLargeValueStaging(txId);
      if (ready) return await f(node);
    } finally {
      db.node.node.unlock();
    }
    await db.node.settleLocalPublications();
    // Reacquire through the normal admission/tombstone checks after suspension.
  }
}

async function withNode<T>(db: Db, f: (node: X.NodeState) => Promise<T> | T): Promise<T> {
  const node = await db.node.node.lock();
  try {
    return await f(node);
  } finally {
    db.node.node.unlock();
  }
}

/** Admission identity for a queued transaction begin. */
function queuedBeginIdentity(
  author: AuthorSubject | undefined,
  attribution: AuthorSubject | undefined,
): WriteIdentity {
  if (author !== undefined && attribution !== undefined)
    throw dbError("WriteRejected", "attributed transaction cannot override admission identity");
  if (attribution !== undefined) return { kind: "Attribution", author: attribution };
  if (author !== undefined) return { kind: "Session", author };
  return { kind: "Database" };
}

const ZERO_ROW = () => X.RowUuidT.fromBytes(new Uint8Array(16));

export const transactionMethods = {
  async transactionIsExclusive(this: Db, id: TxId_): Promise<boolean> {
    return withTx(this, id, (node) => node.transactionIsExclusive(id));
  },

  /** The queries an exclusive read hydrates for the sources it reads beyond its root. */
  async exclusiveSourceHydrationQueries(
    this: Db,
    id: TxId_,
    prepared: PreparedQuery,
    includeDeleted: boolean,
  ): Promise<X.Query[]> {
    return withTx(this, id, (node) =>
      node.exclusiveSourceHydrationQueries(prepared.shape(), prepared.binding(), includeDeleted),
    );
  },

  /** Build a mergeable transaction that commits multiple writes under one id. */
  async mergeableTx(this: Db): Promise<MergeableTx> {
    const txId = X.OpenTransactionIdT.new();
    await this.beginMergeable(txId);
    return new MergeableTx(this, txId);
  },

  /** Run `callback` in a mergeable transaction and commit all staged writes as one transaction. */
  async transaction<T>(this: Db, callback: (tx: MergeableTx) => Promise<T>): Promise<[T, TxId]> {
    const tx: MergeableTx = await this.mergeableTx();
    let value: T;
    try {
      value = await callback(tx);
    } catch (e) {
      tx.drop();
      throw e;
    }
    return [value, await tx.commit()];
  },

  async mergeableTxForIdentity(this: Db, author: AuthorSubject): Promise<MergeableTx> {
    const txId = X.OpenTransactionIdT.new();
    await this.beginMergeableForIdentity(txId, author);
    return new MergeableTx(this, txId);
  },

  async transactionForIdentity<T>(
    this: Db,
    author: AuthorSubject,
    callback: (tx: MergeableTx) => Promise<T>,
  ): Promise<[T, TxId]> {
    const tx: MergeableTx = await this.mergeableTxForIdentity(author);
    let value: T;
    try {
      value = await callback(tx);
    } catch (e) {
      tx.drop();
      throw e;
    }
    return [value, await tx.commit()];
  },

  /** Open a mergeable transaction; the caller owns its lifetime. */
  beginMergeable(this: Db, id: TxId_): Promise<void> {
    return this.beginMergeableWithIdentity(id, { kind: "Database" });
  },

  beginMergeableForIdentity(this: Db, id: TxId_, author: AuthorSubject): Promise<void> {
    return this.beginMergeableWithIdentity(id, { kind: "Session", author });
  },

  async beginMergeableWithIdentity(this: Db, id: TxId_, identity: WriteIdentity): Promise<void> {
    this.ensureMutationOperationAdmitted();
    const { madeBy, permissionSubject } = this.resolveWriteIdentity(identity);
    await withTxOpen(this, id, (node) => node.openMergeable(id, madeBy, permissionSubject));
  },

  /** Queue mergeable transaction admission behind earlier owner operations. */
  enqueueBeginMergeable(
    this: Db,
    id: TxId_,
    author?: AuthorSubject,
    attribution?: AuthorSubject,
  ): void {
    const identity = queuedBeginIdentity(author, attribution);
    const db: Db = this.cloneForOwnerOperation();
    this.node.enqueueTransactionOperation(id, () => db.beginMergeableWithIdentity(id, identity));
  },

  /** Non-owning operations handle for an already-open mergeable transaction. */
  mergeableTxRef(this: Db, txId: TxId_): MergeableTxRef {
    return new MergeableTxRef(this, txId);
  },

  async rejectAttributedMergeableBranch(this: Db, txId: TxId_): Promise<void> {
    if (await withTx(this, txId, (node) => node.transactionIsAttributed(txId)))
      throw dbError(
        "WriteRejected",
        "backend-attributed transactions do not support branch targets",
      );
  },

  async stageMergeableInsert(
    this: Db,
    txId: TxId_,
    table: string,
    row: X.RowUuid,
    cellsIn: RowCells,
    nowMsIn: number | undefined,
    knownFreshRow: boolean,
  ): Promise<void> {
    const nowMs = nowMsIn ?? this.nextNowMsValue();
    const cells = this.applyInsertDefaults(table, cellsIn);
    await withTx(this, txId, (node) =>
      node.txWriteMergeableInSchema(
        txId,
        this.schemaVersionId,
        table,
        row,
        cells,
        undefined,
        nowMs,
        knownFreshRow,
      ),
    );
  },

  stageMergeableInsertInBranch(
    this: Db,
    txId: TxId_,
    table: string,
    branch: X.BranchSelector,
    row: X.RowUuid,
    cells: RowCells,
    nowMs: number | undefined,
    knownFreshRow: boolean,
  ): Promise<void> {
    return this.stageMergeableInsertInBranchWithVerifiedInheritedCells(
      txId,
      table,
      branch,
      row,
      cells,
      nowMs,
      knownFreshRow,
      undefined,
      false,
      undefined,
    );
  },

  /** Stage an exact branch write; a branch-view fallback carries engine-read base cells. */
  async stageMergeableInsertInBranchWithVerifiedInheritedCells(
    this: Db,
    txId: TxId_,
    table: string,
    branch: X.BranchSelector,
    row: X.RowUuid,
    cellsIn: RowCells,
    nowMsIn: number | undefined,
    knownFreshRow: boolean,
    verifiedInheritedCells: RowCells | undefined,
    replacePendingDeletion: boolean,
    branchViewCopy: X.BranchViewCopyEvidence | undefined,
  ): Promise<void> {
    await this.rejectAttributedMergeableBranch(txId);
    const nowMs = nowMsIn ?? this.nextNowMsValue();
    const cells = this.applyInsertDefaults(table, cellsIn);
    await withTx(this, txId, (node) =>
      node.txWriteMergeableInSchemaAndBranchWithVerifiedInheritedCells(
        txId,
        this.schemaVersionId,
        table,
        row,
        cells,
        undefined,
        nowMs,
        branch,
        knownFreshRow,
        verifiedInheritedCells,
        replacePendingDeletion,
        branchViewCopy,
      ),
    );
  },

  async stageMergeableUpdate(
    this: Db,
    txId: TxId_,
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    nowMsIn?: number,
  ): Promise<void> {
    // Staged inserts or restores remain writable, but committed tombstones reject a patch.
    const nowMs = nowMsIn ?? this.nextNowMsValue();
    await withTx(this, txId, async (node) => {
      if (
        (await node.txVisibleCurrentCellsInBranch(txId, table, row, X.BranchSelector.default())) ===
          undefined &&
        (await node.localDeletionWinnerTxIdInSchema(this.schemaVersionId, table, row)) !== undefined
      )
        throw rowAlreadyDeleted(row);
      await node.txPatchMergeableInSchema(txId, this.schemaVersionId, table, row, patch, nowMs);
    });
  },

  async stageMergeableUpdateInBranchView(
    this: Db,
    txId: TxId_,
    table: string,
    head: X.BranchSelector,
    base: X.BranchViewBase | undefined,
    row: X.RowUuid,
    patch: RowCells,
    nowMsIn?: number,
  ): Promise<void> {
    await this.rejectAttributedMergeableBranch(txId);
    if (patch.size === 0)
      throw dbError("Schema", "branch-view update requires at least one authored column");
    const nowMs = nowMsIn ?? this.nextNowMsValue();
    const headCells = await withTx(this, txId, (node) =>
      node.visibleCurrentCellsInBranch(table, head, row),
    );
    if (headCells !== undefined) {
      await withTx(this, txId, (node) =>
        node.txPatchMergeableInSchemaAndBranch(
          txId,
          this.schemaVersionId,
          table,
          row,
          patch,
          nowMs,
          head,
          false,
        ),
      );
      return;
    }
    const inherited: RowCells | undefined = await withTx(this, txId, (node) =>
      node.visibleCurrentCellsInBranchView(table, head, base, row),
    );
    if (!inherited) throw notVisibleInBranchView(row);
    const verifiedInheritedCells = new Map(inherited);
    const branchViewCopy = await withTx(this, txId, (node) =>
      node.inheritedBranchViewCopyEvidence(table, head, base, row),
    );
    extend(inherited, patch);
    return this.stageMergeableInsertInBranchWithVerifiedInheritedCells(
      txId,
      table,
      head,
      row,
      inherited,
      nowMs,
      false,
      verifiedInheritedCells,
      false,
      branchViewCopy,
    );
  },

  /** Resolve a staged upsert, optionally checking typed insert completeness. */
  async stageMergeableUpsert(
    this: Db,
    txId: TxId_,
    table: string,
    row: X.RowUuid,
    cells: RowCells,
    options: UpsertOptions,
    requireCompleteInsert: boolean,
  ): Promise<void> {
    ensureTransactionIdentity(options.identity);
    const target = options.target ?? { kind: "Root" };
    if (target.kind === "Root") {
      if (await this.mergeableTransactionUpsertExists(txId, table, row))
        return this.stageMergeableUpdate(txId, table, row, cells, options.updatedAtMs);
      if (requireCompleteInsert) this.validateCompleteInsert(table, cells);
      return this.stageMergeableInsert(txId, table, row, cells, options.updatedAtMs, false);
    }
    return this.stageMergeableUpsertInBranchView(
      txId,
      table,
      target.head,
      target.base,
      row,
      cells,
      options.updatedAtMs,
      requireCompleteInsert,
    );
  },

  async mergeableTransactionUpsertExists(
    this: Db,
    txId: TxId_,
    table: string,
    row: X.RowUuid,
  ): Promise<boolean> {
    const exists = (await this.transactionRead(txId, table, row)) !== undefined;
    if (!exists) await this.ensureRowNotDeleted(table, row);
    return exists;
  },

  async stageMergeableUpsertInBranchView(
    this: Db,
    txId: TxId_,
    table: string,
    head: X.BranchSelector,
    base: X.BranchViewBase | undefined,
    row: X.RowUuid,
    cells: RowCells,
    nowMsIn: number | undefined,
    requireCompleteInsert: boolean,
  ): Promise<void> {
    await this.rejectAttributedMergeableBranch(txId);
    await this.ensureBranchViewRowNotDeleted(table, head, base, row);
    const [headExists, replacePendingDeletion, inherited] = await withNode(this, async (node) => {
      const state: X.TransactionBranchRowState = await node.txCurrentRowStateInBranch(
        txId,
        table,
        row,
        head,
      );
      let headExists = false;
      let replace = false;
      if (state.kind === "Visible") headExists = true;
      else if (state.kind === "PendingDeletion") {
        headExists = (await node.visibleCurrentCellsInBranch(table, head, row)) !== undefined;
        replace = true;
      }
      const inherited: RowCells | undefined = headExists
        ? undefined
        : await node.visibleCurrentCellsInBranchView(table, head, base, row);
      return [headExists, replace, inherited] as const;
    });

    if (headExists) {
      if (cells.size === 0)
        throw dbError("Schema", "branch upsert update requires at least one authored column");
      const nowMs = nowMsIn ?? this.nextNowMsValue();
      await withNode(this, (node) =>
        node.txPatchMergeableInSchemaAndBranch(
          txId,
          this.schemaVersionId,
          table,
          row,
          cells,
          nowMs,
          head,
          replacePendingDeletion,
        ),
      );
      return;
    }

    const verifiedInheritedCells = inherited ? new Map(inherited) : undefined;
    const branchViewCopy = inherited
      ? await withTx(this, txId, (node) =>
          node.inheritedBranchViewCopyEvidence(table, head, base, row),
        )
      : undefined;
    const inserted = extend(inherited ?? new Map(), cells);
    if (requireCompleteInsert) this.validateCompleteInsert(table, inserted);
    return this.stageMergeableInsertInBranchWithVerifiedInheritedCells(
      txId,
      table,
      head,
      row,
      inserted,
      nowMsIn,
      false,
      verifiedInheritedCells,
      replacePendingDeletion,
      branchViewCopy,
    );
  },

  async stageMergeableDelete(
    this: Db,
    txId: TxId_,
    table: string,
    row: X.RowUuid,
    nowMsIn?: number,
  ): Promise<void> {
    const nowMs = nowMsIn ?? this.nextNowMsValue();
    await withTx(this, txId, (node) =>
      node.txWriteMergeableInSchema(
        txId,
        this.schemaVersionId,
        table,
        row,
        new Map(),
        "Deleted",
        nowMs,
        false,
      ),
    );
  },

  async stageMergeableDeleteInBranchView(
    this: Db,
    txId: TxId_,
    table: string,
    head: X.BranchSelector,
    base: X.BranchViewBase | undefined,
    row: X.RowUuid,
    nowMsIn?: number,
  ): Promise<void> {
    await this.rejectAttributedMergeableBranch(txId);
    if (
      (await withTx(this, txId, (node) =>
        node.visibleCurrentCellsInBranchView(table, head, base, row),
      )) === undefined
    )
      throw notVisibleInBranchView(row);
    const nowMs = nowMsIn ?? this.nextNowMsValue();
    await withTx(this, txId, (node) =>
      node.txWriteMergeableInSchemaAndBranch(
        txId,
        this.schemaVersionId,
        table,
        row,
        new Map(),
        "Deleted",
        nowMs,
        head,
        false,
      ),
    );
  },

  async stageMergeableRestore(
    this: Db,
    txId: TxId_,
    table: string,
    row: X.RowUuid,
    cellsIn: RowCells,
    nowMsIn?: number,
  ): Promise<void> {
    await this.rejectAttributedMergeableBranch(txId);
    const nowMs = nowMsIn ?? this.nextNowMsValue();
    const cells = this.applyInsertDefaults(table, cellsIn);
    await withTx(this, txId, async (node) => {
      await node.txWriteMergeableInSchema(
        txId,
        this.schemaVersionId,
        table,
        row,
        cells,
        undefined,
        nowMs,
        false,
      );
      await node.txWriteMergeableInSchema(
        txId,
        this.schemaVersionId,
        table,
        row,
        new Map(),
        "Restored",
        nowMs,
        false,
      );
    });
  },

  async stageMergeableRestoreInBranch(
    this: Db,
    txId: TxId_,
    table: string,
    branch: X.BranchSelector,
    row: X.RowUuid,
    cellsIn: RowCells,
    nowMsIn?: number,
  ): Promise<void> {
    const nowMs = nowMsIn ?? this.nextNowMsValue();
    const cells = this.applyInsertDefaults(table, cellsIn);
    await withTx(this, txId, (node) => {
      node.txWriteMergeableInSchemaAndBranch(
        txId,
        this.schemaVersionId,
        table,
        row,
        cells,
        undefined,
        nowMs,
        branch,
        false,
      );
      node.txWriteMergeableInSchemaAndBranch(
        txId,
        this.schemaVersionId,
        table,
        row,
        new Map(),
        "Restored",
        nowMs,
        branch,
        false,
      );
    });
  },

  /** Commit an owned mergeable transaction handle. */
  async commitMergeableHandle(this: Db, openTxId: TxId_): Promise<TxId> {
    const published = await withTxCommit(this, openTxId, (node) =>
      node.commitMergeableOpen(openTxId, () => this.nextNowMsValue()),
    );
    const txId = published.txId;
    await this.settleLocalPublication(published, undefined);
    return txId;
  },

  /** Reserve the final identity and retain mergeable commit/finalization on the node owner. */
  enqueueCommitMergeableHandle(this: Db, openTxId: TxId_): WriteHandle {
    return this.enqueueCommitMergeableHandleAtMs(openTxId, this.nextNowMsValue());
  },

  enqueueCommitMergeableHandleAtMs(this: Db, openTxId: TxId_, nowMs: number): WriteHandle {
    const txId = this.reserveTransactionIdAtMs(nowMs);
    const db: Db = this.cloneForReservedTransaction(txId);
    const status = this.node.enqueueTransactionCommit(openTxId, txId, "Mergeable", async () => {
      const published = await withTxCommit(db, openTxId, (node) =>
        node.commitMergeableOpenAt(openTxId, txId, () => nowMs),
      );
      await db.settleLocalPublication(published, undefined);
    });
    return this.queuedWriteHandle(ZERO_ROW(), txId, status, undefined);
  },

  /** Abandon an owned open transaction handle. */
  abandonTransactionHandle(this: Db, openTxId: TxId_): void {
    this.node.markTransactionAbandoned(openTxId);
    const node = this.node.node.tryLock();
    if (!node) throw new Error("synchronous node operation reentered a suspended operation");
    try {
      node.abandonTx(openTxId);
    } finally {
      // Eviction is async: the next tick releases this transaction's staged large-value roots.
      if (node.hasReleasedLargeValues()) this.node.scheduleTick("Immediate");
      this.node.node.unlock();
      this.node.clearTransactionAbandonment(openTxId);
    }
  },

  /** Queue rollback after all earlier admission/staging work. */
  enqueueAbandonTransactionHandle(this: Db, openTxId: TxId_): void {
    const db: Db = this.cloneForOwnerOperation();
    this.node.enqueueTransactionCleanup(async () => {
      let failure: unknown;
      try {
        await withNode(db, (node) => node.abandonTx(openTxId));
      } catch (e) {
        failure = e;
      }
      db.node.retireQueuedTransactionError(openTxId);
      if (failure !== undefined && !X.isMissingOpenBatch(failure)) throw failure;
      await db.node.evictReleasedLargeValues();
    });
  },

  /** Open an exclusive transaction over the current local snapshot. */
  async exclusiveTx(this: Db): Promise<ExclusiveTx> {
    const txId = X.OpenTransactionIdT.new();
    await this.openExclusiveHandle(txId);
    return new ExclusiveTx(this, txId);
  },

  beginExclusive(this: Db, id: TxId_): Promise<void> {
    return this.beginExclusiveWithIdentity(id, { kind: "Database" });
  },

  beginExclusiveForIdentity(this: Db, id: TxId_, author: AuthorSubject): Promise<void> {
    return this.beginExclusiveWithIdentity(id, { kind: "Session", author });
  },

  async beginExclusiveWithIdentity(this: Db, id: TxId_, identity: WriteIdentity): Promise<void> {
    this.ensureMutationOperationAdmitted();
    const { madeBy, permissionSubject } = this.resolveWriteIdentity(identity);
    await withTxOpen(this, id, (node) =>
      node.openExclusiveWithIdentity(id, madeBy, permissionSubject ?? madeBy),
    );
  },

  enqueueBeginExclusive(
    this: Db,
    id: TxId_,
    author?: AuthorSubject,
    attribution?: AuthorSubject,
  ): void {
    const identity = queuedBeginIdentity(author, attribution);
    const db: Db = this.cloneForOwnerOperation();
    this.node.enqueueTransactionOperation(id, () => db.beginExclusiveWithIdentity(id, identity));
  },

  exclusiveTxRef(this: Db, txId: TxId_): ExclusiveTxRef {
    return new ExclusiveTxRef(this, txId);
  },

  enqueueTransactionInsert(
    this: Db,
    id: TxId_,
    table: string,
    cells: RowCells,
    options: InsertOptions,
  ): X.RowUuid {
    const explicitRowId = options.rowId !== undefined;
    const row = options.rowId ?? this.rowIdSource.nextRowId();
    const opts: InsertOptions = { ...options, rowId: row };
    const db: Db = this.cloneForOwnerOperation();
    this.node.enqueueTransactionOperation(id, async () => {
      if (await db.transactionIsExclusive(id)) {
        // Only a caller-supplied id gets the create-only check.
        ensureTransactionIdentity(opts.identity);
        ensureExclusiveTarget(opts.target);
        await db.stageExclusiveInsert(id, table, row, cells, opts.updatedAtMs, explicitRowId);
      } else {
        await db.mergeableTxRef(id).insert(table, cells, opts);
      }
    });
    return row;
  },

  enqueueTransactionUpdate(
    this: Db,
    id: TxId_,
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    options: UpdateOptions,
  ): void {
    const db: Db = this.cloneForOwnerOperation();
    this.node.enqueueTransactionOperation(id, async () =>
      (await db.transactionIsExclusive(id))
        ? db.exclusiveTxRef(id).update(table, row, patch, options)
        : db.mergeableTxRef(id).update(table, row, patch, options),
    );
  },

  /** Queue typed partial large-value descriptors behind earlier operations of an open transaction. */
  enqueueTransactionLargeValueUpdate(
    this: Db,
    id: TxId_,
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    mutations: LargeValueUpdate[],
    updatedAtMs: number | undefined,
  ): void {
    validateUpdatedAtMs(updatedAtMs);
    const db: Db = this.cloneForOwnerOperation();
    this.node.enqueueTransactionOperation(id, () =>
      db.stageTransactionLargeValueUpdate(id, table, row, patch, mutations, updatedAtMs),
    );
  },

  enqueueTransactionUpsert(
    this: Db,
    id: TxId_,
    table: string,
    row: X.RowUuid,
    cells: RowCells,
    options: UpsertOptions,
  ): void {
    const db: Db = this.cloneForOwnerOperation();
    this.node.enqueueTransactionOperation(id, async () => {
      const exclusive = await db.transactionIsExclusive(id);
      // Typed bindings submit patches without a synchronous preflight.
      if (
        (options.target ?? { kind: "Root" }).kind === "Root" &&
        (await db.transactionReadRaw(id, table, row)) === undefined
      )
        db.validateCompleteInsert(table, cells);
      if (exclusive) {
        ensureTransactionIdentity(options.identity);
        ensureExclusiveViewTarget(options.target);
        return db.stageExclusiveUpsert(id, table, row, cells, options.updatedAtMs, true);
      }
      return db.stageMergeableUpsert(id, table, row, cells, options, true);
    });
  },

  enqueueTransactionDelete(
    this: Db,
    id: TxId_,
    table: string,
    row: X.RowUuid,
    options: DeleteOptions,
  ): void {
    const db: Db = this.cloneForOwnerOperation();
    this.node.enqueueTransactionOperation(id, async () =>
      (await db.transactionIsExclusive(id))
        ? db.exclusiveTxRef(id).delete(table, row, options)
        : db.mergeableTxRef(id).delete(table, row, options),
    );
  },

  enqueueTransactionRestore(
    this: Db,
    id: TxId_,
    table: string,
    row: X.RowUuid,
    cells: RowCells | undefined,
    options: RestoreOptions,
  ): void {
    const db: Db = this.cloneForOwnerOperation();
    this.node.enqueueTransactionOperation(id, async () =>
      (await db.transactionIsExclusive(id))
        ? db.exclusiveTxRef(id).restore(table, row, cells, options)
        : db.mergeableTxRef(id).restore(table, row, cells, options),
    );
  },

  async transactionRead(
    this: Db,
    txId: TxId_,
    table: string,
    row: X.RowUuid,
  ): Promise<RowCells | undefined> {
    return withTx(this, txId, async (node) => {
      const cells: RowCells | undefined = await node.txReadInSchema(
        txId,
        this.schemaVersionId,
        table,
        row,
      );
      if (cells) await node.hydrateLargeValueCells(cells);
      return cells;
    });
  },

  /** Read the storage-form transaction row used as a mutation base. */
  async transactionReadRaw(
    this: Db,
    txId: TxId_,
    table: string,
    row: X.RowUuid,
  ): Promise<RowCells | undefined> {
    return withTx(this, txId, (node) =>
      node.txReadInSchema(txId, this.schemaVersionId, table, row),
    );
  },

  transactionAll(
    this: Db,
    txId: TxId_,
    prepared: PreparedQuery,
    opts: ReadOpts,
  ): Promise<CurrentRow[]> {
    return this.transactionAllInAuthorizationMode(
      txId,
      prepared,
      this.identity.author,
      opts,
      "ClientLocal",
    );
  },

  transactionAllForIdentity(
    this: Db,
    txId: TxId_,
    prepared: PreparedQuery,
    author: AuthorSubject,
    opts: ReadOpts,
  ): Promise<CurrentRow[]> {
    return this.transactionAllInAuthorizationMode(txId, prepared, author, opts, "TrustedServing");
  },

  /** Binding-facing transaction read selected by open id. */
  allInOpenTransaction(
    this: Db,
    txId: TxId_,
    prepared: PreparedQuery,
    opts: ReadOpts,
    author?: AuthorSubject,
  ): Promise<CurrentRow[]> {
    return author !== undefined
      ? this.transactionAllForIdentity(txId, prepared, author, opts)
      : this.transactionAll(txId, prepared, opts);
  },

  transactionRelationSnapshot(
    this: Db,
    txId: TxId_,
    prepared: PreparedQuery,
    opts: ReadOpts,
  ): Promise<X.RelationSnapshot> {
    return this.transactionRelationSnapshotInAuthorizationMode(
      txId,
      prepared,
      this.identity.author,
      opts,
      "ClientLocal",
    );
  },

  transactionRelationSnapshotForIdentity(
    this: Db,
    txId: TxId_,
    prepared: PreparedQuery,
    author: AuthorSubject,
    opts: ReadOpts,
  ): Promise<X.RelationSnapshot> {
    return this.transactionRelationSnapshotInAuthorizationMode(
      txId,
      prepared,
      author,
      opts,
      "TrustedServing",
    );
  },

  relationSnapshotInOpenTransaction(
    this: Db,
    txId: TxId_,
    prepared: PreparedQuery,
    opts: ReadOpts,
    author?: AuthorSubject,
  ): Promise<X.RelationSnapshot> {
    return author !== undefined
      ? this.transactionRelationSnapshotForIdentity(txId, prepared, author, opts)
      : this.transactionRelationSnapshot(txId, prepared, opts);
  },

  async transactionRelationSnapshotInAuthorizationMode(
    this: Db,
    txId: TxId_,
    prepared: PreparedQuery,
    author: AuthorSubject,
    opts: ReadOpts,
    mode: X.QueryAuthorizationMode,
  ): Promise<X.RelationSnapshot> {
    ensureDefaultReadView(opts);
    return withTx(this, txId, async (owner) => {
      const node = prepared.scopedNode(owner, author);
      const snapshot: X.RelationSnapshot =
        mode === "ClientLocal"
          ? await node.txRelationSnapshotWithOptions(
              txId,
              prepared.shape(),
              prepared.binding(),
              opts.includeDeleted,
            )
          : await node.txRelationSnapshotForIdentityWithOptions(
              txId,
              prepared.shape(),
              prepared.binding(),
              author,
              opts.includeDeleted,
            );
      await node.hydrateCurrentRows(snapshot.rows);
      return snapshot;
    });
  },

  async transactionAllInAuthorizationMode(
    this: Db,
    txId: TxId_,
    prepared: PreparedQuery,
    author: AuthorSubject,
    opts: ReadOpts,
    mode: X.QueryAuthorizationMode,
  ): Promise<CurrentRow[]> {
    ensureDefaultReadView(opts);
    return withTx(this, txId, async (owner) => {
      const node = prepared.scopedNode(owner, author);
      const rows: CurrentRow[] =
        mode === "ClientLocal"
          ? await node.txQueryWithOptions(
              txId,
              prepared.shape(),
              prepared.binding(),
              opts.includeDeleted,
            )
          : await node.txQueryForIdentityWithOptions(
              txId,
              prepared.shape(),
              prepared.binding(),
              author,
              opts.includeDeleted,
            );
      await node.hydrateCurrentRows(rows);
      return rows;
    });
  },

  async transactionCurrentRows(this: Db, txId: TxId_, table: string): Promise<CurrentRow[]> {
    return withTx(this, txId, async (node) => {
      const rows: CurrentRow[] = await node.txCurrentRows(txId, table);
      await node.hydrateCurrentRows(rows);
      return rows;
    });
  },

  async stageExclusiveInsert(
    this: Db,
    txId: TxId_,
    table: string,
    row: X.RowUuid,
    cellsIn: RowCells,
    updatedAtMs: number | undefined,
    explicitRowId: boolean,
  ): Promise<void> {
    const nowMs = updatedAtMs ?? this.nextNowMsValue();
    const cells = this.applyInsertDefaults(table, cellsIn);
    // An explicit-id insert only creates; deciding existence is a user read,
    // resolved exactly as UPSERT does.
    if (
      explicitRowId &&
      (await this.exclusiveTransactionTargetForWrite(txId, table, row, "UPSERT", true)) !==
        undefined
    )
      throw rowAlreadyExists(table, row);
    await withTx(this, txId, async (node) => {
      if (
        explicitRowId &&
        (await node.txInsertTargetStateInSchema(txId, this.schemaVersionId, table, row)) ===
          "Deleted"
      )
        throw rowAlreadyDeleted(row);
      await node.txWriteInSchemaAtMs(
        txId,
        this.schemaVersionId,
        table,
        row,
        cells,
        undefined,
        nowMs,
      );
    });
  },

  async stageExclusiveUpdate(
    this: Db,
    txId: TxId_,
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    updatedAtMs?: number,
  ): Promise<void> {
    const nowMs = updatedAtMs ?? this.nextNowMsValue();
    const cells: RowCells | undefined = await this.exclusiveTransactionTargetForWrite(
      txId,
      table,
      row,
      "UPDATE",
      false,
    );
    if (!cells) throw new Error("exclusive UPDATE requires a visible target");
    extend(cells, patch);
    await withTx(this, txId, (node) =>
      node.txWriteInSchemaAtMs(txId, this.schemaVersionId, table, row, cells, undefined, nowMs),
    );
  },

  async stageExclusiveUpsert(
    this: Db,
    txId: TxId_,
    table: string,
    row: X.RowUuid,
    patch: RowCells,
    updatedAtMs: number | undefined,
    requireCompleteInsert: boolean,
  ): Promise<void> {
    const nowMs = updatedAtMs ?? this.nextNowMsValue();
    const existing: RowCells | undefined = await this.exclusiveTransactionTargetForWrite(
      txId,
      table,
      row,
      "UPSERT",
      true,
    );
    if (requireCompleteInsert && !existing) this.validateCompleteInsert(table, patch);
    const cells = this.applyInsertDefaults(table, extend(existing ?? new Map(), patch));
    await withTx(this, txId, (node) =>
      node.txWriteInSchemaAtMs(txId, this.schemaVersionId, table, row, cells, undefined, nowMs),
    );
  },

  /** Resolve an exclusive mutation target through the identity fixed when the transaction opened. */
  async exclusiveTransactionTargetForWrite(
    this: Db,
    txId: TxId_,
    table: string,
    row: X.RowUuid,
    operation: string,
    allowAbsent: boolean,
  ): Promise<RowCells | undefined> {
    // The subject lookup rejects a mergeable id.
    await withTx(this, txId, (node) => node.exclusiveTransactionPermissionSubject(txId));
    return this.transactionTargetForWrite(txId, table, row, operation, allowAbsent);
  },

  /**
   * Read the storage-form row a transaction write builds on, after proving the
   * transaction's permission subject may read it.
   */
  async transactionTargetForWrite(
    this: Db,
    txId: TxId_,
    table: string,
    row: X.RowUuid,
    operation: string,
    allowAbsent: boolean,
  ): Promise<RowCells | undefined> {
    const identity: AuthorSubject = await withTx(this, txId, (node) =>
      node.transactionPermissionSubject(txId),
    );
    const readPolicy = this.tableSchema(table).readPolicy;
    const target: RowCells | undefined = await this.transactionReadRaw(txId, table, row);
    let visible = false;
    if (target !== undefined) {
      if (X.authorSubjectEq(identity, X.AuthorSubjectT.SYSTEM) || !readPolicy) visible = true;
      else
        visible = await withTx(this, txId, (node) =>
          node.readPolicyQueryAllowsOpenTxRow(
            txId,
            readPolicy,
            this.schemaVersionId,
            row,
            identity,
          ),
        );
    }
    if ((target !== undefined && !visible) || (target === undefined && !allowAbsent))
      throw readForWriteDenied(operation, table);
    return target;
  },

  async stageExclusiveDelete(
    this: Db,
    txId: TxId_,
    table: string,
    row: X.RowUuid,
    updatedAtMs?: number,
  ): Promise<void> {
    const nowMs = updatedAtMs ?? this.nextNowMsValue();
    await withTx(this, txId, (node) =>
      node.txWriteInSchemaAtMs(
        txId,
        this.schemaVersionId,
        table,
        row,
        new Map<string, Value>(),
        "Deleted",
        nowMs,
      ),
    );
  },

  async stageExclusiveRestore(
    this: Db,
    txId: TxId_,
    table: string,
    row: X.RowUuid,
    cellsIn: RowCells,
    updatedAtMs?: number,
  ): Promise<void> {
    const nowMs = updatedAtMs ?? this.nextNowMsValue();
    const cells = this.applyInsertDefaults(table, cellsIn);
    // Restore needs one content version and one deletion-register version.
    await withTx(this, txId, async (node) => {
      await node.txWriteInSchemaAtMs(
        txId,
        this.schemaVersionId,
        table,
        row,
        cells,
        undefined,
        nowMs,
      );
      await node.txWriteInSchemaAtMs(
        txId,
        this.schemaVersionId,
        table,
        row,
        new Map<string, Value>(),
        "Restored",
        nowMs,
      );
    });
  },

  /** Commit an owned exclusive transaction handle. */
  async commitExclusiveHandle(this: Db, openTxId: TxId_): Promise<TxId> {
    const [published, unit] = await withTxCommit(this, openTxId, (node) =>
      node.commitExclusiveBound(openTxId, this.nextNowMsValue()),
    );
    return this.finishExclusivePublication(published, unit);
  },

  enqueueCommitExclusiveHandle(this: Db, openTxId: TxId_): WriteHandle {
    return this.enqueueCommitExclusiveHandleAtMs(openTxId, this.nextNowMsValue());
  },

  enqueueCommitExclusiveHandleAtMs(this: Db, openTxId: TxId_, nowMs: number): WriteHandle {
    const txId = this.reserveTransactionIdAtMs(nowMs);
    const db: Db = this.cloneForReservedTransaction(txId);
    const status = this.node.enqueueTransactionCommit(openTxId, txId, "Exclusive", async () => {
      const [published, unit] = await withTxCommit(db, openTxId, (node) =>
        node.commitExclusiveBoundAt(openTxId, txId),
      );
      await db.finishExclusivePublication(published, unit);
    });
    return this.queuedWriteHandle(ZERO_ROW(), txId, status, undefined);
  },

  /** Commit an owned exclusive transaction as an explicit policy identity. */
  async commitExclusiveHandleForIdentity(
    this: Db,
    openTxId: TxId_,
    author: AuthorSubject,
  ): Promise<TxId> {
    const [published, unit] = await withTxCommit(this, openTxId, (node) =>
      node.commitExclusive(openTxId, author, this.nextNowMsValue()),
    );
    return this.finishExclusivePublication(published, unit);
  },

  async finishExclusivePublication(
    this: Db,
    published: X.PublishedTransaction,
    unit: SyncMessage,
  ): Promise<TxId> {
    const txId = published.txId;
    await this.settleLocalPublication(published, unit);
    return txId;
  },

  abandonExclusiveHandle(this: Db, openTxId: TxId_): void {
    this.abandonTransactionHandle(openTxId);
  },

  openExclusiveHandle(this: Db, id: TxId_): Promise<void> {
    return this.openExclusiveHandleForIdentity(id, this.identity.author);
  },

  async openExclusiveHandleForIdentity(this: Db, id: TxId_, author: AuthorSubject): Promise<void> {
    await withTxOpen(this, id, (node) => node.openExclusiveForIdentity(id, author));
  },
};

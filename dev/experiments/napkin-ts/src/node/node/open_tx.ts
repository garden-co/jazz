// Open transaction lifecycle and snapshot-overlay reads: tx_read, tx_query support,
// mergeable/exclusive write staging, and commit-unit construction for open transactions.
import { SortedMap, SortedSet, cmpTuple, cmpStr } from "../../support/index.js";
import {
  type AuthorSubject,
  type BranchSelector,
  type DeletionEvent,
  type GlobalTime,
  type RowUuid,
  type SchemaVersionId,
  type TxId,
  type Value,
  AuthorSubjectSystem,
  BranchKeyDefault,
  QueryFrom,
  RowAuthor,
  SnapshotExclusiveBase,
  TransactionRowRecord,
  TxIdNew,
  TxTimeOps,
  VersionRecordEncode,
  cellsFromPositional,
  cmpRowUuid,
  cmpTxId,
  currentRowFromCellsWithExplicitProvenance,
  currentRowFromPositionalCells,
  currentVersionIndex,
  positionalCellsFromMap,
  schemaIdKey,
  sortCurrentRows,
  valueEquals,
} from "../../_extern/node-state.js";
import {
  type CurrentRow,
  type NodeState,
  type RowProvenance,
  MergeableCommit,
  PublishedTransaction,
  collectIndirectDescriptors,
  invalidCommit,
  invalidStored,
  nodeErr,
  validateMergeableWriteShape,
  valueContainsIndirectDescriptor,
  valueNeedsLargeValueStaging,
} from "./index.js";

export type OpenTransactionId = any;
const idKey = (id: OpenTransactionId) => String(id);

export type TransactionBranchRowState =
  | { kind: "Visible"; cells: Map<string, Value>; staged: boolean }
  | { kind: "PendingDeletion" }
  | { kind: "Absent" };

export enum TransactionInsertTargetState {
  Absent,
  Deleted,
}

export type OpenTransactionKind =
  | { kind: "Exclusive"; madeBy: AuthorSubject; permissionSubject: AuthorSubject }
  | { kind: "Mergeable"; madeBy: AuthorSubject; permissionSubject?: AuthorSubject };

type PendingCells =
  | { kind: "Replace"; cells: Map<string, Value> }
  | { kind: "Patch"; cells: Map<string, Value> };

export type PendingWrite = {
  table: string;
  rowUuid: RowUuid;
  schemaVersion: SchemaVersionId;
  branch: BranchSelector;
  cells: PendingCells;
  deletion?: DeletionEvent;
  nowMs?: number;
  knownFreshRow: boolean;
  verifiedInheritedCells?: Map<string, Value>;
  branchViewCopy?: any;
  stagedLargeCells: Map<string, any>;
};

export type SnapshotRow = {
  contentCells?: (Value | undefined)[];
  contentVersion?: TxId;
  deletionVersion?: TxId;
  readVersion?: TxId;
  deleted: boolean;
  provenance?: [any, any];
};

/** Narrowed reads for the non-root sources of the query being evaluated. */
export class SourceNarrowing {
  recording = false;
  offered = new Map<string, any>();
  payload = new Set<string>();
  claimed: any[] = [];
}

const sourceKey = (s: any) => `${s.table}|${JSON.stringify(s.path)}`;

/** Locally open transactions and local-only permission attribution. */
export class OpenTxState {
  openTransactions = new Map<string, OpenTransaction>();
  closedBatches = new Set<string>();
  localPermissionSubjects = new SortedMap<TxId, AuthorSubject>(cmpTxId);
  releasedLargeValues: any[] = [];
  pendingForeignTransactions = new SortedSet<TxId>(cmpTxId);
}

export class OpenTransaction {
  baseSnapshotRows = new Map<string, Map<string, SnapshotRow>>();
  rowReads: { table: string; rowUuid: RowUuid; version: TxId }[] = [];
  absentReads: { table: string; rowUuid: RowUuid }[] = [];
  predicateReads: any[] = [];
  sourceNarrowing = new SourceNarrowing();
  narrowedPredicateReads = new Set<string>();
  writes = new StagedWrites();
  supersededLargeValues: any[] = [];
  userMetadataJson?: string;

  constructor(
    readonly kind: OpenTransactionKind,
    readonly provisionalAuthor: AuthorSubject,
    readonly baseSnapshot: any,
  ) {}

  clone(): OpenTransaction {
    const c = new OpenTransaction(this.kind, this.provisionalAuthor, this.baseSnapshot);
    c.baseSnapshotRows = new Map([...this.baseSnapshotRows].map(([k, v]) => [k, new Map(v)]));
    c.rowReads = [...this.rowReads];
    c.absentReads = [...this.absentReads];
    c.predicateReads = [...this.predicateReads];
    c.sourceNarrowing = this.sourceNarrowing;
    c.narrowedPredicateReads = new Set(this.narrowedPredicateReads);
    c.writes = this.writes.clone();
    c.supersededLargeValues = [...this.supersededLargeValues];
    c.userMetadataJson = this.userMetadataJson;
    return c;
  }

  /** Record row proofs for `table`, skipping ones already recorded. */
  recordRowReads(table: string, proofs: Iterable<[RowUuid, TxId]>): void {
    const recorded = new Set(
      this.rowReads
        .filter((r) => r.table === table)
        .map((r) => `${String(r.rowUuid)}|${String(r.version)}`),
    );
    for (const [rowUuid, version] of proofs) {
      const k = `${String(rowUuid)}|${String(version)}`;
      if (recorded.has(k)) continue;
      recorded.add(k);
      this.rowReads.push({ table, rowUuid, version });
    }
  }

  /** Every Groove root this transaction staged and has not yet released. */
  stagedLargeValueIds(): any[] {
    return [
      ...this.supersededLargeValues,
      ...[...this.writes.iter()].flatMap((w) => [...w.stagedLargeCells.values()].map((s) => s.id)),
    ];
  }
}

/** Staged writes in staging order, indexed by table and row (O(n log n) staging). */
export class StagedWrites {
  private slots: (PendingWrite | undefined)[] = [];
  private positions = new Map<string, SortedMap<RowUuid, number[]>>();
  private live = 0;

  clone(): StagedWrites {
    const c = new StagedWrites();
    c.slots = this.slots.map((s) => s && { ...s });
    for (const [t, rows] of this.positions) {
      const m = new SortedMap<RowUuid, number[]>(cmpRowUuid);
      for (const [r, p] of rows) m.set(r, [...p]);
      c.positions.set(t, m);
    }
    c.live = this.live;
    return c;
  }
  len(): number {
    return this.live;
  }
  *iter(): IterableIterator<PendingWrite> {
    for (const s of this.slots) if (s) yield s;
  }
  tableRows(table: string): RowUuid[] {
    return [...(this.positions.get(table)?.keysArray() ?? [])];
  }
  rowWrites(table: string, rowUuid: RowUuid): PendingWrite[] {
    return this.rowPositions(table, rowUuid).map((p) => this.liveSlot(p));
  }
  findRowWrite(
    table: string,
    rowUuid: RowUuid,
    matches: (w: PendingWrite) => boolean,
  ): number | undefined {
    return this.rowPositions(table, rowUuid).find((p) => matches(this.liveSlot(p)));
  }
  getMut(position: number): PendingWrite {
    return this.liveSlot(position);
  }
  set(position: number, write: PendingWrite): void {
    this.slots[position] = write;
  }
  push(write: PendingWrite): void {
    let rows = this.positions.get(write.table);
    if (!rows) this.positions.set(write.table, (rows = new SortedMap(cmpRowUuid)));
    rows.getOrInsert(write.rowUuid, () => []).push(this.slots.length);
    this.slots.push(write);
    this.live++;
  }
  /** Unstage the selected writes of one row and return them in staging order. */
  removeRowWrites(
    table: string,
    rowUuid: RowUuid,
    remove: (w: PendingWrite) => boolean,
  ): PendingWrite[] {
    const rows = this.positions.get(table);
    const positions = rows?.get(rowUuid);
    if (!rows || !positions) return [];
    const removed: PendingWrite[] = [];
    const kept = positions.filter((p) => {
      const write = this.liveSlot(p);
      if (!remove(write)) return true;
      removed.push(write);
      this.slots[p] = undefined;
      return false;
    });
    if (kept.length) rows.set(rowUuid, kept);
    else {
      rows.delete(rowUuid);
      if (!rows.size) this.positions.delete(table);
    }
    this.live -= removed.length;
    if (this.slots.length - this.live > Math.max(this.live, 32)) this.compact();
    return removed;
  }
  private compact(): void {
    this.slots = this.slots.filter((s) => s !== undefined);
    for (const rows of this.positions.values()) for (const [, p] of rows) p.length = 0;
    this.slots.forEach((w, position) =>
      this.positions.get(w!.table)!.get(w!.rowUuid)!.push(position),
    );
  }
  private rowPositions(table: string, rowUuid: RowUuid): number[] {
    return this.positions.get(table)?.get(rowUuid) ?? [];
  }
  private liveSlot(position: number): PendingWrite {
    const w = this.slots[position];
    if (!w) throw new Error("staged write positions point at live slots");
    return w;
  }
}

const stagedCells = (w: PendingWrite) => w.cells.cells;

/** Keep only claims whose column still holds exactly the claimed descriptor. */
function retainCurrentLargeValueClaims(write: PendingWrite, superseded: any[]): void {
  const claims = write.stagedLargeCells;
  write.stagedLargeCells = new Map();
  for (const [column, staged] of claims) {
    const value = stagedCells(write).get(column);
    if (value !== undefined && valueIsExactDescriptor(value, staged.valueRef))
      write.stagedLargeCells.set(column, staged);
    else superseded.push(staged.id);
  }
}

/** Combine a pending write's claims with a later write's claims for the same row. */
function mergeLargeValueClaims(
  earlier: Map<string, any>,
  later: Map<string, any>,
  superseded: any[],
): Map<string, any> {
  for (const [column, staged] of later) {
    const previous = earlier.get(column);
    earlier.set(column, staged);
    if (previous && [...earlier.values()].every((kept) => String(kept.id) !== String(previous.id)))
      superseded.push(previous.id);
  }
  return earlier;
}

/** Whether `value` is exactly the indirect descriptor `target`, and nothing else. */
function valueIsExactDescriptor(value: Value, target: any): boolean {
  const descriptors: any[] = [];
  collectIndirectDescriptors(value, descriptors);
  return descriptors.length === 1 && valueEquals(descriptors[0], target);
}

/** Merge a row's staged content replacement with its staged delete/restore event. */
function coalesceExclusiveWrites(writes: StagedWrites): [PendingWrite, boolean][] {
  const merged: [PendingWrite, boolean][] = [];
  const positions = new Map<string, number>();
  for (const write of writes.iter()) {
    const hasContent = write.deletion === undefined;
    const key = [
      write.table,
      String(write.rowUuid),
      schemaIdKey(write.schemaVersion),
      JSON.stringify(write.branch),
    ].join("|");
    const index = positions.get(key);
    if (index === undefined) {
      positions.set(key, merged.length);
      merged.push([{ ...write }, hasContent]);
      continue;
    }
    const entry = merged[index];
    const candidate = entry[0];
    const resurrects = hasContent && candidate.deletion === "Deleted";
    if (hasContent) {
      candidate.cells = write.cells;
      candidate.stagedLargeCells = write.stagedLargeCells;
      entry[1] = true;
    }
    candidate.deletion = resurrects ? "Restored" : (write.deletion ?? candidate.deletion);
    candidate.nowMs = write.nowMs ?? candidate.nowMs;
  }
  return merged;
}

const intentCmp = cmpTuple(
  (a: any, b: any) => cmpStr(String(a), String(b)),
  (a: any, b: any) => cmpStr(schemaIdKey(a), schemaIdKey(b)),
  cmpRowUuid,
  (a: any, b: any) => a.compare(b),
);
const intentKey = (i: any) => [i.physicalTableId, i.authoredSchema, i.rowUuid, i.head];

const isVisibleFate = (state: any) => state !== undefined && state[0].kind !== "Rejected";

export const openTxMethods = {
  async openExclusive(this: NodeState, id: OpenTransactionId): Promise<void> {
    await this.openTransaction(
      id,
      { kind: "Exclusive", madeBy: AuthorSubjectSystem, permissionSubject: AuthorSubjectSystem },
      AuthorSubjectSystem,
    );
  },

  async openExclusiveForIdentity(
    this: NodeState,
    id: OpenTransactionId,
    madeBy: AuthorSubject,
  ): Promise<void> {
    await this.openExclusiveWithIdentity(id, madeBy, madeBy);
  },

  async openExclusiveWithIdentity(
    this: NodeState,
    id: OpenTransactionId,
    madeBy: AuthorSubject,
    permissionSubject: AuthorSubject,
  ): Promise<void> {
    await this.openTransaction(id, { kind: "Exclusive", madeBy, permissionSubject }, madeBy);
  },

  async openMergeable(
    this: NodeState,
    id: OpenTransactionId,
    madeBy: AuthorSubject,
    permissionSubject?: AuthorSubject,
  ): Promise<void> {
    await this.openTransaction(id, { kind: "Mergeable", madeBy, permissionSubject }, madeBy);
  },

  /** Whether an open transaction separates durable provenance from its permission subject. */
  transactionIsAttributed(this: NodeState, id: OpenTransactionId): boolean {
    const kind: OpenTransactionKind = this.openTxGet(id).kind;
    if (kind.permissionSubject === undefined) return false;
    return String(kind.permissionSubject) !== String(kind.madeBy);
  },

  transactionIsExclusive(this: NodeState, id: OpenTransactionId): boolean {
    return this.openTxGet(id).kind.kind === "Exclusive";
  },

  transactionPermissionSubject(this: NodeState, id: OpenTransactionId): AuthorSubject {
    const kind: OpenTransactionKind = this.openTxGet(id).kind;
    return kind.permissionSubject ?? kind.madeBy;
  },

  exclusiveTransactionPermissionSubject(this: NodeState, id: OpenTransactionId): AuthorSubject {
    const kind: OpenTransactionKind = this.openTxGet(id).kind;
    if (kind.kind !== "Exclusive") throw invalidCommit("open transaction is not exclusive");
    return kind.permissionSubject;
  },

  async openTransaction(
    this: NodeState,
    id: OpenTransactionId,
    kind: OpenTransactionKind,
    provisionalAuthor: AuthorSubject,
  ): Promise<void> {
    this.requireCatalogueReady();
    if (this.openTx.openTransactions.has(idKey(id)) || this.openTx.closedBatches.has(idKey(id)))
      throw nodeErr("DuplicateOpenBatch", id);
    let author: AuthorSubject;
    try {
      author = RowAuthor.fromSession(provisionalAuthor, this.nodeUuidValue).asAuthorSubject();
    } catch {
      throw nodeErr("UnadmittedWriteAuthor");
    }
    const localBase = this.txTimeHighWater();
    // Every time applied past the frontier was recorded with its transaction (#3390).
    const dots: TxId[] = [];
    for (const globalTime of this.clock.appliedGlobalTimesAfterFrontier.toArray()) {
      const known = this.clock.frontierDots.get(globalTime);
      dots.push(...(known ?? (await this.transactionIdsForGlobalTime(globalTime))));
    }
    dots.push(...(await this.pendingForeignTransactionIds()));
    let baseSnapshot: any;
    try {
      baseSnapshot = SnapshotExclusiveBase(
        this.nodeUuidValue,
        this.clock.committedGlobalTime,
        localBase,
        dots,
      );
    } catch (e) {
      throw invalidStored(String(e));
    }
    this.openTx.openTransactions.set(idKey(id), new OpenTransaction(kind, author, baseSnapshot));
  },

  /** Read a row inside an exclusive transaction. */
  async txRead(this: NodeState, txId: OpenTransactionId, table: string, rowUuid: RowUuid) {
    this.table(table);
    return this.txReadUnchecked(txId, this.catalogue.activeSchema.schema, table, rowUuid);
  },

  async txReadInSchema(
    this: NodeState,
    txId: OpenTransactionId,
    schemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
  ) {
    this.tableInSchemaRef(table, schemaVersion);
    return this.txReadUnchecked(txId, schemaVersion, table, rowUuid);
  },

  async txReadUnchecked(
    this: NodeState,
    txId: OpenTransactionId,
    schemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
  ): Promise<Map<string, Value> | undefined> {
    const snapshot = this.openTxGet(txId).baseSnapshot;
    const snapshotRow: SnapshotRow = await this.snapshotRowInSchema(
      schemaVersion,
      table,
      rowUuid,
      snapshot,
    );
    const open: OpenTransaction = this.openTxGet(txId);
    const rowKey = `${table}|${String(rowUuid)}`;
    let bySchema = open.baseSnapshotRows.get(rowKey);
    if (!bySchema) open.baseSnapshotRows.set(rowKey, (bySchema = new Map()));
    bySchema.set(schemaIdKey(schemaVersion), snapshotRow);
    const result = this.overlayPendingWritesInSchema(
      txId,
      schemaVersion,
      table,
      rowUuid,
      snapshotRow,
    );
    const version = snapshotRow.readVersion;
    if (version !== undefined) {
      if (
        !open.rowReads.some(
          (r) =>
            r.table === table &&
            String(r.rowUuid) === String(rowUuid) &&
            cmpTxId(r.version, version) === 0,
        )
      )
        open.rowReads.push({ table, rowUuid, version });
    } else if (
      !open.absentReads.some((r) => r.table === table && String(r.rowUuid) === String(rowUuid))
    ) {
      open.absentReads.push({ table, rowUuid });
    }
    return result;
  },

  /** Pin the snapshot version of each root row a transaction query returned (#3694). */
  async recordTxQueryRowReads(
    this: NodeState,
    txId: OpenTransactionId,
    schemaVersion: SchemaVersionId,
    table: string,
    rows: Iterable<RowUuid>,
  ): Promise<void> {
    const snapshot = this.openTxGet(txId).baseSnapshot;
    const proofs: [RowUuid, TxId][] = [];
    for (const rowUuid of rows) {
      const version = (await this.snapshotRowInSchema(schemaVersion, table, rowUuid, snapshot))
        .readVersion;
      if (version !== undefined) proofs.push([rowUuid, version]);
    }
    this.openTxGet(txId).recordRowReads(table, proofs);
  },

  /** Classify an explicit exclusive insert target that the overlaid point read reports absent. */
  async txInsertTargetStateInSchema(
    this: NodeState,
    txId: OpenTransactionId,
    schemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
  ): Promise<TransactionInsertTargetState> {
    const open: OpenTransaction = this.openTxGet(txId);
    const snapshotRow: SnapshotRow =
      open.baseSnapshotRows.get(`${table}|${String(rowUuid)}`)?.get(schemaIdKey(schemaVersion)) ??
      (await this.snapshotRowInSchema(schemaVersion, table, rowUuid, open.baseSnapshot));
    const staged = this.openTxGet(txId).writes.rowWrites(table, rowUuid).length > 0;
    return snapshotRow.contentCells !== undefined || snapshotRow.deleted || staged
      ? TransactionInsertTargetState.Deleted
      : TransactionInsertTargetState.Absent;
  },

  async txCurrentRows(
    this: NodeState,
    txId: OpenTransactionId,
    table: string,
  ): Promise<CurrentRow[]> {
    const schemaVersion = this.catalogue.activeSchema.schema;
    return this.txCurrentRowsWithTable(txId, schemaVersion, table, this.table(table), false, true);
  },

  async txCurrentRowsInSchema(
    this: NodeState,
    txId: OpenTransactionId,
    schemaVersion: SchemaVersionId,
    table: string,
  ) {
    return this.txCurrentRowsWithTable(
      txId,
      schemaVersion,
      table,
      this.tableInSchema(table, schemaVersion),
      false,
      true,
    );
  },

  /** Offer `reads` to the sources of the query an open exclusive transaction evaluates. */
  offerTxNarrowedSourceReads(
    this: NodeState,
    txId: OpenTransactionId,
    reads: any,
  ): SourceNarrowing {
    const open: OpenTransaction = this.openTxGet(txId);
    const previous = open.sourceNarrowing;
    const offered = new SourceNarrowing();
    offered.recording = previous.recording;
    for (const [source, read] of reads.reads) offered.offered.set(sourceKey(source), read);
    for (const source of reads.payload) offered.payload.add(sourceKey(source));
    open.sourceNarrowing = offered;
    return previous;
  },

  withdrawTxNarrowedSourceReads(
    this: NodeState,
    txId: OpenTransactionId,
    previous: SourceNarrowing,
  ): any[] {
    const open: OpenTransaction = this.openTxGet(txId);
    const narrowing = open.sourceNarrowing;
    open.sourceNarrowing = previous;
    return narrowing.claimed;
  },

  /** Claim the read of a non-root `source`. */
  claimTxSourceRead(this: NodeState, txId: OpenTransactionId, source: any): void {
    const exclusive = this.transactionIsExclusive(txId);
    const narrowing: SourceNarrowing = this.openTxGet(txId).sourceNarrowing;
    const key = sourceKey(source);
    if (narrowing.recording || narrowing.payload.has(key)) return;
    const read = narrowing.offered.get(key);
    if (read !== undefined) narrowing.claimed.push(read);
    else if (exclusive)
      throw nodeErr("UnsupportedExclusiveRead", `\`${source.table}\` beyond the root of its query`);
  },

  async txCurrentRowsInSchemaWithOptions(
    this: NodeState,
    txId: OpenTransactionId,
    schemaVersion: SchemaVersionId,
    table: string,
    includeDeleted: boolean,
  ) {
    return this.txCurrentRowsWithTable(
      txId,
      schemaVersion,
      table,
      this.tableInSchema(table, schemaVersion),
      includeDeleted,
      false,
    );
  },

  async txCurrentRowsWithTable(
    this: NodeState,
    txId: OpenTransactionId,
    schemaVersion: SchemaVersionId,
    table: string,
    tableSchema: any,
    includeDeleted: boolean,
    recordTableRead: boolean,
  ): Promise<CurrentRow[]> {
    const snapshot = this.openTxGet(txId).baseSnapshot;
    const proofs: [RowUuid, TxId][] = [];
    const snapshotRows: SortedMap<RowUuid, SnapshotRow> = await this.snapshotRowsInSchema(
      schemaVersion,
      table,
      snapshot,
    );
    const rows = new SortedSet<RowUuid>(cmpRowUuid, snapshotRows.keysArray());
    for (const r of this.openTxGet(txId).writes.tableRows(table)) rows.add(r);
    const current: CurrentRow[] = [];
    for (const rowUuid of rows) {
      const snapshotRow =
        snapshotRows.delete(rowUuid) ??
        this.snapshotRowFromWinners(schemaVersion, table, undefined, undefined);
      const snapshotProvenance = snapshotRow.provenance;
      const readVersion = snapshotRow.readVersion;
      const open: OpenTransaction = this.openTxGet(txId);
      const pendingWrites = open.writes.rowWrites(table, rowUuid);
      const [cells, deleted] = this.overlayPendingCellsAndDeletionWithTable(
        txId,
        tableSchema,
        table,
        rowUuid,
        snapshotRow,
      );
      if (cells === undefined || (deleted && !includeDeleted)) continue;
      const projection = snapshotProvenance && {
        provenance: {
          createdBy: snapshotProvenance[0].createdBy(),
          createdAt: snapshotProvenance[0].createdAt().physicalMs(),
          updatedBy: snapshotProvenance[1].updatedBy(),
          updatedAt: snapshotProvenance[1].updatedAt().physicalMs(),
        } as RowProvenance,
        tx: [snapshotProvenance[1].txTime(), snapshotProvenance[1].txNodeAlias()],
      };
      let provenance = projection?.provenance;
      for (const write of pendingWrites) {
        if (write.nowMs === undefined) continue;
        const updatedAt = write.nowMs;
        provenance = provenance
          ? { ...provenance, updatedBy: open.provisionalAuthor, updatedAt }
          : {
              createdBy: open.provisionalAuthor,
              createdAt: updatedAt,
              updatedBy: open.provisionalAuthor,
              updatedAt,
            };
      }
      const row = provenance
        ? currentRowFromCellsWithExplicitProvenance(
            tableSchema,
            rowUuid,
            cells,
            provenance,
            projection?.tx,
          )
        : currentRowFromPositionalCells(
            tableSchema,
            rowUuid,
            positionalCellsFromMap(tableSchema, cells),
          );
      current.push(deleted ? row.intoDeleted() : row);
      if (recordTableRead && readVersion !== undefined) proofs.push([rowUuid, readVersion]);
    }
    this.openTxGet(txId).recordRowReads(table, proofs);
    sortCurrentRows(current);
    if (!recordTableRead) return current;
    const schema = this.catalogue.catalogueSchemas.get(schemaIdKey(schemaVersion));
    if (!schema) throw invalidStored("transaction schema is unknown");
    const shape = QueryFrom(table).validate(schema.schema);
    const binding = shape.bind(new Map());
    this.openTxGet(txId).predicateReads.push({
      table,
      shapeId: shape.shapeId(),
      shape: shape.query(),
      bindingId: binding.bindingId(),
      bindingValues: binding.values(),
    });
    return current;
  },

  async txWrite(
    this: NodeState,
    txId: OpenTransactionId,
    table: string,
    rowUuid: RowUuid,
    cells: Map<string, Value>,
    deletion?: DeletionEvent,
  ) {
    return this.txWriteInSchema(
      txId,
      this.catalogue.activeSchema.schema,
      table,
      rowUuid,
      cells,
      deletion,
    );
  },

  async txWriteInSchema(
    this: NodeState,
    txId: OpenTransactionId,
    schema: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
    cells: Map<string, Value>,
    deletion?: DeletionEvent,
  ) {
    return this.txWriteInSchemaAtMs(txId, schema, table, rowUuid, cells, deletion, undefined);
  },

  async txWriteInSchemaAtMs(
    this: NodeState,
    txId: OpenTransactionId,
    schema: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
    cells: Map<string, Value>,
    deletion: DeletionEvent | undefined,
    nowMs: number | undefined,
  ) {
    return this.txWriteInSchemaWithStagedLargeValues(
      txId,
      schema,
      table,
      rowUuid,
      cells,
      deletion,
      nowMs,
      new Map(),
    );
  },

  /** Stage an exclusive row replacement that may own partial large-value roots (#2087). */
  async txWriteInSchemaWithStagedLargeValues(
    this: NodeState,
    txId: OpenTransactionId,
    writeSchemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
    cells: Map<string, Value>,
    deletion: DeletionEvent | undefined,
    nowMs: number | undefined,
    stagedLargeCells: Map<string, any>,
  ): Promise<void> {
    if (this.openTxGet(txId).kind.kind !== "Exclusive")
      throw invalidCommit("open transaction is not exclusive");
    this.tableInSchemaRef(table, writeSchemaVersion);
    validateMergeableWriteShape(cells.size === 0, deletion !== undefined);
    positionalCellsFromMap(this.tableInSchemaRef(table, writeSchemaVersion), cells);
    const pending: PendingWrite = {
      table,
      rowUuid,
      schemaVersion: writeSchemaVersion,
      branch: { values: [] },
      cells: { kind: "Replace", cells: new Map(cells) },
      deletion,
      nowMs,
      knownFreshRow: false,
      stagedLargeCells,
    };
    const open: OpenTransaction = this.openTxGet(txId);
    open.baseSnapshotRows.delete(`${table}|${String(rowUuid)}`);
    const position = open.writes.findRowWrite(
      table,
      rowUuid,
      (w) => (w.deletion !== undefined) === (pending.deletion !== undefined),
    );
    if (position !== undefined) {
      const existing = open.writes.getMut(position);
      pending.stagedLargeCells = mergeLargeValueClaims(
        existing.stagedLargeCells,
        pending.stagedLargeCells,
        open.supersededLargeValues,
      );
      open.writes.set(position, pending);
    }
    retainCurrentLargeValueClaims(pending, open.supersededLargeValues);
    if (position === undefined) open.writes.push(pending);
  },

  async txWriteMergeable(
    this: NodeState,
    txId: OpenTransactionId,
    table: string,
    rowUuid: RowUuid,
    cells: Map<string, Value>,
    deletion?: DeletionEvent,
    nowMs?: number,
  ) {
    return this.txWriteMergeableInSchema(
      txId,
      this.catalogue.activeSchema.schema,
      table,
      rowUuid,
      cells,
      deletion,
      nowMs,
      false,
    );
  },

  async txWriteMergeableInSchema(
    this: NodeState,
    txId: OpenTransactionId,
    schema: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
    cells: Map<string, Value>,
    deletion: DeletionEvent | undefined,
    nowMs: number | undefined,
    knownFreshRow: boolean,
  ) {
    return this.txWriteMergeableInSchemaAndBranch(
      txId,
      schema,
      table,
      rowUuid,
      cells,
      deletion,
      nowMs,
      { values: [] },
      knownFreshRow,
    );
  },

  txWriteMergeableInSchemaAndBranch(
    this: NodeState,
    txId: OpenTransactionId,
    schema: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
    cells: Map<string, Value>,
    deletion: DeletionEvent | undefined,
    nowMs: number | undefined,
    branch: BranchSelector,
    knownFreshRow: boolean,
  ): void {
    this.txWriteMergeableInSchemaAndBranchWithVerifiedInheritedCells(
      txId,
      schema,
      table,
      rowUuid,
      cells,
      deletion,
      nowMs,
      branch,
      knownFreshRow,
      undefined,
      false,
      undefined,
    );
  },

  /** Stage a branch-local replacement whose unchanged cells were read from a branch-view base. */
  txWriteMergeableInSchemaAndBranchWithVerifiedInheritedCells(
    this: NodeState,
    txId: OpenTransactionId,
    writeSchemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
    cells: Map<string, Value>,
    deletion: DeletionEvent | undefined,
    nowMs: number | undefined,
    branch: BranchSelector,
    knownFreshRow: boolean,
    verifiedInheritedCells: Map<string, Value> | undefined,
    replacePendingDeletion: boolean,
    branchViewCopy: any,
  ): void {
    if (this.openTxGet(txId).kind.kind !== "Mergeable")
      throw invalidCommit("open transaction is not mergeable");
    validateMergeableWriteShape(cells.size === 0, deletion !== undefined);
    positionalCellsFromMap(this.tableInSchemaRef(table, writeSchemaVersion), cells);
    this.stageMergeableWrite(
      txId,
      {
        table,
        rowUuid,
        schemaVersion: writeSchemaVersion,
        branch,
        cells: { kind: "Replace", cells },
        deletion,
        nowMs,
        knownFreshRow,
        verifiedInheritedCells,
        branchViewCopy,
        stagedLargeCells: new Map(),
      },
      replacePendingDeletion,
    );
  },

  async txPatchMergeable(
    this: NodeState,
    txId: OpenTransactionId,
    table: string,
    rowUuid: RowUuid,
    patch: Map<string, Value>,
    nowMs?: number,
  ) {
    return this.txPatchMergeableInSchema(
      txId,
      this.catalogue.activeSchema.schema,
      table,
      rowUuid,
      patch,
      nowMs,
    );
  },

  async txPatchMergeableInSchema(
    this: NodeState,
    txId: OpenTransactionId,
    schema: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
    patch: Map<string, Value>,
    nowMs?: number,
  ) {
    return this.txPatchMergeableInSchemaAndBranch(
      txId,
      schema,
      table,
      rowUuid,
      patch,
      nowMs,
      { values: [] },
      false,
    );
  },

  async txPatchMergeableInSchemaAndBranch(
    this: NodeState,
    txId: OpenTransactionId,
    schema: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
    patch: Map<string, Value>,
    nowMs: number | undefined,
    branch: BranchSelector,
    replacePendingDeletion: boolean,
  ) {
    return this.txPatchMergeableWithStagedLargeValues(
      txId,
      schema,
      table,
      rowUuid,
      patch,
      nowMs,
      branch,
      replacePendingDeletion,
      new Map(),
    );
  },

  async txPatchMergeableWithStagedLargeValues(
    this: NodeState,
    txId: OpenTransactionId,
    writeSchemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
    patch: Map<string, Value>,
    nowMs: number | undefined,
    branch: BranchSelector,
    replacePendingDeletion: boolean,
    stagedLargeCells: Map<string, any>,
  ): Promise<void> {
    if (this.openTxGet(txId).kind.kind !== "Mergeable")
      throw invalidCommit("open transaction is not mergeable");
    const staged = new Map<string, Value>(
      (await this.txVisibleCurrentCellsInBranch(txId, table, rowUuid, branch)) ?? new Map(),
    );
    for (const [k, v] of patch) staged.set(k, v);
    validateMergeableWriteShape(staged.size === 0, false);
    positionalCellsFromMap(this.tableInSchemaRef(table, writeSchemaVersion), patch);
    this.stageMergeableWrite(
      txId,
      {
        table,
        rowUuid,
        schemaVersion: writeSchemaVersion,
        branch,
        cells: { kind: "Patch", cells: patch },
        nowMs,
        knownFreshRow: false,
        stagedLargeCells,
      },
      replacePendingDeletion,
    );
  },

  async txCurrentRowStateInBranch(
    this: NodeState,
    txId: OpenTransactionId,
    table: string,
    rowUuid: RowUuid,
    branch: BranchSelector,
  ): Promise<TransactionBranchRowState> {
    let cells: Map<string, Value> | undefined = await this.visibleCurrentCellsInBranch(
      table,
      branch,
      rowUuid,
    );
    let pendingDeletion = false;
    let stagedContent = false;
    const sameBranch = (b: BranchSelector) => JSON.stringify(b) === JSON.stringify(branch);
    for (const write of this.openTxGet(txId).writes.rowWrites(table, rowUuid) as PendingWrite[]) {
      if (!sameBranch(write.branch)) continue;
      if (write.deletion === "Deleted") {
        cells = undefined;
        pendingDeletion = true;
        continue;
      }
      if (write.deletion === "Restored") pendingDeletion = false;
      stagedContent = true;
      if (write.cells.kind === "Replace") cells = new Map(write.cells.cells);
      else {
        cells ??= new Map();
        for (const [k, v] of write.cells.cells) cells.set(k, v);
      }
    }
    if (pendingDeletion) return { kind: "PendingDeletion" };
    return cells ? { kind: "Visible", cells, staged: stagedContent } : { kind: "Absent" };
  },

  async txVisibleCurrentCellsInBranch(
    this: NodeState,
    txId: OpenTransactionId,
    table: string,
    rowUuid: RowUuid,
    branch: BranchSelector,
  ) {
    const state: TransactionBranchRowState = await this.txCurrentRowStateInBranch(
      txId,
      table,
      rowUuid,
      branch,
    );
    return state.kind === "Visible" ? state.cells : undefined;
  },

  stageMergeableWrite(
    this: NodeState,
    txId: OpenTransactionId,
    pending: PendingWrite,
    replacePendingDeletion: boolean,
  ): void {
    const open: OpenTransaction = this.openTxGet(txId);
    open.baseSnapshotRows.delete(`${pending.table}|${String(pending.rowUuid)}`);
    const sameBranch = (w: PendingWrite) =>
      JSON.stringify(w.branch) === JSON.stringify(pending.branch);
    // A branch-view upsert may supersede a delete staged by this same transaction.
    if (replacePendingDeletion)
      open.writes.removeRowWrites(
        pending.table,
        pending.rowUuid,
        (w) => sameBranch(w) && w.deletion === "Deleted",
      );
    if (pending.deletion === undefined) {
      const position = open.writes.findRowWrite(
        pending.table,
        pending.rowUuid,
        (w) => sameBranch(w) && w.deletion === undefined,
      );
      if (position === undefined) {
        retainCurrentLargeValueClaims(pending, open.supersededLargeValues);
        open.writes.push(pending);
        return;
      }
      const existing = open.writes.getMut(position);
      pending.knownFreshRow ||= existing.knownFreshRow;
      if (pending.cells.kind === "Patch") {
        pending.verifiedInheritedCells ??= existing.verifiedInheritedCells;
        pending.branchViewCopy ??= existing.branchViewCopy;
      }
      let cells: PendingCells;
      if (pending.cells.kind === "Patch") {
        const merged = new Map(existing.cells.cells);
        for (const [k, v] of pending.cells.cells) merged.set(k, v);
        cells = { kind: existing.cells.kind, cells: merged };
      } else cells = { kind: "Replace", cells: new Map(pending.cells.cells) };
      const stagedLargeCells = mergeLargeValueClaims(
        existing.stagedLargeCells,
        pending.stagedLargeCells,
        open.supersededLargeValues,
      );
      const next = { ...pending, cells, stagedLargeCells };
      retainCurrentLargeValueClaims(next, open.supersededLargeValues);
      open.writes.set(position, next);
      return;
    }
    const removed = open.writes.removeRowWrites(
      pending.table,
      pending.rowUuid,
      (existing) =>
        sameBranch(existing) &&
        (pending.deletion === "Deleted" ||
          (pending.deletion === "Restored" && existing.deletion !== undefined)),
    );
    open.supersededLargeValues.push(
      ...removed.flatMap((w) => [...w.stagedLargeCells.values()].map((s) => s.id)),
    );
    open.writes.push(pending);
  },

  txSetMetadata(this: NodeState, txId: OpenTransactionId, json: string): void {
    this.openTxGet(txId).userMetadataJson = json;
  },

  transactionNeedsLargeValueStaging(this: NodeState, id: OpenTransactionId): boolean {
    const open: OpenTransaction = this.openTxGet(id);
    return (
      open.supersededLargeValues.length > 0 ||
      [...open.writes.iter()].some(
        (w) =>
          w.stagedLargeCells.size > 0 ||
          [...stagedCells(w).values()].some(valueNeedsLargeValueStaging),
      )
    );
  },

  async commitExclusiveBound(this: NodeState, openBatchId: OpenTransactionId, nowMs: number) {
    const kind: OpenTransactionKind = this.openTxGet(openBatchId).kind;
    if (kind.kind !== "Exclusive") throw nodeErr("OpenTransactionIdentityMismatch");
    return this.commitExclusive(openBatchId, kind.madeBy, nowMs);
  },

  async commitExclusiveBoundAt(this: NodeState, openBatchId: OpenTransactionId, reserved: TxId) {
    const kind: OpenTransactionKind = this.openTxGet(openBatchId).kind;
    if (kind.kind !== "Exclusive") throw nodeErr("OpenTransactionIdentityMismatch");
    return this.commitExclusiveInner(
      openBatchId,
      kind.madeBy,
      TxTimeOps.physicalMs(reserved.time),
      reserved,
    );
  },

  async commitExclusive(
    this: NodeState,
    openBatchId: OpenTransactionId,
    madeBy: AuthorSubject,
    nowMs: number,
  ) {
    return this.commitExclusiveInner(openBatchId, madeBy, nowMs, undefined);
  },

  async commitExclusiveInner(
    this: NodeState,
    openBatchId: OpenTransactionId,
    requestedMadeBy: AuthorSubject,
    nowMs: number,
    reserved: TxId | undefined,
  ): Promise<[PublishedTransaction, any]> {
    const kind: OpenTransactionKind = this.openTxGet(openBatchId).kind;
    if (kind.kind !== "Exclusive") throw invalidCommit("open transaction is not exclusive");
    if (String(kind.madeBy) !== String(requestedMadeBy))
      throw nodeErr("OpenTransactionIdentityMismatch");
    let madeBy: AuthorSubject;
    try {
      madeBy = RowAuthor.fromSession(kind.madeBy, this.nodeUuidValue).asAuthorSubject();
    } catch {
      throw nodeErr("UnadmittedWriteAuthor");
    }
    if (!(await this.openExclusiveIsLocallySerializable(openBatchId)))
      throw nodeErr("TransactionConflict");
    const open: OpenTransaction | undefined = this.openTx.openTransactions
      .get(idKey(openBatchId))
      ?.clone();
    if (!open) throw nodeErr("MissingOpenBatch", openBatchId);
    const rangeError = "exclusive write now_ms exceeds packed HLC physical-millisecond range";
    for (const write of open.writes.iter())
      if (write.nowMs !== undefined) {
        try {
          TxTimeOps.fromPhysicalMs(write.nowMs);
        } catch {
          throw invalidCommit(rangeError);
        }
      }
    let txId: TxId;
    if (reserved) {
      if (String(reserved.node) !== String(this.nodeUuidValue))
        throw invalidCommit("reserved transaction identity belongs to another node");
      this.mergeTxTime(reserved.time);
      txId = reserved;
    } else txId = TxIdNew(this.mintTxTime(nowMs), this.nodeUuidValue);
    const snapshot = open.baseSnapshot;
    const versions: any[] = [];
    const claimed = new Set<string>();
    const claimedIds: any[] = [];
    const superseded = [...open.supersededLargeValues];
    for (const [write, hasContent] of coalesceExclusiveWrites(open.writes)) {
      const snapshotContent = await this.snapshotWinner(
        write.schemaVersion,
        write.table,
        write.rowUuid,
        snapshot,
      );
      this.tableInSchemaRef(write.table, write.schemaVersion);
      if (write.cells.kind !== "Replace")
        throw invalidCommit("exclusive transaction cannot contain update patches");
      let cells = new Map(write.cells.cells);
      const snapshotRow: SnapshotRow = await this.snapshotRowInSchema(
        write.schemaVersion,
        write.table,
        write.rowUuid,
        snapshot,
      );
      const tableSchema = this.tableInSchemaRef(write.table, write.schemaVersion);
      const inherited = new Map<string, Value>();
      tableSchema.columns.forEach((column: any, i: number) => {
        const value = snapshotRow.contentCells?.[i];
        if (value !== undefined) inherited.set(column.name, value);
      });
      for (const [column, value] of cells) {
        const prior = inherited.get(column);
        if (
          !valueContainsIndirectDescriptor(value) ||
          (prior !== undefined && valueEquals(prior, value))
        )
          continue;
        const staged = write.stagedLargeCells.get(column);
        if (!staged || !valueIsExactDescriptor(value, staged.valueRef))
          throw invalidCommit(
            "exclusive transaction contains an unverified large-value descriptor",
          );
        claimed.add(String(staged.id));
        claimedIds.push(staged.id);
      }
      for (const staged of write.stagedLargeCells.values())
        if (!claimed.has(String(staged.id))) superseded.push(staged.id);
      // A delete or restore without replacement content writes the snapshot's image.
      if (!hasContent) cells = new Map(inherited);
      const deletion = write.deletion ?? snapshotContent?.deletion();
      for (const [column, value] of cells) {
        const kind =
          tableSchema.columns.find((c: any) => c.name === column)?.largeValueKind ?? "NotLarge";
        const holder = { value };
        await this.prepareAndStageLargeScalar(holder, kind);
        cells.set(column, holder.value);
      }
      const positional = positionalCellsFromMap(tableSchema, cells);
      let provenanceAt: any;
      try {
        provenanceAt = TxTimeOps.fromPhysicalMs(write.nowMs ?? nowMs);
      } catch {
        throw invalidCommit(rangeError);
      }
      const createdBy = snapshotContent ? snapshotContent.createdBy() : madeBy;
      const createdAt = snapshotContent ? snapshotContent.createdAt() : provenanceAt;
      versions.push(
        VersionRecordEncode(
          tableSchema,
          write.schemaVersion,
          write.rowUuid,
          createdBy,
          TxTimeOps.physicalMs(createdAt),
          madeBy,
          TxTimeOps.physicalMs(provenanceAt),
          positional,
          deletion,
        ),
      );
    }
    if (versions.length > 0xffffffff) throw invalidCommit("transaction write count exceeds u32");
    const tx = {
      txId,
      kind: "Exclusive",
      nTotalWrites: versions.length,
      madeBy,
      permissionSubject: kind.permissionSubject,
      baseSnapshot: open.baseSnapshot,
      rowReadSet: open.rowReads,
      absentReadSet: open.absentReads,
      predicateReadSet: open.predicateReads,
      userMetadataJson: open.userMetadataJson,
      contributionMerge: undefined,
    };
    await this.ensureLargeValueStagesCurrent(claimedIds);
    // Evict before publishing: a resident publication holds Groove's lifecycle.
    await this.evictSupersededLargeValues(superseded);
    const publication = await this.publishPendingTransactionAndVersions(
      tx,
      [...versions],
      this.authoredCommitDurability,
    );
    this.openTx.openTransactions.delete(idKey(openBatchId));
    this.openTx.closedBatches.add(idKey(openBatchId));
    return [publication, { kind: "CommitUnit", tx, versions }];
  },

  async openExclusiveIsLocallySerializable(
    this: NodeState,
    openBatchId: OpenTransactionId,
  ): Promise<boolean> {
    const open: OpenTransaction = this.openTxGet(openBatchId).clone();
    const uncovered = async (version: any) => {
      const txId = this.versionTxId(version);
      const visible = isVisibleFate(await this.queryTransactionState(txId));
      return visible && !(await this.snapshotCovers(txId, open.baseSnapshot)) ? txId : undefined;
    };
    const rows: [string, RowUuid][] = [
      ...open.rowReads.map((r): [string, RowUuid] => [r.table, r.rowUuid]),
      ...open.absentReads.map((r): [string, RowUuid] => [r.table, r.rowUuid]),
      ...[...open.writes.iter()].map((w): [string, RowUuid] => [w.table, w.rowUuid]),
    ];
    for (const [table, rowUuid] of rows)
      for (const version of await this.queryRowVersions(table, rowUuid))
        if ((await uncovered(version)) !== undefined) return false;
    for (const predicate of open.predicateReads) {
      let comparison: any;
      for (const version of await this.queryTableVersions(predicate.table)) {
        const txId = await uncovered(version);
        if (txId === undefined) continue;
        if (!comparison) {
          const query = predicate.shape;
          // Only root-table history advances; relational reads stay conservative.
          const narrowed = open.narrowedPredicateReads.has(
            `${String(predicate.shapeId)}|${String(predicate.bindingId)}`,
          );
          if (
            query.aggregate !== undefined ||
            (!narrowed && (query.joins.length || query.arraySubqueries.length)) ||
            query.flatJoin !== undefined ||
            query.policyBranches.length ||
            query.reachable.length ||
            query.inherits.length ||
            query.includes.length ||
            query.relation !== undefined ||
            this.predicateReadIsDegenerateWholeTable(predicate)
          )
            return false;
          comparison = { ...open.baseSnapshot, dots: [...open.baseSnapshot.dots] };
        }
        comparison.dots.push(txId);
      }
      if (comparison) {
        comparison.dots.sort(cmpTxId);
        comparison.dots = comparison.dots.filter(
          (d: TxId, i: number, a: TxId[]) => i === 0 || cmpTxId(a[i - 1], d) !== 0,
        );
        // A newer row outside the filter is not a phantom.
        if (await this.shapePredicateOutputsDiffer(predicate, open.baseSnapshot, comparison))
          return false;
      }
    }
    return true;
  },

  async commitMergeableOpen(
    this: NodeState,
    openBatchId: OpenTransactionId,
    nextNowMs: () => number,
  ) {
    return this.commitMergeableOpenInner(openBatchId, nextNowMs, undefined);
  },

  async commitMergeableOpenAt(
    this: NodeState,
    openBatchId: OpenTransactionId,
    reserved: TxId,
    nextNowMs: () => number,
  ) {
    return this.commitMergeableOpenInner(openBatchId, nextNowMs, reserved);
  },

  async commitMergeableOpenInner(
    this: NodeState,
    openBatchId: OpenTransactionId,
    nextNowMs: () => number,
    reserved: TxId | undefined,
  ): Promise<PublishedTransaction> {
    if (this.openTxGet(openBatchId).kind.kind !== "Mergeable")
      throw invalidCommit("open transaction is not mergeable");
    const open: OpenTransaction | undefined = this.openTx.openTransactions
      .get(idKey(openBatchId))
      ?.clone();
    if (!open) throw nodeErr("MissingOpenBatch", openBatchId);
    const kind = open.kind as Extract<OpenTransactionKind, { kind: "Mergeable" }>;
    // Capture a sorted exact-version intent for every non-root branch write.
    const intents: any[] = [];
    for (const write of open.writes.iter()) {
      if (!write.branch.values.length) continue;
      const schema = this.catalogue.catalogueSchemas.get(schemaIdKey(write.schemaVersion))?.schema;
      if (!schema) throw invalidStored("write schema is missing");
      const table = this.tableInSchemaRef(write.table, write.schemaVersion);
      let head: any;
      try {
        [head] = schema.projectBranchViewSelector(table, write.branch);
      } catch (e) {
        throw nodeErr("InvalidBranchKey", String(e));
      }
      intents.push({
        version: 1,
        physicalTableId: this.physicalTableIdForSchema(write.schemaVersion, write.table),
        authoredSchema: write.schemaVersion,
        rowUuid: write.rowUuid,
        head,
        operation: write.branchViewCopy
          ? { kind: "ViewUpdateCopy", evidence: write.branchViewCopy }
          : { kind: write.cells.kind === "Patch" ? "ExactHeadUpdate" : "ExactHeadInsert" },
      });
    }
    intents.sort((a, b) => intentCmp(intentKey(a), intentKey(b)));
    const sameCoordinate = (a: any, b: any) => intentCmp(intentKey(a), intentKey(b)) === 0;
    for (let i = 1; i < intents.length; i++)
      if (
        sameCoordinate(intents[i - 1], intents[i]) &&
        JSON.stringify(intents[i - 1].operation) !== JSON.stringify(intents[i].operation)
      )
        throw invalidCommit("branch writes for one physical row must have one operation identity");
    const branchWriteIntents = intents.filter(
      (x, i) => i === 0 || !sameCoordinate(intents[i - 1], x),
    );
    const branchViewCopies = branchWriteIntents
      .filter((i) => i.operation.kind === "ViewUpdateCopy")
      .map((i) => i.operation.evidence);
    const commits: [SchemaVersionId, MergeableCommit][] = [];
    const superseded = [...open.supersededLargeValues];
    let index = 0;
    for (const write of open.writes.iter()) {
      const stagedLarge = write.stagedLargeCells;
      let cells: Map<string, Value>;
      let authoredColumns: Set<string> | undefined;
      let patchPreimage: Map<string, Value> | undefined;
      if (write.cells.kind === "Replace") cells = write.cells.cells;
      else {
        const patch = write.cells.cells;
        // The engine reads this preimage physically; keep untouched cells as proven provenance (#3507).
        const preimage = new Map<string, Value>(
          (await this.visibleCurrentCellsInBranch(write.table, write.branch, write.rowUuid)) ??
            new Map(),
        );
        cells = new Map(preimage);
        for (const k of patch.keys()) preimage.delete(k);
        authoredColumns = new Set(patch.keys());
        for (const [k, v] of patch) cells.set(k, v);
        patchPreimage = preimage;
      }
      let commit = new MergeableCommit(write.table, write.rowUuid, write.nowMs ?? nextNowMs())
        .withBranch(write.branch)
        .withMadeBy(kind.madeBy)
        .cells(cells);
      if (write.verifiedInheritedCells)
        commit = commit.verifiedInheritedLargeCells(write.verifiedInheritedCells);
      if (patchPreimage) commit = commit.verifiedInheritedLargeCells(patchPreimage);
      for (const [column, staged] of stagedLarge) {
        const value = commit.cellsMap.get(column);
        if (value !== undefined && valueIsExactDescriptor(value, staged.valueRef)) {
          commit.preparedLargeColumns.add(column);
          commit.stagedLargeValues.push(staged.id);
        } else superseded.push(staged.id);
      }
      if (authoredColumns) commit = commit.authoredColumns(authoredColumns);
      if (kind.permissionSubject !== undefined)
        commit = commit.withPermissionSubject(kind.permissionSubject);
      if (write.deletion) commit = commit.deletion(write.deletion);
      if (write.knownFreshRow) commit = commit.knownFreshRow();
      if (index === 0 && open.userMetadataJson !== undefined)
        commit = commit.userMetadata(open.userMetadataJson);
      commits.push([write.schemaVersion, commit]);
      index++;
    }
    // Validate every lowered write before advancing HLC/parent state.
    for (const [, commit] of commits) commit.validate();
    const first = commits[0];
    if (!first) throw invalidCommit("mergeable transaction requires at least one write");
    let madeAt: any;
    if (reserved) {
      if (String(reserved.node) !== String(this.nodeUuidValue))
        throw invalidCommit("reserved transaction identity belongs to another node");
      this.mergeTxTime(reserved.time);
      madeAt = reserved.time;
    } else madeAt = this.mintTxTime(first[1].nowMs);
    const contributionMerge =
      branchViewCopies.length === 0 && branchWriteIntents.length === 0
        ? undefined
        : {
            source: BranchKeyDefault(),
            target: BranchKeyDefault(),
            substitutions: [],
            branchViewCopies,
            branchWriteIntents,
          };
    await this.evictSupersededLargeValues(superseded);
    const committed = await this.commitMergeableManyAtWithSchemaVersionsAndProvenance(
      commits,
      madeAt,
      contributionMerge,
    );
    this.openTx.openTransactions.delete(idKey(openBatchId));
    this.openTx.closedBatches.add(idKey(openBatchId));
    return committed;
  },

  /** Evict Groove roots a committing transaction staged but no longer references. */
  async evictSupersededLargeValues(this: NodeState, ids: any[]): Promise<void> {
    for (const id of ids) {
      try {
        await this.database.evictStagedLargeValue(id);
      } catch {
        /* idempotent; the staging TTL reclaims survivors */
      }
    }
  },

  /** Abandon an open transaction; its staged roots are queued for eviction. */
  abandonTx(this: NodeState, txId: OpenTransactionId): void {
    const open = this.openTx.openTransactions.get(idKey(txId));
    if (!open) throw nodeErr("MissingOpenBatch", txId);
    this.openTx.openTransactions.delete(idKey(txId));
    this.openTx.closedBatches.add(idKey(txId));
    this.openTx.releasedLargeValues.push(...open.stagedLargeValueIds());
  },

  abandonAllOpenTransactions(this: NodeState): void {
    for (const [key, open] of this.openTx.openTransactions) {
      this.openTx.closedBatches.add(key);
      this.openTx.releasedLargeValues.push(...open.stagedLargeValueIds());
    }
    this.openTx.openTransactions.clear();
  },

  hasReleasedLargeValues(this: NodeState): boolean {
    return this.openTx.releasedLargeValues.length > 0;
  },

  async evictReleasedLargeValues(this: NodeState): Promise<void> {
    const released = this.openTx.releasedLargeValues;
    this.openTx.releasedLargeValues = [];
    await this.evictSupersededLargeValues(released);
  },

  openExclusiveSnapshotMoved(this: NodeState, txId: OpenTransactionId): boolean {
    return TxTimeOps.cmp(this.txTimeHighWater(), this.openTxGet(txId).baseSnapshot.localBase) > 0;
  },

  openTxGet(this: NodeState, txId: OpenTransactionId): OpenTransaction {
    const open = this.openTx.openTransactions.get(idKey(txId));
    if (!open) throw nodeErr("MissingOpenBatch", txId);
    return open;
  },

  recordAppliedGlobalTime(this: NodeState, globalTime: GlobalTime, txId: TxId): GlobalTime[] {
    const clock = this.clock;
    clock.globalTimeRegister = Math.max(clock.globalTimeRegister, globalTime);
    if (globalTime <= clock.committedGlobalTime) return [];
    const newlyApplied = clock.appliedGlobalTimesAfterFrontier.add(globalTime);
    const dots = clock.frontierDots.getOrInsert(globalTime, () => []);
    if (!dots.some((d: TxId) => cmpTxId(d, txId) === 0)) dots.push(txId);
    const locallyMinted = clock.locallyMintedGlobalTimes.delete(globalTime);
    if (this.historyComplete || locallyMinted) {
      clock.committedGlobalTime = globalTime;
      clock.appliedGlobalTimesAfterFrontier = new SortedSet(
        (a: number, b: number) => a - b,
        clock.appliedGlobalTimesAfterFrontier.toArray().filter((t: number) => t > globalTime),
      );
      clock.frontierDots.retain((t: number) => t > globalTime);
    }
    return newlyApplied ? [globalTime] : [];
  },

  /** Advance the highest authority-committed cut known to this node. */
  recordAuthoritativeSettledThrough(this: NodeState, settledThrough: GlobalTime): void {
    const clock = this.clock;
    if (settledThrough === 0) return;
    clock.globalTimeRegister = Math.max(clock.globalTimeRegister, settledThrough);
    if (settledThrough <= clock.committedGlobalTime) return;
    clock.committedGlobalTime = settledThrough;
    clock.appliedGlobalTimesAfterFrontier = new SortedSet(
      (a: number, b: number) => a - b,
      clock.appliedGlobalTimesAfterFrontier.toArray().filter((t: number) => t > settledThrough),
    );
    clock.frontierDots.retain((t: number) => t > settledThrough);
  },

  openTransactionSnapshot(this: NodeState, txId: OpenTransactionId): any {
    return this.openTxGet(txId).baseSnapshot;
  },

  async transactionIdsForGlobalTime(this: NodeState, globalTime: GlobalTime): Promise<TxId[]> {
    const txIds: TxId[] = [];
    for (const raw of await this.database.indexScanRaw("jazz_transactions", "by_global_time", [
      { kind: "Nullable", value: { kind: "U64", value: globalTime } },
    ])) {
      const record = raw.record();
      const node = this.nodeForAlias(record.getU64(TransactionRowRecord.FIELD_NODE_ID_IDX));
      if (node === undefined) throw invalidStored("transaction node alias must exist");
      txIds.push(TxIdNew(record.getU64(TransactionRowRecord.FIELD_TIME_IDX), node));
    }
    return txIds;
  },

  /** Pending, unsequenced transactions of other nodes that this runtime ingested. */
  async pendingForeignTransactionIds(this: NodeState): Promise<TxId[]> {
    const txIds: TxId[] = [];
    for (const txId of this.openTx.pendingForeignTransactions.toArray()) {
      const state = await this.queryTransactionState(txId);
      if (state && state[0].kind === "Pending" && state[1] === undefined) txIds.push(txId);
      else this.openTx.pendingForeignTransactions.delete(txId);
    }
    return txIds;
  },

  async snapshotCovers(this: NodeState, txId: TxId, snapshot: any): Promise<boolean> {
    let found: any;
    try {
      found = await this.queryTransactionGlobalTime(txId);
    } catch {
      return false;
    }
    if (found === undefined) return false;
    const globalTime = found as GlobalTime | null;
    return (
      (globalTime !== null && globalTime !== undefined && globalTime <= snapshot.globalBase) ||
      (String(txId.node) === String(snapshot.owner) &&
        TxTimeOps.cmp(txId.time, snapshot.localBase) <= 0) ||
      snapshot.dots.some((d: TxId) => cmpTxId(d, txId) === 0)
    );
  },

  async snapshotRowInSchema(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
    snapshot: any,
  ): Promise<SnapshotRow> {
    const content = await this.snapshotWinner(schemaVersion, table, rowUuid, snapshot);
    const deletion = await this.snapshotWinner(schemaVersion, table, rowUuid, snapshot);
    return this.snapshotRowFromWinners(schemaVersion, table, content, deletion);
  },

  /** Derive every row's snapshot state from one scan of the table's history. */
  async snapshotRowsInSchema(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    snapshot: any,
  ): Promise<SortedMap<RowUuid, SnapshotRow>> {
    const versions: any[] = await this.queryVersionsInSchema(schemaVersion, table, undefined);
    const covered = new Map<string, boolean>();
    const rows = new SortedMap<RowUuid, SnapshotRow>(cmpRowUuid);
    let start = 0;
    while (start < versions.length) {
      let end = start + 1;
      while (
        end < versions.length &&
        String(versions[end].rowUuid()) === String(versions[start].rowUuid())
      )
        end++;
      const group = versions.slice(start, end);
      start = end;
      const candidates: number[] = [];
      let undecodable = false;
      for (let idx = 0; idx < group.length; idx++) {
        let txId: TxId;
        try {
          txId = this.versionTxId(group[idx]);
        } catch {
          undecodable = true;
          break;
        }
        let isCovered = covered.get(String(txId));
        if (isCovered === undefined) {
          isCovered = await this.snapshotCovers(txId, snapshot);
          covered.set(String(txId), isCovered as boolean);
        }
        if (isCovered) candidates.push(idx);
      }
      const winnerIdx = undecodable
        ? undefined
        : currentVersionIndex(group, candidates, this.nodeAliases);
      const winner = winnerIdx === undefined ? undefined : group[winnerIdx];
      rows.set(
        group[0].rowUuid(),
        this.snapshotRowFromWinners(schemaVersion, table, winner, winner),
      );
    }
    return rows;
  },

  snapshotRowFromWinners(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    content: any | undefined,
    deletion: any | undefined,
  ): SnapshotRow {
    const deleted = deletion?.deletion() === "Deleted";
    this.tableInSchemaRef(table, schemaVersion);
    let contentCells: (Value | undefined)[] | undefined;
    if (content) {
      const sourceSchema = this.schemaVersionForAlias(content.schemaVersionAlias());
      if (sourceSchema === undefined)
        throw invalidStored("history schema version alias must exist");
      const sourceTable = this.tableInSchemaRef(content.table(), sourceSchema);
      const cells: Map<string, Value> = this.materializedCellsForVersion(sourceTable, content);
      const projected = this.translateCells(sourceSchema, schemaVersion, content.table(), cells);
      if (projected === table)
        contentCells = this.tableInSchemaRef(table, schemaVersion).columns.map((c: any) =>
          cells.get(c.name),
        );
    }
    let provenance: [any, any] | undefined;
    if (content) {
      let updated = content;
      if (deletion) {
        const deletionTx = this.versionTxId(deletion);
        const contentTx = this.versionTxId(content);
        if (
          TxTimeOps.sortKeyCmp(
            deletion.txTime(),
            deletionTx.node,
            content.txTime(),
            contentTx.node,
          ) > 0
        )
          updated = deletion;
      }
      provenance = [content, updated];
    }
    const txOf = (v: any) => {
      if (!v) return undefined;
      try {
        return this.versionTxId(v);
      } catch {
        return undefined;
      }
    };
    return {
      contentCells,
      contentVersion: txOf(content),
      deletionVersion: txOf(deletion),
      readVersion: deleted ? txOf(deletion) : txOf(content),
      deleted,
      provenance,
    };
  },

  async snapshotWinner(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
    snapshot: any,
  ) {
    // Domination over the fixed member set is stable for the whole transaction lifetime.
    let versions: any[];
    try {
      versions = await this.queryVersionsInSchema(schemaVersion, table, rowUuid);
    } catch {
      return undefined;
    }
    const candidates: number[] = [];
    for (let idx = 0; idx < versions.length; idx++) {
      let txId: TxId;
      try {
        txId = this.versionTxId(versions[idx]);
      } catch {
        return undefined;
      }
      if (await this.snapshotCovers(txId, snapshot)) candidates.push(idx);
    }
    const idx = currentVersionIndex(versions, candidates, this.nodeAliases);
    return idx === undefined ? undefined : versions[idx];
  },

  async snapshotContentWitness(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
    snapshot: any,
  ) {
    const version = await this.snapshotWinner(schemaVersion, table, rowUuid, snapshot);
    if (!version) return undefined;
    try {
      return this.versionTxId(version);
    } catch {
      return undefined;
    }
  },

  overlayPendingWritesInSchema(
    this: NodeState,
    txId: OpenTransactionId,
    schemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
    snapshotRow: SnapshotRow,
  ): Map<string, Value> | undefined {
    const tableSchema = this.tableInSchemaRef(table, schemaVersion);
    const [cells, deleted] = this.overlayPendingCellsAndDeletionWithTable(
      txId,
      tableSchema,
      table,
      rowUuid,
      snapshotRow,
    );
    return deleted ? undefined : cells;
  },

  overlayPendingCellsAndDeletionWithTable(
    this: NodeState,
    txId: OpenTransactionId,
    tableSchema: any,
    table: string,
    rowUuid: RowUuid,
    snapshotRow: SnapshotRow,
  ): [Map<string, Value> | undefined, boolean] {
    let cells: Map<string, Value> | undefined = snapshotRow.contentCells
      ? cellsFromPositional(tableSchema, snapshotRow.contentCells)
      : undefined;
    let deleted = snapshotRow.deleted;
    for (const write of this.openTxGet(txId).writes.rowWrites(table, rowUuid) as PendingWrite[]) {
      if (write.cells.kind === "Replace") {
        if (write.cells.cells.size) cells = new Map(write.cells.cells);
      } else {
        cells ??= new Map();
        for (const [k, v] of write.cells.cells) cells.set(k, v);
      }
      if (write.deletion === "Deleted") deleted = true;
      else if (write.deletion === "Restored") deleted = false;
    }
    return [cells, deleted];
  },
};

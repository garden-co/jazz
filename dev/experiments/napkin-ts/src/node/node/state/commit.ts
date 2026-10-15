// Local mergeable commit boundary, persistence settlement and current-row materialization.
import { SortedSet, cmpNum } from "../../../support/collections.js";
import { compareBytes, toHex } from "../../../support/bytes.js";
import {
  type BranchKey,
  type BranchSelector,
  type RowUuid,
  type SchemaVersionId,
  type TxId,
  type TxTime,
  type Value,
  BorrowedRecord,
  BranchKeyDefault,
  BranchWriteOperation,
  GlobalCurrentRowRecord,
  LiteralValue,
  StaticScanSpec,
  TX_TOUCHED_ROWS_TABLE,
  TouchedRows,
  branchSelectorEquals,
  currentRowFromVersionProjection,
  decodeCurrentRow,
  deletionEventFromValue,
  DELETION_COLUMN_NAME,
  GlobalTimeOps,
  HistoryRowRecord,
  INLINE_VALUE_MAX_BYTES,
  LargeValueKind,
  PhysicalWriteTarget,
  QueryFrom,
  RowAuthor,
  TxIdNew,
  TxTimeOps,
  VersionRow,
  cmpRowUuid,
  cmpTxId,
  physicalAheadCurrentTableName,
  schemaIdKey,
  transactionValues,
  txIdKey,
  uuidEquals,
  valueEquals,
} from "../../../_extern/node-state.js";
import { splitMergeOps } from "../merge_ops.js";
import { Layer, compose, tryRefLayers } from "../layer_stack.js";
import { refPhysicalClass } from "../refs.js";
import {
  type MergeableCommit,
  type PublicationOutcome,
  type NodeState,
  PublishedTransaction,
  TX_VERSION_TABLE_CACHE_MAX_ENTRIES,
  collectIndirectDescriptors,
  invalidCommit,
  invalidStored,
  nodeErr,
  valueContainsIndirectDescriptor,
  V,
} from "../index.js";

type SchemaCommit = [SchemaVersionId, MergeableCommit];

function validateBatchCommits(commits: MergeableCommit[]): void {
  if (!commits.length) throw invalidCommit("mergeable transaction requires at least one write");
  const subject = commits[0].effectivePermissionSubject();
  for (const commit of commits) {
    commit.validate();
    if (
      !valueEquals(commit.effectivePermissionSubject(), subject) &&
      commit.effectivePermissionSubject() !== subject
    )
      throw invalidCommit("mergeable transaction permission subjects must match");
  }
}

const branchErr = (f: () => any) => {
  try {
    return f();
  } catch (e) {
    throw nodeErr("InvalidBranchKey", String((e as Error).message ?? e));
  }
};

export const commitMethods = {
  /** Commit a local mergeable write and leave its fate pending. */
  async commitMergeable(this: NodeState, commit: MergeableCommit): Promise<PublishedTransaction> {
    commit.validate();
    return this.commitMergeableAt(commit, this.mintTxTime(commit.nowMs));
  },
  /** Commit one local mergeable write under an admitted authored schema. */
  commitMergeableInSchema(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    commit: MergeableCommit,
  ) {
    return this.commitMergeableManyInSchema(schemaVersion, [commit]);
  },
  /** Commit multiple local mergeable writes as one transaction. */
  async commitMergeableMany(
    this: NodeState,
    commits: MergeableCommit[],
  ): Promise<PublishedTransaction> {
    validateBatchCommits(commits);
    return this.commitMergeableManyAt(commits, this.mintTxTime(commits[0].nowMs));
  },
  async commitMergeableManyInSchema(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    commits: MergeableCommit[],
  ) {
    this.requireCatalogueReady();
    if (!this.catalogue.catalogueSchemas.has(schemaIdKey(schemaVersion)))
      throw invalidCommit("authored schema version is not admitted");
    validateBatchCommits(commits);
    const madeAt = this.mintTxTime(commits[0].nowMs);
    return this.commitMergeableManyAtWithSchemaVersions(
      commits.map((c) => [schemaVersion, c]),
      madeAt,
    );
  },
  /** Commit prepared local writes under a transaction identity reserved before preparation began. */
  async commitMergeableManyInSchemaAt(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    commits: MergeableCommit[],
    reserved: TxId,
  ) {
    if (!uuidEquals(reserved.node, this.nodeUuidValue))
      throw invalidCommit("reserved transaction belongs to another node");
    this.requireCatalogueReady();
    if (!this.catalogue.catalogueSchemas.has(schemaIdKey(schemaVersion)))
      throw invalidCommit("authored schema version is not admitted");
    validateBatchCommits(commits);
    this.mergeTxTime(reserved.time);
    return this.commitMergeableManyAtWithSchemaVersions(
      commits.map((c) => [schemaVersion, c]),
      reserved.time,
    );
  },
  commitMergeableAt(this: NodeState, commit: MergeableCommit, madeAt: TxTime) {
    return this.commitMergeableManyAt([commit], madeAt);
  },
  commitMergeableManyAt(this: NodeState, commits: MergeableCommit[], madeAt: TxTime) {
    this.requireCatalogueReady();
    const write = this.catalogue.activeSchema.schema;
    return this.commitMergeableManyAtWithSchemaVersions(
      commits.map((c) => [write, c]),
      madeAt,
    );
  },
  commitMergeableManyAtWithSchemaVersions(
    this: NodeState,
    commits: SchemaCommit[],
    madeAt: TxTime,
  ) {
    return this.commitMergeableManyAtWithSchemaVersionsAndProvenance(commits, madeAt, undefined);
  },

  async commitMergeableManyAtWithSchemaVersionsAndProvenance(
    this: NodeState,
    input: SchemaCommit[],
    madeAt: TxTime,
    contributionMergeIn: any,
  ): Promise<PublishedTransaction> {
    // Trusted internal paths use `*_at` too; validate before any staging or batch mutation.
    for (const [, commit] of input) commit.validate();
    // One row has one image per transaction.
    const commits = coalesceSameRowCommits(input);
    // Authorization uses the session selected before durable attribution is normalized.
    const permissionSubject = commits[0][1].effectivePermissionSubject();
    for (const [, commit] of commits) {
      try {
        commit.madeBy = RowAuthor.fromSession(commit.madeBy, this.nodeUuidValue).asAuthorSubject();
      } catch {
        throw nodeErr("UnadmittedWriteAuthor");
      }
    }
    // Lower present optional-JSON values to the bare stored scalar.
    for (const [schemaVersion, commit] of commits) {
      const table = this.tableInSchemaRef(commit.table, schemaVersion);
      for (const [name, value] of commit.cellsMap) {
        const column = table.columns.find((c: any) => c.name === name);
        if (column) commit.cellsMap.set(name, column.storageValue(value));
      }
    }
    // Fill exact branch operations not already classified by a higher view-copy path.
    const contributionMerge = contributionMergeIn ?? {
      source: BranchKeyDefault,
      target: BranchKeyDefault,
      substitutions: [],
      branchViewCopies: [],
      branchWriteIntents: [],
    };
    for (const [schemaVersion, commit] of commits) {
      if (!commit.branch.values.length) continue;
      const schema = this.catalogue.catalogueSchemas.get(schemaIdKey(schemaVersion))?.schema;
      if (!schema) throw invalidStored("write schema is missing");
      const table = this.tableInSchemaRef(commit.table, schemaVersion);
      // Project only through the target table.
      const [head] = branchErr(() => schema.projectBranchSelector(table, commit.branch));
      branchErr(() => schema.validateAuthoredBranchKey(table, head));
      const tableId = this.physicalTableIdForSchema(schemaVersion, commit.table);
      if (
        contributionMerge.branchWriteIntents.some(
          (i: any) =>
            i.physicalTableId === tableId &&
            uuidEquals(i.authoredSchema, schemaVersion) &&
            uuidEquals(i.rowUuid, commit.rowUuid) &&
            i.head.equals(head),
        )
      )
        continue;
      const exists =
        (await this.queryLocalWinnerInBranch(commit.table, head, commit.rowUuid)) !== undefined ||
        (await this.queryGlobalWinnerInBranch(commit.table, head, commit.rowUuid)) !== undefined;
      contributionMerge.branchWriteIntents.push({
        version: 1,
        physicalTableId: tableId,
        authoredSchema: schemaVersion,
        rowUuid: commit.rowUuid,
        head,
        operation: exists
          ? BranchWriteOperation.ExactHeadUpdate
          : BranchWriteOperation.ExactHeadInsert,
      });
    }
    contributionMerge.branchWriteIntents.sort(
      (a: any, b: any) =>
        a.physicalTableId - b.physicalTableId ||
        compareBytes(a.authoredSchema, b.authoredSchema) ||
        cmpRowUuid(a.rowUuid, b.rowUuid) ||
        a.head.cmp(b.head),
    );
    const hasProvenance =
      contributionMerge.substitutions.length ||
      contributionMerge.branchViewCopies.length ||
      contributionMerge.branchWriteIntents.length;
    const txId = TxIdNew(madeAt, this.nodeUuidValue);
    const madeBy = commits[0][1].madeBy;
    if (commits.length > 0xffff_ffff) throw invalidCommit("transaction write count exceeds u32");
    const tx = {
      txId,
      kind: "Mergeable",
      nTotalWrites: commits.length,
      madeBy,
      permissionSubject,
      userMetadataJson: commits[0][1].userMetadataJson,
      contributionMerge: hasProvenance ? contributionMerge : undefined,
    };
    const storedMerge = this.admitContributionMergeForStorage(tx);
    await this.prepareAndStageLargeCommitValues(commits);
    const stagedIds = new Set<any>();
    for (const [, commit] of commits) for (const id of commit.stagedLargeValues) stagedIds.add(id);
    await this.ensureLargeValueStagesCurrent(stagedIds);
    const txNodeAlias = await this.ensureNodeAlias(txId.node);
    const batch = this.database.openBatch();
    for (const [, commit] of commits)
      for (const id of commit.stagedLargeValues) batch.acceptLargeValue(id);
    batch.insert(
      "jazz_transactions",
      transactionValues(
        txNodeAlias,
        tx,
        { kind: "Pending" },
        undefined,
        this.authoredCommitDurability,
        storedMerge,
      ),
    );
    const storedVersions: any[] = [];
    for (const [writeSchema, commit] of commits) {
      let provenanceAt: TxTime;
      try {
        provenanceAt = TxTimeOps.fromPhysicalMs(commit.nowMs);
      } catch {
        throw invalidCommit("commit now_ms exceeds packed HLC physical-millisecond range");
      }
      const schemaVersionAlias = await this.ensureSchemaVersionAlias(writeSchema);
      const tableSchema = this.tableInSchema(commit.table, writeSchema);
      const schema = this.catalogue.catalogueSchemas.get(schemaIdKey(writeSchema))?.schema;
      if (!schema) throw invalidStored("commit schema missing");
      const [branchKey, branchCells] = branchErr(() =>
        schema.projectBranchSelector(tableSchema, commit.branch),
      );
      const knownFresh = commit.knownFresh;
      let previous = knownFresh
        ? undefined
        : await this.queryLocalWinnerInBranch(tableSchema.name, branchKey, commit.rowUuid);
      if (!previous && !knownFresh)
        previous = await this.queryGlobalWinnerInBranch(
          tableSchema.name,
          branchKey,
          commit.rowUuid,
        );
      const [createdBy, createdAt] = previous
        ? [previous.createdBy(), previous.createdAt()]
        : [commit.madeBy, provenanceAt];
      // The image this write is made over (SPEC 4 §4.6, "Base of a write").
      const base: { pending?: any; seq?: any } = {};
      if (previous) {
        base.pending = await this.newestOwnPendingWriteInBatch(
          batch,
          tableSchema.name,
          branchKey,
          commit.rowUuid,
        );
        base.seq = await this.globalCurrentSeqInBatch(
          batch,
          writeSchema,
          tableSchema.name,
          branchKey,
          commit.rowUuid,
        );
      }
      const cells = commit.cellsMap;
      for (const [column, value] of branchCells as Map<string, Value>) {
        const authored = cells.get(column);
        if (authored !== undefined && !valueEquals(authored, value))
          throw invalidCommit("branch column does not match exact branch key");
        cells.set(column, value);
      }
      const authoredColumns = new Set(commit.authoredColumnsSet ?? cells.keys());
      // The version is a complete row image: unauthored cells carry the previous image forward.
      let deletion = previous?.deletion();
      if (commit.deletionEvent !== undefined) {
        authoredColumns.add(DELETION_COLUMN_NAME);
        deletion = commit.deletionEvent;
      }
      let baseCells = new Map<string, Value>();
      if (previous) {
        const previousSchema = this.schemaVersionForAlias(previous.schemaVersionAlias());
        if (previousSchema === undefined)
          throw invalidStored("previous row image schema alias must exist");
        const previousTable = this.tableInSchema(previous.table(), previousSchema);
        const inherited: Map<string, Value> = previous.cells(previousTable);
        const projected = uuidEquals(previousSchema, writeSchema)
          ? tableSchema.name
          : this.translateCells(previousSchema, writeSchema, previous.table(), inherited);
        if (projected === tableSchema.name) {
          baseCells = new Map(inherited);
          for (const [column, value] of inherited)
            if (!authoredColumns.has(column) && !cells.has(column)) cells.set(column, value);
        }
      }
      // Merge columns travel as ops over the image this write saw.
      const [imageCells, counterSigns] = splitMergeOps(
        tableSchema,
        authoredColumns,
        baseCells,
        cells,
      );
      const authoredColumnIds = this.authoredColumnIdsForNames(
        writeSchema,
        tableSchema.name,
        authoredColumns,
      );
      const historyDescriptor = this.preparedPhysicalWritePlan(
        writeSchema,
        tableSchema.name,
        PhysicalWriteTarget.History,
      ).logicalDescriptor;
      const rowVersion = (rowCells: Map<string, Value>, signs: Uint8Array) =>
        VersionRow.fromPartsWithSchemaVersion(
          tableSchema,
          {
            table: commit.table,
            branchKey,
            rowUuid: commit.rowUuid,
            txNodeAlias,
            schemaVersionAlias,
            txTime: madeAt,
            createdBy,
            createdAt,
            updatedBy: commit.madeBy,
            updatedAt: provenanceAt,
            cells: rowCells,
            authoredColumns: authoredColumnIds,
            deletion,
            // A pending write has no seq yet.
            seq: GlobalTimeOps.zero,
            base,
            counterSigns: signs,
          },
          uuidEquals(writeSchema, this.catalogue.localSchemaVersionId) ? undefined : writeSchema,
          historyDescriptor,
        );
      // The overlay is the local image: it carries no ops, so no signs.
      const overlay = imageCells ? rowVersion(imageCells, new Uint8Array()) : undefined;
      const stored = rowVersion(cells, counterSigns);
      const [historyTable, record] = this.versionStorageWriteBinding(stored, tx.madeBy);
      batch.insertRaw(historyTable, this.versionStoragePrimaryKey(stored), record);
      this.writeAheadCurrentInsert(batch, overlay ?? stored);
      storedVersions.push(stored);
    }
    const persistence = await this.applyNodeBatch(batch);
    this.cacheTxVersions(txId, storedVersions.slice());
    if (!valueEquals(permissionSubject, madeBy) && permissionSubject !== madeBy)
      this.openTx.localPermissionSubjects.set(txIdKey(txId), permissionSubject);
    this.pendingPersistence.add(txId);
    return new PublishedTransaction(txId, persistence);
  },

  /** Lower oversized ordinary scalar cells and stage their immutable nodes before publication. */
  async prepareAndStageLargeCommitValues(this: NodeState, commits: SchemaCommit[]): Promise<void> {
    for (const [schemaVersion, commit] of commits) {
      const tableSchema = this.tableInSchemaRef(commit.table, schemaVersion);
      const inherited: Map<string, Value> = [...commit.cellsMap.values()].some(
        valueContainsIndirectDescriptor,
      )
        ? ((await this.currentPhysicalCellsInBranchSchema(
            schemaVersion,
            commit.table,
            commit.branch,
            commit.rowUuid,
          )) ?? new Map())
        : new Map();
      for (const [column, value] of commit.cellsMap) {
        const kind =
          tableSchema.columns.find((c: any) => c.name === column)?.largeValueKind ?? "NotLarge";
        const prior = inherited.get(column);
        if (
          valueContainsIndirectDescriptor(value) &&
          prior !== undefined &&
          valueEquals(prior, value)
        ) {
          commit.preparedLargeColumns.add(column);
          continue;
        }
        const holder = { value };
        const staged = await this.prepareAndStageLargeScalar(holder, kind);
        if (!staged) continue;
        commit.cellsMap.set(column, holder.value);
        commit.stagedLargeValues.push(staged.id);
      }
    }
  },

  /** Lower one top-level scalar cell, preserving its nullable wrapper. */
  async prepareAndStageLargeScalar(
    this: NodeState,
    holder: { value: Value },
    semanticKind: string,
  ): Promise<any> {
    const textKind = semanticKind === "Json" ? LargeValueKind.Json : LargeValueKind.String;
    const candidate = (v: Value): [any, Uint8Array] | undefined => {
      if (v.kind === "String") {
        const bytes = new TextEncoder().encode(v.value);
        return bytes.length > INLINE_VALUE_MAX_BYTES ? [textKind, bytes] : undefined;
      }
      if (v.kind === "Bytes" && v.value.length > INLINE_VALUE_MAX_BYTES)
        return [LargeValueKind.Bytes, v.value];
      return undefined;
    };
    const value = holder.value;
    const nullable = value.kind === "Nullable" && value.value !== undefined;
    const found = nullable ? candidate(value.value) : candidate(value);
    if (!found) return undefined;
    const staged = await this.database.prepareAndStageLargeValue(found[0], found[1]);
    await this.enforceLargeValueStagingPolicy(staged);
    const descriptor = V("Large", staged.valueRef);
    holder.value = nullable ? V("Nullable", descriptor) : descriptor;
    return staged;
  },

  /** Commit a local mergeable write and return its sync commit unit. */
  async commitMergeableUnit(
    this: NodeState,
    commit: MergeableCommit,
  ): Promise<[PublishedTransaction, any]> {
    const published = await this.commitMergeable(commit);
    return [published, await this.residentCommitUnit(published.txId)];
  },
  async residentCommitUnit(this: NodeState, txId: TxId): Promise<any> {
    // Use the transaction actually published by the common commit path.
    const found = await this.queryTransaction(txId);
    if (!found) throw nodeErr("MissingTransaction", txId);
    const cached = this.cachedTxVersions(found.tx.txId);
    if (!cached) throw new Error("newly published transaction retains its resident versions");
    return {
      kind: "CommitUnit",
      tx: found.tx,
      versions: cached.map((row: any) => this.versionRecordFromRow(row)),
    };
  },
  /** Settle a completed persistence receipt and release its storage boundary. */
  settlePublishedTransaction(this: NodeState, txId: TxId, persistence: any): void {
    this.database.finishPersistence(persistence);
    this.pendingPersistence.delete(txId);
  },
  async persistAndSettleTransaction(
    this: NodeState,
    published: PublishedTransaction,
  ): Promise<TxId> {
    const persistence = await this.database.persistWithProgress(published.persistence);
    this.settlePublishedTransaction(published.txId, persistence);
    return published.txId;
  },
  /** Persist and settle every resident publication attached to an outcome. */
  async persistAndSettleOutcome<T>(this: NodeState, outcome: PublicationOutcome<T>): Promise<T> {
    let [value, publications, work] = outcome.intoParts();
    work = work.slice();
    for (;;) {
      for (const publication of publications) {
        const persistence = await this.database.persistWithProgress(publication.persistence);
        this.settlePublishedTransaction(publication.txId, persistence);
      }
      const message = work.shift();
      if (message === undefined) break;
      // Internal continuation work: its publications matter, its protocol response does not.
      const next = await this.applySyncMessage(message);
      publications = next.publications;
      work.push(...next.postSettlementWork);
    }
    return value;
  },
  /** Rebuild the sync commit unit for an already-committed local transaction from stored versions. */
  async commitUnitFor(this: NodeState, txId: TxId): Promise<any> {
    const found = await this.queryTransaction(txId);
    if (!found) throw nodeErr("MissingTransaction", txId);
    const versions = (await this.queryVersionsForTx(txId)).map((row: any) =>
      this.versionRecordFromRow(row),
    );
    return { kind: "CommitUnit", tx: found.tx, versions };
  },

  // ---- current-cell reads

  async visibleCurrentCells(
    this: NodeState,
    table: string,
    rowUuid: RowUuid,
  ): Promise<Map<string, Value> | undefined> {
    const row = (await this.currentRows(table, "Local")).find((r: any) =>
      uuidEquals(r.rowUuid(), rowUuid),
    );
    if (!row) return undefined;
    const tableSchema = this.table(table);
    const out = new Map<string, Value>();
    for (const column of tableSchema.columns) {
      const v = row.cell(tableSchema, column.name);
      if (v !== undefined) out.set(column.name, v);
    }
    return out;
  },
  /** Read one exact branch-local row for mutation preparation. */
  visibleCurrentCellsInBranch(
    this: NodeState,
    table: string,
    branch: BranchSelector,
    rowUuid: RowUuid,
  ) {
    return this.visibleCurrentPhysicalCellsInBranchSchema(
      this.catalogue.activeSchema.schema,
      table,
      branch,
      rowUuid,
    );
  },
  /** A branch-local winner projected into one schema, retaining indirect scalar descriptors. */
  async visibleCurrentPhysicalCellsInBranchSchema(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    branch: BranchSelector,
    rowUuid: RowUuid,
  ): Promise<Map<string, Value> | undefined> {
    return (
      await this.visibleCurrentPhysicalCellsAndWinnerInBranchSchema(
        schemaVersion,
        table,
        branch,
        rowUuid,
      )
    )?.[0];
  },
  async visibleCurrentPhysicalCellsAndWinnerInBranchSchema(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    branch: BranchSelector,
    rowUuid: RowUuid,
  ): Promise<[Map<string, Value>, TxId] | undefined> {
    const tableSchema = this.tableInSchemaRef(table, schemaVersion);
    const schema = this.catalogue.catalogueSchemas.get(schemaIdKey(schemaVersion))?.schema;
    if (!schema) throw invalidStored("registered read schema missing");
    const [branchKey] = branchErr(() => schema.projectBranchSelector(tableSchema, branch));
    const content =
      (await this.queryLocalWinnerInBranch(table, branchKey, rowUuid)) ??
      (await this.queryGlobalWinnerInBranch(table, branchKey, rowUuid));
    if (!content || content.isDeleted()) return undefined;
    const contentTx = this.versionTxId(content);
    const authoredSchema = this.schemaVersionForAlias(content.schemaVersionAlias());
    if (authoredSchema === undefined)
      throw invalidStored("current version schema alias must exist");
    const authoredTable = this.tableInSchemaRef(content.table(), authoredSchema);
    const cells = this.materializedCellsForVersion(authoredTable, content);
    const projected = this.translateCells(authoredSchema, schemaVersion, content.table(), cells);
    if (projected === undefined) return undefined;
    if (projected !== table) throw invalidStored("current version projects to an unexpected table");
    return [cells, contentTx];
  },
  /** The exact local content parent for a branch-local row. A deleted image is not visible content. */
  async localContentWinnerTxIdInBranch(
    this: NodeState,
    table: string,
    branch: BranchSelector,
    rowUuid: RowUuid,
  ) {
    const version = await this.localWinnerInBranchSelector(table, branch, rowUuid);
    return version && !version.isDeleted() ? this.versionTxId(version) : undefined;
  },
  async localDeletionWinnerTxIdInBranch(
    this: NodeState,
    table: string,
    branch: BranchSelector,
    rowUuid: RowUuid,
  ) {
    const version = await this.localWinnerInBranchSelector(table, branch, rowUuid);
    return version && version.isDeleted() ? this.versionTxId(version) : undefined;
  },
  async localWinnerInBranchSelector(
    this: NodeState,
    table: string,
    branch: BranchSelector,
    rowUuid: RowUuid,
  ) {
    const schemaVersion = this.catalogue.activeSchema.schema;
    const tableSchema = this.tableInSchemaRef(table, schemaVersion);
    const schema = this.catalogue.catalogueSchemas.get(schemaIdKey(schemaVersion))?.schema;
    if (!schema) throw invalidStored("current write schema missing");
    const [branchKey] = branchErr(() => schema.projectBranchSelector(tableSchema, branch));
    return this.queryLocalViewWinnerInBranch(table, branchKey, rowUuid);
  },
  /** Current rows at the requested durability tier. */
  async currentRows(this: NodeState, table: string, settled: any): Promise<any[]> {
    this.requireCatalogueReady();
    const shape = QueryFrom(table).validate(this.catalogue.schema);
    return this.queryRows(shape, shape.bind(new Map()), settled);
  },
  localContentWinnerTxId(this: NodeState, table: string, rowUuid: RowUuid) {
    return this.localContentWinnerTxIdInSchema(this.catalogue.activeSchema.schema, table, rowUuid);
  },
  async localContentWinnerTxIdInSchema(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
  ) {
    const tableSchema = this.tableInSchema(table, schemaVersion);
    // Only the winner's identity is needed: read the local image by key.
    const image = await this.queryLocalViewWinnerInBranch(
      tableSchema.name,
      BranchKeyDefault,
      rowUuid,
    );
    return image && !image.isDeleted() ? this.versionTxId(image) : undefined;
  },
  localDeletionWinnerTxId(this: NodeState, table: string, rowUuid: RowUuid) {
    return this.localDeletionWinnerTxIdInSchema(this.catalogue.activeSchema.schema, table, rowUuid);
  },
  async localDeletionWinnerTxIdInSchema(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
  ) {
    const tableSchema = this.tableInSchema(table, schemaVersion);
    // Deletion is stamped into the row image.
    const image = await this.queryLocalViewWinnerInBranch(
      tableSchema.name,
      BranchKeyDefault,
      rowUuid,
    );
    return image && image.deletion() === "Deleted" ? this.versionTxId(image) : undefined;
  },

  // ---- indexes

  async rebuildAheadCurrentKeys(this: NodeState): Promise<void> {
    this.aheadCurrentKeys.clear();
    const tableIds = new SortedSet<number>(cmpNum);
    for (const mapping of this.catalogue.physicalMappings.values())
      for (const table of mapping.tables.values()) tableIds.add(table.tableId);
    for (const tableId of tableIds) {
      for (const raw of await this.database.primaryKeyScanRaw(
        physicalAheadCurrentTableName(tableId),
        [],
      )) {
        const record = raw.record();
        const txTime = record.getU64(HistoryRowRecord.FIELD_TX_TIME_IDX);
        const node = this.nodeForAlias(record.getU64(HistoryRowRecord.FIELD_TX_NODE_ID_IDX));
        if (node === undefined) throw invalidStored("overlay node alias must exist");
        this.aheadCurrentKeys.insert([tableId, raw.key()], TxIdNew(txTime, node));
      }
    }
  },
  /** Note that `version`'s history row is written in the open batch. */
  markTxTouchedRow(this: NodeState, tableId: number, version: any): void {
    this.txTouchedDirty
      .getOrInsert([version.txTime(), version.txNodeAlias()], () => new TouchedRows())
      .insert(tableId, version.branchKey().canonicalBytes(), version.rowUuid());
  },
  /** Apply a batch this node built, first flushing touched-row marks into their transactions' lists. */
  async applyNodeBatch(this: NodeState, batch: any): Promise<any> {
    await this.flushTxTouchedRows(batch);
    return this.database.applyBatch(batch);
  },
  /** The history rows this node listed for a transaction. */
  async loadTxTouchedRows(
    this: NodeState,
    batch: any,
    txTime: TxTime,
    nodeAlias: number,
  ): Promise<any> {
    const key = [V("U64", txTime), V("U64", nodeAlias)];
    const raw = batch
      ? await this.database.primaryKeyGetRawInBatch(batch, TX_TOUCHED_ROWS_TABLE, key)
      : await this.database.primaryKeyGetRaw(TX_TOUCHED_ROWS_TABLE, key);
    return raw ? TouchedRows.fromListRecord(raw.record()) : new TouchedRows();
  },
  /** Extend each marked transaction's list (as staged in this batch) with rows marked since the last flush. */
  async flushTxTouchedRows(this: NodeState, batch: any): Promise<void> {
    if (!this.txTouchedDirty.size) return;
    const dirty = this.txTouchedDirty.clone();
    this.txTouchedDirty.clear();
    for (const [[txTime, nodeAlias], rows] of dirty) {
      const stored = await this.loadTxTouchedRows(batch, txTime, nodeAlias);
      const touched = stored.clone();
      touched.extend(rows);
      if (touched.equals(stored)) continue;
      batch.update(TX_TOUCHED_ROWS_TABLE, touched.listRecordValues(txTime, nodeAlias));
    }
  },

  // ---- transaction version cache

  cachedTxVersions(this: NodeState, txId: TxId): any[] | undefined {
    return this.query.txVersionsCache.get(txIdKey(txId))?.versions.slice();
  },
  cacheTxVersions(this: NodeState, txId: TxId, versions: any[]): void {
    this.touchTxVersionCacheEntry(txId);
    this.query.txVersionsCache.set(txIdKey(txId), { versions });
    this.boundTxVersionCache();
  },
  touchTxVersionCacheEntry(this: NodeState, txId: TxId): void {
    const key = txIdKey(txId);
    if (!this.query.txVersionTablesCacheOrderSet.has(key)) {
      this.query.txVersionTablesCacheOrderSet.add(key);
      this.query.txVersionTablesCacheOrder.push(txId);
    }
  },
  boundTxVersionCache(this: NodeState): void {
    const q = this.query;
    while (q.txVersionsCache.size > TX_VERSION_TABLE_CACHE_MAX_ENTRIES) {
      const oldest = q.txVersionTablesCacheOrder.shift();
      if (oldest === undefined) break;
      const key = txIdKey(oldest);
      if (!q.txVersionTablesCacheOrderSet.delete(key)) continue;
      q.txVersionsCache.delete(key);
    }
  },
  invalidateTxVersionTablesCache(this: NodeState, txId: TxId): void {
    const key = txIdKey(txId);
    this.query.txVersionsCache.delete(key);
    this.query.txVersionTablesCacheOrderSet.delete(key);
  },

  // ---- row materialization

  materializeCurrentRow(this: NodeState, _table: any, row: any) {
    return row;
  },
  currentRowFromMaterializedVersion(this: NodeState, table: any, version: any) {
    return currentRowFromVersionProjection(table, version);
  },
  materializedCellsForVersion(this: NodeState, table: any, version: any): Map<string, Value> {
    return version.cells(table);
  },
  localCurrentRow(this: NodeState, table: string, rowUuid: RowUuid) {
    return this.localCurrentRowInSchema(table, rowUuid, this.catalogue.activeSchema.schema);
  },
  /** Resolve an engine-owned indirect descriptor from the current physical row (caller authorizes). */
  async currentPhysicalCellInSchema(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
    column: string,
  ): Promise<Value | undefined> {
    return (await this.currentPhysicalCellsInSchema(schemaVersion, table, rowUuid))?.get(column);
  },
  currentPhysicalCellsInSchema(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
  ) {
    return this.currentPhysicalCellsInBranchSchema(schemaVersion, table, { values: [] }, rowUuid);
  },
  currentPhysicalCellsAndWinnerInSchema(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    rowUuid: RowUuid,
  ) {
    return this.visibleCurrentPhysicalCellsAndWinnerInBranchSchema(
      schemaVersion,
      table,
      { values: [] },
      rowUuid,
    );
  },
  currentPhysicalCellsInBranchSchema(
    this: NodeState,
    schemaVersion: SchemaVersionId,
    table: string,
    branch: BranchSelector,
    rowUuid: RowUuid,
  ) {
    return this.visibleCurrentPhysicalCellsInBranchSchema(schemaVersion, table, branch, rowUuid);
  },
  /** Seal several independently staged large values into one ordinary row commit. */
  async sealLargeValueUpdates(
    this: NodeState,
    commit: MergeableCommit,
    staged: [string, any][],
    schemaVersion: SchemaVersionId,
  ): Promise<MergeableCommit> {
    const inherited = await this.currentPhysicalCellsInBranchSchema(
      schemaVersion,
      commit.table,
      commit.branch,
      commit.rowUuid,
    );
    if (!inherited) throw invalidCommit("partial large-value update target is not observed");
    for (const [column, value] of commit.cellsMap) {
      if (!valueContainsIndirectDescriptor(value)) continue;
      const target = staged.find(([c]) => c === column)?.[1].valueRef;
      let valid: boolean;
      if (target) {
        const descriptors: any[] = [];
        collectIndirectDescriptors(value, descriptors);
        valid = descriptors.length === 1 && descriptors[0].equals(target);
      } else {
        const prior = inherited.get(column);
        valid = prior !== undefined && valueEquals(prior, value);
      }
      if (!valid)
        throw invalidCommit("partial large-value update contains an unverified descriptor");
      commit.preparedLargeColumns.add(column);
    }
    commit.stagedLargeValues.push(...staged.map(([, s]) => s.id));
    return commit;
  },
  async sealInheritedLargeValues(
    this: NodeState,
    commit: MergeableCommit,
    schemaVersion: SchemaVersionId,
    allowInherited: boolean,
  ): Promise<MergeableCommit> {
    if (![...commit.cellsMap.values()].some(valueContainsIndirectDescriptor)) return commit;
    if (!allowInherited)
      throw invalidCommit("complete row replacement contains an unverified large-value descriptor");
    const inherited =
      (await this.currentPhysicalCellsInBranchSchema(
        schemaVersion,
        commit.table,
        commit.branch,
        commit.rowUuid,
      )) ?? new Map<string, Value>();
    for (const [column, value] of commit.cellsMap) {
      if (!valueContainsIndirectDescriptor(value)) continue;
      // Only `verifiedInheritedLargeCells` can pre-mark a cell; others must equal the observed physical cell.
      const prior = inherited.get(column);
      if (
        !(prior !== undefined && valueEquals(prior, value)) &&
        !commit.preparedLargeColumns.has(column)
      )
        throw invalidCommit("row update contains an unverified large-value descriptor");
      commit.preparedLargeColumns.add(column);
    }
    return commit;
  },
  async localCurrentRowInSchema(
    this: NodeState,
    table: string,
    rowUuid: RowUuid,
    schemaVersion: SchemaVersionId,
  ) {
    const tableSchema = this.tableInSchema(table, schemaVersion);
    const content = await this.localCurrentContentRowCandidate(tableSchema, rowUuid, schemaVersion);
    if (!content || content[2]) return undefined;
    const row = this.materializeCurrentRow(tableSchema, content[0]);
    // Exact local reads bypass the query terminal's publication binder.
    this.bindCurrentRowColumnsInSchema(schemaVersion, row);
    return row;
  },
  async localCurrentContentRowCandidate(
    this: NodeState,
    table: any,
    rowUuid: RowUuid,
    schemaVersion: SchemaVersionId,
  ): Promise<[any, [TxTime, any], boolean] | undefined> {
    const prefix = [LiteralValue.from(V("Uuid", rowUuid))];
    // The row's `[local, origin]` stack, both refs read at the row's key.
    const layers = tryRefLayers("Local", (r) => {
      const scan = r === "Local" ? StaticScanSpec.Prefix(prefix) : StaticScanSpec.Point(prefix);
      return new Layer(
        this.physicalCurrentSourceScanGraph(schemaVersion, table.name, refPhysicalClass(r), scan),
      );
    });
    let result: any;
    try {
      result = await this.database.queryGraph(compose(layers));
    } catch (e) {
      throw malformedCurrentQueryError(table.name, rowUuid, e);
    }
    const delta = result.deltas.find((d: any) => d.weight > 0);
    if (!delta) return undefined;
    const record = BorrowedRecord.new(delta.record, result.descriptor);
    const field = result.descriptor.fieldIndex("_deletion");
    let deleted = false;
    if (field !== undefined) {
      let tag: any;
      try {
        tag = record.getNullableEnum(field);
      } catch (e) {
        throw malformedCurrentQueryError(table.name, rowUuid, {
          kind: "RecordEncoding",
          source: e,
        });
      }
      if (tag !== undefined) {
        try {
          deleted = deletionEventFromValue(V("EnumTag", tag)) === "Deleted";
        } catch {
          deleted = false;
        }
      }
    }
    const tx = this.currentRecordSortKey(table.name, rowUuid, record);
    return [decodeCurrentRow(table, record), tx, deleted];
  },
  currentRecordSortKey(
    this: NodeState,
    table: string,
    rowUuid: RowUuid,
    record: any,
  ): [TxTime, any] {
    const read = (idx: number) => {
      try {
        return record.getU64(idx);
      } catch (source) {
        throw nodeErr("MalformedCurrentRow", { table, rowUuid, source });
      }
    };
    const txTime = read(GlobalCurrentRowRecord.FIELD_TX_TIME_IDX);
    const node = this.nodeAliases.nodeForAlias(read(GlobalCurrentRowRecord.FIELD_TX_NODE_ID_IDX));
    if (node === undefined) throw invalidStored("current row references unknown node alias");
    return [txTime, node];
  },
};

function malformedCurrentQueryError(table: string, rowUuid: RowUuid, error: any) {
  const source =
    error?.kind === "RecordEncoding"
      ? error.source
      : error?.kind === "IvmRuntime" && error.source?.kind === "RecordEncoding"
        ? error.source.source
        : undefined;
  if (source === undefined) return nodeErr("Groove", error);
  return nodeErr("MalformedCurrentRow", { table, rowUuid, source });
}

/**
 * Merge commits addressing the same row of the same schema/branch into one row-image commit, in authoring
 * order: later cells and deletion events win, authored column sets are unioned.
 */
export function coalesceSameRowCommits(commits: SchemaCommit[]): SchemaCommit[] {
  if (commits.length < 2) return commits;
  const byRow = new Map<string, number[]>();
  const merged: SchemaCommit[] = [];
  for (const [schemaVersion, commit] of commits) {
    const rowKey = `${commit.table}\0${toHex(commit.rowUuid)}`;
    let candidates = byRow.get(rowKey);
    if (!candidates) byRow.set(rowKey, (candidates = []));
    const index = candidates.find(
      (i) =>
        uuidEquals(merged[i][0], schemaVersion) &&
        branchSelectorEquals(merged[i][1].branch, commit.branch),
    );
    if (index === undefined) {
      candidates.push(merged.length);
      merged.push([schemaVersion, commit]);
      continue;
    }
    const base = merged[index][1];
    const authored = new Set(base.authoredColumnsSet ?? base.cellsMap.keys());
    for (const c of commit.authoredColumnsSet ?? commit.cellsMap.keys()) authored.add(c);
    base.authoredColumnsSet = authored;
    // Content written after a delete in the same transaction resurrects the row.
    const resurrects = commit.deletionEvent === undefined && base.deletionEvent === "Deleted";
    for (const [c, v] of commit.cellsMap) base.cellsMap.set(c, v);
    base.deletionEvent = resurrects ? "Restored" : (commit.deletionEvent ?? base.deletionEvent);
    base.nowMs = Math.max(base.nowMs, commit.nowMs);
    base.knownFresh &&= commit.knownFresh;
    for (const c of commit.preparedLargeColumns) base.preparedLargeColumns.add(c);
    base.stagedLargeValues.push(...commit.stagedLargeValues);
    if (commit.userMetadataJson !== undefined) base.userMetadataJson = commit.userMetadataJson;
  }
  return merged;
}

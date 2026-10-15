import { KeyedMap, compareBytes, cmpStr } from "../../../support/index.js";
import type {
  NodeState,
  RelationEdgeEntry,
  SchemaVersionId,
  RowUuid,
  RowVersionRefEntry,
  VersionRow,
  TableSchema,
  RecordDeltas,
  CurrentRow,
  Query,
  AppRowSchema,
  ResultMemberEntry,
  ResultMemberPayloadEntry,
  TxId,
  ValidatedQuery,
  ReadViewSpec,
  MultisinkDeltas,
  QueryProgram,
  RelationSnapshot,
  ArraySubquery,
  OutputOccurrenceId,
} from "../../../_extern/node-qe-b.js";
import {
  Err,
  installNodeMethods,
  debugFmt,
  txIdKey,
  decodeCurrentRow,
  currentRowFields,
  currentRowFromMaterializedCells,
  decodeAggregatePayloadRecord,
  decodeCurrentPayloadRecord,
  requiredFieldIdx,
  TxIdNew,
  sortCurrentRows,
  CurrentRowNewWithPublicationFields,
  terminalRootOccurrenceIdWithRootUnion,
  terminalRootPublicationFields,
  relationOutputProjectionIfPresent,
  cmpOutputOccurrenceId,
  SchemaFamilyId,
} from "../../../_extern/node-qe-b.js";
import {
  JAZZ_APP_ROWS_SINK,
  aggregateCurrentRowFromRecord,
  aggregateResultMemberRowUuid,
  publicResultMemberOccurrenceId,
  localMaintainedViewContentWitness,
  currentRowHasRequiredSubscriptionCells,
  sortQueryDefaultRows,
  applyQueryWindow,
  queryOrderValue,
} from "../query_eval.js";
import {
  aggregateOutputSchema,
  aggregateQueryRowUuid,
  aggregateResultTable,
  compareOptionalValues,
  sortAggregateRows,
} from "./query_result_rows.js";
import { materializationAppRowSchema } from "./lowering.js";

export type RelationSnapshotWindow = { offset: number; limit: number | undefined };
type MaterializationCache = { txVersions: KeyedMap<TxId, VersionRow[]> };
export type LocalMaintainedRelationSnapshot = {
  snapshot: RelationSnapshot;
  rootOccurrenceIds: OutputOccurrenceId[];
};

async function projectRelationEdgeThroughReadSchema(
  this: NodeState,
  edge: RelationEdgeEntry,
  readSchema: SchemaVersionId,
) {
  return {
    sourceTable: await this.projectRelationEdgeTableThroughReadSchema(
      edge.sourceTable,
      edge.sourceRow,
      edge.sourceVersion,
      readSchema,
    ),
    sourceRow: edge.sourceRow,
    relation: edge.path,
    targetTable: await this.projectRelationEdgeTableThroughReadSchema(
      edge.targetTable,
      edge.targetRow,
      edge.targetVersion,
      readSchema,
    ),
    targetRow: edge.targetRow,
  };
}

function authoredSchemaFor(node: NodeState, version: VersionRow): SchemaVersionId {
  const authored = node.schemaVersionForAlias(version.schemaVersionAlias());
  if (authored === undefined)
    throw Err.InvalidStoredValue("relation edge witness schema version alias must exist");
  return authored;
}

async function projectRelationEdgeTableThroughReadSchema(
  this: NodeState,
  canonicalTable: string,
  rowUuid: RowUuid,
  versionRef: RowVersionRefEntry | undefined,
  readSchema: SchemaVersionId,
): Promise<string> {
  if (!versionRef) {
    this.tableInSchemaRef(canonicalTable, readSchema);
    return canonicalTable;
  }
  const version = await this.resolveRelationEdgeVersion(canonicalTable, rowUuid, versionRef);
  const projected = this.translateCells(
    authoredSchemaFor(this, version),
    readSchema,
    version.table(),
    new Map(),
  );
  if (projected === undefined)
    throw Err.InvalidStoredValue("relation edge witness does not project into the read schema");
  return projected;
}

async function resolveRelationEdgeVersion(
  this: NodeState,
  canonicalTable: string,
  rowUuid: RowUuid,
  versionRef: RowVersionRefEntry,
): Promise<VersionRow> {
  const alias = this.nodeAliases.get(versionRef.tx.node);
  if (alias === undefined) throw Err.MissingTransaction(versionRef.tx);
  const version = await this.queryVersionByAlias(
    canonicalTable,
    rowUuid,
    versionRef.tx.time,
    alias,
  );
  if (version === undefined) throw Err.MissingTransaction(versionRef.tx);
  return version;
}

async function resolveRelationTerminalVersion(
  this: NodeState,
  emittedTable: string,
  rowUuid: RowUuid,
  versionRef: RowVersionRefEntry,
  readSchema: SchemaVersionId,
): Promise<VersionRow> {
  const matching: VersionRow[] = [];
  for (const version of await this.queryVersionsForTx(versionRef.tx)) {
    if (compareBytes(version.rowUuid(), rowUuid) !== 0) continue;
    if (
      this.translateCells(
        authoredSchemaFor(this, version),
        readSchema,
        version.table(),
        new Map(),
      ) === emittedTable
    )
      matching.push(version);
  }
  if (matching.length === 1) return matching[0];
  throw Err.InvalidStoredValue(
    matching.length
      ? "relation edge terminal witness is ambiguous after projection"
      : "relation edge terminal witness does not project to its emitted table",
  );
}

function relationEdgeBranchId(versionRef: RowVersionRefEntry): any {
  const bytes = versionRef.branchOrPrefix;
  if (bytes === undefined) return undefined;
  if (bytes.length !== 16)
    throw Err.InvalidStoredValue("relation edge branch discriminator must be a UUID");
  return SchemaFamilyId.fromBytes(bytes);
}

function decodePositiveRows(
  node: NodeState,
  table: TableSchema,
  deltas: RecordDeltas,
): CurrentRow[] {
  const rows: CurrentRow[] = [];
  for (const [record, weight] of deltas)
    if (weight > 0) rows.push(node.materializeCurrentRow(table, decodeCurrentRow(table, record)));
  return rows;
}

function materializeHistoricalQueryRows(
  this: NodeState,
  table: TableSchema,
  deltas: RecordDeltas,
): CurrentRow[] {
  return decodePositiveRows(this, table, deltas);
}

function materializeIncludeDeletedQueryRows(
  this: NodeState,
  table: TableSchema,
  deltas: RecordDeltas,
): CurrentRow[] {
  const deletedIdx = currentRowFields(table).length;
  const rows: CurrentRow[] = [];
  for (const [record, weight] of deltas) {
    if (weight <= 0) continue;
    const deleted = record.getBool(deletedIdx);
    const row = this.materializeCurrentRow(table, decodeCurrentRow(table, record));
    rows.push(deleted ? row.intoDeleted() : row);
  }
  return rows;
}

function materializeInlineCurrentQueryRows(
  this: NodeState,
  table: TableSchema,
  deltas: RecordDeltas,
): CurrentRow[] {
  return decodePositiveRows(this, table, deltas);
}

function materializeAggregateQueryRows(
  this: NodeState,
  query: Query,
  output: AppRowSchema,
  deltas: RecordDeltas,
): CurrentRow[] {
  const rows: CurrentRow[] = [];
  for (const [record, weight] of deltas)
    if (weight > 0)
      rows.push(
        aggregateCurrentRowFromRecord(query, output, aggregateQueryRowUuid(output, record), record),
      );
  return rows;
}

async function preloadLocalMaintainedMaterializationCache(
  this: NodeState,
  local: any,
): Promise<MaterializationCache> {
  const cache: MaterializationCache = { txVersions: new KeyedMap(txIdKey) };
  if (local.maintained.usesStorageBackedResultMaterialization()) return cache;
  const txIds = new KeyedMap<TxId, TxId>(txIdKey);
  const note = (tx: TxId) => {
    txIds.set(tx, tx);
    cache.txVersions.getOrInsert(tx, () => local.maintained.versionsByTx(tx));
  };
  for (const member of local.resultSet) {
    const row = member.asRow();
    if (row) note(row[2]);
  }
  for (const fact of local.programFacts)
    if (fact.kind === "RelationEdge" && fact.edge.targetVersion) note(fact.edge.targetVersion.tx);
  await this.preloadTxVersionsForMaterialization([...txIds.values()], cache.txVersions);
  return cache;
}

async function materializeLocalMaintainedViewRelationEdgeRowWithCache(
  this: NodeState,
  local: any,
  tableName: string,
  rowUuid: RowUuid,
  txId: TxId,
  cache: MaterializationCache,
): Promise<CurrentRow | undefined> {
  const version = localMaintainedViewContentWitness(
    this.localMaintainedTxVersions(local, txId, cache),
    tableName,
    rowUuid,
  );
  return version
    ? this.projectedCurrentRowFromMaterializedVersionInReadSchema(
        local.resultSchemaVersion,
        version,
      )
    : undefined;
}

async function materializeLocalMaintainedViewResultMember(
  this: NodeState,
  local: any,
  member: ResultMemberEntry,
): Promise<CurrentRow | undefined> {
  let row = await this.materializeLocalMaintainedViewResultMemberUnbound(local, member);
  if (row) {
    const columns = local.relationProjectionForMember(member);
    if (columns) {
      row = row.projectRelation(this.table(local.resultTable), columns);
      this.bindAppRowSchemaFields(row, local.terminalSchemas.directAppRowSchema());
    }
    this.bindCurrentRowColumnsInSchema(local.resultSchemaVersion, row);
  }
  return row;
}

function aggregateMember(node: NodeState, local: any, member: ResultMemberEntry): CurrentRow {
  const payload = local.resultPayloads.get(member);
  if (!payload) throw Err.InvalidStoredValue("aggregate result member is missing its payload");
  return node.currentRowFromAggregateResultPayload(
    local.resultSchemaVersion,
    local.resultQuery,
    local.terminalSchemas.aggregateAppRowSchema(),
    member,
    payload,
  );
}

function flatJoinPayload(local: any, member: ResultMemberEntry): ResultMemberPayloadEntry {
  const payload = local.resultPayloads.get(member);
  if (!payload)
    throw Err.InvalidStoredValue("flat joined result member is missing its tuple payload");
  return payload;
}

const nonRowMember = () =>
  Err.InvalidStoredValue(
    "local maintained subscription cannot materialize non-row result member yet",
  );

async function exactWitness(
  node: NodeState,
  local: any,
  entry: [string, RowUuid, TxId],
): Promise<VersionRow | undefined> {
  const versions = local.maintained.usesStorageBackedResultMaterialization()
    ? await node.storageBackedMaintainedResultVersions(entry[0], entry[1], entry[2])
    : await node.queryVersionsForTx(entry[2]);
  return node.maintainedWitnessForResultMember(
    versions,
    local.resultSchemaVersion,
    local.resultTable,
    entry[1],
  );
}

async function materializeLocalMaintainedViewResultMemberUnbound(
  this: NodeState,
  local: any,
  member: ResultMemberEntry,
): Promise<CurrentRow | undefined> {
  if (member.kind === "Synthetic" && local.resultQuery.aggregate !== undefined)
    return aggregateMember(this, local, member);
  const entry = member.asRow();
  if (!entry) throw nonRowMember();
  const table = this.table(local.resultTable);
  if (local.resultQuery.flatJoin !== undefined)
    return this.currentRowFromResultPayload(
      table,
      flatJoinPayload(local, member),
      local.terminalSchemas.currentPayloadSchema(),
    );
  if (local.resultSelect !== undefined) {
    const payload = local.resultPayloads.get(member);
    if (payload)
      return this.currentRowFromResultPayload(
        table,
        payload,
        local.terminalSchemas.currentPayloadSchema(),
      ).project(table, local.resultSelect);
  }
  const txVersions: VersionRow[] = local.maintained.versionsByTx(entry[2]);
  let version = this.maintainedWitnessForResultMember(
    txVersions,
    local.resultSchemaVersion,
    local.resultTable,
    entry[1],
  );
  if (!version) {
    const [contentWinner] = local.maintained.replacementFor(entry[0], entry[1]);
    if (contentWinner) {
      if (txIdKey(this.versionTxId(contentWinner)) !== txIdKey(entry[2])) return undefined;
      version = contentWinner;
    } else {
      version = await exactWitness(this, local, entry);
      if (!version) return undefined;
    }
  }
  let row = this.projectedCurrentRowFromMaterializedVersionInReadSchema(
    local.resultSchemaVersion,
    version,
  );
  if (!row)
    throw Err.InvalidStoredValue("maintained result witness does not project into the read schema");
  if (local.resultSelect !== undefined) row = row.project(table, local.resultSelect);
  return row;
}

async function materializeLocalMaintainedViewResultMemberWithCache(
  this: NodeState,
  local: any,
  member: ResultMemberEntry,
  cache: MaterializationCache,
): Promise<CurrentRow | undefined> {
  if (member.kind === "Synthetic" && local.resultQuery.aggregate !== undefined)
    return aggregateMember(this, local, member);
  const entry = member.asRow();
  if (!entry) throw nonRowMember();
  const table = this.table(local.resultTable);
  if (local.resultQuery.flatJoin !== undefined) {
    let row = this.currentRowFromResultPayload(
      table,
      flatJoinPayload(local, member),
      local.terminalSchemas.currentPayloadSchema(),
    );
    const columns = local.relationProjectionForMember(member);
    if (columns) {
      row = row.projectRelation(table, columns);
      this.bindAppRowSchemaFields(row, local.terminalSchemas.directAppRowSchema());
    }
    return row;
  }
  let version = this.maintainedWitnessForResultMember(
    this.localMaintainedTxVersions(local, entry[2], cache),
    local.resultSchemaVersion,
    local.resultTable,
    entry[1],
  );
  if (!version) {
    version = await exactWitness(this, local, entry);
    if (!version) return undefined;
  }
  return this.projectedCurrentRowFromMaterializedVersionInReadSchema(
    local.resultSchemaVersion,
    version,
  );
}

function localMaintainedTxVersions(
  this: NodeState,
  local: any,
  txId: TxId,
  cache: MaterializationCache,
): VersionRow[] {
  return cache.txVersions.getOrInsert(txId, () => local.maintained.versionsByTx(txId));
}

async function storageBackedMaintainedResultVersions(
  this: NodeState,
  table: string,
  rowUuid: RowUuid,
  txId: TxId,
): Promise<VersionRow[]> {
  const stored = await this.queryTransaction(txId);
  if (!stored) throw Err.MissingTransaction(txId);
  return this.queryVersionsForTxRowsByAlias(txId, stored.nodeAlias, [[table, rowUuid]]);
}

async function preloadTxVersionsForMaterialization(
  this: NodeState,
  txIds: TxId[],
  cache: KeyedMap<TxId, VersionRow[]>,
): Promise<void> {
  for (const txId of txIds) {
    if (cache.get(txId)?.length) continue;
    const cached = this.cachedTxVersions(txId);
    if (cached) {
      cache.set(txId, cached);
      continue;
    }
    if (this.nodeAliases.get(txId.node) === undefined) continue;
    cache.set(txId, await this.queryVersionsForTx(txId));
  }
}

function projectedCurrentRowFromMaterializedVersionInReadSchema(
  this: NodeState,
  readSchema: SchemaVersionId,
  version: VersionRow,
): CurrentRow | undefined {
  const authoredSchema = authoredSchemaFor(this, version);
  const authoredTable = this.tableInSchemaRef(version.table(), authoredSchema);
  const cells = this.materializedCellsForVersion(authoredTable, version);
  const projectedTable = this.translateCells(authoredSchema, readSchema, version.table(), cells);
  if (projectedTable === undefined) return undefined;
  const row = currentRowFromMaterializedCells(
    this.tableInSchemaRef(projectedTable, readSchema),
    version,
    cells,
  );
  this.bindCurrentRowColumnsInSchema(readSchema, row);
  return row;
}

function currentRowFromAggregateResultPayload(
  this: NodeState,
  readSchema: SchemaVersionId,
  query: Query,
  output: AppRowSchema,
  member: ResultMemberEntry,
  payload: ResultMemberPayloadEntry,
): CurrentRow {
  const decoded = decodeAggregatePayloadRecord(payload, aggregateOutputSchema(output));
  const row = aggregateCurrentRowFromRecord(
    query,
    output,
    aggregateResultMemberRowUuid(member),
    decoded.borrowed(),
  );
  this.bindCurrentRowColumnsInSchema(readSchema, row);
  return row;
}

function currentRowFromResultPayload(
  this: NodeState,
  table: TableSchema,
  payload: ResultMemberPayloadEntry,
  schema: any,
): CurrentRow {
  return decodeCurrentPayloadRecord(table.name, payload, schema);
}

function branchDiscriminator(record: any, idx: number | undefined): Uint8Array | undefined {
  if (idx === undefined) return undefined;
  let value = record.getIdx(idx);
  if (value.kind === "Nullable") {
    if (value.value == null) return undefined;
    value = value.value;
  }
  if (value.kind === "Uuid" || value.kind === "Bytes") return value.value;
  throw Err.InvalidStoredValue("relation edge branch discriminator must be UUID or bytes");
}

async function materializeRelationSnapshotFromQueryEngine(
  this: NodeState,
  shape: ValidatedQuery,
  readView: ReadViewSpec,
  snapshots: MultisinkDeltas,
  program: QueryProgram,
): Promise<RelationSnapshot> {
  const rootRows = this.materializeRelationSnapshotRootRows(shape, snapshots, program);
  const snapshot: RelationSnapshot = { rootCount: rootRows.length, rows: rootRows, edges: [] };
  if (shape.query().arraySubqueries.length || shape.query().aggregate !== undefined)
    return snapshot;
  const rowKey = (table: string, row: RowUuid) => `${table}|${debugFmt(row)}`;
  const rowKeys = new Set(snapshot.rows.map((r) => rowKey(r.table(), r.rowUuid())));
  const edges = snapshots.get("maintained.relation_edges");
  if (!edges) return snapshot;
  const windows = this.relationSnapshotNoOrderWindows(shape.query().arraySubqueries);
  const descriptor = edges.descriptor;
  const idx = (name: string) => requiredFieldIdx(descriptor, name);
  const optIdx = (name: string) => {
    const i = descriptor.fields().findIndex((f: any) => f.name === name);
    return i < 0 ? undefined : i;
  };
  const sourceTableIdx = idx("source_table");
  const sourceRowIdx = idx("source_row");
  const sourceTxTimeIdx = idx("source_tx_time");
  const sourceTxNodeIdx = idx("source_tx_node_id");
  const sourceBranchIdx = optIdx("source_branch_or_prefix");
  const relationIdx = idx("path");
  const targetTableIdx = idx("target_table");
  const targetRowIdx = idx("target_row");
  const targetTxTimeIdx = idx("target_tx_time");
  const targetTxNodeIdx = idx("target_tx_node_id");
  const targetBranchIdx = optIdx("target_branch_or_prefix");
  const versionRef = (
    time: number,
    alias: number,
    branchOrPrefix: Uint8Array | undefined,
  ): RowVersionRefEntry => {
    const node = this.nodeAliases.nodeForAlias(alias);
    if (node === undefined) throw Err.InvalidStoredValue("relation edge node alias is missing");
    return {
      tx: TxIdNew(time, node),
      schemaVersion: undefined,
      layer: "Content",
      batch: undefined,
      branchOrPrefix,
      rowDigest: undefined,
    };
  };
  const candidates: any[] = [];
  for (const [record, weight] of edges) {
    if (weight <= 0) continue;
    const sourceTable = record.getStr(sourceTableIdx);
    const sourceRow = record.getUuid(sourceRowIdx);
    const relation = record.getStr(relationIdx);
    const targetTableName = record.getStr(targetTableIdx);
    const targetRow = record.getUuid(targetRowIdx);
    const targetTxTime = record.getU64(targetTxTimeIdx);
    const targetTxNode = record.getU64(targetTxNodeIdx);
    const sourceVersion = versionRef(
      record.getU64(sourceTxTimeIdx),
      record.getU64(sourceTxNodeIdx),
      branchDiscriminator(record, sourceBranchIdx),
    );
    const targetVersion = versionRef(
      targetTxTime,
      targetTxNode,
      branchDiscriminator(record, targetBranchIdx),
    );
    const canonicalSource = await this.resolveRelationTerminalVersion(
      sourceTable,
      sourceRow,
      sourceVersion,
      shape.schemaVersion(),
    );
    const canonicalTarget = await this.resolveRelationTerminalVersion(
      targetTableName,
      targetRow,
      targetVersion,
      shape.schemaVersion(),
    );
    candidates.push({
      edge: {
        sourceTable: await this.projectRelationEdgeTableThroughReadSchema(
          canonicalSource.table(),
          sourceRow,
          sourceVersion,
          shape.schemaVersion(),
        ),
        sourceRow,
        relation,
        targetTable: await this.projectRelationEdgeTableThroughReadSchema(
          canonicalTarget.table(),
          targetRow,
          targetVersion,
          shape.schemaVersion(),
        ),
        targetRow,
      },
      canonicalTargetTable: canonicalTarget.table(),
      targetTxTime,
      targetTxNode,
    });
  }
  candidates.sort(
    (a, b) =>
      cmpStr(a.edge.sourceTable, b.edge.sourceTable) ||
      compareBytes(a.edge.sourceRow, b.edge.sourceRow) ||
      cmpStr(a.edge.relation, b.edge.relation) ||
      compareBytes(a.edge.targetRow, b.edge.targetRow),
  );
  const counts = new Map<string, number>();
  for (const candidate of candidates) {
    const group = `${candidate.edge.sourceTable}|${debugFmt(candidate.edge.sourceRow)}|${candidate.edge.relation}`;
    const ordinal = counts.get(group) ?? 0;
    counts.set(group, ordinal + 1);
    const window = windows.get(candidate.edge.relation);
    if (
      window &&
      (ordinal < window.offset ||
        (window.limit !== undefined && ordinal >= window.offset + window.limit))
    )
      continue;
    const key = rowKey(candidate.edge.targetTable, candidate.edge.targetRow);
    if (!rowKeys.has(key)) {
      rowKeys.add(key);
      snapshot.rows.push(
        await this.materializeRelationEdgeTargetRow(
          readView,
          shape.schemaVersion(),
          candidate.canonicalTargetTable,
          candidate.edge.targetRow,
          candidate.targetTxTime,
          candidate.targetTxNode,
        ),
      );
    }
    snapshot.edges.push(candidate.edge);
  }
  return snapshot;
}

async function materializeRelationEdgeTargetRow(
  this: NodeState,
  _readView: ReadViewSpec,
  readSchema: SchemaVersionId,
  targetTableName: string,
  targetRow: RowUuid,
  targetTxTime: number,
  targetTxNode: number,
): Promise<CurrentRow> {
  const version = await this.queryVersionByAlias(
    targetTableName,
    targetRow,
    targetTxTime,
    targetTxNode,
  );
  if (!version) throw Err.InvalidStoredValue("relation edge target version is missing");
  const row = this.projectedCurrentRowFromMaterializedVersionInReadSchema(readSchema, version);
  if (!row)
    throw Err.InvalidStoredValue(
      "relation edge target version does not project into the read schema",
    );
  return row;
}

function relationSnapshotNoOrderWindows(
  this: NodeState,
  subqueries: ArraySubquery[],
): Map<string, RelationSnapshotWindow> {
  const windows = new Map<string, RelationSnapshotWindow>();
  for (const subquery of subqueries) {
    if (!subquery.orderBy.length && (subquery.limit !== undefined || subquery.offset !== 0))
      windows.set(subquery.columnName, { offset: subquery.offset, limit: subquery.limit });
    for (const [k, v] of this.relationSnapshotNoOrderWindows(subquery.nestedArrays))
      windows.set(k, v);
  }
  return windows;
}

function materializeRelationSnapshotRootRows(
  this: NodeState,
  shape: ValidatedQuery,
  snapshots: MultisinkDeltas,
  program: QueryProgram,
): CurrentRow[] {
  const appRows = snapshots.get(JAZZ_APP_ROWS_SINK);
  if (!appRows) throw Err.QueryLowering("relation snapshot program did not emit app rows");
  if (shape.query().aggregate !== undefined) {
    const rows = this.materializeAggregateQueryRows(
      shape.query(),
      materializationAppRowSchema(undefined, program),
      appRows,
    );
    this.finishEngineQueryRowsInSchema(shape.query(), shape.schemaVersion(), rows);
    return rows;
  }
  const table = this.tableInSchemaRef(shape.query().table, shape.schemaVersion());
  const rows = decodePositiveRows(this, table, appRows);
  for (const row of rows) this.bindCompiledAppRowFields(row, program);
  if (shape.query().relation === undefined)
    this.applyQueryOrderInSchema(shape.query(), shape.schemaVersion(), rows);
  return rows;
}

function bindCompiledAppRowFields(this: NodeState, row: CurrentRow, program: QueryProgram): void {
  const schema = program.lowered.output.terminals.find((o: any) => o.kind === "AppRows");
  if (!schema) throw Err.InvalidStoredValue("compiled query has no application row schema");
  this.bindAppRowSchemaFields(row, schema.rows);
}

function bindAppRowSchemaFields(this: NodeState, row: CurrentRow, schema: AppRowSchema): void {
  row.publicationFields = row.record
    .descriptor()
    .fields()
    .map((field: any) => {
      const name = field.name;
      if (name === undefined) throw Err.InvalidStoredValue("application row field is unnamed");
      return (
        schema.publicationFields.get(name) ?? {
          kind: "ResultField",
          name,
          visibility: "HiddenMetadata",
        }
      );
    });
}

function bindCurrentRowColumnsInSchema(
  this: NodeState,
  schemaVersion: SchemaVersionId,
  row: CurrentRow,
): void {
  if (!row.publicationFields.some((f: any) => f.kind === "UnresolvedSourceCell")) return;
  const mapping = this.catalogue.physicalMappings.get(schemaVersion)?.tables.get(row.table());
  if (!mapping) throw Err.InvalidStoredValue("native row has no read-schema physical mapping");
  row.publicationFields = row.publicationFields.map((field: any) => {
    if (field.kind !== "UnresolvedSourceCell") return field;
    const id = mapping.columns.get(field.outputName);
    if (id === undefined)
      throw Err.InvalidStoredValue("source application cell has no catalogue column identity");
    return { kind: "StoredColumn", id, outputName: field.outputName };
  });
}

function finishEngineQueryRowsInSchema(
  this: NodeState,
  query: Query,
  schemaVersion: SchemaVersionId,
  rows: CurrentRow[],
): void {
  for (const row of rows) this.bindCurrentRowColumnsInSchema(schemaVersion, row);
  if (query.aggregate !== undefined) {
    this.applyQueryOrderInSchema(query, schemaVersion, rows);
    const windowed = applyQueryWindow(query, rows);
    rows.splice(0, rows.length, ...windowed);
    return;
  }
  if (query.relation !== undefined) return;
  this.applyQueryOrderInSchema(query, schemaVersion, rows);
}

function queryOutputTable(
  this: NodeState,
  query: Query,
  schemaVersion: SchemaVersionId,
): TableSchema {
  const source = this.tableInSchema(query.table, schemaVersion);
  return query.aggregate !== undefined ? aggregateResultTable(query, source) : source;
}

function orderedComparator(query: Query, table: TableSchema) {
  return (left: CurrentRow, right: CurrentRow) => {
    for (const order of query.orderBy) {
      let o = compareOptionalValues(
        queryOrderValue(left, table, order.column),
        queryOrderValue(right, table, order.column),
      );
      if (order.direction === "Desc") o = -o;
      if (o) return o;
    }
    return 0;
  };
}

function sortQueryRowsWithOccurrences(
  this: NodeState,
  query: Query,
  table: TableSchema | undefined,
  rows: CurrentRow[],
  occurrenceIds: OutputOccurrenceId[],
): void {
  if (rows.length !== occurrenceIds.length)
    throw Err.InvalidStoredValue("maintained root occurrence sidecar length does not match rows");
  const paired: [CurrentRow, OutputOccurrenceId][] = rows.map((r, i) => [r, occurrenceIds[i]]);
  const tail = (a: [CurrentRow, OutputOccurrenceId], b: [CurrentRow, OutputOccurrenceId]) =>
    compareBytes(a[0].rowUuid(), b[0].rowUuid()) ||
    compareBytes(a[0].record.raw(), b[0].record.raw()) ||
    cmpOutputOccurrenceId(a[1], b[1]);
  if (!query.orderBy.length) {
    const rowOrder = (a: CurrentRow, b: CurrentRow) =>
      compareBytes(a.rowUuid(), b.rowUuid()) ||
      cmpStr(debugFmt(a.projectedTxAlias()), debugFmt(b.projectedTxAlias())) ||
      compareBytes(a.record.raw(), b.record.raw());
    paired.sort((a, b) => rowOrder(a[0], b[0]) || cmpOutputOccurrenceId(a[1], b[1]));
  } else if (query.aggregate !== undefined) sortAggregateRows(query, paired, (p) => p[0], tail);
  else {
    if (!table)
      throw Err.InvalidStoredValue("ordered maintained rows are missing their table schema");
    const cmp = orderedComparator(query, table);
    paired.sort((a, b) => cmp(a[0], b[0]) || tail(a, b));
  }
  paired.forEach(([row, occurrence], i) => {
    rows[i] = row;
    occurrenceIds[i] = occurrence;
  });
}

export function applyAggregateWindowWithOccurrences(
  query: Query,
  rows: CurrentRow[],
  occurrenceIds: OutputOccurrenceId[],
): void {
  if (rows.length !== occurrenceIds.length)
    throw Err.InvalidStoredValue(
      "maintained aggregate occurrence sidecar length does not match rows",
    );
  const offset = Math.min(query.offset, rows.length);
  const end = Math.min(offset + (query.limit ?? Math.max(rows.length - offset, 0)), rows.length);
  if (offset > 0 || end < rows.length) {
    rows.splice(end);
    occurrenceIds.splice(end);
    rows.splice(0, offset);
    occurrenceIds.splice(0, offset);
  }
}

function applyQueryOrderInSchema(
  this: NodeState,
  query: Query,
  schemaVersion: SchemaVersionId,
  rows: CurrentRow[],
): void {
  if (!query.orderBy.length) return sortQueryDefaultRows(rows);
  sortCurrentRows(rows);
  if (query.aggregate !== undefined)
    return sortAggregateRows(
      query,
      rows,
      (r) => r,
      (a, b) => compareBytes(a.rowUuid(), b.rowUuid()),
    );
  const cmp = orderedComparator(query, this.tableInSchemaRef(query.table, schemaVersion));
  rows.sort((a, b) => cmp(a, b) || compareBytes(a.rowUuid(), b.rowUuid()));
}

function applyProjectionInSchema(
  this: NodeState,
  query: Query,
  schemaVersion: SchemaVersionId,
  rows: CurrentRow[],
): void {
  const columns = query.select;
  if (columns === undefined) return;
  const table = this.tableInSchemaRef(query.table, schemaVersion);
  for (let i = 0; i < rows.length; i++) rows[i] = rows[i].project(table, columns);
}

function relationSnapshotHasMaterializedRequiredCells(
  this: NodeState,
  shape: ValidatedQuery,
  snapshot: RelationSnapshot,
): boolean {
  const query = shape.query();
  if (query.aggregate !== undefined || query.flatJoin !== undefined) return true;
  const relationProjection = query.relation
    ? relationOutputProjectionIfPresent(query.relation)
    : undefined;
  for (let index = 0; index < snapshot.rows.length; index++) {
    const row = snapshot.rows[index];
    const isRoot = index < snapshot.rootCount && row.table() === query.table;
    if (
      isRoot &&
      relationProjection &&
      relationProjection.every((c: any) => row.rawField(c.alias) !== undefined)
    )
      continue;
    const table = this.tableInSchemaRef(row.table(), shape.schemaVersion());
    if (!currentRowHasRequiredSubscriptionCells(row, table, isRoot ? query.select : undefined))
      return false;
  }
  return true;
}

async function materializeLocalMaintainedRelationSnapshotWithOccurrences(
  this: NodeState,
  local: any,
): Promise<LocalMaintainedRelationSnapshot> {
  if (local.terminalSchemas.hasRootCollector()) {
    const rootOccurrenceIds: OutputOccurrenceId[] = [];
    const rows: CurrentRow[] = [];
    const layout = local.terminalRootLayout();
    for (const [terminalKey, record] of local.maintained.structuredAppRowsByTerminalKey()) {
      let occurrence;
      try {
        occurrence = terminalRootOccurrenceIdWithRootUnion(
          terminalKey,
          layout?.rootUnionArm ?? false,
        );
      } catch {
        throw Err.InvalidStoredValue("collector terminal key cannot identify its root occurrence");
      }
      if (!layout) throw Err.InvalidStoredValue("collector terminal has no root layout");
      rootOccurrenceIds.push(occurrence);
      rows.push(
        CurrentRowNewWithPublicationFields(
          local.resultTable,
          record,
          terminalRootPublicationFields(layout),
        ),
      );
    }
    return { snapshot: { rootCount: rows.length, rows, edges: [] }, rootOccurrenceIds };
  }
  const cache = await this.preloadLocalMaintainedMaterializationCache(local);
  const rows: CurrentRow[] = [];
  const rootOccurrenceIds: OutputOccurrenceId[] = [];
  const rowKeys = new Set<string>();
  const aggregateQuery = local.resultQuery.aggregate !== undefined;
  for (const member of local.resultSet) {
    const row = await this.materializeLocalMaintainedViewResultMemberWithCache(
      local,
      member,
      cache,
    );
    if (!row) continue;
    const occurrence = publicResultMemberOccurrenceId(member, local.resultTable, aggregateQuery);
    if (occurrence === undefined)
      throw Err.InvalidStoredValue("maintained root member has no occurrence identity");
    rowKeys.add(`${row.table()}|${debugFmt(row.rowUuid())}`);
    rows.push(row);
    rootOccurrenceIds.push(occurrence);
  }
  this.applyQueryOrderWithOccurrences(local.resultQuery, rows, rootOccurrenceIds);
  if (aggregateQuery)
    applyAggregateWindowWithOccurrences(local.resultQuery, rows, rootOccurrenceIds);
  if (local.resultRelationProjections !== undefined) {
    const table = this.table(local.resultTable);
    const schema = local.terminalSchemas.directAppRowSchema();
    rows.forEach((row, i) => {
      const columns = local.relationProjectionForOccurrence(rootOccurrenceIds[i]);
      if (columns) {
        rows[i] = row.projectRelation(table, columns);
        this.bindAppRowSchemaFields(rows[i], schema);
      }
    });
  } else if (local.resultRelationProjection !== undefined) {
    const table = this.table(local.resultTable);
    const schema = local.terminalSchemas.directAppRowSchema();
    rows.forEach((row, i) => {
      rows[i] = row.projectRelation(table, local.resultRelationProjection);
      this.bindAppRowSchemaFields(rows[i], schema);
    });
  }
  this.applyProjection(local.resultQuery, rows);
  const rootCount = rows.length;
  const edges: any[] = [];
  for (const fact of local.programFacts) {
    if (fact.kind !== "RelationEdge") continue;
    const edge = fact.edge;
    const readEdge = await this.projectRelationEdgeThroughReadSchema(
      edge,
      local.resultSchemaVersion,
    );
    const key = `${readEdge.targetTable}|${debugFmt(readEdge.targetRow)}`;
    if (!rowKeys.has(key)) {
      rowKeys.add(key);
      if (edge.targetVersion) {
        const row = await this.materializeLocalMaintainedViewRelationEdgeRowWithCache(
          local,
          edge.targetTable,
          edge.targetRow,
          edge.targetVersion.tx,
          cache,
        );
        if (row) rows.push(row);
      }
    }
    edges.push(readEdge);
  }
  return { snapshot: { rootCount, rows, edges }, rootOccurrenceIds };
}

installNodeMethods({
  projectRelationEdgeThroughReadSchema,
  projectRelationEdgeTableThroughReadSchema,
  resolveRelationEdgeVersion,
  resolveRelationTerminalVersion,
  relationEdgeBranchId,
  materializeHistoricalQueryRows,
  materializeIncludeDeletedQueryRows,
  materializeInlineCurrentQueryRows,
  materializeAggregateQueryRows,
  preloadLocalMaintainedMaterializationCache,
  materializeLocalMaintainedViewRelationEdgeRowWithCache,
  materializeLocalMaintainedViewResultMember,
  materializeLocalMaintainedViewResultMemberUnbound,
  materializeLocalMaintainedViewResultMemberWithCache,
  localMaintainedTxVersions,
  storageBackedMaintainedResultVersions,
  preloadTxVersionsForMaterialization,
  projectedCurrentRowFromMaterializedVersionInReadSchema,
  currentRowFromAggregateResultPayload,
  currentRowFromResultPayload,
  materializeRelationSnapshotFromQueryEngine,
  materializeRelationEdgeTargetRow,
  relationSnapshotNoOrderWindows,
  materializeRelationSnapshotRootRows,
  bindCompiledAppRowFields,
  bindAppRowSchemaFields,
  bindCurrentRowColumnsInSchema,
  finishEngineQueryRowsInSchema,
  queryOutputTable,
  sortQueryRowsWithOccurrences,
  applyAggregateWindowWithOccurrences,
  applyQueryOrderInSchema,
  applyProjectionInSchema,
  relationSnapshotHasMaterializedRequiredCells,
  materializeLocalMaintainedRelationSnapshotWithOccurrences,
});

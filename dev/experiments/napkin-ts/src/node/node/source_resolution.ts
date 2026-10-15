// Snapshot projection used by query-engine source lowering. Restricted to
// frozen historical, transaction-overlay and other snapshot-valued inputs.
import { SortedMap, SortedSet, cmpStr } from "../../support/collections.js";
import {
  BranchColumnValue,
  applyCompiledLensPath,
  branchViewStack,
  cmpGlobalTime,
  cmpSortKey,
  currentRowFromMaterializedCellsWithLayerProvenance,
  deepEqual,
  durabilityAtLeast,
  isUnrepresentableEnumProjection,
  isNodeError,
  nodeError,
  nodeInvalidStoredValue,
  sortCurrentRows,
  topWins,
  txIdEquals,
} from "../../_extern/node-policy-peer.js";

type VersionRow = any;
type RowMap = SortedMap<string, VersionRow>;
const rowMap = (): RowMap => new SortedMap<string, VersionRow>(cmpStr);
const invalidBranchKey = (m: string) => nodeError("InvalidBranchKey", m);

function projectSelector(schema: any, table: any, selector: any): any {
  const r = schema.projectBranchViewSelector(table, selector);
  if (r.error !== undefined) throw invalidBranchKey(r.error);
  return r.value[0];
}

/** Resolve the exact inherited content version for a first head overlay. */
export async function inheritedBranchViewCopyEvidence(
  node: any,
  table: string,
  headSelector: any,
  baseView: any | undefined,
  rowUuid: string,
): Promise<any> {
  if (!baseView) return undefined;
  const schemaVersion = node.catalogue.activeSchema.schema;
  const entry = node.catalogue.catalogueSchemas.get(schemaVersion);
  if (!entry) throw nodeInvalidStoredValue("current write schema missing");
  const schema = entry.schema;
  const tableSchema = node.tableInSchemaRef(table, schemaVersion);
  const head = projectSelector(schema, tableSchema, headSelector);
  const base =
    baseView.kind === "Current"
      ? { kind: "Current", branch: projectSelector(schema, tableSchema, baseView.selector) }
      : {
          kind: "Snapshot",
          branch: projectSelector(schema, tableSchema, baseView.branch),
          snapshot: baseView.snapshot,
        };
  const [headContent, headDeletions] = await branchWinnersForSchema(
    node,
    table,
    schemaVersion,
    "Local",
    head,
  );
  if (headContent.has(rowUuid) || headDeletions.has(rowUuid)) return undefined;
  const [content, deletions] = await branchWinnersForSchema(
    node,
    table,
    schemaVersion,
    "Local",
    base.branch,
    base.kind === "Snapshot" ? base.snapshot : undefined,
  );
  const source = content.get(rowUuid);
  if (!source) return undefined;
  if (deletions.get(rowUuid)?.deletion() === "Deleted") return undefined;
  return { version: 1, head, base, table, rowUuid, sourceVersion: node.versionTxId(source) };
}

/** Re-resolve non-causal branch-view copy evidence at an authority. */
export async function resolveBranchViewCopyEvidence(
  node: any,
  evidence: any,
  authoredSchema: string,
  candidateTxId: any | undefined,
): Promise<VersionRow | undefined> {
  if (evidence.version !== 1) return undefined;
  try {
    node.tableInSchemaRef(evidence.table, authoredSchema);
  } catch (error) {
    if (isNodeError(error, "TableNotFound")) return undefined;
    throw error;
  }
  const base = evidence.base;
  const snapshot = base.kind === "Snapshot" ? base.snapshot : undefined;
  const [headContent, headDeletions] = await branchWinnersForSchema(
    node,
    evidence.table,
    authoredSchema,
    "Local",
    evidence.head,
  );
  const winners = [headContent.get(evidence.rowUuid), headDeletions.get(evidence.rowUuid)]
    .filter((v) => v !== undefined)
    .map((v) => node.versionTxId(v));
  if (winners.some((w) => candidateTxId === undefined || !txIdEquals(w, candidateTxId)))
    return undefined;
  const [content, deletions] = await branchWinnersForSchema(
    node,
    evidence.table,
    authoredSchema,
    "Local",
    base.branch,
    snapshot,
  );
  const source = content.get(evidence.rowUuid);
  if (!source) return undefined;
  if (
    deletions.get(evidence.rowUuid)?.deletion() === "Deleted" ||
    !txIdEquals(node.versionTxId(source), evidence.sourceVersion)
  )
    return undefined;
  return source;
}

const branchKeyId = (k: any) =>
  JSON.stringify(k.values.map(([n, v]: [string, any]) => [n, v.encoded ?? v]));

/** Every persisted spelling of a logical branch key across the table's schema history. */
export function equivalentStoredBranchKeys(
  node: any,
  table: string,
  readSchemaVersion: string,
  selected: any,
): any[] {
  const tableId = node.physicalTableIdForSchema(readSchemaVersion, table);
  const readSchema = node.catalogue.catalogueSchemas.get(readSchemaVersion);
  if (!readSchema) throw nodeInvalidStoredValue("read schema is missing");
  const readTable = readSchema.schema.tables.find((t: any) => t.name === table);
  if (!readTable) throw nodeError("TableNotFound", table);
  const readMapping = node.catalogue.physicalMappings.get(readSchemaVersion);
  if (!readMapping) throw nodeInvalidStoredValue("read schema physical mapping is missing");
  const readTableMapping = readMapping.tables.get(table);
  if (!readTableMapping) throw nodeError("TableNotFound", table);
  const selectedByName = new SortedMap<string, any>(cmpStr, selected.values);
  const selectedByPhysical = new Map<number, any>();
  for (const columnName of readTable.branchBy) {
    const physical = readTableMapping.columns.get(columnName);
    if (physical === undefined)
      throw nodeInvalidStoredValue("branch column physical mapping is missing");
    const value = selectedByName.get(columnName);
    if (value === undefined) throw invalidBranchKey(`selected branch key missing ${columnName}`);
    selectedByPhysical.set(physical, value);
  }
  const keys = new Map<string, any>();
  for (const [schemaVersion, catalogueSchema] of node.catalogue.catalogueSchemas) {
    const mapping = node.catalogue.physicalMappings.get(schemaVersion);
    if (!mapping) continue;
    let found: [string, any] | undefined;
    for (const [name, m] of mapping.tables) if (m.tableId === tableId) found ??= [name, m];
    if (!found) continue;
    const [logicalTable, tableMapping] = found;
    const histTable = catalogueSchema.schema.tables.find((t: any) => t.name === logicalTable);
    if (!histTable) continue;
    const historical = new Set<number>(
      histTable.branchBy
        .map((n: string) => tableMapping.columns.get(n))
        .filter((p: any) => p !== undefined),
    );
    const nonDefault = readTable.branchBy.some((columnName: string) => {
      const physical = readTableMapping.columns.get(columnName);
      if (physical === undefined) return true;
      if (historical.has(physical)) return false;
      const column = readTable.columns.find((c: any) => c.name === columnName);
      const def =
        column?.default !== undefined
          ? BranchColumnValue.encodeTyped(column.default, column.columnType)
          : undefined;
      const sel = selectedByPhysical.get(physical);
      return def === undefined ? sel !== undefined : sel === undefined || !deepEqual(sel, def);
    });
    if (nonDefault) continue;
    const values: [string, any][] = histTable.branchBy.map((columnName: string) => {
      const physical = tableMapping.columns.get(columnName);
      if (physical === undefined)
        throw nodeInvalidStoredValue("historical branch column physical mapping is missing");
      const value = selectedByPhysical.get(physical);
      if (value === undefined)
        throw nodeInvalidStoredValue("selected branch key missing historical table branch column");
      return [columnName, value];
    });
    values.sort((a, b) => cmpStr(a[0], b[0]));
    const key = { values };
    keys.set(branchKeyId(key), key);
  }
  if (keys.size === 0) return [{ values: [...selectedByName] }];
  return [...keys.entries()].sort((a, b) => cmpStr(a[0], b[0])).map(([, k]) => k);
}

/** Resolve one row through a live head-over-base branch view for mutation helpers. */
export async function visibleCurrentCellsInBranchView(
  node: any,
  table: string,
  headSelector: any,
  baseView: any | undefined,
  rowUuid: string,
): Promise<Map<string, any> | undefined> {
  const schemaVersion = node.catalogue.activeSchema.schema;
  const entry = node.catalogue.catalogueSchemas.get(schemaVersion);
  if (!entry) throw nodeInvalidStoredValue("current write schema missing");
  const tableSchema = node.tableInSchema(table, schemaVersion);
  const stack = branchViewStack(entry.schema, tableSchema, headSelector, baseView);
  if (stack.error !== undefined) throw invalidBranchKey(stack.error);
  const [head, base] = stack.value!;
  const rows = await branchViewRowsForSchema(node, table, schemaVersion, "Local", head, base);
  const row = rows.find((r) => r.rowUuid() === rowUuid);
  if (!row) return undefined;
  const cells = new SortedMap<string, any>(cmpStr);
  for (const column of tableSchema.columns) {
    const value = row.cell(tableSchema, column.name);
    if (value !== undefined) cells.set(column.name, value);
  }
  return cells as any;
}

export async function branchWinnersForSchema(
  node: any,
  table: string,
  readSchemaVersion: string,
  tier: string,
  key: any,
  snapshotRef?: any,
): Promise<[RowMap, RowMap]> {
  const content = rowMap();
  const txIds = new Map<string, any>();
  const snapshot = snapshotRef && {
    owner: snapshotRef.owner,
    globalBase: snapshotRef.globalBase,
    localBase: snapshotRef.localBase,
    dots: snapshotRef.dots.slice(),
  };
  for (const storedKey of equivalentStoredBranchKeys(node, table, readSchemaVersion, key)) {
    for (const version of await node.queryTableVersionsInBranch(table, storedKey)) {
      const txId = node.versionTxId(version);
      const tx = await node.queryTransaction(txId);
      if (!tx) continue;
      const visible = snapshot
        ? await node.snapshotCovers(txId, snapshot)
        : visibleAtTier(tx, tier);
      if (visible) keepLatestVersion(content, txIds, txId, version);
    }
  }
  return [content, deletionMarkerWinners(content)];
}

export async function branchViewRowsForSchema(
  node: any,
  table: string,
  readSchemaVersion: string,
  tier: string,
  head: any,
  base: any | undefined,
): Promise<any[]> {
  const [headContent, headDeletions] = await branchWinnersForSchema(
    node,
    table,
    readSchemaVersion,
    tier,
    head,
  );
  let baseLayers: [RowMap, RowMap] = [rowMap(), rowMap()];
  if (base && !deepEqual(base.key, head))
    baseLayers = await branchWinnersForSchema(
      node,
      table,
      readSchemaVersion,
      tier,
      base.key,
      base.kind === "Snapshot" ? base.snapshot : undefined,
    );
  return materializeBranchViewWinners(
    node,
    table,
    readSchemaVersion,
    head,
    headContent,
    headDeletions,
    ...baseLayers,
  );
}

/** Materialize only the frozen base relation for a snapshot-backed branch view. */
export async function branchSnapshotRowsForSchema(
  node: any,
  table: string,
  readSchemaVersion: string,
  outputBranch: any,
  baseBranch: any,
  snapshot: any,
): Promise<[any[], any[]]> {
  const [baseContent, baseDeletions] = await branchWinnersForSchema(
    node,
    table,
    readSchemaVersion,
    "Local",
    baseBranch,
    snapshot,
  );
  const deleted = new Set<string>();
  for (const [rowUuid, version] of baseDeletions)
    if (version.deletion() === "Deleted") deleted.add(rowUuid);
  const baseRows = materializeBranchViewWinners(
    node,
    table,
    readSchemaVersion,
    outputBranch,
    rowMap(),
    rowMap(),
    baseContent,
    rowMap(),
  );
  return [baseRows, baseRows.filter((r) => deleted.has(r.rowUuid()))];
}

export function materializeBranchViewWinners(
  node: any,
  table: string,
  readSchemaVersion: string,
  outputBranch: any,
  headContent: RowMap,
  headDeletions: RowMap,
  baseContent: RowMap,
  baseDeletions: RowMap,
): any[] {
  const readTable = node.tableInSchema(table, readSchemaVersion);
  const rowIds = new SortedSet<string>(cmpStr, [
    ...headContent.keysArray(),
    ...baseContent.keysArray(),
  ]);
  const rows: any[] = [];
  for (const rowUuid of rowIds) {
    const version = topWins([headContent, baseContent], rowUuid);
    if (!version) continue;
    const deletion = topWins([headDeletions, baseDeletions], rowUuid);
    if (deletion?.deletion() === "Deleted") continue;
    const cells = projectVersionCells(node, table, readSchemaVersion, version);
    if (!cells) continue;
    for (const columnName of readTable.branchBy) {
      const pair = outputBranch.values.find(([name]: [string, any]) => name === columnName);
      if (!pair) throw invalidBranchKey("head branch key missing table column");
      let decoded: any;
      try {
        decoded = pair[1].decode();
      } catch {
        throw invalidBranchKey("invalid head branch value");
      }
      cells.set(columnName, decoded);
    }
    const updated = newerOfDeletion(node, version, deletion);
    rows.push(
      currentRowFromMaterializedCellsWithLayerProvenance(
        readTable,
        version,
        version,
        updated,
        cells,
      ),
    );
  }
  sortCurrentRows(rows);
  return rows;
}

export async function projectedHistoricalCurrentRows(
  node: any,
  table: string,
  readSchemaVersion: string,
  position: any,
): Promise<any[]> {
  const readTable = node.tableInSchemaRef(table, readSchemaVersion);
  const content = await winningVersions(node, table, { kind: "GlobalAt", position });
  return projectVersionWinners(node, table, readSchemaVersion, readTable, content);
}

export async function projectedSnapshotCurrentRows(
  node: any,
  table: string,
  readSchemaVersion: string,
  snapshot: any,
): Promise<any[]> {
  const readTable = node.tableInSchemaRef(table, readSchemaVersion);
  const content = await winningVersions(node, table, { kind: "Snapshot", snapshot });
  return projectVersionWinners(node, table, readSchemaVersion, readTable, content);
}

type VersionVisibility = { kind: "GlobalAt"; position: any } | { kind: "Snapshot"; snapshot: any };

async function winningVersions(
  node: any,
  table: string,
  visibility: VersionVisibility,
): Promise<RowMap> {
  const content = rowMap();
  const txIds = new Map<string, any>();
  for (const version of await node.queryTableVersions(table)) {
    const txId = node.versionTxId(version);
    let visible: boolean;
    if (visibility.kind === "Snapshot")
      visible = await node.snapshotCovers(txId, visibility.snapshot);
    else {
      const tx = await node.queryTransaction(txId);
      visible =
        !!tx &&
        tx.fate.kind === "Accepted" &&
        durabilityAtLeast(tx.durability, "Global") &&
        tx.globalTime !== undefined &&
        cmpGlobalTime(tx.globalTime, visibility.position) <= 0;
    }
    if (visible) keepLatestVersion(content, txIds, txId, version);
  }
  return content;
}

/** Materialize `version` in its own schema and translate into the read schema. */
function projectVersionCells(
  node: any,
  table: string,
  readSchemaVersion: string,
  version: VersionRow,
): Map<string, any> | undefined {
  const sourceSchema = node.schemaVersionForAlias(version.schemaVersionAlias());
  if (sourceSchema === undefined)
    throw nodeInvalidStoredValue("history schema version alias must exist");
  const sourceTable = node.tableInSchemaRef(version.table(), sourceSchema);
  const cells = node.materializedCellsForVersion(sourceTable, version);
  const projected = translateCells(node, sourceSchema, readSchemaVersion, version.table(), cells);
  return projected === table ? cells : undefined;
}

function newerOfDeletion(
  node: any,
  content: VersionRow,
  deletion: VersionRow | undefined,
): VersionRow {
  const sortKey = (v: VersionRow) => {
    const id = node.versionTxId(v);
    return id.time.sortKey(id.node);
  };
  return deletion && cmpSortKey(sortKey(deletion), sortKey(content)) > 0 ? deletion : content;
}

function projectVersionWinners(
  node: any,
  table: string,
  readSchemaVersion: string,
  readTable: any,
  content: RowMap,
): any[] {
  const deletions = deletionMarkerWinners(content);
  const rows: any[] = [];
  for (const [rowUuid, version] of content) {
    if (deletions.get(rowUuid)?.deletion() === "Deleted") continue;
    const cells = projectVersionCells(node, table, readSchemaVersion, version);
    if (!cells) continue;
    const updated = newerOfDeletion(node, version, deletions.get(rowUuid));
    try {
      rows.push(
        currentRowFromMaterializedCellsWithLayerProvenance(
          readTable,
          version,
          version,
          updated,
          cells,
        ),
      );
    } catch (error) {
      if (!isUnrepresentableEnumProjection(error)) throw error;
    }
  }
  sortCurrentRows(rows);
  return rows;
}

export function translateCells(
  node: any,
  source: string,
  target: string,
  table: string,
  cells: Map<string, any>,
): string | undefined {
  if (source === target) return table;
  const path = node.compiledLensPath(source, target, table);
  return path ? applyCompiledLensPath(path, cells) : undefined;
}

function deletionMarkerWinners(content: RowMap): RowMap {
  const out = rowMap();
  for (const [rowUuid, version] of content)
    if (version.deletion() !== undefined) out.set(rowUuid, version);
  return out;
}

function visibleAtTier(tx: any, tier: string): boolean {
  return tier === "Global"
    ? tx.fate.kind === "Accepted" && durabilityAtLeast(tx.durability, "Global")
    : tx.fate.kind !== "Rejected";
}

function keepLatestVersion(
  content: RowMap,
  txIds: Map<string, any>,
  txId: any,
  version: VersionRow,
): void {
  const key = version.rowUuid();
  const existing = txIds.get(key);
  if (
    existing === undefined ||
    cmpSortKey(
      version.txTime().sortKey(txId.node),
      content.get(key).txTime().sortKey(existing.node),
    ) > 0
  ) {
    txIds.set(key, txId);
    content.set(key, version);
  }
}

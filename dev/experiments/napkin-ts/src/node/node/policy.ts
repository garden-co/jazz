// Write-policy admission and policy-pinned row projection. Policy predicates
// execute through the query program in query_eval; this module selects the
// operation clause, projects old/candidate data into the pinned policy schema
// and fail-closes write ingest. It also retains the transaction memo.
import { SortedMap, cmpStr } from "../../support/collections.js";
import {
  AUTHOR_SYSTEM,
  RowAuthor,
  TransactionOverlayTable,
  TransactionWriteOverlay,
  VersionRecord,
  applyCompiledLensPath,
  authorEquals,
  authorizationQueryFromReadPolicy,
  columnTypeEquals,
  currentRowFromCellsWithExplicitProvenance,
  deepEqual,
  nodeError,
  nodeInvalidStoredValue,
  queryFrom,
  txIdEquals,
} from "../../_extern/node-policy-peer.js";
import { resolveBranchViewCopyEvidence } from "./source_resolution.js";

type Cells = Map<string, any>;
type TableKey = string; // `${schema}\0${table}`
const tableKey = (schema: string, table: string): TableKey => `${schema}\0${table}`;
const cellMap = (entries?: Iterable<[string, any]>): Cells =>
  new SortedMap<string, any>(cmpStr, entries) as any;

export class ViewEvaluationContext {
  txRows = new Map<string, any>();
}

const versionProvenance = (v: any) => ({
  createdBy: v.createdBy(),
  createdAt: v.createdAtMs(),
  updatedBy: v.updatedBy(),
  updatedAt: v.updatedAtMs(),
});

const updatedProvenance = (previous: any, v: any) => ({
  createdBy: previous.createdBy,
  createdAt: previous.createdAt,
  updatedBy: v.updatedBy(),
  updatedAt: v.updatedAtMs(),
});

const storedVersionProvenance = (v: any) => ({
  createdBy: v.createdBy(),
  createdAt: v.createdAt().physicalMs(),
  updatedBy: v.updatedBy(),
  updatedAt: v.updatedAt().physicalMs(),
});

const reconstructedPolicySubjectRow = (table: any, rowUuid: string, cells: Cells, version: any) =>
  currentRowFromCellsWithExplicitProvenance(
    table,
    rowUuid,
    cells,
    storedVersionProvenance(version),
    [version.txTime(), version.txNodeAlias()],
  );

/** A reconstructed candidate without retained metadata cannot prove provenance ownership. */
const unresolvedProvenance = () => ({
  createdBy: AUTHOR_SYSTEM,
  createdAt: 0,
  updatedBy: AUTHOR_SYSTEM,
  updatedAt: 0,
});

/** One row a candidate commit unit writes on the main branch (`INV-RLS-9`). */
class CandidateEvidenceRow {
  constructor(
    readonly authoredTable: string,
    readonly rowUuid: string,
    readonly policySchema: string,
    readonly policyTable: string,
    readonly after: any | undefined,
    readonly before: any | undefined,
  ) {}
  replacesCommitted(): boolean {
    return (
      this.before !== undefined && this.after !== undefined && !deepEqual(this.before, this.after)
    );
  }
  tableKey(): TableKey {
    return tableKey(this.policySchema, this.policyTable);
  }
  shown(grounded: boolean): any | undefined {
    if (this.after === undefined) return undefined;
    return grounded ? this.after : this.before;
  }
}

type OverlayTables = Map<TableKey, any>;

class CandidateUnitEvidence {
  constructor(readonly rows: CandidateEvidenceRow[] = []) {}

  rowIndex(table: string, rowUuid: string): number | undefined {
    let lo = 0;
    let hi = this.rows.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      const r = this.rows[mid];
      const c = cmpStr(r.authoredTable, table) || cmpStr(r.rowUuid, rowUuid);
      if (c === 0) return mid;
      if (c < 0) lo = mid + 1;
      else hi = mid;
    }
    return undefined;
  }

  rebuildTables(tables: OverlayTables, grounded: boolean[], changed?: Set<TableKey>): void {
    const rebuilt = new Map<TableKey, SortedMap<string, any>>();
    this.rows.forEach((row, index) => {
      const key = row.tableKey();
      if (changed && !changed.has(key)) return;
      let rows = rebuilt.get(key);
      if (!rows) rebuilt.set(key, (rows = new SortedMap<string, any>(cmpStr)));
      rows.set(row.rowUuid, row.shown(grounded[index]));
    });
    for (const [key, rows] of rebuilt) tables.set(key, TransactionOverlayTable.new(rows));
  }
}

/** Whether a write-policy clause can only grant more as rows are added. */
function writePolicyQueryIsMonotone(q: any): boolean {
  return (
    !q.aggregate &&
    q.limit === undefined &&
    q.offset === 0 &&
    q.arraySubqueries.length === 0 &&
    predicatesAreMonotone(q.filters) &&
    q.joins.every(joinIsMonotone) &&
    q.reachable.every(reachableIsMonotone) &&
    q.policyBranches.every(
      (b: any) =>
        predicatesAreMonotone(b.filters) &&
        b.joins.every(joinIsMonotone) &&
        b.reachable.every(reachableIsMonotone),
    ) &&
    (!q.relation || relationIsMonotone(q.relation.rel))
  );
}

const queryInherits = (q: any): boolean =>
  q.inherits.length > 0 || q.policyBranches.some((b: any) => b.inherits.length > 0);
const predicatesAreMonotone = (ps: any[]): boolean => ps.every(predicateIsMonotone);

function predicateIsMonotone(p: any): boolean {
  switch (p.kind) {
    case "Not":
      return false;
    case "All":
    case "Any":
      return predicatesAreMonotone(p.children);
    case "EnumMatch":
      return predicateIsMonotone(p.payload);
    default:
      return true;
  }
}

const joinIsMonotone = (j: any): boolean =>
  predicatesAreMonotone(j.filters) && j.nestedJoins.every(joinIsMonotone);
const reachableIsMonotone = (r: any): boolean =>
  predicatesAreMonotone(r.accessFilters) && predicatesAreMonotone(r.edgeFilters);

function relationIsMonotone(r: any): boolean {
  switch (r.kind) {
    case "TableScan":
      return true;
    case "Filter":
      return relationIsMonotone(r.input) && relationPredicateIsMonotone(r.predicate);
    case "Union":
      return r.inputs.every((arm: any) => relationIsMonotone(arm.input));
    case "Join":
      return r.joinKind === "Inner" && relationIsMonotone(r.left) && relationIsMonotone(r.right);
    case "Project":
    case "Distinct":
    case "OrderBy":
      return relationIsMonotone(r.input);
    case "Gather":
      return relationIsMonotone(r.seed) && relationIsMonotone(r.step);
    default:
      return false;
  }
}

function relationPredicateIsMonotone(p: any): boolean {
  switch (p.kind) {
    case "Not":
      return false;
    case "And":
    case "Or":
      return p.children.every(relationPredicateIsMonotone);
    case "EnumMatch":
      return relationPredicateIsMonotone(p.payload);
    default:
      return true;
  }
}

export type UnitWritePolicyDecision =
  | { kind: "Allowed" }
  | { kind: "Denied" }
  | { kind: "Unsupported"; message: string };
const ALLOWED: UnitWritePolicyDecision = { kind: "Allowed" };
const DENIED: UnitWritePolicyDecision = { kind: "Denied" };

export const MAX_TRANSACTION_POLICY_EVIDENCE_ROWS = 1 << 18;

const unsupportedTransactionPolicyEvidence = (): UnitWritePolicyDecision => ({
  kind: "Unsupported",
  message: `Reading more than ${MAX_TRANSACTION_POLICY_EVIDENCE_ROWS} rows of a transaction's own writes in its write-policy checks is not supported yet`,
});

function currentRowCells(table: any, row: any): Cells {
  const cells = cellMap();
  for (const column of table.columns) {
    const value = row.cell(table, column.name);
    if (value !== undefined) cells.set(column.name, value);
  }
  return cells;
}

function durableAuthorPolicyPreview(node: any, commit: any): [any, any] {
  const permissionSubject = commit.effectivePermissionSubject();
  let author: any;
  try {
    author = RowAuthor.fromSession(commit.madeBy, node.nodeUuid).asAuthorSubject();
  } catch {
    throw nodeError("UnadmittedWriteAuthor");
  }
  return [{ ...commit, madeBy: author }, permissionSubject];
}

/** Reconstruct operation-specific authorization actions, excluding the candidate tx. */
export async function authorizationActionsForVersionsInTransaction(
  node: any,
  versions: any[],
  candidateTxId?: any,
): Promise<any[]> {
  const actions: any[] = [];
  for (const version of versions) {
    const [policySchema, table, cells] = policyProjectionForVersionRecord(node, version);
    if (version.deletesRow()) {
      actions.push({ kind: "Delete", table: table.name, row: version.rowUuid() });
      continue;
    }
    const isUpdate =
      (await policyPreviousContentSubjectRow(node, policySchema, table, version, candidateTxId)) !==
      undefined;
    if (isUpdate) {
      const authored: Set<string> | undefined = version.authoredColumns();
      const patch = authored ? cellMap([...cells].filter(([c]) => authored.has(c))) : cells;
      actions.push({ kind: "Update", table: table.name, row: version.rowUuid(), patch });
    } else actions.push({ kind: "Insert", table: table.name, cells });
  }
  return actions;
}

export function writePolicyAllowsVersionRecord(
  node: any,
  version: any,
  author: any,
  candidateTxId: any | undefined,
  candidateVersions: any[],
): Promise<boolean> {
  return writePolicyAllowsVersionRecordForView(
    node,
    version,
    author,
    undefined,
    candidateTxId,
    candidateVersions,
    TransactionWriteOverlay.default(),
  );
}

/** Decide every write policy of one commit unit (`INV-RLS-9`) by fixpoint over grounded rows. */
export async function commitUnitWritePoliciesAllow(
  node: any,
  versions: any[],
  author: any,
  candidateTxId: any,
): Promise<UnitWritePolicyDecision> {
  const evidence = await candidateUnitEvidence(node, versions, author, candidateTxId);
  if (evidence.rows.length === 0) {
    for (const version of versions) {
      const ok = await writePolicyAllowsVersionRecordForView(
        node,
        version,
        author,
        undefined,
        candidateTxId,
        versions,
        TransactionWriteOverlay.acceptedState(),
      );
      if (!ok) return DENIED;
    }
    return ALLOWED;
  }
  const ownRow = versions.map((v) =>
    v.branchKey().values.length === 0 ? evidence.rowIndex(v.table(), v.rowUuid()) : undefined,
  );
  const unpassed = new Array<number>(evidence.rows.length).fill(0);
  for (const own of ownRow) if (own !== undefined) unpassed[own]++;
  const ungroundedLive = new Map<TableKey, number>();
  const ungroundedUpdates = new Map<TableKey, number>();
  const bump = (m: Map<TableKey, number>, k: TableKey, d: number) => m.set(k, (m.get(k) ?? 0) + d);
  for (const row of evidence.rows) {
    if (row.after !== undefined) bump(ungroundedLive, row.tableKey(), 1);
    if (row.replacesCommitted()) bump(ungroundedUpdates, row.tableKey(), 1);
  }
  const grounded = new Array<boolean>(evidence.rows.length).fill(false);
  const sawPostState = (own: number | undefined, monotone: boolean, reads: Set<TableKey>) =>
    [...reads].every((table) => {
      const ownRowEv = own !== undefined && !grounded[own] ? evidence.rows[own] : undefined;
      const mine = ownRowEv && ownRowEv.tableKey() === table ? ownRowEv : undefined;
      if (monotone)
        return (ungroundedUpdates.get(table) ?? 0) === (mine?.replacesCommitted() ? 1 : 0);
      return (ungroundedLive.get(table) ?? 0) === (mine && mine.after !== undefined ? 1 : 0);
    });
  const monotone = unitWritePoliciesMonotone(node, versions);
  const postStateChecked = new Array<boolean>(versions.length).fill(false);
  const reads = versions.map(() => new Set<TableKey>());
  const tables: OverlayTables = new Map();
  let firstRound = true;
  const spent = { n: 0 };
  const failed = new Set<number>();
  let pending = versions.map((_, i) => i);
  for (;;) {
    const overlay = TransactionWriteOverlay.fromTables(new Map(tables));
    const newlyGrounded: number[] = [];
    for (const index of pending) {
      const allowed = await unitWritePolicyCheck(
        node,
        versions,
        index,
        ownRow[index],
        evidence,
        overlay,
        author,
        candidateTxId,
        reads,
        spent,
      );
      if (allowed === undefined) return unsupportedTransactionPolicyEvidence();
      if (allowed) {
        failed.delete(index);
        postStateChecked[index] = sawPostState(ownRow[index], monotone[index], reads[index]);
        const own = ownRow[index];
        if (own !== undefined && --unpassed[own] === 0) newlyGrounded.push(own);
      } else failed.add(index);
    }
    if (failed.size === 0) break;
    const changed = new Set<TableKey>();
    for (const own of newlyGrounded) {
      grounded[own] = true;
      const row = evidence.rows[own];
      if (row.after !== undefined) {
        changed.add(row.tableKey());
        if (ungroundedLive.has(row.tableKey())) bump(ungroundedLive, row.tableKey(), -1);
      }
      if (row.replacesCommitted() && ungroundedUpdates.has(row.tableKey()))
        bump(ungroundedUpdates, row.tableKey(), -1);
    }
    if (changed.size === 0) return DENIED;
    evidence.rebuildTables(tables, grounded, firstRound ? undefined : changed);
    firstRound = false;
    pending = [...failed]
      .sort((a, b) => a - b)
      .filter((i) => [...reads[i]].some((t) => changed.has(t)));
  }
  const finalPass = versions.map((_, i) => i).filter((i) => !postStateChecked[i]);
  if (finalPass.length === 0) return ALLOWED;
  grounded.fill(true);
  evidence.rebuildTables(tables, grounded);
  const overlay = TransactionWriteOverlay.fromTables(tables);
  for (const index of finalPass) {
    const allowed = await unitWritePolicyCheck(
      node,
      versions,
      index,
      ownRow[index],
      evidence,
      overlay,
      author,
      candidateTxId,
      reads,
      spent,
    );
    if (allowed === undefined) return unsupportedTransactionPolicyEvidence();
    if (!allowed) return DENIED;
  }
  return ALLOWED;
}

/** Whether each version's WITH CHECK policies are monotone (`INV-RLS-9`). */
function unitWritePoliciesMonotone(node: any, versions: any[]): boolean[] {
  const byTable = new Map<TableKey, boolean>();
  return versions.map((version) => {
    const key = tableKey(version.schemaVersion(), version.table());
    const cached = byTable.get(key);
    if (cached !== undefined) return cached;
    const [policySchema, table] = policyProjectionForVersionRecord(node, version);
    const checks = [table.writePolicies.insertCheck, table.writePolicies.updateCheck].filter(
      (p) => p !== undefined,
    );
    let known = checks.every(writePolicyQueryIsMonotone);
    if (known && checks.some(queryInherits)) {
      const schema = node.catalogue.schemaForVersion(policySchema);
      known =
        !!schema &&
        schema.tables.every((t: any) =>
          [
            ...(t.readPolicy ? [t.readPolicy] : []),
            ...t.writePolicies.iter().map(([, p]: [string, any]) => p),
          ].every(writePolicyQueryIsMonotone),
        );
    }
    byTable.set(key, known);
    return known;
  });
}

/** Check one write against a round's shared overlay; `undefined` once the budget is spent. */
async function unitWritePolicyCheck(
  node: any,
  versions: any[],
  index: number,
  ownRow: number | undefined,
  evidence: CandidateUnitEvidence,
  shared: any,
  author: any,
  candidateTxId: any,
  reads: Set<TableKey>[],
  spent: { n: number },
): Promise<boolean | undefined> {
  if (spent.n > MAX_TRANSACTION_POLICY_EVIDENCE_ROWS) return undefined;
  const recorder = new Set<TableKey>();
  let overlay = shared.recording(recorder);
  if (ownRow !== undefined) {
    const row = evidence.rows[ownRow];
    overlay = overlay.excluding(row.policySchema, row.policyTable, row.rowUuid);
  }
  const allowed = await writePolicyAllowsVersionRecordForView(
    node,
    versions[index],
    author,
    undefined,
    candidateTxId,
    versions,
    overlay,
  );
  reads[index] = recorder;
  spent.n += overlay.rowsIn(recorder);
  return allowed;
}

/** Prepare the main-branch rows of a candidate unit as evidence for its other writes. */
async function candidateUnitEvidence(
  node: any,
  versions: any[],
  author: any,
  candidateTxId: any,
): Promise<CandidateUnitEvidence> {
  if (authorEquals(author, AUTHOR_SYSTEM)) return new CandidateUnitEvidence();
  const byRow = new SortedMap<[string, string], any[]>(
    (a, b) => cmpStr(a[0], b[0]) || cmpStr(a[1], b[1]),
  );
  for (const version of versions) {
    if (version.branchKey().values.length) continue;
    byRow.getOrInsert([version.table(), version.rowUuid()], () => []).push(version);
  }
  if (byRow.size < 2) return new CandidateUnitEvidence();
  const rows: CandidateEvidenceRow[] = [];
  for (const [[authoredTable, rowUuid], rowVersions] of byRow) {
    if (rowVersions.some((v) => v.deletion() === "Deleted")) continue;
    const content = rowVersions.find((v) => v.deletion() === undefined);
    const subject = content ?? rowVersions[0];
    const [policySchema, table, cells] = policyProjectionForVersionRecord(node, subject);
    const previous = await policyPreviousContentSubjectRow(
      node,
      policySchema,
      table,
      subject,
      candidateTxId,
    );
    let after: any;
    let before: any;
    if (content) {
      const afterCells = previous ? currentRowCells(table, previous) : cellMap();
      for (const [k, v] of cells) afterCells.set(k, v);
      const provenance = previous
        ? updatedProvenance(previous.provenance() ?? unresolvedProvenance(), content)
        : versionProvenance(content);
      after = currentRowFromCellsWithExplicitProvenance(
        table,
        rowUuid,
        afterCells,
        provenance,
        undefined,
      );
      before = previous;
    } else {
      after = previous;
    }
    rows.push(
      new CandidateEvidenceRow(authoredTable, rowUuid, policySchema, table.name, after, before),
    );
  }
  return new CandidateUnitEvidence(rows);
}

/** An update of an existing row also requires that the fate authority can read it. */
export async function versionSatisfiesReadForWriteVisibility(
  node: any,
  version: any,
  author: any,
  candidateTxId?: any,
): Promise<boolean> {
  if (authorEquals(author, AUTHOR_SYSTEM) || version.deletesRow()) return true;
  const [policySchema, table] = policyProjectionForVersionRecord(node, version);
  const previous = await policyPreviousContentSubjectRow(
    node,
    policySchema,
    table,
    version,
    candidateTxId,
  );
  if (!previous) return true;
  return node.readPolicyQueryAllowsCandidateWithProvenanceForSchema(
    policySchema,
    table,
    authorizationQueryFromReadPolicy(table),
    previous.rowUuid(),
    currentRowCells(table, previous),
    author,
    previous.provenance() ?? unresolvedProvenance(),
  );
}

/** Prove read-for-write visibility of the branch-view source copied into a first head overlay. */
export async function branchViewCopySatisfiesReadForWriteVisibility(
  node: any,
  evidence: any,
  authoredSchema: string,
  author: any,
  candidateTxId?: any,
): Promise<boolean> {
  if (authorEquals(author, AUTHOR_SYSTEM)) return true;
  const resolved = await resolveBranchViewCopyEvidence(
    node,
    evidence,
    authoredSchema,
    candidateTxId,
  );
  if (!resolved) return false;
  const source = node.versionRecordFromRow(resolved);
  const [policySchema, table, cells] = policyProjectionForVersionRecord(node, source);
  return node.readPolicyQueryAllowsCandidateWithProvenanceForSchema(
    policySchema,
    table,
    authorizationQueryFromReadPolicy(table),
    source.rowUuid(),
    cells,
    author,
    versionProvenance(source),
  );
}

function recordCells(table: any, version: any): Cells {
  const cells = cellMap();
  table.columns.forEach((column: any, idx: number) => {
    const value = version.optionalCellAt(idx);
    if (value !== undefined) cells.set(column.name, value);
  });
  return cells;
}

async function writePolicyAllowsVersionRecordForView(
  node: any,
  version: any,
  author: any,
  exactView: any | undefined,
  candidateTxId: any | undefined,
  candidateVersions: any[],
  overlay: any,
): Promise<boolean> {
  if (authorEquals(author, AUTHOR_SYSTEM)) return true;
  let policySchema: string;
  let table: any;
  let cells: Cells;
  if (exactView) {
    table = exactView.tables.find((t: any) => t.name === version.table());
    if (!table) throw nodeError("TableNotFound", version.table());
    cells = recordCells(table, version);
    policySchema = version.schemaVersion();
  } else [policySchema, table, cells] = policyProjectionForVersionRecord(node, version);
  const wp = table.writePolicies;
  if (version.deletesRow()) {
    const policy = wp.deleteUsing;
    if (!policy) return false;
    let current = await policyDeleteSubjectRow(
      node,
      policySchema,
      table,
      version,
      candidateTxId,
      candidateVersions,
    );
    if (!current) {
      if (cells.size === 0) return false;
      const insertPolicy = wp.insertCheck;
      if (!insertPolicy) return false;
      const ok = await node.writePolicyQueryAllowsCandidateWithProvenanceForSchema(
        policySchema,
        table,
        insertPolicy,
        version.rowUuid(),
        cells,
        author,
        true,
        versionProvenance(version),
      );
      if (!ok) return false;
      current = currentRowFromCellsWithExplicitProvenance(
        table,
        version.rowUuid(),
        cells,
        versionProvenance(version),
        undefined,
      );
    }
    return node.writePolicyQueryAllowsCandidateOverTransaction(
      policySchema,
      table,
      policy,
      current.rowUuid(),
      currentRowCells(table, current),
      author,
      false,
      current.provenance() ?? unresolvedProvenance(),
      overlay.committedView(),
    );
  }
  const previous = await policyPreviousContentSubjectRow(
    node,
    policySchema,
    table,
    version,
    candidateTxId,
  );
  if (previous) {
    const previousCells = currentRowCells(table, previous);
    const previousProvenance = previous.provenance() ?? unresolvedProvenance();
    if (wp.updateUsing) {
      const ok = await node.writePolicyQueryAllowsCandidateOverTransaction(
        policySchema,
        table,
        wp.updateUsing,
        previous.rowUuid(),
        previousCells,
        author,
        false,
        previousProvenance,
        overlay.committedView(),
      );
      if (!ok) return false;
    }
    if (!wp.updateUsing && !wp.updateCheck) return false;
    if (!wp.updateCheck) return true;
    for (const [k, v] of cells) previousCells.set(k, v);
    return node.writePolicyQueryAllowsCandidateOverTransaction(
      policySchema,
      table,
      wp.updateCheck,
      version.rowUuid(),
      previousCells,
      author,
      false,
      updatedProvenance(previousProvenance, version),
      overlay,
    );
  }
  if (!wp.insertCheck) return false;
  return node.writePolicyQueryAllowsCandidateOverTransaction(
    policySchema,
    table,
    wp.insertCheck,
    version.rowUuid(),
    cells,
    author,
    true,
    versionProvenance(version),
    overlay,
  );
}

export function dryRunInsertAllows(node: any, commit: any): Promise<boolean> {
  return dryRunMergeableWriteAllowsInSchema(node, node.catalogue.activeSchema.schema, commit);
}

export async function dryRunMergeableWriteAllowsInSchema(
  node: any,
  writeSchema: string,
  commit: any,
): Promise<boolean> {
  const [previewed, permissionSubject] = durableAuthorPolicyPreview(node, commit);
  const table = node.tableInSchemaRef(previewed.table, writeSchema);
  const version = VersionRecord.fromCommit(previewed, table, writeSchema);
  return writePolicyAllowsVersionRecord(node, version, permissionSubject, undefined, []);
}

export function dryRunReadCurrentAllows(
  node: any,
  tableName: string,
  rowUuid: string,
  identity: any,
): Promise<boolean> {
  return dryRunReadCurrentAllowsInSchema(
    node,
    tableName,
    rowUuid,
    node.catalogue.localSchemaVersionId,
    identity,
    false,
  );
}

/** Evaluate a point-read policy in the schema that named the wire request. */
export async function dryRunReadCurrentAllowsInSchema(
  node: any,
  tableName: string,
  rowUuid: string,
  schemaVersion: string,
  identity: any,
  includeDeleted: boolean,
): Promise<boolean> {
  const schema = node.catalogue.schemaForVersion(schemaVersion);
  if (!schema) throw nodeInvalidStoredValue("repair request schema is missing from catalogue");
  const shape = queryFrom(tableName).validateWithSchemaVersion(schema, schemaVersion);
  const binding = shape.bind(new Map());
  const rows = includeDeleted
    ? await node.queryReadableCurrentRowIncludingDeleted(shape, binding, "Local", identity, rowUuid)
    : await node.queryRowVisibilityForLinkPhysicalRow(shape, binding, "Local", identity, rowUuid);
  return rows.some((r: any) => r.rowUuid() === rowUuid);
}

export async function dryRunDeleteCurrentAllows(
  node: any,
  tableName: string,
  rowUuid: string,
  author: any,
): Promise<boolean> {
  if (authorEquals(author, AUTHOR_SYSTEM)) return true;
  const table = node.table(tableName);
  const row = await policyLocalCurrentSubjectRow(node, table, rowUuid);
  if (!row) return false;
  const policy = table.writePolicies.deleteUsing;
  if (!policy) return false;
  return node.writePolicyQueryAllowsCurrentRow(policy, row.rowUuid(), author);
}

/** Whether `rowUuid` is a live local row of `table`. */
export async function localCurrentRowExists(
  node: any,
  tableName: string,
  rowUuid: string,
): Promise<boolean> {
  return (await policyLocalCurrentSubjectRow(node, node.table(tableName), rowUuid)) !== undefined;
}

async function policyLocalCurrentSubjectRow(node: any, table: any, rowUuid: string): Promise<any> {
  const version = await node.queryLocalWinner(table.name, rowUuid);
  if (!version || version.deletion() === "Deleted") return undefined;
  const [, projected, cells] = policyProjectionForVersionRow(node, version);
  if (projected.name !== table.name) return undefined;
  return reconstructedPolicySubjectRow(table, rowUuid, cells, version);
}

function policyProjectionForVersionRow(node: any, version: any): [string, any, Cells] {
  const source = node.schemaVersionForAlias(version.schemaVersionAlias());
  if (source === undefined) throw nodeInvalidStoredValue("history schema version alias must exist");
  const cells = version.cells(node.tableInSchemaRef(version.table(), source));
  return translatePolicyCells(node, source, version.table(), cells);
}

export function policyProjectionForVersionRecord(node: any, version: any): [string, any, Cells] {
  const source = version.schemaVersion();
  const sourceTable = node.tableInSchemaRef(version.table(), source);
  return translatePolicyCells(node, source, version.table(), recordCells(sourceTable, version));
}

function translatePolicyCells(
  node: any,
  source: string,
  table: string,
  cells: Cells,
): [string, any, Cells] {
  const target = policyTargetSchemaForSource(node, source, table);
  if (source === target) return [target, node.tableInSchema(table, target), cells];
  const path = node.compiledLensPath(source, target, table);
  if (path) {
    const translated = applyCompiledLensPath(path, cells);
    return [target, node.tableInSchema(translated, target), cells];
  }
  const targetTable = node.tableInSchemaRef(table, target);
  if (policyTablesAreDirectlyCompatible(node.tableInSchemaRef(table, source), targetTable))
    return [target, targetTable, cells];
  throw nodeError("InvalidCatalogueUpdate", "lens chain is unknown");
}

function tableResolves(node: any, table: string, schema: string): boolean {
  try {
    node.tableInSchemaRef(table, schema);
    return true;
  } catch {
    return false;
  }
}

export function readPolicySchemaForTableName(
  node: any,
  table: string,
  querySchema: string,
  shape: any,
): string {
  const writeSchema = node.catalogue.activeSchema.schema;
  const currentSchema = node.catalogue.localSchemaVersionId;
  if (
    tableResolves(node, table, writeSchema) &&
    policySchemaResolvesQuerySources(node, writeSchema, shape)
  )
    return writeSchema;
  if (policySchemaResolvesQuerySources(node, currentSchema, shape)) return currentSchema;
  return querySchema;
}

function policySchemaResolvesQuerySources(node: any, schema: string, shape: any): boolean {
  const tables: string[] = [];
  for (const [, n] of shape.nodes) if (n.kind === "Source") tables.push(n.source.table);
  for (const s of shape.auxiliarySources) tables.push(s.table);
  return tables.every((t) => tableResolves(node, t, schema));
}

function policyTargetSchemaForSource(node: any, source: string, table: string): string {
  const writeSchema = node.catalogue.activeSchema.schema;
  if (sourceReachesWritePolicyTable(node, source, writeSchema, table)) return writeSchema;
  const local = node.catalogue.localSchemaVersionId;
  if (
    sourceReachesWritePolicyTable(node, source, local, table) ||
    tableResolves(node, table, local)
  )
    return local;
  return source;
}

function sourceReachesWritePolicyTable(
  node: any,
  source: string,
  target: string,
  table: string,
): boolean {
  if (source === target) return tableResolves(node, table, target);
  const path = node.compiledLensPath(source, target, table);
  if (path) return tableResolves(node, applyCompiledLensPath(path, cellMap()), target);
  return tableResolves(node, table, target);
}

async function policyDeleteSubjectRow(
  node: any,
  policySchema: string,
  table: any,
  version: any,
  candidateTxId: any,
  candidateVersions: any[],
): Promise<any> {
  const previous = await policyPreviousContentSubjectRow(
    node,
    policySchema,
    table,
    version,
    candidateTxId,
  );
  if (previous) return previous;
  for (const candidate of candidateVersions) {
    if (
      candidate.rowUuid() !== version.rowUuid() ||
      candidate.deletesRow() ||
      !deepEqual(candidate.branchKey(), version.branchKey())
    )
      continue;
    const [, projected, cells] = policyProjectionForVersionRecord(node, candidate);
    if (projected.name === table.name)
      return currentRowFromCellsWithExplicitProvenance(
        table,
        version.rowUuid(),
        cells,
        versionProvenance(candidate),
        undefined,
      );
  }
  return undefined;
}

async function policyPreviousContentSubjectRow(
  node: any,
  _policySchema: string,
  table: any,
  version: any,
  candidateTxId: any | undefined,
): Promise<any> {
  const subjectTable = tableResolves(node, version.table(), node.catalogue.activeSchema.schema)
    ? version.table()
    : table.name;
  const localPrevious =
    candidateTxId !== undefined
      ? await node.queryLocalWinnerInBranchExcludingTx(
          subjectTable,
          version.branchKey(),
          version.rowUuid(),
          candidateTxId,
        )
      : await node.queryLocalWinnerInBranch(subjectTable, version.branchKey(), version.rowUuid());
  if (localPrevious) {
    const [, projected, cells] = policyProjectionForVersionRow(node, localPrevious);
    if (projected.name === table.name)
      return reconstructedPolicySubjectRow(table, version.rowUuid(), cells, localPrevious);
  }
  const globalVersion = await node.queryGlobalWinnerInBranch(
    subjectTable,
    version.branchKey(),
    version.rowUuid(),
  );
  if (
    globalVersion &&
    !(candidateTxId !== undefined && txIdEquals(candidateTxId, node.versionTxId(globalVersion)))
  ) {
    const [, projected, cells] = policyProjectionForVersionRow(node, globalVersion);
    if (projected.name === table.name)
      return reconstructedPolicySubjectRow(table, version.rowUuid(), cells, globalVersion);
  }
  return undefined;
}

export async function queryTransactionMemo(
  node: any,
  txId: any,
  context: ViewEvaluationContext,
): Promise<any> {
  const key = String(txId);
  if (!context.txRows.has(key)) context.txRows.set(key, await node.queryTransaction(txId));
  return context.txRows.get(key);
}

function policyTablesAreDirectlyCompatible(source: any, target: any): boolean {
  return (
    source.name === target.name &&
    source.columns.length === target.columns.length &&
    source.columns.every(
      (c: any, i: number) =>
        c.name === target.columns[i].name &&
        columnTypeEquals(c.columnType, target.columns[i].columnType),
    )
  );
}

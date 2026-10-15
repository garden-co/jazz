// Turn logical query read requirements into concrete Groove source graphs.
import { SortedMap, cmpStr, equalBytes, toHex } from "../../../support/index.js";
import * as X from "../../../_extern/node-qe-a.js";
import {
  type NodeState,
  type SourceId,
  nodeError,
  Value,
  GraphBuilder,
  PredicateExpr,
  ProjectField,
  RecordDescriptor,
  DescriptorField,
  FieldIdentity,
  coveredInputTrace,
  debugFmt,
  cmpSourceId,
} from "../../../_extern/node-qe-a.js";
const sre = (request: any, gap: any): any => sourceResolutionError(request, gap);
const sourceResolutionErrorFromPolicyProof = (request: any, error: any): any =>
  sourceResolutionErrorFromPolicyProofImpl(request, error);
import { isCurrentAppSource, excludeLocalUnavailableGraph } from "./unavailable_inputs.js";
import {
  staticScanForPrefix,
  physicalCurrentIndexValue,
  selectCurrentAccessPath,
  selectCompositeLeadingEqualityAccessPath,
  normalizedRowSetShape,
  rootSourceId,
  rootLiteralEqualities,
  literalEqualitiesForFilters,
  reachableSeedSourceId,
  reachableEdgeSourceId,
  reachableAccessSourceId,
} from "./normalization.js";

type Graph = any;
type Req = any;

function currentRowColumnField(column: any, valueType: any): any {
  return DescriptorField.new(X.userColumnField(column.name), valueType).withIdentity(
    FieldIdentity.Name(column.name),
  );
}

const overlayKey = (schema: any, table: string) => `${debugFmt(schema)}\0${table}`;

/** One table's overlaid rows for one evaluation round: a row shows that content, `undefined` hides it. */
export class TransactionOverlayTable {
  liveRecords: [any, [Uint8Array, Uint8Array][]][] = [];
  writtenRecords: [any, [Uint8Array, Uint8Array][]][] = [];
  constructor(
    readonly rows: SortedMap<Uint8Array, any | undefined> = new SortedMap((a, b) =>
      cmpStr(toHex(a), toHex(b)),
    ),
  ) {}
  get length(): number {
    return this.rows.size;
  }
  isEmpty(): boolean {
    return this.rows.size === 0;
  }
  static cached(
    cache: [any, [Uint8Array, Uint8Array][]][],
    descriptor: any,
    encode: () => [Uint8Array, Uint8Array][],
  ): [Uint8Array, Uint8Array][] {
    const hit = cache.find(([d]) => X.recordDescriptorEq(d, descriptor));
    if (hit) return hit[1];
    const records = encode();
    cache.push([descriptor, records]);
    return records;
  }
}

/** A candidate transaction's own writes as write-policy evidence. */
export class TransactionWriteOverlay {
  constructor(
    readonly tables: Map<string, TransactionOverlayTable> = new Map(),
    readonly excluded: [any, string, Uint8Array] | undefined = undefined,
    readonly reads: Map<string, [any, string]> | undefined = undefined,
    readonly evidenceTierValue: string = "Local",
  ) {}
  static fromTables(tables: Map<string, TransactionOverlayTable>): TransactionWriteOverlay {
    return new TransactionWriteOverlay(tables, undefined, undefined, "Global");
  }
  static acceptedState(): TransactionWriteOverlay {
    return TransactionWriteOverlay.fromTables(new Map());
  }
  committedView(): TransactionWriteOverlay {
    return new TransactionWriteOverlay(new Map(), undefined, undefined, this.evidenceTierValue);
  }
  evidenceTier(): string {
    return this.evidenceTierValue;
  }
  excluding(schema: any, table: string, row: Uint8Array): TransactionWriteOverlay {
    return new TransactionWriteOverlay(
      this.tables,
      [schema, table, row],
      this.reads,
      this.evidenceTierValue,
    );
  }
  recording(reads: Map<string, [any, string]>): TransactionWriteOverlay {
    return new TransactionWriteOverlay(this.tables, this.excluded, reads, this.evidenceTierValue);
  }
  isActive(): boolean {
    return this.reads !== undefined || [...this.tables.values()].some((t) => !t.isEmpty());
  }
  rowsIn(tables: Iterable<[any, string]>): number {
    let n = 0;
    for (const [s, t] of tables) n += this.tables.get(overlayKey(s, t))?.length ?? 0;
    return n;
  }
  recordRead(schema: any, table: string): void {
    this.reads?.set(overlayKey(schema, table), [schema, table]);
  }
  table(schema: any, table: string): TransactionOverlayTable | undefined {
    const t = this.tables.get(overlayKey(schema, table));
    return t && !t.isEmpty() ? t : undefined;
  }
  excludes(schema: any, table: string, row: Uint8Array): boolean {
    const e = this.excluded;
    return !!e && X.schemaVersionEq(e[0], schema) && e[1] === table && equalBytes(e[2], row);
  }
}

export interface CurrentSourceGraph {
  graph: Graph;
  descriptor: any;
  metadata: Map<any, any>;
}

export type HydrationLifetime = "FirstResult" | "Retained";
export type AccessPathSelector = "Ordinary" | "CompositeEquality";

export interface CurrentIndexCandidateFilter {
  table: string;
  column: string;
  orderColumn: string | undefined;
  prefix: any[];
  sourceColumn: string;
}

export type CurrentAccessPath =
  | { kind: "PrimaryKey"; values: any[] }
  | { kind: "PrimaryKeyPage"; cap: number }
  | {
      kind: "Index";
      column: string;
      orderColumn: string | undefined;
      reverse: boolean;
      prefix: any[];
      intersections: [string, any[]][];
      candidateFilter: CurrentIndexCandidateFilter | undefined;
      maintained: boolean;
      sourceLimit: number | undefined;
    };

function primaryKeyPageScan(cap: number): any {
  return { kind: "PrefixLimit", prefix: [], maxItems: cap };
}

const resolved = (
  request: Req,
  table: any,
  storedColumnIds: any,
  graph: Graph,
  descriptor: any,
  metadata: any,
  extra: any = {},
) => ({
  storedColumnIds,
  tableSchema: table,
  graph,
  rowShape: { source: request.source, descriptor, rowUuidField: "row_uuid", metadata },
  routingFields: new Set<string>(),
  requiresResultPayload: false,
  contentVersion: undefined,
  deletionRegister: undefined,
  authorizedDeletionPreimage: undefined,
  ...extra,
});

const isPendingLocalOnly = (overlays: any) =>
  overlays.entries.length === 1 && overlays.entries[0].kind === "PendingLocal";

export class JazzSourceGraphPreparer {
  localUnavailableScope: any;
  node!: NodeState;
  readView: any;
  inlineSources = new SortedMap<SourceId, any[]>(cmpSourceId);
  coveredInputSources = new SortedMap<SourceId, Graph>(cmpSourceId);
  coveredInputDescriptors = new SortedMap<SourceId, any>(cmpSourceId);
  accessPaths = new SortedMap<SourceId, CurrentAccessPath>(cmpSourceId);
  boundedDeletionRegister: [SourceId, Graph] | undefined;
  countAccessPathMetrics = true;
  currentProjectionTargets = new SortedMap<SourceId, string>(cmpSourceId);
  policySubplan = false;
  transactionOverlay = new TransactionWriteOverlay();

  constructor(init: Partial<JazzSourceGraphPreparer>) {
    Object.assign(this, init);
  }

  localExclusionScope(request: Req): any {
    if (request.authorization.kind === "PolicyProof") return undefined;
    const source = this.readView.sources.get(request.source);
    return source && isCurrentAppSource(source) ? this.localUnavailableScope : undefined;
  }

  excludesBelowPending(request: Req): boolean {
    if (this.localExclusionScope(request) === undefined || this.inlineSources.has(request.source))
      return false;
    const s = this.readView.sources.get(request.source);
    if (s?.kind === "VisibleCurrent") return s.tier === "Local";
    if (s?.kind === "WithOverlays") return isPendingLocalOnly(s.overlays);
    return false;
  }

  async excludeSettledArm(request: Req, graph: Graph, pendingAhead: boolean): Promise<Graph> {
    if (!this.excludesBelowPending(request)) return graph;
    const scope = this.localExclusionScope(request);
    try {
      return await excludeLocalUnavailableGraph(
        this.node,
        scope,
        this.readView.readSchema,
        request,
        graph,
        pendingAhead,
      );
    } catch {
      throw sre(request, "LocalAvailabilityInput");
    }
  }

  storedColumnIdsForReadTable(request: Req, table: any): Map<string, any> {
    const mapping = this.node.catalogue.physicalMappings.get(this.readView.readSchema);
    const tableMapping = mapping?.tables.get(table.name);
    if (!tableMapping) throw sre(request, "SchemaProjection");
    const out = new Map<string, any>();
    for (const column of table.columns) {
      const id = tableMapping.columns.get(column.name);
      if (id === undefined) throw sre(request, "SchemaProjection");
      out.set(column.name, id);
    }
    return out;
  }

  private tableFor(request: Req): any {
    try {
      return this.node.tableInSchema(request.source.table, this.readView.readSchema);
    } catch {
      throw sre(request, "SchemaProjection");
    }
  }

  /** The complete source-family dispatcher. */
  async prepareSourceGraphDispatch(request: Req): Promise<any> {
    let source = this.readView.sources.get(request.source);
    if (!source) throw sre(request, "Coverage");
    let pendingCurrent: any;
    if (source.kind === "WithOverlays" && isPendingLocalOnly(source.overlays)) {
      if (source.input.kind !== "SettledBindingView") throw sre(request, "Coverage");
      pendingCurrent = {
        kind: "VisibleCurrent",
        projection: source.input.projection,
        data: { kind: "Current" },
        tier: "Local",
      };
    }
    const pendingOverlay = pendingCurrent !== undefined;
    source = pendingCurrent ?? source;
    const visible = request.visibility === "Visible";
    const coveredInputSource = visible ? this.coveredInputSources.get(request.source) : undefined;
    const coveredInputDescriptor = visible
      ? this.coveredInputDescriptors.get(request.source)
      : undefined;
    if ((coveredInputSource !== undefined) !== (coveredInputDescriptor !== undefined))
      throw sre(request, "Coverage");
    const receiverLocalOverlay = coveredInputSource !== undefined && pendingOverlay;
    if (coveredInputTrace() && this.coveredInputSources.size) {
      console.error(
        `JAZZ_COVERED_INPUT_TRACE stage=covered_input_lookup request=${debugFmt(request.source)} matched=${coveredInputSource !== undefined} candidates=${debugFmt([...this.coveredInputSources.keysArray()])}`,
      );
    }
    if (
      coveredInputSource !== undefined &&
      (source.kind === "SettledBindingView" ||
        (source.kind === "VisibleCurrent" && X.tierAtLeast(source.tier, "Global")))
    ) {
      if (!visible) throw sre(request, "Coverage");
      const table = this.tableFor(request);
      const metadata = coveredInputSourceMetadata(request.requirements, table);
      return resolved(
        request,
        table,
        this.storedColumnIdsForReadTable(request, table),
        coveredInputSource,
        coveredInputDescriptor,
        metadata,
      );
    }
    let projection,
      graphTier,
      historyPosition,
      snapshot,
      openTxOverlay,
      branchView: [any, any] | undefined;
    switch (source.kind) {
      case "VisibleCurrent":
        if (source.data.kind !== "Current") throw sre(request, "HistoricalStorageCut");
        projection = source.projection;
        graphTier = source.tier;
        break;
      case "BranchView":
        projection = source.projection;
        graphTier = source.tier;
        branchView = [source.head, source.base];
        break;
      case "HistoryCut":
        if (source.data.kind !== "Current") throw sre(request, "HistoricalStorageCut");
        projection = source.projection;
        historyPosition = source.position;
        break;
      case "SnapshotRef":
        if (source.data.kind !== "Current") throw sre(request, "HistoricalStorageCut");
        projection = source.projection;
        snapshot = source.snapshot;
        break;
      case "SettledBindingView":
        throw sre(request, "Coverage");
      case "WithOverlays": {
        const input = source.input;
        if (input.kind === "VisibleCurrent" && input.data.kind === "Current") {
          projection = input.projection;
          graphTier = input.tier;
        } else if (input.kind === "SnapshotRef" && input.data.kind === "Current") {
          projection = input.projection;
        } else throw sre(request, "TransactionReadOverlay");
        const entries = source.overlays.entries;
        if (entries.length !== 1 || entries[0].kind !== "OpenTransaction")
          throw sre(request, "TransactionReadOverlay");
        openTxOverlay = entries[0].txId;
        break;
      }
      default:
        throw sre(request, "HistoricalStorageCut");
    }
    if (
      projection.schemaFamily.kind !== "Current" ||
      !(
        projection.storage.kind === "Single" || projection.storage.kind === "CompatiblePartitions"
      ) ||
      projection.lens.kind !== "Canonical"
    ) {
      throw sre(request, "SchemaProjection");
    }
    const table = this.tableFor(request);
    const authorization =
      request.authorization.kind === "PolicyProof" ? { kind: "System" } : request.authorization;
    const inlineRows = this.inlineSources.get(request.source);
    if (inlineRows) {
      if (!visible || authorization.kind !== "System") throw sre(request, "Coverage");
      let graph, descriptor, metadata;
      try {
        const alias = await this.node.ensureSchemaVersionAlias(this.readView.readSchema);
        [graph, descriptor, metadata] = inlineCurrentGraphWithSourceMetadata(
          table,
          inlineRows,
          alias,
          "inline-current",
          request.requirements,
        );
      } catch {
        throw sre(request, "Coverage");
      }
      return resolved(
        request,
        table,
        this.storedColumnIdsForReadTable(request, table),
        graph,
        descriptor,
        metadata,
      );
    }
    const snapshotSource = snapshot !== undefined;
    let snapshotContentVersion: any;
    let graph: Graph, descriptor: any, metadata: any, routingFields: Set<string>;
    if (branchView) {
      const [head, base] = branchView;
      const baseIsCurrent = base === undefined || base.kind === "Current";
      const liveBase =
        base?.kind === "Current" && !X.branchKeyEq(base.key, head) ? base.key : undefined;
      if (request.visibility === "IncludeDeleted" && baseIsCurrent) {
        const tier = graphTier;
        const [content, deletions] = await this.liveBranchViewRegisters(
          request,
          table,
          tier,
          head,
          liveBase,
        );
        let baseGraph;
        try {
          baseGraph = includeDeletedBranchGraph(table, head, content, deletions);
        } catch {
          throw sre(request, "Coverage");
        }
        let g = baseGraph;
        if (authorization.kind !== "System") {
          const { permissionSubject, plan } = authorization;
          let policyRequest = this.node.tableReadPolicyAuthorizationRequestForIncludeDeleted(
            this.readView.policySchema,
            table.name,
            permissionSubject,
            tier,
            plan.bindingSourceShape,
            plan.bindingUserParams,
            plan.bindingClaimParams,
          );
          if (policyRequest)
            policyRequest.reads.primary = policyReadViewProjectedThrough(
              policyRequest.reads.primary,
              this.readView,
            );
          const outputFields = [...currentRowFields(table), "__jazz_deleted"];
          try {
            g = this.node.composePolicyFilteredCurrentSourceGraph(
              policyRequest,
              baseGraph,
              outputFields,
            ).graph;
          } catch (error) {
            throw sourceResolutionErrorFromPolicyProof(request, error);
          }
        }
        try {
          [graph, descriptor, metadata] = withRequestedAuthorPaths(
            g,
            includeDeletedCurrentRowDescriptor(table),
            new Map(),
            request.requirements,
          );
        } catch {
          throw sre(request, "Coverage");
        }
        return resolved(
          request,
          table,
          this.storedColumnIdsForReadTable(request, table),
          graph,
          descriptor,
          metadata,
        );
      }
      if (!visible) throw sre(request, "Coverage");
      if (baseIsCurrent) {
        const branchWitnessField = table.branchBy.length ? "supplying_branch_key" : undefined;
        const [content, deletions] = await this.liveBranchViewRegisters(
          request,
          table,
          graphTier,
          head,
          liveBase,
        );
        const needsVersion = request.requirements.metadata.some(
          (r: any) =>
            r.kind === "VersionPayloads" ||
            r.kind === "VersionWitnesses" ||
            r.kind === "Provenance",
        );
        const contentVersion = needsVersion
          ? {
              graph: content.projectFields([
                ...maintainedViewHistoryStorageFieldNames(table).map((f: string) =>
                  ProjectField.named(f),
                ),
                ProjectField.renamed("branch_key", "supplying_branch_key"),
              ]),
              rowUuidField: "row_uuid",
            }
          : undefined;
        const deletionRegister = request.requirements.metadata.some(
          (r: any) => r.kind === "DeletionMarkers",
        )
          ? {
              graph: deletions.projectFields([
                ...registerStorageFieldNames().map((f: string) => ProjectField.named(f)),
                ProjectField.renamed("branch_key", "supplying_branch_key"),
              ]),
              rowUuidField: "row_uuid",
            }
          : undefined;
        const deleted = deletions
          .filter(PredicateExpr.eq("_deletion", Value.EnumTag(0)))
          .project(["row_uuid"]);
        const selectedBase = GraphBuilder.antiJoin(
          content,
          deleted,
          ["row_uuid"],
          ["row_uuid"],
        ).projectFields(this.branchFields(request, table, head));
        [graph, descriptor, metadata, routingFields] = this.resolveCurrent(
          request,
          table,
          graphTier,
          authorization,
          branchWitnessField,
          selectedBase,
        );
        return resolved(
          request,
          table,
          this.storedColumnIdsForReadTable(request, table),
          graph,
          descriptor,
          metadata,
          {
            routingFields,
            requiresResultPayload: true,
            contentVersion,
            deletionRegister,
          },
        );
      }

      if (base?.kind === "Snapshot") {
        const branchWitnessField = table.branchBy.length ? "supplying_branch_key" : undefined;
        const tier = graphTier;
        const [frozenBaseKey, frozenSnapshot] = [base.key, base.snapshot];
        let headKeys;
        try {
          headKeys = this.node.equivalentStoredBranchKeys(
            request.source.table,
            this.readView.readSchema,
            head,
          );
        } catch {
          throw sre(request, "Coverage");
        }
        const headContent = await this.projectedBranchContentSourceGraph(
          request,
          table,
          tier,
          headKeys,
          { kind: "Full" },
        );
        const headDeletions = await this.projectedBranchDeletionSourceGraph(
          request,
          tier,
          headKeys,
          { kind: "Full" },
          undefined,
        );
        const headContentLayer = X.Layer.new(headContent);
        const headDeletionLayer = X.Layer.new(headDeletions);
        const deleted = headDeletions
          .filter(PredicateExpr.eq("_deletion", Value.EnumTag(0)))
          .project(["row_uuid"]);
        const restored = headDeletions
          .filter(PredicateExpr.eq("_deletion", Value.EnumTag(1)))
          .project(["row_uuid"]);
        const selectedHead = GraphBuilder.antiJoin(
          headContent,
          deleted,
          ["row_uuid"],
          ["row_uuid"],
        ).projectFields(this.branchFields(request, table, head));
        const [liveHead, , headMetadata, headRouting] = this.resolveCurrent(
          request,
          table,
          tier,
          { kind: "System" },
          branchWitnessField,
          selectedHead,
        );
        const desc = currentRowDescriptorWithHiddenSourceFieldsForBranch(
          table,
          headMetadata,
          branchWitnessField !== undefined,
        );
        let frozenRows, frozenDeletedRows, alias;
        try {
          [frozenRows, frozenDeletedRows] = await this.node.branchSnapshotRowsForSchema(
            request.source.table,
            this.readView.readSchema,
            head,
            frozenBaseKey,
            frozenSnapshot,
          );
          alias = await this.node.ensureSchemaVersionAlias(this.readView.readSchema);
        } catch {
          throw sre(request, "Coverage");
        }
        const witness =
          branchWitnessField !== undefined
            ? ([branchWitnessField, frozenBaseKey] as [string, any])
            : undefined;
        const inlineFrozen = (rows: any[], label: string) => {
          let out;
          try {
            out = inlineCurrentGraphWithSourceMetadataAndBranchWitness(
              table,
              rows,
              alias,
              label,
              request.requirements,
              witness,
            );
          } catch {
            throw sre(request, "Coverage");
          }
          if (!X.recordDescriptorEq(desc, out[1]) || !X.metadataEq(headMetadata, out[2]))
            throw sre(request, "SchemaProjection");
          return out[0];
        };
        const opening = inlineFrozen(frozenRows, "frozen-branch-base");
        let inherited = X.exposed(opening, [headContentLayer]);
        if (!frozenDeletedRows.length) {
          inherited = GraphBuilder.union([
            X.exposed(inherited, [headDeletionLayer]),
            GraphBuilder.semiJoin(inherited, restored, ["row_uuid"], ["row_uuid"]),
          ]);
        } else {
          const frozenDeletions = inlineFrozen(frozenDeletedRows, "frozen-branch-base-deletions");
          const withoutFrozen = GraphBuilder.antiJoin(
            inherited,
            frozenDeletions.project(["row_uuid"]),
            ["row_uuid"],
            ["row_uuid"],
          );
          inherited = GraphBuilder.union([
            X.exposed(withoutFrozen, [headDeletionLayer]),
            GraphBuilder.semiJoin(inherited, restored, ["row_uuid"], ["row_uuid"]),
          ]);
        }
        const unfiltered = GraphBuilder.union([liveHead, inherited]);
        const outputFields = [
          ...currentRowFields(table),
          ...(branchWitnessField ? [branchWitnessField] : []),
        ];
        const g =
          authorization.kind === "System"
            ? unfiltered
            : this.policyFilter(request, table, authorization, unfiltered, outputFields, {
                tier,
                checkPlan: false,
              }).graph;
        const needsVersion = request.requirements.metadata.some(
          (r: any) =>
            r.kind === "VersionPayloads" ||
            r.kind === "VersionWitnesses" ||
            r.kind === "Provenance",
        );
        return resolved(
          request,
          table,
          this.storedColumnIdsForReadTable(request, table),
          g,
          desc,
          headMetadata,
          {
            routingFields: headRouting,
            requiresResultPayload: true,
            contentVersion: needsVersion
              ? {
                  graph: headContent.projectFields([
                    ...maintainedViewHistoryStorageFieldNames(table).map((f: string) =>
                      ProjectField.named(f),
                    ),
                    ProjectField.renamed("branch_key", "supplying_branch_key"),
                  ]),
                  rowUuidField: "row_uuid",
                }
              : undefined,
          },
        );
      }
      let rows;
      try {
        rows = await this.node.branchViewRowsForSchema(
          request.source.table,
          this.readView.readSchema,
          graphTier,
          head,
          base,
        );
      } catch {
        throw sre(request, "Coverage");
      }
      let baseGraph;
      try {
        if (!request.requirements.metadata.length) {
          [baseGraph, descriptor, metadata] = [
            inlineCurrentGraph(table, rows),
            currentRowDescriptor(table),
            new Map(),
          ];
        } else {
          const alias = await this.node.ensureSchemaVersionAlias(this.readView.readSchema);
          [baseGraph, descriptor, metadata] = inlineCurrentGraphWithSourceMetadata(
            table,
            rows,
            alias,
            "branch-view",
            request.requirements,
          );
        }
      } catch {
        throw sre(request, "Coverage");
      }
      graph =
        authorization.kind === "System"
          ? baseGraph
          : this.policyFilter(request, table, authorization, baseGraph, currentRowFields(table), {
              tier: graphTier,
            }).graph;
      routingFields = new Set();
    } else if (historyPosition !== undefined) {
      if (!visible) throw sre(request, "HistoricalStorageCut");
      const base = await this.projectedHistoricalSourceGraph(request, table, historyPosition);
      descriptor = base.descriptor;
      metadata = base.metadata;
      graph =
        authorization.kind === "System"
          ? base.graph
          : this.policyFilter(
              request,
              table,
              authorization,
              base.graph,
              this.descriptorFields(request, descriptor, "HistoricalStorageCut"),
              {
                at: historyPosition,
                gap: "HistoricalStorageCut",
              },
            ).graph;
      routingFields = new Set();
    } else if (snapshot !== undefined) {
      if (!visible) throw sre(request, "HistoricalStorageCut");
      let base;
      try {
        const rows = await this.node.projectedSnapshotCurrentRows(
          request.source.table,
          this.readView.readSchema,
          snapshot,
        );
        const alias = await this.node.ensureSchemaVersionAlias(this.readView.readSchema);
        [base, descriptor, metadata] = inlineCurrentGraphWithSourceMetadata(
          table,
          rows,
          alias,
          "snapshot",
          request.requirements,
        );
      } catch {
        throw sre(request, "HistoricalStorageCut");
      }
      if (request.requirements.metadata.some((r: any) => r.kind === "VersionPayloads")) {
        snapshotContentVersion = {
          graph: base.project(maintainedViewHistoryStorageFieldNames(table)),
          rowUuidField: "row_uuid",
        };
      }
      if (authorization.kind === "System") {
        graph = base;
        routingFields = new Set();
      } else {
        const f = this.policyFilter(
          request,
          table,
          authorization,
          base,
          this.descriptorFields(request, descriptor, "HistoricalStorageCut"),
          {
            tier: "Global",
            gap: "HistoricalStorageCut",
          },
        );
        graph = f.graph;
        routingFields = f.routeFields;
      }
    } else if (openTxOverlay !== undefined) {
      const includeDeleted = request.visibility === "IncludeDeleted";
      const comps = request.source.path.components;
      if (!this.policySubplan && !(comps.length === 1 && comps[0].kind === "Root")) {
        try {
          this.node.claimTxSourceRead(openTxOverlay, request.source);
        } catch {
          throw sre(request, "TransactionReadOverlay");
        }
      }
      let base;
      try {
        const rows: any[] = await this.node.txCurrentRowsInSchemaWithOptions(
          openTxOverlay,
          this.readView.readSchema,
          request.source.table,
          includeDeleted,
        );
        const alias = await this.node.ensureSchemaVersionAlias(this.readView.readSchema);
        [base, descriptor, metadata] = includeDeleted
          ? inlineSnapshotIncludeDeletedCurrentGraphWithSourceMetadata(
              table,
              rows.map((row) => [row, row.isDeleted()]),
              alias,
              "open-transaction",
              request.requirements,
            )
          : inlineCurrentGraphWithSourceMetadata(
              table,
              rows,
              alias,
              "open-transaction",
              request.requirements,
            );
      } catch {
        throw sre(request, "TransactionReadOverlay");
      }
      if (authorization.kind === "System") graph = base;
      else {
        const fields = this.descriptorFields(request, descriptor, "TransactionReadOverlay");
        if (includeDeleted) fields.push("__jazz_deleted");
        graph = this.policyFilter(request, table, authorization, base, fields, {
          tier: "Global",
          includeDeleted,
        }).graph;
      }
      routingFields = new Set();
    } else if (
      visible &&
      (this.needsProjectedCurrentSource(request.source.table) ||
        this.excludesBelowPending(request)) &&
      !receiverLocalOverlay
    ) {
      [graph, descriptor, metadata, routingFields] = await this.projectedVisibleCurrent(
        request,
        table,
        graphTier,
        authorization,
      );
    } else if (request.visibility === "IncludeDeleted" && pendingOverlay) {
      graph = this.pendingDeletionWinnerGraph(request)
        .filter(PredicateExpr.eq("_deletion", Value.EnumTag(0)))
        .projectFields([
          ProjectField.named("row_uuid"),
          ProjectField.named("tx_time"),
          ProjectField.named("tx_node_id"),
          ProjectField.literal("__jazz_deleted", Value.Bool(true)),
        ]);
      descriptor = includeDeletedCurrentRowDescriptor(table);
      metadata = new Map();
      routingFields = new Set();
    } else if (request.visibility === "IncludeDeleted") {
      const tier = graphTier;
      const base =
        this.needsProjectedCurrentSource(request.source.table) || this.excludesBelowPending(request)
          ? await this.projectedIncludeDeletedCurrentSourceGraph(request, table, tier)
          : includeDeletedCurrentGraph(table, tier);
      if (authorization.kind === "System") {
        graph = base;
        routingFields = new Set();
      } else {
        const f = this.policyFilter(
          request,
          table,
          authorization,
          base,
          [...currentRowFields(table), "__jazz_deleted"],
          {
            tier,
            includeDeleted: true,
          },
        );
        graph = f.graph;
        routingFields = f.routeFields;
      }
      descriptor = includeDeletedCurrentRowDescriptor(table);
      metadata = new Map();
    } else {
      const tier = graphTier;
      let selectedBase: Graph | undefined;
      if (receiverLocalOverlay) {
        this.node.queryEngineReadMetrics.sourceFullScans += 1;
        selectedBase = await this.pendingLocalCurrentSourceGraph(request, table);
      } else {
        selectedBase = await this.selectedGlobalOrPolicyPath(request, table, tier);
        if (selectedBase === undefined) this.node.queryEngineReadMetrics.sourceFullScans += 1;
      }
      [graph, descriptor, metadata, routingFields] = this.resolveCurrent(
        request,
        table,
        tier,
        authorization,
        undefined,
        selectedBase,
      );
    }
    if (request.visibility === "IncludeDeleted" && !pendingOverlay) {
      try {
        [graph, descriptor, metadata] = withRequestedAuthorPaths(
          graph,
          descriptor,
          metadata,
          request.requirements,
        );
      } catch {
        throw sre(request, "Coverage");
      }
    }
    const deletionRegister = snapshotSource
      ? undefined
      : this.deletionRegisterSourceForRequest(
          request,
          table,
          graphTier,
          historyPosition,
          openTxOverlay,
        );
    const contentVersion = snapshotSource
      ? snapshotContentVersion
      : await this.contentVersionSourceForRequest(
          request,
          table,
          graphTier,
          historyPosition,
          openTxOverlay,
        );
    descriptor = coveredInputDescriptor ?? descriptor;
    if (coveredInputSource !== undefined) {
      if (coveredInputTrace()) {
        console.error(
          `JAZZ_COVERED_INPUT_TRACE stage=union_covered_input request=${debugFmt(request.source)} descriptor=${debugFmt(descriptor)}`,
        );
      }
      let covered = coveredInputSource;
      if (receiverLocalOverlay) {
        const target = this.currentProjectionTarget(request, table);
        let accepted;
        try {
          accepted = this.node
            .physicalCurrentSourceGraphWithProjectionTarget(
              this.readView.readSchema,
              request.source.table,
              "Global",
              target,
            )
            .project(["row_uuid"]);
        } catch {
          throw sre(request, "SchemaProjection");
        }
        const outOfScope = GraphBuilder.antiJoin(
          accepted,
          covered.project(["row_uuid"]),
          ["row_uuid"],
          ["row_uuid"],
        );
        graph = GraphBuilder.antiJoin(graph, outOfScope, ["row_uuid"], ["row_uuid"]);
      }
      covered = await this.excludeSettledArm(request, covered, false);
      const winner = receiverLocalOverlay
        ? X.compose([X.Layer.new(graph), X.Layer.new(covered)])
        : GraphBuilder.argMaxBy(
            GraphBuilder.union([covered, graph]),
            ["row_uuid"],
            ["tx_time", "tx_node_id"],
          );
      if (receiverLocalOverlay) {
        const deleted = this.pendingDeletionWinnerGraph(request)
          .filter(PredicateExpr.eq("_deletion", Value.EnumTag(0)))
          .project(["row_uuid"]);
        graph = GraphBuilder.antiJoin(winner, deleted, ["row_uuid"], ["row_uuid"]);
      } else graph = winner;
    }
    return resolved(
      request,
      table,
      this.storedColumnIdsForReadTable(request, table),
      graph,
      descriptor,
      metadata,
      {
        routingFields,
        contentVersion,
        deletionRegister,
      },
    );
  }

  private branchFields(request: Req, table: any, head: any): any[] {
    try {
      return branchViewStorageSourceFields(table, head);
    } catch {
      throw sre(request, "SchemaProjection");
    }
  }

  private resolveCurrent(
    request: Req,
    table: any,
    tier: any,
    authorization: any,
    branchWitnessField: string | undefined,
    baseGraph: Graph | undefined,
  ): [Graph, any, any, Set<string>] {
    try {
      return resolvedCurrentSourceGraph(
        this.node,
        table,
        tier,
        request.requirements,
        authorization,
        this.readView.policySchema,
        this.readView,
        branchWitnessField,
        baseGraph,
      );
    } catch (error) {
      throw sourceResolutionErrorFromPolicyProof(request, error);
    }
  }

  private descriptorFields(request: Req, descriptor: any, gap: string): string[] {
    try {
      return X.descriptorFieldNames(descriptor);
    } catch {
      throw sre(request, gap);
    }
  }

  /** Filter `base` through the table read policy for this request's authorization. */
  private policyFilter(
    request: Req,
    table: any,
    authorization: any,
    base: Graph,
    outputFields: string[],
    opts: { tier?: any; at?: any; includeDeleted?: boolean; gap?: string; checkPlan?: boolean },
  ): { graph: Graph; routeFields: Set<string> } {
    const { permissionSubject, plan } = authorization;
    if (
      opts.checkPlan !== false &&
      (plan.protectedSource.table !== table.name ||
        plan.role !== "Read" ||
        plan.protectedRowField !== "row_uuid")
    ) {
      throw sre(request, opts.gap ?? "Coverage");
    }
    const policyRequest = X.tryResult(() => {
      let r;
      if (opts.at !== undefined) {
        return this.node.tableReadPolicyAuthorizationRequestAt(
          this.readView.policySchema,
          table.name,
          permissionSubject,
          "InlineAllReachableSeeds",
          opts.at,
          plan.bindingSourceShape,
          plan.bindingUserParams,
          plan.bindingClaimParams,
        );
      }
      if (opts.includeDeleted) {
        r = this.node.tableReadPolicyAuthorizationRequestForIncludeDeleted(
          this.readView.policySchema,
          table.name,
          permissionSubject,
          opts.tier,
          plan.bindingSourceShape,
          plan.bindingUserParams,
          plan.bindingClaimParams,
        );
      } else {
        r = this.node.tableReadPolicyAuthorizationRequest(
          this.readView.policySchema,
          table.name,
          permissionSubject,
          plan.bindingSourceShape !== undefined ? "RetainAllParams" : "InlineAllReachableSeeds",
          opts.tier,
          plan.bindingSourceShape,
          plan.bindingUserParams,
          plan.bindingClaimParams,
        );
      }
      r.reads.primary = policyReadViewProjectedThrough(r.reads.primary, this.readView);
      return r;
    });
    try {
      return this.node.composePolicyFilteredCurrentSourceGraph(policyRequest, base, outputFields);
    } catch (error) {
      throw sourceResolutionErrorFromPolicyProof(request, error);
    }
  }

  private async selectedGlobalOrPolicyPath(
    request: Req,
    table: any,
    tier: any,
  ): Promise<Graph | undefined> {
    const selected = await this.selectedGlobalCurrentSourceGraph(request, table, tier);
    if (selected !== undefined) return selected;
    const accessPath = this.cachedPolicyAuthorizationAccessPath(request);
    return accessPath
      ? this.selectedGlobalCurrentSourceGraphForAccessPath(request, table, tier, accessPath)
      : undefined;
  }

  /** Shared body of the projected visible-current paths. */
  private async projectedVisibleCurrent(
    request: Req,
    table: any,
    tier: any,
    authorization: any,
  ): Promise<[Graph, any, any, Set<string>]> {
    if (request.requirements.metadata.length) {
      const source = await this.projectedMaintainedVisibleCurrentSourceGraph(request, table, tier);
      try {
        return resolvedCurrentSourceGraph(
          this.node,
          table,
          tier,
          request.requirements,
          authorization,
          this.readView.policySchema,
          this.readView,
          undefined,
          source.graph,
        );
      } catch {
        throw sre(request, "Coverage");
      }
    }
    let selectedPolicyBase: Graph | undefined;
    if (!this.accessPaths.has(request.source) && !this.excludesBelowPending(request)) {
      const accessPath = this.cachedPolicyAuthorizationAccessPath(request);
      if (accessPath)
        selectedPolicyBase = await this.selectedGlobalCurrentSourceGraphForAccessPath(
          request,
          table,
          tier,
          accessPath,
        );
    }
    const source: CurrentSourceGraph = selectedPolicyBase
      ? {
          graph: selectedPolicyBase.projectFields(
            storageToCanonicalCurrentSourceFields(table, true, false),
          ),
          descriptor: currentRowDescriptor(table),
          metadata: new Map(),
        }
      : await this.projectedVisibleCurrentSourceGraph(request, table, tier);
    if (authorization.kind === "System")
      return [source.graph, source.descriptor, source.metadata, new Set()];
    const f = this.policyFilter(
      request,
      table,
      authorization,
      source.graph,
      currentRowFields(table),
      { tier },
    );
    return [f.graph, source.descriptor, source.metadata, f.routeFields];
  }

  private checkCanonicalProjection(request: Req): any {
    const s = this.readView.sources.get(request.source);
    if (s?.kind !== "VisibleCurrent" || s.data.kind !== "Current") throw sre(request, "Coverage");
    const p = s.projection;
    if (
      p.schemaFamily.kind !== "Current" ||
      !(p.storage.kind === "Single" || p.storage.kind === "CompatiblePartitions") ||
      p.lens.kind !== "Canonical"
    ) {
      throw sre(request, "SchemaProjection");
    }
    return p;
  }

  /** The ordinary current-row hot path for regular reads and policy admission. */
  async prepareUnprojectedVisibleCurrentSourceGraph(request: Req, tier: any): Promise<any> {
    this.checkCanonicalProjection(request);
    const table = this.tableFor(request);
    const authorization =
      request.authorization.kind === "PolicyProof" ? { kind: "System" } : request.authorization;
    const selectedBase = await this.selectedGlobalOrPolicyPath(request, table, tier);
    if (selectedBase === undefined) this.node.queryEngineReadMetrics.sourceFullScans += 1;
    const [graph, descriptor, metadata, routingFields] = this.resolveCurrent(
      request,
      table,
      tier,
      authorization,
      undefined,
      selectedBase,
    );
    const deletionRegister = this.deletionRegisterSourceForRequest(
      request,
      table,
      tier,
      undefined,
      undefined,
    );
    const contentVersion = await this.contentVersionSourceForRequest(
      request,
      table,
      tier,
      undefined,
      undefined,
    );
    return resolved(
      request,
      table,
      this.storedColumnIdsForReadTable(request, table),
      graph,
      descriptor,
      metadata,
      {
        routingFields,
        contentVersion,
        deletionRegister,
      },
    );
  }

  async prepareProjectedVisibleCurrentSourceGraph(request: Req, tier: any): Promise<any> {
    this.checkCanonicalProjection(request);
    const table = this.tableFor(request);
    const authorization =
      request.authorization.kind === "PolicyProof" ? { kind: "System" } : request.authorization;
    const [graph, descriptor, metadata, routingFields] = await this.projectedVisibleCurrent(
      request,
      table,
      tier,
      authorization,
    );
    const deletionRegister = this.deletionRegisterSourceForRequest(
      request,
      table,
      tier,
      undefined,
      undefined,
    );
    const contentVersion = await this.contentVersionSourceForRequest(
      request,
      table,
      tier,
      undefined,
      undefined,
    );
    return resolved(
      request,
      table,
      this.storedColumnIdsForReadTable(request, table),
      graph,
      descriptor,
      metadata,
      {
        routingFields,
        contentVersion,
        deletionRegister,
      },
    );
  }

  async prepareInlineVisibleCurrentSourceGraph(request: Req): Promise<any> {
    const rows = this.inlineSources.get(request.source);
    const s = this.readView.sources.get(request.source);
    if (s?.kind !== "VisibleCurrent" || s.data.kind !== "Current" || !rows)
      throw sre(request, "Coverage");
    this.checkCanonicalProjection(request);
    const authorization =
      request.authorization.kind === "PolicyProof" ? { kind: "System" } : request.authorization;
    if (request.visibility !== "Visible" || authorization.kind !== "System")
      throw sre(request, "Coverage");
    const table = this.tableFor(request);
    let graph, descriptor, metadata;
    try {
      const alias = await this.node.ensureSchemaVersionAlias(this.readView.readSchema);
      [graph, descriptor, metadata] = inlineCurrentGraphWithSourceMetadata(
        table,
        rows,
        alias,
        "inline-current",
        request.requirements,
      );
    } catch {
      throw sre(request, "Coverage");
    }
    return resolved(
      request,
      table,
      this.storedColumnIdsForReadTable(request, table),
      graph,
      descriptor,
      metadata,
    );
  }

  /** SourceGraphPreparer::prepare_source_graph */
  async prepareSourceGraph(request: Req): Promise<any> {
    const exclusionScope = this.localExclusionScope(request);
    const ap = this.accessPaths.get(request.source);
    if (exclusionScope !== undefined && ap?.kind === "Index") ap.sourceLimit = undefined;
    const r = await this.prepareSourceGraphWithoutLocalExclusions(request);
    if (this.transactionOverlay.isActive()) await this.overlayTransactionWrites(request, r);
    if (exclusionScope !== undefined && !this.excludesBelowPending(request)) {
      try {
        r.graph = await excludeLocalUnavailableGraph(
          this.node,
          exclusionScope,
          this.readView.readSchema,
          request,
          r.graph,
          false,
        );
      } catch {
        throw sre(request, "LocalAvailabilityInput");
      }
    }
    return r;
  }

  /** Layer the candidate transaction's own writes over one committed write-policy evidence source. */
  async overlayTransactionWrites(request: Req, r: any): Promise<void> {
    const s = this.readView.sources.get(request.source);
    if (
      !this.policySubplan ||
      request.visibility !== "Visible" ||
      this.inlineSources.has(request.source) ||
      this.coveredInputSources.has(request.source) ||
      !(s?.kind === "VisibleCurrent" && s.data.kind === "Current")
    ) {
      return;
    }
    const readSchema = this.readView.readSchema;
    const tableName = request.source.table;
    this.transactionOverlay.recordRead(readSchema, tableName);
    const rows = this.transactionOverlay.table(readSchema, tableName);
    if (!rows) return;
    const overlayError = () => sre(request, "TransactionReadOverlay");
    if (r.routingFields.size) throw overlayError();
    try {
      const alias = await this.node.ensureSchemaVersionAlias(readSchema);
      const descriptor = this.node.database.graphOutputDescriptor(r.graph);
      const liveRecords = TransactionOverlayTable.cached(rows.liveRecords, descriptor, () => {
        const out: [Uint8Array, Uint8Array][] = [];
        for (const [rowUuid, row] of rows.rows) {
          if (row === undefined) continue;
          out.push([
            rowUuid,
            inlineCurrentRecordForOutput(
              r.tableSchema,
              descriptor,
              row,
              alias,
              "transaction-overlay",
              request.requirements,
            ),
          ]);
        }
        return out;
      });
      const rowField = r.rowShape.rowUuidField;
      const writtenDescriptor = RecordDescriptor.new([[rowField, "Uuid"]]);
      const writtenRecords = TransactionOverlayTable.cached(
        rows.writtenRecords,
        writtenDescriptor,
        () =>
          rows.rows
            .keysArray()
            .map((u): [Uint8Array, Uint8Array] => [u, writtenDescriptor.create([Value.Uuid(u)])]),
      );
      const keep = (u: Uint8Array) => !this.transactionOverlay.excludes(readSchema, tableName, u);
      const live = GraphBuilder.inlineRecords(
        descriptor,
        liveRecords.filter(([u]) => keep(u)).map(([, rec]) => rec),
      );
      const committed = GraphBuilder.antiJoin(
        r.graph,
        GraphBuilder.inlineRecords(
          writtenDescriptor,
          writtenRecords.filter(([u]) => keep(u)).map(([, rec]) => rec),
        ),
        [rowField],
        [rowField],
      );
      r.graph = GraphBuilder.union([committed, live]);
    } catch (e) {
      throw overlayError();
    }
  }

  prepareSourceGraphWithoutLocalExclusions(request: Req): Promise<any> {
    if (this.coveredInputSources.has(request.source))
      return this.prepareSourceGraphDispatch(request);
    const s = this.readView.sources.get(request.source);
    if (s?.kind === "VisibleCurrent" && s.data.kind === "Current") {
      if (this.inlineSources.has(request.source))
        return this.prepareInlineVisibleCurrentSourceGraph(request);
      if (request.visibility === "Visible") {
        return this.needsProjectedCurrentSource(request.source.table)
          ? this.prepareProjectedVisibleCurrentSourceGraph(request, s.tier)
          : this.prepareUnprojectedVisibleCurrentSourceGraph(request, s.tier);
      }
    }
    return this.prepareSourceGraphDispatch(request);
  }

  policyDependencyRequest(request: Req): any {
    const auth = request.authorization;
    if (auth.kind !== "PolicyFiltered") return undefined;
    const { permissionSubject, plan } = auth;
    if (
      plan.protectedSource.table !== request.source.table ||
      plan.role !== "Read" ||
      plan.protectedRowField !== "row_uuid"
    ) {
      throw nodeError(
        "QueryCapability",
        "policy authorization plan does not match source dependency",
      );
    }
    const source = this.readView.sources.get(request.source);
    if (!source) throw nodeError("InvalidStoredValue", "query source dependency missing");
    const mode =
      plan.bindingSourceShape !== undefined ? "RetainAllParams" : "InlineAllReachableSeeds";
    const args = [plan.bindingSourceShape, plan.bindingUserParams, plan.bindingClaimParams];
    const policySchema = this.readView.policySchema;
    const table = request.source.table;
    let dependency;
    try {
      switch (source.kind) {
        case "HistoryCut":
          if (source.data.kind !== "Current") return undefined;
          dependency = this.node.tableReadPolicyAuthorizationRequestAt(
            policySchema,
            table,
            permissionSubject,
            "InlineAllReachableSeeds",
            source.position,
            ...args,
          );
          break;
        case "SnapshotRef":
          if (source.data.kind !== "Current") return undefined;
          dependency = this.node.tableReadPolicyAuthorizationRequest(
            policySchema,
            table,
            permissionSubject,
            mode,
            "Global",
            ...args,
          );
          break;
        case "VisibleCurrent":
        case "BranchView":
        case "SettledBindingView":
        case "WithOverlays": {
          const tier = source.currentTier() ?? "Global";
          dependency =
            request.visibility === "IncludeDeleted"
              ? this.node.tableReadPolicyAuthorizationRequestForIncludeDeleted(
                  policySchema,
                  table,
                  permissionSubject,
                  tier,
                  ...args,
                )
              : this.node.tableReadPolicyAuthorizationRequest(
                  policySchema,
                  table,
                  permissionSubject,
                  mode,
                  tier,
                  ...args,
                );
          break;
        }
        default:
          return undefined;
      }
    } catch (error: any) {
      if (error?.kind === "QueryCapability") {
        if (String(error.message).includes("PolicyProofCycle")) throw error;
        return undefined;
      }
      throw error;
    }
    dependency.reads.primary = policyReadViewProjectedThrough(
      dependency.reads.primary,
      this.readView,
    );
    return dependency;
  }

  currentProjectionTarget(request: Req, table: any): string {
    const hit = this.currentProjectionTargets.get(request.source);
    if (hit !== undefined) return hit;
    const required = this.currentProjectionRequiredFields(request, table);
    let target;
    try {
      target = this.node.ensurePhysicalCurrentProjectionForEnumColumns(
        this.readView.readSchema,
        table.name,
        required,
      );
    } catch {
      throw sre(request, "SchemaProjection");
    }
    this.currentProjectionTargets.set(request.source, target);
    return target;
  }

  currentProjectionRequiredFields(request: Req, table: any): Set<string> {
    const f = request.requirements.appFields;
    switch (f.kind) {
      case "All":
        return new Set(table.columns.map((c: any) => c.name));
      case "None":
        return new Set();
      default:
        return new Set(f.fields);
    }
  }

  async selectedGlobalCurrentSourceGraph(
    request: Req,
    table: any,
    tier: any,
  ): Promise<Graph | undefined> {
    const accessPath = this.accessPaths.get(request.source);
    if (!accessPath) return undefined;
    return this.selectedGlobalCurrentSourceGraphForAccessPath(
      request,
      table,
      tier,
      structuredClone(accessPath),
    );
  }

  /** The only storage-backed half of a Local-first covered receiver input. */
  async pendingLocalCurrentSourceGraph(request: Req, table: any): Promise<Graph> {
    const graph = await this.visibleCurrentPhysicalSourceGraph(request, table, "Ahead", "Local");
    return this.excludeSettledArm(request, graph, true);
  }

  async visibleCurrentPhysicalSourceGraph(
    request: Req,
    table: any,
    currentClass: string,
    deletionTier: any,
  ): Promise<Graph> {
    const target = this.currentProjectionTarget(request, table);
    let content;
    try {
      content = this.node.physicalCurrentSourceGraphWithProjectionTarget(
        this.readView.readSchema,
        request.source.table,
        currentClass,
        target,
      );
    } catch {
      throw sre(request, "SchemaProjection");
    }
    const deletedWinners = this.projectedDeletionRegisterCurrentSourceGraph(request, deletionTier)
      .filter(PredicateExpr.eq("_deletion", Value.EnumTag(0)))
      .project(["row_uuid"]);
    return GraphBuilder.antiJoin(content, deletedWinners, ["row_uuid"], ["row_uuid"]);
  }

  /** Reuse the access paths chosen while compiling the already-prepared policy dependency. */
  cachedPolicyAuthorizationAccessPath(request: Req): CurrentAccessPath | undefined {
    let policyRequest;
    try {
      policyRequest = this.policyDependencyRequest(request);
    } catch (error) {
      throw sourceResolutionErrorFromPolicyProof(request, error);
    }
    if (!policyRequest) return undefined;
    const authorization = this.node.policyAuthorizationGraphCacheGet(
      X.policyAuthorizationGraphCacheKey(policyRequest),
    );
    if (!authorization) throw sre(request, "Coverage");
    const ap = authorization.accessPaths.get(request.source);
    return ap && structuredClone(ap);
  }

  async selectedGlobalCurrentSourceGraphForAccessPath(
    request: Req,
    table: any,
    tier: any,
    accessPath: CurrentAccessPath,
  ): Promise<Graph | undefined> {
    const metrics = this.node.queryEngineReadMetrics;
    switch (accessPath.kind) {
      case "PrimaryKey":
        if (this.countAccessPathMetrics) metrics.sourcePrimaryKeyScans += 1;
        return selectedVisibleCurrentPrimaryKeyGraph(table, tier, accessPath.values);
      case "PrimaryKeyPage": {
        if (tier !== "Global") return undefined;
        const target = this.currentProjectionTarget(request, table);
        try {
          return this.node.physicalCurrentSourceScanGraphWithProjectionTarget(
            this.readView.readSchema,
            request.source.table,
            "Global",
            target,
            primaryKeyPageScan(accessPath.cap),
          );
        } catch {
          throw sre(request, "SchemaProjection");
        }
      }
      case "Index": {
        if (tier !== "Global") return undefined;
        const sourceLimit =
          accessPath.orderColumn !== undefined || request.visibility === "IncludeDeleted"
            ? accessPath.sourceLimit
            : undefined;
        const target = this.currentProjectionTarget(request, table);
        let rows;
        try {
          rows = physicalGlobalCurrentSourceForIndexScan(
            this.node,
            table,
            this.readView.readSchema,
            accessPath.column,
            accessPath.orderColumn,
            accessPath.reverse,
            accessPath.prefix,
            accessPath.intersections,
            false,
            sourceLimit,
            undefined,
            target,
          );
        } catch {
          throw sre(request, "Coverage");
        }
        metrics.sourceIndexProbes += 1 + accessPath.intersections.length;
        return rows;
      }
    }
  }

  pendingDeletionWinnerGraph(request: Req): Graph {
    let ahead;
    if (this.needsProjectedCurrentSource(request.source.table)) {
      try {
        this.node.physicalTableIdForSchema(this.readView.readSchema, request.source.table);
        ahead = this.node.physicalCurrentMarkerSourceGraph(
          this.readView.readSchema,
          request.source.table,
          "Ahead",
          X.BranchKey.default(),
          undefined,
        );
      } catch {
        throw sre(request, "SchemaProjection");
      }
    } else ahead = GraphBuilder.table(X.aheadCurrentTableName(request.source.table));
    const fields = registerStorageFieldsForQueryEngine("");
    return GraphBuilder.argMaxBy(
      ahead.projectFields(fields),
      ["row_uuid"],
      ["tx_time", "tx_node_id"],
    )
      .projectFields(fields)
      .filter(PredicateExpr.isNotNull("_deletion"));
  }

  deletionRegisterSourceForRequest(
    request: Req,
    table: any,
    graphTier: any,
    historyPosition: any,
    openTxOverlay: any,
  ): any {
    if (!request.requirements.metadata.some((r: any) => r.kind === "DeletionMarkers"))
      return undefined;
    if (graphTier === undefined) throw sre(request, "Coverage");
    if (
      request.visibility !== "Visible" ||
      historyPosition !== undefined ||
      openTxOverlay !== undefined
    ) {
      throw sre(request, "Coverage");
    }
    const s = this.readView.sources.get(request.source);
    let graph;
    if (s?.kind === "WithOverlays" && isPendingLocalOnly(s.overlays))
      graph = this.pendingDeletionWinnerGraph(request);
    else if (this.needsProjectedCurrentSource(request.source.table)) {
      graph = this.projectedDeletionRegisterCurrentSourceGraph(request, graphTier);
    } else graph = deletionRegisterCurrentSourceGraph(table.name, graphTier);
    return { graph, rowUuidField: "row_uuid" };
  }

  async contentVersionSourceForRequest(
    request: Req,
    table: any,
    graphTier: any,
    historyPosition: any,
    openTxOverlay: any,
  ): Promise<any> {
    if (!request.requirements.metadata.some((r: any) => r.kind === "VersionPayloads"))
      return undefined;
    if (graphTier === undefined) throw sre(request, "Coverage");
    if (
      request.visibility !== "Visible" ||
      historyPosition !== undefined ||
      openTxOverlay !== undefined
    ) {
      throw sre(request, "Coverage");
    }
    const graph = this.needsProjectedCurrentSource(request.source.table)
      ? await this.projectedContentCurrentSourceGraph(request, table, graphTier, false, false)
      : contentVersionCurrentSourceGraph(table, graphTier, false);
    return { graph, rowUuidField: "row_uuid" };
  }

  async projectedHistoricalSourceGraph(
    request: Req,
    table: any,
    position: any,
  ): Promise<CurrentSourceGraph> {
    const metrics = this.node.queryEngineReadMetrics;
    try {
      let rows;
      if (this.canUseBoundedHistoricalSource(request.source.table)) {
        metrics.sourceGlobalTimeRangeScans += 1;
        rows = await this.node.boundedHistoricalCurrentRows(request.source.table, position);
      } else {
        metrics.sourceFullScans += 1;
        rows = await this.node.projectedHistoricalCurrentRows(
          request.source.table,
          this.readView.readSchema,
          position,
        );
      }
      const alias = await this.node.ensureSchemaVersionAlias(this.readView.readSchema);
      const [graph, descriptor, metadata] = inlineCurrentGraphWithSourceMetadata(
        table,
        rows,
        alias,
        "historical",
        request.requirements,
      );
      return { graph, descriptor, metadata };
    } catch (e) {
      throw sre(request, "HistoricalStorageCut");
    }
  }

  async projectedMaintainedVisibleCurrentSourceGraph(
    request: Req,
    table: any,
    tier: any,
  ): Promise<CurrentSourceGraph> {
    const graph = await this.projectedContentCurrentSourceGraph(request, table, tier, true, true);
    return { graph, descriptor: currentRowDescriptor(table), metadata: new Map() };
  }

  /** How this branch-view source reads its branch layers. */
  branchLayerAccess(request: Req): any {
    const ap = this.accessPaths.get(request.source);
    if (ap?.kind === "PrimaryKey") return { kind: "PrimaryKey", prefix: ap.values };
    if (ap?.kind === "Index") {
      return {
        kind: "Index",
        column: ap.column,
        orderColumn: ap.orderColumn,
        prefix: ap.prefix,
        intersections: ap.intersections,
        maintained: ap.maintained,
      };
    }
    return { kind: "Full" };
  }

  /** The content and deletion registers of a live branch view. */
  async liveBranchViewRegisters(
    request: Req,
    table: any,
    tier: any,
    head: any,
    base: any,
  ): Promise<[Graph, Graph]> {
    const access = this.branchLayerAccess(request);
    const storedKeys = (key: any) => {
      try {
        return this.node.equivalentStoredBranchKeys(
          request.source.table,
          this.readView.readSchema,
          key,
        );
      } catch {
        throw sre(request, "Coverage");
      }
    };
    const indexed = access.kind === "Index";
    const narrow = indexed && !sourceInRecursiveStep(request);
    const headKeys = storedKeys(head);
    const headContent = await this.projectedBranchContentSourceGraph(
      request,
      table,
      tier,
      headKeys,
      access,
    );
    if (base === undefined) {
      const candidates = narrow ? headContent.project(["row_uuid"]) : undefined;
      return [
        headContent,
        await this.projectedBranchDeletionSourceGraph(request, tier, headKeys, access, candidates),
      ];
    }
    const baseKeys = storedKeys(base);
    const baseContent = await this.projectedBranchContentSourceGraph(
      request,
      table,
      tier,
      baseKeys,
      access,
    );
    const candidates = narrow
      ? GraphBuilder.union([headContent.project(["row_uuid"]), baseContent.project(["row_uuid"])])
      : undefined;
    const headDeletions = await this.projectedBranchDeletionSourceGraph(
      request,
      tier,
      headKeys,
      access,
      candidates,
    );
    const baseDeletions = await this.projectedBranchDeletionSourceGraph(
      request,
      tier,
      baseKeys,
      access,
      candidates,
    );
    let headLayer;
    if (indexed) {
      if (sourceInRecursiveStep(request))
        this.recordBranchLayerRead((m) => (m.sourceBranchRegisterFullScans += 1));
      const presence = this.presence(request, X.Ref.stack(tier), headKeys, {
        kind: "Lookup",
        rowIds: baseContent.project(["row_uuid"]),
      });
      headLayer = X.Layer.maskedBy(headContent, { kind: "Presence", graph: presence });
    } else headLayer = X.Layer.new(headContent);
    return [
      X.compose([headLayer, X.Layer.new(baseContent)]),
      X.compose([X.Layer.new(headDeletions), X.Layer.new(baseDeletions)]),
    ];
  }

  /** How the local delta masks origin under an index prefix: whichever reads fewer rows. */
  async localMaskRead(
    request: Req,
    table: any,
    storedKeys: any[],
    column: string,
    orderColumn: string | undefined,
    prefix: any[],
    originCandidates: Graph,
  ): Promise<any> {
    if (sourceInRecursiveStep(request)) return { kind: "Whole" };
    const readSchema = this.readView.readSchema;
    const metrics = this.node.queryEngineReadMetrics;
    let tableId;
    try {
      tableId = this.node.physicalTableIdForSchema(readSchema, table.name);
    } catch {
      metrics.localMaskCountFallbacks += 1;
      return { kind: "Whole" };
    }
    let pending = 0;
    for (const key of storedKeys) pending += this.node.aheadCurrentKeys.branchRows(tableId, key);
    let settled = 0;
    for (const key of storedKeys) {
      if (settled >= pending) return { kind: "Whole" };
      try {
        settled += await physicalCurrentIndexPrefixRowsUpTo(
          this.node,
          "Global",
          table,
          readSchema,
          key,
          column,
          orderColumn,
          prefix,
          pending - settled,
        );
      } catch {
        metrics.localMaskCountFallbacks += 1;
        return { kind: "Whole" };
      }
    }
    return settled < pending
      ? { kind: "Lookup", rowIds: originCandidates.project(["row_uuid"]) }
      : { kind: "Whole" };
  }

  /** The `row_uuid`s a branch key holds in `refs`, whatever their values. */
  presence(request: Req, refs: any[], storedKeys: any[], among: any): Graph {
    const rowIds =
      among.kind === "Lookup" && !sourceInRecursiveStep(request) ? among.rowIds : undefined;
    const rows: Graph[] = [];
    for (const r of refs) {
      for (const key of storedKeys) {
        try {
          const marker = this.node.physicalCurrentMarkerSourceGraph(
            this.readView.readSchema,
            request.source.table,
            r.physicalClass(),
            key,
            undefined,
          );
          rows.push(
            (rowIds === undefined ? marker : lookupByRowId(marker, rowIds)).project(["row_uuid"]),
          );
        } catch {
          throw sre(request, "SchemaProjection");
        }
      }
    }
    return GraphBuilder.union(rows);
  }

  recordBranchLayerRead(record: (m: any) => void): void {
    if (this.countAccessPathMetrics) record(this.node.queryEngineReadMetrics);
  }

  /** Build the maintained content-winner relation for one logical branch key. */
  async projectedBranchContentSourceGraph(
    request: Req,
    table: any,
    tier: any,
    storedKeys: any[],
    access: any,
  ): Promise<Graph> {
    const required = this.currentProjectionRequiredFields(request, table);
    let target, physicalFields, postWinnerFields;
    try {
      [target, physicalFields] = await this.node.ensurePhysicalCurrentWinnerProjection(
        this.readView.readSchema,
        request.source.table,
      );
      postWinnerFields = this.node.physicalCurrentPostWinnerProjectionFields(
        this.readView.readSchema,
        request.source.table,
        required,
      );
    } catch {
      throw sre(request, "SchemaProjection");
    }
    const refs = X.Ref.stack(tier).length;
    const keys = storedKeys.length;
    this.recordBranchLayerRead((m) => {
      if (access.kind === "Full") m.sourceFullScans += 1;
      else if (access.kind === "PrimaryKey") m.sourcePrimaryKeyScans += 1;
      else m.sourceIndexProbes += refs * keys * (1 + access.intersections.length);
    });
    const classRows = (cls: string): Graph => {
      try {
        return GraphBuilder.union(
          storedKeys.map((key) => {
            switch (access.kind) {
              case "Full":
                return this.node.physicalCurrentBranchSourceGraphWithProjectionTarget(
                  this.readView.readSchema,
                  request.source.table,
                  cls,
                  target,
                  key,
                  undefined,
                );
              case "PrimaryKey":
                return this.node.physicalCurrentBranchSourceGraphWithProjectionTarget(
                  this.readView.readSchema,
                  request.source.table,
                  cls,
                  target,
                  key,
                  rowPrefixScan(access.prefix),
                );
              default:
                return physicalCurrentSourceForIndexScanIn(
                  this.node,
                  cls,
                  table,
                  this.readView.readSchema,
                  key,
                  access.column,
                  access.orderColumn,
                  false,
                  access.prefix,
                  access.intersections,
                  access.maintained,
                  undefined,
                  undefined,
                  target,
                );
            }
          }),
        );
      } catch {
        throw sre(request, "SchemaProjection");
      }
    };
    const originRef = X.Ref.Remote("Origin");
    const origin = classRows(originRef.physicalClass()).project(physicalFields);
    const localMask =
      access.kind === "Index" && tier !== "Global"
        ? await this.localMaskRead(
            request,
            table,
            storedKeys,
            access.column,
            access.orderColumn,
            access.prefix,
            origin,
          )
        : undefined;
    const layers = X.tryRefLayers(tier, (r: any) => {
      if (X.refEq(r, originRef)) return X.Layer.new(origin);
      const rows = classRows(r.physicalClass()).project(physicalFields);
      if (access.kind === "Index" && localMask) {
        return X.Layer.maskedBy(rows, {
          kind: "Presence",
          graph: this.presence(request, [r], storedKeys, localMask),
        });
      }
      return X.Layer.new(rows);
    });
    const content =
      tier === "Global" ? X.compose(layers) : X.compose(layers).project(physicalFields);
    return content.projectFields(postWinnerFields);
  }

  /** Build the maintained deletion-register winner relation for one logical branch key. */
  async projectedBranchDeletionSourceGraph(
    request: Req,
    tier: any,
    storedKeys: any[],
    access: any,
    rowIds: Graph | undefined,
  ): Promise<Graph> {
    const fields = ["branch_key", ...registerStorageFieldNames()];
    let scan: any;
    if (access.kind === "PrimaryKey") scan = rowPrefixScan(access.prefix);
    else if (rowIds === undefined)
      this.recordBranchLayerRead((m) => (m.sourceBranchRegisterFullScans += 1));
    const layers = X.tryRefLayers(tier, (r: any) => {
      try {
        const rows = storedKeys.map((key) => {
          const g = this.node.physicalCurrentMarkerSourceGraph(
            this.readView.readSchema,
            request.source.table,
            r.physicalClass(),
            key,
            scan,
          );
          return rowIds === undefined ? g : lookupByRowId(g, rowIds);
        });
        return X.Layer.new(GraphBuilder.union(rows).project(fields));
      } catch {
        throw sre(request, "SchemaProjection");
      }
    });
    const register = tier === "Global" ? X.compose(layers) : X.compose(layers).project(fields);
    return register.filter(PredicateExpr.isNotNull("_deletion"));
  }

  private globalScan(
    request: Req,
    readTable: any,
    target: any,
    accessPath: CurrentAccessPath | undefined,
    excludeDeleted: boolean,
    rawOutput?: any,
  ): Graph {
    const metrics = this.node.queryEngineReadMetrics;
    const schema = this.readView.readSchema;
    const tableName = request.source.table;
    try {
      switch (accessPath?.kind) {
        case "PrimaryKey":
          if (this.countAccessPathMetrics) metrics.sourcePrimaryKeyScans += 1;
          return this.node.physicalCurrentSourceScanGraphWithProjectionTarget(
            schema,
            tableName,
            "Global",
            target,
            staticScanForPrefix(accessPath.values, 1),
          );
        case "PrimaryKeyPage":
          return this.node.physicalCurrentSourceScanGraphWithProjectionTarget(
            schema,
            tableName,
            "Global",
            target,
            primaryKeyPageScan(accessPath.cap),
          );
        case "Index": {
          const limitAllowed =
            rawOutput === undefined
              ? accessPath.orderColumn !== undefined || !excludeDeleted
              : !excludeDeleted;
          const sourceLimit = limitAllowed ? accessPath.sourceLimit : undefined;
          const candidate = rawOutput === undefined ? accessPath.candidateFilter : undefined;
          metrics.sourceIndexProbes += 1 + accessPath.intersections.length + (candidate ? 1 : 0);
          try {
            return rawOutput === undefined
              ? physicalGlobalCurrentSourceForIndexScan(
                  this.node,
                  readTable,
                  schema,
                  accessPath.column,
                  accessPath.orderColumn,
                  accessPath.reverse,
                  accessPath.prefix,
                  accessPath.intersections,
                  accessPath.maintained,
                  sourceLimit,
                  candidate,
                  target,
                )
              : physicalGlobalCurrentSourceForIndexScanWithOutput(
                  this.node,
                  readTable,
                  schema,
                  accessPath.column,
                  accessPath.orderColumn,
                  accessPath.reverse,
                  accessPath.prefix,
                  accessPath.intersections,
                  accessPath.maintained,
                  sourceLimit,
                  undefined,
                  target,
                  rawOutput,
                );
          } catch {
            throw sre(request, "Coverage");
          }
        }
        default:
          metrics.sourceFullScans += 1;
          return this.node.physicalCurrentSourceGraphWithProjectionTarget(
            schema,
            tableName,
            "Global",
            target,
          );
      }
    } catch (e) {
      if (e && (e as any).gap !== undefined) throw e;
      throw sre(request, "SchemaProjection");
    }
  }

  async projectedContentCurrentSourceGraph(
    request: Req,
    readTable: any,
    tier: any,
    includeGlobalTime: boolean,
    excludeDeleted: boolean,
  ): Promise<Graph> {
    const fields: string[] = globalCurrentStorageFields(readTable, true, includeGlobalTime);
    if (!excludeDeleted) fields.push("_deletion");
    const finish = (content: Graph) =>
      excludeDeleted ? content.filter(X.notDeletedPredicate()) : content.project(fields);
    if (tier === "Global") {
      const target = this.currentProjectionTarget(request, readTable);
      return finish(
        this.globalScan(
          request,
          readTable,
          target,
          this.accessPaths.get(request.source),
          excludeDeleted,
        ),
      );
    }
    const required = this.currentProjectionRequiredFields(request, readTable);
    let target, physicalFields, postWinnerFields, rawGlobalOutput;
    try {
      [target, physicalFields] = await this.node.ensurePhysicalCurrentWinnerProjection(
        this.readView.readSchema,
        request.source.table,
      );
      postWinnerFields = this.node.physicalCurrentPostWinnerProjectionFields(
        this.readView.readSchema,
        request.source.table,
        required,
      );
      const tableId = this.node.physicalTableIdForSchema(
        this.readView.readSchema,
        request.source.table,
      );
      rawGlobalOutput = this.node.database
        .tableSchema(X.physicalGlobalCurrentTableName(tableId))
        .recordSchema();
    } catch {
      throw sre(request, "SchemaProjection");
    }
    const accessPath = this.accessPaths.get(request.source);
    let global = this.globalScan(
      request,
      readTable,
      target,
      accessPath,
      excludeDeleted,
      rawGlobalOutput,
    );
    global = await this.excludeSettledArm(request, global, false);
    const metrics = this.node.queryEngineReadMetrics;
    let content: Graph;
    if (tier === "Global") content = global;
    else {
      global = global.project(physicalFields);
      if (
        accessPath?.kind === "Index" &&
        (excludeDeleted || accessPath.sourceLimit === undefined)
      ) {
        metrics.sourceIndexProbes += 1 + accessPath.intersections.length;
        let ahead;
        try {
          ahead = physicalCurrentSourceForIndexScanIn(
            this.node,
            "Ahead",
            readTable,
            this.readView.readSchema,
            X.BranchKey.default(),
            accessPath.column,
            accessPath.orderColumn,
            accessPath.reverse,
            accessPath.prefix,
            accessPath.intersections,
            accessPath.maintained,
            undefined,
            undefined,
            target,
          );
        } catch {
          throw sre(request, "SchemaProjection");
        }
        const storedKeys = [X.BranchKey.default()];
        const among = await this.localMaskRead(
          request,
          readTable,
          storedKeys,
          accessPath.column,
          accessPath.orderColumn,
          accessPath.prefix,
          global,
        );
        const presence = this.presence(request, [X.Ref.Local], storedKeys, among);
        ahead = (await this.excludeSettledArm(request, ahead, true)).project(physicalFields);
        return finish(
          X.compose([
            X.Layer.maskedBy(ahead, { kind: "Presence", graph: presence }),
            X.Layer.new(global),
          ])
            .project(physicalFields)
            .projectFields(postWinnerFields),
        );
      }
      let ahead;
      try {
        if (accessPath?.kind === "PrimaryKey") {
          ahead = this.node.physicalCurrentSourceScanGraphWithProjectionTarget(
            this.readView.readSchema,
            request.source.table,
            "Ahead",
            target,
            staticScanForPrefix(accessPath.values, 3),
          );
        } else {
          if (accessPath?.kind === "Index") metrics.sourceFullScans += 1;
          ahead = this.node.physicalCurrentSourceGraphWithProjectionTarget(
            this.readView.readSchema,
            request.source.table,
            "Ahead",
            target,
          );
        }
      } catch {
        throw sre(request, "SchemaProjection");
      }
      ahead = (await this.excludeSettledArm(request, ahead, true)).project(physicalFields);
      content = X.compose([X.Layer.new(ahead), X.Layer.new(global)]).project(physicalFields);
    }
    return finish(content.projectFields(postWinnerFields));
  }

  projectedDeletionRegisterCurrentSourceGraph(request: Req, tier: any): Graph {
    const bounded = this.boundedDeletionRegister;
    if (tier === "Global" && bounded && cmpSourceId(bounded[0], request.source) === 0)
      return bounded[1];
    try {
      this.node.physicalTableIdForSchema(this.readView.readSchema, request.source.table);
    } catch {
      throw sre(request, "SchemaProjection");
    }
    const fields = registerStorageFieldsForQueryEngine("");
    const layers = X.tryRefLayers(tier, (r: any) => {
      try {
        return X.Layer.new(
          this.node
            .physicalCurrentMarkerSourceGraph(
              this.readView.readSchema,
              request.source.table,
              r.physicalClass(),
              X.BranchKey.default(),
              undefined,
            )
            .projectFields(fields),
        );
      } catch {
        throw sre(request, "SchemaProjection");
      }
    });
    const register =
      tier === "Global" ? X.compose(layers) : X.compose(layers).projectFields(fields);
    return register.filter(PredicateExpr.isNotNull("_deletion"));
  }

  async projectedVisibleCurrentSourceGraph(
    request: Req,
    table: any,
    tier: any,
  ): Promise<CurrentSourceGraph> {
    const content = await this.projectedContentCurrentSourceGraph(
      request,
      table,
      tier,
      false,
      true,
    );
    return {
      graph: content.projectFields(storageToCanonicalCurrentSourceFields(table, true, false)),
      descriptor: currentRowDescriptor(table),
      metadata: new Map(),
    };
  }

  async projectedIncludeDeletedCurrentSourceGraph(
    request: Req,
    table: any,
    tier: any,
  ): Promise<Graph> {
    const fields = [
      ...storageToCanonicalCurrentSourceFields(table, true, false),
      ProjectField.named("_deletion"),
    ];
    const content = (
      await this.projectedContentCurrentSourceGraph(request, table, tier, false, false)
    ).projectFields(fields);
    return includeDeletedFromImages(table, content);
  }

  canUseBoundedHistoricalSource(table: string): boolean {
    if (!X.schemaVersionEq(this.readView.readSchema, this.node.catalogue.localSchemaVersionId))
      return false;
    return this.needsProjectedCurrentSource(table);
  }

  needsProjectedCurrentSource(table: string): boolean {
    try {
      this.node.physicalTableIdForSchema(this.readView.readSchema, table);
      return true;
    } catch {
      return false;
    }
  }
}

const globalTable = (name: string) => GraphBuilder.table(X.globalCurrentTableName(name));
const aheadTable = (name: string) => GraphBuilder.table(X.aheadCurrentTableName(name));

export function deletionRegisterCurrentSourceGraph(table: string, tier: any): Graph {
  const fields = registerStorageFieldsForQueryEngine("");
  const global = globalTable(table).projectFields(fields);
  const current =
    tier === "Global"
      ? global
      : GraphBuilder.argMaxBy(
          GraphBuilder.union([global, aheadTable(table).projectFields(fields)]),
          ["row_uuid"],
          ["tx_time", "tx_node_id"],
        ).projectFields(fields);
  return current.filter(PredicateExpr.isNotNull("_deletion"));
}

export function contentVersionCurrentSourceGraph(
  table: any,
  tier: any,
  includeGlobalTime: boolean,
): Graph {
  const fields: string[] = maintainedViewHistoryStorageFieldNames(table);
  if (includeGlobalTime) fields.push("global_time");
  if (tier === "Global") return globalTable(table.name).project(fields);
  return GraphBuilder.argMaxBy(
    GraphBuilder.union([
      globalTable(table.name).project(fields),
      aheadTable(table.name).project(fields),
    ]),
    ["row_uuid"],
    ["tx_time", "tx_node_id"],
  ).project(fields);
}

export function selectedVisibleCurrentPrimaryKeyGraph(table: any, tier: any, prefix: any[]): Graph {
  const contentFields = [
    "row_uuid",
    "schema_version",
    "authored_columns",
    ...table.columns.map((c: any) => X.userColumnField(c.name)),
    "created_by",
    "created_at",
    "updated_by",
    "updated_at",
    "tx_time",
    "tx_node_id",
  ];
  const scan = staticScanForPrefix(prefix, 1);
  const imageFields = [...contentFields, "_deletion"];
  const g = GraphBuilder.tableScan(X.globalCurrentTableName(table.name), scan).project(imageFields);
  const current =
    tier === "Global"
      ? g
      : GraphBuilder.argMaxBy(
          GraphBuilder.union([
            g,
            GraphBuilder.tableScan(X.aheadCurrentTableName(table.name), scan).project(imageFields),
          ]),
          ["row_uuid"],
          ["tx_time", "tx_node_id"],
        ).project(imageFields);
  return current.filter(X.notDeletedPredicate()).project(contentFields);
}

function lookupByRowId(rows: Graph, rowIds: Graph): Graph {
  const g = GraphBuilder.lookup(rows, rowIds, ["row_uuid"]);
  if (g === undefined)
    throw nodeError("InvalidStoredValue", "a keyed lookup needs a physical current table source");
  return g;
}

function sourceInRecursiveStep(request: Req): boolean {
  return request.source.path.components.some((r: any) => r.kind === "RecursiveStep");
}

export function rowPrefixScan(prefix: any[]): any {
  return { kind: "Prefix", values: prefix.map((v) => X.LiteralValue.from(v)) };
}

export function registerStorageFieldsForQueryEngine(prefix: string): any[] {
  return registerStorageFieldNames().map((f) => ProjectField.renamed(`${prefix}${f}`, f));
}

export function registerStorageFieldNames(): string[] {
  return [
    "row_uuid",
    "tx_time",
    "tx_node_id",
    "schema_version",
    "created_by",
    "created_at",
    "updated_by",
    "updated_at",
    "_deletion",
  ];
}

export function sourceResolutionError(request: Req, gap: any): any {
  return { request, gap };
}

export function sourceResolutionErrorFromPolicyProofImpl(request: Req, error: any): any {
  if (error?.kind === "PolicyProofCycle") {
    return sourceResolutionError(request, {
      kind: "PolicyProofCycle",
      table: error.table,
      depth: error.depth,
    });
  }
  if (error?.kind === "QueryCapability") {
    const cycle = policyProofCycleFromCapability(String(error.message));
    return sourceResolutionError(
      request,
      cycle ? { kind: "PolicyProofCycle", table: cycle[0], depth: cycle[1] } : "Coverage",
    );
  }
  return sourceResolutionError(request, "Coverage");
}

function policyProofCycleFromCapability(message: string): [string, number] | undefined {
  const marker = 'PolicyProofCycle { table: "';
  const i = message.lastIndexOf(marker);
  if (i < 0) return undefined;
  const rest = message.slice(i + marker.length);
  const q = rest.indexOf('"');
  if (q < 0) return undefined;
  const table = rest.slice(0, q);
  const d = rest.indexOf(", depth: ", q);
  if (d < 0) return undefined;
  const digits = /^\d+/.exec(rest.slice(d + 9));
  return digits ? [table, Number(digits[0])] : undefined;
}

export function policyReadViewProjectedThrough(policyView: any, enclosingView: any): any {
  const projected = { ...policyView, sources: policyView.sources.clone() };
  let frozen: [any, any] | undefined;
  for (const source of enclosingView.sources.valuesArray()) {
    if (source.kind === "SnapshotRef") frozen = [source.snapshot, undefined];
    else if (source.kind === "WithOverlays" && source.input.kind === "SnapshotRef")
      frozen = [source.input.snapshot, source.overlays];
    if (frozen) break;
  }
  for (const [policySource, projectedSource] of [...projected.sources]) {
    const match = [...enclosingView.sources].find(
      ([id]: [SourceId, any]) => id.table === policySource.table,
    );
    if (match) projected.sources.set(policySource, match[1]);
    else if (
      frozen &&
      projectedSource.kind === "VisibleCurrent" &&
      projectedSource.data.kind === "Current"
    ) {
      const snapshotSource = {
        kind: "SnapshotRef",
        projection: projectedSource.projection,
        data: { kind: "Current" },
        snapshot: frozen[0],
      };
      projected.sources.set(
        policySource,
        frozen[1]
          ? { kind: "WithOverlays", input: snapshotSource, overlays: frozen[1] }
          : snapshotSource,
      );
    }
  }
  return projected;
}

export function capabilityTraceEnabled(): boolean {
  return (
    X.envVar("JAZZ_CAPABILITY_TRACE") !== undefined ||
    X.envVar("JAZZ_CAPABILITY_TRACE_FILE") !== undefined
  );
}

export function traceCapabilityCompile(
  nodeUuid: any,
  nodeAlias: any,
  request: any,
  result: { ok?: any; err?: any },
): void {
  const path = X.envVar("JAZZ_CAPABILITY_TRACE_FILE");
  if (path === undefined) {
    if (X.envVar("JAZZ_CAPABILITY_TRACE") !== undefined) {
      console.error(
        "JAZZ_CAPABILITY_TRACE set without JAZZ_CAPABILITY_TRACE_FILE; capability trace skipped",
      );
    }
    return;
  }
  const event = result.err === undefined ? "compile_success" : "compile_failure";
  const lines = [
    `\n=== JAZZ_CAPABILITY_TRACE ${event} pid=0 node_uuid=${debugFmt(nodeUuid)} node_alias=${debugFmt(nodeAlias)} ===`,
  ];
  lines.push(`request:\n${debugFmt(request)}`);
  if (result.err === undefined) {
    lines.push(
      `explain:\n${debugFmt(result.ok.explain)}`,
      `output:\n${debugFmt(result.ok.lowered.output)}`,
    );
  } else lines.push(`report:\n${debugFmt(result.err)}`);
  try {
    X.appendTraceFile(path, lines.join("\n"));
  } catch (err) {
    console.error(`failed to open JAZZ_CAPABILITY_TRACE_FILE ${JSON.stringify(path)}: ${err}`);
  }
}

/** Expose requested nested-author fields on policy preimages that retain complete author records. */
export function withRequestedAuthorPaths(
  graph: Graph,
  descriptor: any,
  metadata: Map<any, any>,
  requirements: any,
): [Graph, any, Map<any, any>] {
  const missing: string[] = requirements.metadata
    .filter((r: any) => r.kind === "AuthorPath" && !X.metadataHas(metadata, r))
    .map((r: any) => r.name);
  if (!missing.length) return [graph, descriptor, metadata];
  const fields = [...descriptor.fields()];
  const projection = X.descriptorFieldNames(descriptor).map((f: string) => ProjectField.named(f));
  for (const name of missing) {
    const [root, path] = X.AuthorSubject.metadataPath(name);
    projection.push(ProjectField.recordField(root, path, name));
    fields.push(DescriptorField.new(name, X.AuthorSubject.metadataType(name)));
    metadata.set({ kind: "AuthorPath", name }, { kind: "Provenance", field: name });
  }
  return [graph.projectFields(projection), RecordDescriptor.newWithFields(fields), metadata];
}

export function resolvedCurrentSourceGraph(
  node: NodeState,
  table: any,
  tier: any,
  requirements: any,
  authorization: any,
  policySchema: any,
  policyReadView: any,
  branchWitnessField: string | undefined,
  selectedBase: Graph | undefined,
): [Graph, any, Map<any, any>, Set<string>] {
  const fields = canonicalCurrentSourceFields(table, false);
  const reqs: any[] = requirements.metadata;
  for (const r of reqs) {
    if (r.kind === "AuthorPath") {
      const [root, path] = X.AuthorSubject.metadataPath(r.name);
      fields.push(ProjectField.recordField(root, path, r.name));
    }
  }
  const metadata = X.newMetadataMap();
  const needsVersionWitnesses = reqs.some(
    (r) => r.kind === "VersionWitnesses" || r.kind === "Provenance",
  );
  const needsSettlePosition = reqs.some((r) => r.kind === "SettlePosition");
  if (needsVersionWitnesses) {
    fields.push(
      ProjectField.literal("table", Value.String(table.name)),
      ProjectField.literal("layer", Value.String("content")),
      ProjectField.named("schema_version"),
      ProjectField.named("authored_columns"),
      ProjectField.renamed("$createdBy", "created_by"),
      ProjectField.renamed("$createdAt", "created_at"),
      ProjectField.renamed("$updatedBy", "updated_by"),
      ProjectField.renamed("$updatedAt", "updated_at"),
    );
    if (branchWitnessField) fields.push(ProjectField.named(branchWitnessField));
    metadata.set(
      { kind: "VersionWitnesses" },
      {
        kind: "VersionWitnesses",
        schemaVersionField: "schema_version",
        txTimeField: "tx_time",
        txNodeField: "tx_node_id",
        branchOrPrefixField: branchWitnessField,
      },
    );
  }
  if (needsSettlePosition) {
    fields.push(ProjectField.named("settle_position"));
    metadata.set(
      { kind: "SettlePosition" },
      { kind: "SettlePosition", settlePositionField: "settle_position" },
    );
  }
  if (reqs.some((r) => r.kind === "Coverage")) {
    fields.push(ProjectField.literal("coverage", Value.String("visible-current")));
    metadata.set({ kind: "Coverage" }, { kind: "Coverage", coverageField: "coverage" });
  }
  for (const r of reqs) {
    if (r.kind === "AuthorPath") metadata.set(r, { kind: "Provenance", field: r.name });
    if (r.kind === "Provenance")
      metadata.set(r, { kind: "Provenance", field: sourceProvenanceField(r.field) });
  }
  const descriptor =
    branchWitnessField === undefined
      ? currentRowDescriptorWithHiddenSourceFieldsForCurrentStorage(table, metadata)
      : currentRowDescriptorWithHiddenSourceFieldsForBranch(table, metadata, true);
  let base: Graph;
  let routingFields = new Set<string>();
  if (authorization.kind === "System") {
    if (selectedBase !== undefined) {
      const selected = storageToCanonicalCurrentSourceFields(
        table,
        needsVersionWitnesses,
        needsSettlePosition,
      );
      if (branchWitnessField) selected.push(ProjectField.named(branchWitnessField));
      base = selectedBase.projectFields(selected);
    } else if (needsVersionWitnesses) {
      base = node
        .maintainedViewContentCurrentWithVersion(table, tier)
        .projectFields(storageToCanonicalCurrentSourceFields(table, true, needsSettlePosition));
    } else
      base = X.visibleCurrentGraph(table, tier).projectFields(
        canonicalCurrentSourceFields(table, false),
      );
  } else {
    const { permissionSubject, plan } = authorization;
    if (
      plan.protectedSource.table !== table.name ||
      plan.role !== "Read" ||
      plan.protectedRowField !== "row_uuid"
    ) {
      throw nodeError(
        "QueryCapability",
        "policy authorization plan does not match resolved source",
      );
    }
    const policyRequest = X.tryResult(() => {
      const r = node.tableReadPolicyAuthorizationRequest(
        policySchema,
        table.name,
        permissionSubject,
        plan.bindingSourceShape !== undefined ? "RetainAllParams" : "InlineAllReachableSeeds",
        tier,
        plan.bindingSourceShape,
        plan.bindingUserParams,
        plan.bindingClaimParams,
      );
      if (policyReadView)
        r.reads.primary = policyReadViewProjectedThrough(r.reads.primary, policyReadView);
      return r;
    });
    const outputFields: string[] = globalCurrentStorageFields(
      table,
      needsVersionWitnesses,
      needsSettlePosition,
    );
    if (branchWitnessField) outputFields.push(branchWitnessField);
    const storageGraph = node.composePolicyFilteredCurrentSourceGraph(
      policyRequest,
      selectedBase ?? node.maintainedViewContentCurrentWithVersion(table, tier),
      outputFields,
    );
    const canonical = storageToCanonicalCurrentSourceFields(
      table,
      needsVersionWitnesses,
      needsSettlePosition,
    );
    if (branchWitnessField) canonical.push(ProjectField.named(branchWitnessField));
    for (const f of storageGraph.routeFields) canonical.push(ProjectField.named(f));
    base = storageGraph.graph.projectFields(canonical);
    routingFields = storageGraph.routeFields;
  }
  for (const f of routingFields) fields.push(ProjectField.named(f));
  return [
    metadata.size === 0 ? base : base.projectFields(fields),
    descriptor,
    metadata,
    routingFields,
  ];
}

const PROVENANCE_PUBLIC = ["$createdBy", "$createdAt", "$updatedBy", "$updatedAt"];

function userColumnFields(table: any): any[] {
  return table.columns.map((c: any) =>
    ProjectField.namedWithIdentity(X.userColumnField(c.name), FieldIdentity.Name(c.name)),
  );
}

function canonicalCurrentSourceFields(table: any, includeVersion: boolean): any[] {
  const fields = [
    ProjectField.named("row_uuid"),
    ...userColumnFields(table),
    ...PROVENANCE_PUBLIC.map((f) => ProjectField.named(f)),
    ProjectField.named("tx_time"),
    ProjectField.named("tx_node_id"),
  ];
  if (includeVersion)
    fields.push(ProjectField.named("schema_version"), ProjectField.named("authored_columns"));
  return fields;
}

function sourceProvenanceField(field: string): string {
  return (
    {
      CreatedAt: "$createdAt",
      CreatedBy: "$createdBy",
      UpdatedAt: "$updatedAt",
      UpdatedBy: "$updatedBy",
    } as any
  )[field];
}

export function storageToCanonicalCurrentSourceFields(
  table: any,
  includeVersion: boolean,
  includeSettlePosition: boolean,
): any[] {
  const fields = [
    ProjectField.named("row_uuid"),
    ...userColumnFields(table),
    ProjectField.renamed("created_by", "$createdBy"),
    ProjectField.renamed("created_at", "$createdAt"),
    ProjectField.renamed("updated_by", "$updatedBy"),
    ProjectField.renamed("updated_at", "$updatedAt"),
    ProjectField.named("tx_time"),
    ProjectField.named("tx_node_id"),
  ];
  if (includeVersion)
    fields.push(ProjectField.named("schema_version"), ProjectField.named("authored_columns"));
  if (includeSettlePosition) fields.push(ProjectField.renamed("global_time", "settle_position"));
  return fields;
}

function branchViewStorageSourceFields(table: any, head: any): any[] {
  const headValues = new Map<string, any>(head.values);
  const fields = [
    ProjectField.renamed("branch_key", "supplying_branch_key"),
    ProjectField.named("row_uuid"),
    ProjectField.named("schema_version"),
    ProjectField.named("authored_columns"),
  ];
  for (const column of table.columns) {
    if (table.branchBy.includes(column.name)) {
      const encoded = headValues.get(column.name);
      if (encoded === undefined)
        throw nodeError("InvalidBranchKey", "head branch key missing projected table column");
      let value;
      try {
        value = encoded.decode();
      } catch {
        throw nodeError("InvalidBranchKey", "invalid head branch value encoding");
      }
      fields.push(
        ProjectField.literalWithIdentity(
          X.userColumnField(column.name),
          value,
          FieldIdentity.Name(column.name),
        ),
      );
    } else
      fields.push(
        ProjectField.namedWithIdentity(
          X.userColumnField(column.name),
          FieldIdentity.Name(column.name),
        ),
      );
  }
  for (const f of [
    "created_by",
    "created_at",
    "updated_by",
    "updated_at",
    "tx_time",
    "tx_node_id",
    "global_time",
  ]) {
    fields.push(ProjectField.named(f));
  }
  return fields;
}

export function currentRowDescriptorWithHiddenSourceFields(
  table: any,
  metadata: Map<any, any>,
): any {
  return currentRowDescriptorWithHiddenSourceFieldsForBranch(table, metadata, false);
}

/** The current-source compatibility boundary is the schema's canonical current-row representation. */
export function currentRowDescriptorWithHiddenSourceFieldsForCurrentStorage(
  table: any,
  metadata: Map<any, any>,
): any {
  const logical = currentRowDescriptorWithHiddenSourceFields(table, metadata);
  const current = table.globalCurrentStorageTable().recordSchema();
  const currentTypes = new Map<string, [any, any]>();
  for (const column of table.columns) {
    const index = current.fieldIndex(X.appStorageColumnName(column.name));
    const field = index === undefined ? undefined : current.fields()[index];
    if (field) currentTypes.set(X.userColumnField(column.name), [field.identity, field.valueType]);
  }
  return RecordDescriptor.newWithFields(
    logical.fields().map((field: any) => {
      const name: string = field.name;
      const hit = currentTypes.get(name);
      if (!hit) return { name, identity: field.identity, valueType: field.valueType };
      const [storageIdentity, valueType] = hit;
      const li = field.identity;
      let identity;
      if (li?.kind === "Name" && storageIdentity?.kind === "Slot")
        identity = { kind: "NamedSlot", name: li.name, slot: storageIdentity.slot };
      else if (li?.kind === "Name" && storageIdentity?.kind === "NamedSlot") {
        identity = { kind: "NamedSlot", name: li.name, slot: storageIdentity.slot };
      } else if (
        li?.kind === "Name" &&
        storageIdentity?.kind === "Name" &&
        storageIdentity.name === name &&
        li.name !== storageIdentity.name
      ) {
        identity = li;
      } else identity = storageIdentity ?? li;
      return { name, identity, valueType };
    }),
  );
}

export function currentRowDescriptorWithHiddenSourceFieldsForBranch(
  table: any,
  metadata: Map<any, any>,
  nonNullableBranch: boolean,
): any {
  return currentRowDescriptorWithHiddenSourceFieldsForBranchAndDeletion(
    table,
    metadata,
    nonNullableBranch,
    false,
  );
}

const VT = X.ValueType;

export function currentRowDescriptorWithHiddenSourceFieldsForBranchAndDeletion(
  table: any,
  metadata: Map<any, any>,
  nonNullableBranch: boolean,
  includeDeletionMarker: boolean,
): any {
  const df = (n: string, t: any) => DescriptorField.new(n, t);
  const fields = [
    df("row_uuid", VT.Uuid),
    ...table.columns.map((c: any) => {
      const t = X.currentRowColumnType(c);
      return currentRowColumnField(
        c,
        nonNullableBranch && table.branchBy.includes(c.name) ? t : VT.Nullable(t),
      );
    }),
    df("$createdBy", X.RowAuthor.valueType()),
    df("$createdAt", VT.U64),
    df("$updatedBy", X.RowAuthor.valueType()),
    df("$updatedAt", VT.U64),
    df("tx_time", VT.U64),
    df("tx_node_id", VT.U64),
  ];
  const has = (kind: string) => X.metadataHas(metadata, { kind });
  for (const [r] of metadata)
    if (r.kind === "AuthorPath") fields.push(df(r.name, X.AuthorSubject.metadataType(r.name)));
  if (has("VersionWitnesses")) {
    fields.push(
      df("table", VT.String),
      df("layer", VT.String),
      df("schema_version", VT.U64),
      df("authored_columns", VT.Nullable(VT.Array(VT.U64))),
      df("created_by", X.RowAuthor.valueType()),
      df("created_at", VT.U64),
      df("updated_by", X.RowAuthor.valueType()),
      df("updated_at", VT.U64),
    );
    const vw = X.metadataGet(metadata, { kind: "VersionWitnesses" });
    if (vw?.branchOrPrefixField !== undefined) fields.push(df(vw.branchOrPrefixField, VT.Bytes));
  }
  if (nonNullableBranch && !has("VersionWitnesses"))
    fields.push(df("supplying_branch_key", VT.Bytes));
  if (has("SettlePosition")) fields.push(df("settle_position", VT.Nullable(VT.U64)));
  if (has("Coverage")) fields.push(df("coverage", VT.String));
  if (includeDeletionMarker) fields.push(df("__jazz_deleted", VT.Bool));
  return RecordDescriptor.newWithFields(fields);
}

/** Select physical current-source access paths from the normalized program itself. */
export function queryProgramAccessPaths(
  st: NodeState,
  request: any,
  allowSecondaryIndexes: boolean,
): SortedMap<SourceId, CurrentAccessPath> {
  for (const node of request.input.shape.nodes.valuesArray()) {
    if (node.kind === "Union" || node.kind === "Join" || node.kind === "RecursiveRelation")
      return new SortedMap(cmpSourceId);
  }
  return normalizedProgramAccessPaths(
    st,
    request.input,
    request.reads.primary,
    request.policy,
    false,
    allowSecondaryIndexes,
  );
}

function normalizedProgramAccessPaths(
  st: NodeState,
  input: any,
  readView: any,
  policy: any,
  allowLocal: boolean,
  allowSecondaryIndexes: boolean,
): SortedMap<SourceId, CurrentAccessPath> {
  const bySource = new SortedMap<SourceId, Map<string, any>>(cmpSourceId);
  for (const nodeId of input.shape.nodes.keysArray()) {
    const equalities = normalizedProgramEqualities(input.shape, nodeId, input.binding, policy);
    if (!equalities) continue;
    for (const [source, eq] of equalities) {
      const m = bySource.getOrInsert(source, () => new Map());
      for (const [k, v] of eq) m.set(k, v);
    }
  }
  const paths = new SortedMap<SourceId, CurrentAccessPath>(cmpSourceId);
  for (const [source, eq] of bySource) {
    const path = guardedCurrentAccessPath(
      st,
      readView,
      source,
      eq,
      allowLocal,
      allowSecondaryIndexes,
      "Ordinary",
      undefined,
    );
    if (path) paths.set(source, path);
  }
  return paths;
}

/** The single admission guard for a physical current-source access path. */
function guardedCurrentAccessPath(
  st: NodeState,
  readView: any,
  source: SourceId,
  equalities: Map<string, any>,
  allowLocal: boolean,
  allowSecondaryIndexes: boolean,
  selector: AccessPathSelector,
  coveredColumn: string | undefined,
): CurrentAccessPath | undefined {
  const tier = readView.sourceCurrentTier(source);
  if (tier !== "Global" && tier !== "Local") return undefined;
  const table = st.tableInSchemaRef(source.table, readView.readSchema);
  const path: CurrentAccessPath | undefined =
    selector === "Ordinary"
      ? (selectCurrentAccessPath(table, equalities) ??
        (coveredColumn !== undefined
          ? selectCompositeLeadingEqualityAccessPath(table, equalities, coveredColumn)
          : undefined))
      : selectCompositeEqualityAccessPath(table, equalities);
  if (!path) return undefined;
  if (!allowSecondaryIndexes && path.kind === "Index") return undefined;
  if (!allowLocal && path.kind === "Index") path.maintained = true;
  return path;
}

export function oneShotAccessPaths(
  st: NodeState,
  shape: any,
  binding: any,
  tier: any,
): SortedMap<SourceId, CurrentAccessPath> {
  if (tier !== "Local" && tier !== "Global") return new SortedMap(cmpSourceId);
  const normalized = normalizedRowSetShape(st, shape, binding);
  const input = {
    binding: st.programBindingForShape(shape, binding, undefined, new Map(), new Map()),
    shape: normalized,
  };
  const reads = X.currentQueryReadSet(
    input.shape,
    shape.schemaVersion(),
    shape.schemaVersion(),
    tier,
    undefined,
    undefined,
    false,
  );
  const paths = normalizedProgramAccessPaths(
    st,
    input,
    reads.primary,
    { kind: "System" },
    true,
    true,
  );
  const query = shape.query();
  if (query.limit === undefined) return paths;
  if (
    query.offset !== 0 ||
    query.orderBy.length ||
    query.joins.length ||
    query.flatJoin ||
    query.policyBranches.length ||
    query.reachable.length ||
    query.inherits.length ||
    query.includes.length ||
    query.arraySubqueries.length ||
    query.aggregate
  ) {
    return paths;
  }
  const root = rootSourceId(query.table);
  if (st.tableInSchemaRef(query.table, shape.schemaVersion()).hasAnyPolicy()) return paths;
  const ap = paths.get(root);
  if (tier === "Global" && ap?.kind === "Index") ap.sourceLimit = query.limit;
  return paths;
}
/** Binding-specific access paths for an executing current query. */
export function currentQueryHydrationAccessPaths(
  st: NodeState,
  request: any,
  shape: any,
  binding: any,
  lifetime: HydrationLifetime,
): SortedMap<SourceId, CurrentAccessPath> {
  let paths: SortedMap<SourceId, CurrentAccessPath>;
  if (lifetime === "Retained") {
    paths = currentQueryPrimaryKeyAccessPaths(st, shape, binding);
  } else {
    const tier =
      request.reads.primary.sourceCurrentTier(rootSourceId(shape.query().table)) ?? "None";
    paths = new SortedMap(cmpSourceId);
    for (const [s, p] of oneShotAccessPaths(st, shape, binding, tier))
      if (p.kind === "PrimaryKey") paths.set(s, p);
  }
  for (const [s, p] of queryProgramAccessPaths(st, request, true))
    if (p.kind === "Index") paths.set(s, p);
  if (lifetime !== "FirstResult") return paths;
  const query = shape.query();
  const root = rootSourceId(query.table);
  if (
    !query.flatJoin &&
    !query.policyBranches.length &&
    !query.reachable.length &&
    !query.inherits.length &&
    !query.arraySubqueries.length &&
    !query.aggregate &&
    !query.relation
  ) {
    if (!paths.get(root)) {
      const path = guardedCurrentAccessPath(
        st,
        request.reads.primary,
        root,
        rootLiteralEqualities(query, binding),
        true,
        true,
        "Ordinary",
        undefined,
      );
      if (path?.kind === "Index") paths.set(root, path);
    }
    const existing = paths.get(root);
    const replaceable =
      !existing ||
      (existing.kind === "Index" &&
        existing.orderColumn === undefined &&
        existing.sourceLimit === undefined);
    if (replaceable && query.limit === undefined && !query.orderBy.length) {
      const path = guardedCurrentAccessPath(
        st,
        request.reads.primary,
        root,
        rootLiteralEqualities(query, binding),
        true,
        true,
        "CompositeEquality",
        undefined,
      );
      if (path?.kind === "Index") {
        if (existing?.kind === "Index") path.maintained = existing.maintained || path.maintained;
        paths.set(root, path);
      }
    }
  }
  const rootTier = request.reads.primary.sourceCurrentTier(root);
  const rp = paths.get(root);
  // A first-result owner retires this graph after hydration: intersect durable index keys first.
  if (
    (rootTier === "Global" || rootTier === "Local") &&
    rp?.kind === "Index" &&
    rp.intersections.length
  )
    rp.maintained = false;
  if (
    query.joins.length === 1 &&
    !query.flatJoin &&
    !query.policyBranches.length &&
    !query.reachable.length &&
    !query.inherits.length &&
    !query.arraySubqueries.length &&
    !query.relation
  ) {
    const join = query.joins[0];
    if (
      join.target === "Column" &&
      !join.sourceColumn &&
      !join.sourceLookup &&
      !join.correlatedFilters.length &&
      !join.nestedJoins.length
    ) {
      const idValue = rootLiteralEqualities(query, binding).get("id");
      const boundRootId = idValue?.kind === "Uuid" ? idValue.value : undefined;
      const equalities =
        boundRootId !== undefined
          ? new Map([[join.onColumn, Value.Uuid(boundRootId)]])
          : literalEqualitiesForFilters(join.filters, binding);
      const joinSource = singleRootJoinSource(request, join.table);
      if (joinSource) {
        const bothGlobal = [root, joinSource].every(
          (s) => request.reads.primary.sourceCurrentTier(s) === "Global",
        );
        const coveredJoinKey =
          boundRootId === undefined &&
          bothGlobal &&
          query.limit === undefined &&
          !query.orderBy.length &&
          !query.aggregate
            ? (join.onColumn as string)
            : undefined;
        const path = guardedCurrentAccessPath(
          st,
          request.reads.primary,
          joinSource,
          equalities,
          true,
          true,
          "Ordinary",
          coveredJoinKey,
        );
        if (path?.kind === "Index") {
          if (coveredJoinKey !== undefined) {
            const joinTable = st.tableInSchema(join.table, request.reads.primary.readSchema);
            attachCoveredJoinKeyFilter(
              joinTable,
              coveredJoinKey,
              query.table,
              paths.get(root),
              path,
            );
          }
          paths.set(joinSource, path);
        }
      }
    }
  }
  return paths;
}

export function currentQueryPrimaryKeyAccessPaths(
  st: NodeState,
  shape: any,
  binding: any,
): SortedMap<SourceId, CurrentAccessPath> {
  const query = shape.query();
  const accessPaths = new SortedMap<SourceId, CurrentAccessPath>(cmpSourceId);
  const equalities = rootLiteralEqualities(query, binding);
  st.tableInSchemaRef(query.table, shape.schemaVersion());
  // Policy-bearing roots take the point path too (#3511).
  const id = equalities.get("id");
  if (id !== undefined)
    accessPaths.set(rootSourceId(query.table), { kind: "PrimaryKey", values: [id] });
  addReachableAccessPaths(st, query, shape.schemaVersion(), binding, accessPaths);
  return accessPaths;
}

function addReachableAccessPaths(
  st: NodeState,
  query: any,
  schemaVersion: any,
  binding: any,
  accessPaths: SortedMap<SourceId, CurrentAccessPath>,
): void {
  query.reachable.forEach((reachable: any, index: number) => {
    const reachableId = `reachable:${index}`;
    const add = (source: SourceId, table: string, filters: any[]) =>
      addPrimaryKeyAccessPathForFilters(
        st,
        source,
        table,
        schemaVersion,
        filters,
        binding,
        accessPaths,
      );
    if (reachable.seed)
      add(
        reachableSeedSourceId(reachable.seed, reachableId),
        reachable.seed.table,
        reachable.seed.filters,
      );
    add(reachableEdgeSourceId(reachable, reachableId), reachable.edgeTable, reachable.edgeFilters);
    add(
      reachableAccessSourceId(reachable, reachableId),
      reachable.accessTable,
      reachable.accessFilters,
    );
  });
}

function addPrimaryKeyAccessPathForFilters(
  st: NodeState,
  source: SourceId,
  tableName: string,
  schemaVersion: any,
  filters: any[],
  binding: any,
  accessPaths: SortedMap<SourceId, CurrentAccessPath>,
): void {
  const table = st.tableInSchemaRef(tableName, schemaVersion);
  const path = selectCurrentAccessPath(table, literalEqualitiesForFilters(filters, binding));
  if (path?.kind === "PrimaryKey") accessPaths.set(source, path);
}

export function physicalGlobalCurrentSourceForIndexScan(
  st: NodeState,
  table: any,
  schemaVersion: any,
  column: string,
  orderColumn: string | undefined,
  reverse: boolean,
  prefix: any[],
  intersections: [string, any[]][],
  maintained: boolean,
  sourceLimit: number | undefined,
  candidateFilter: CurrentIndexCandidateFilter | undefined,
  projectionTarget: string,
): Graph {
  return physicalGlobalCurrentSourceForIndexScanWithOutput(
    st,
    table,
    schemaVersion,
    column,
    orderColumn,
    reverse,
    prefix,
    intersections,
    maintained,
    sourceLimit,
    candidateFilter,
    projectionTarget,
    table.globalCurrentStorageTable().recordSchema(),
  );
}

export function physicalGlobalCurrentSourceForIndexScanWithOutput(
  st: NodeState,
  table: any,
  schemaVersion: any,
  column: string,
  orderColumn: string | undefined,
  reverse: boolean,
  prefix: any[],
  intersections: [string, any[]][],
  maintained: boolean,
  sourceLimit: number | undefined,
  candidateFilter: CurrentIndexCandidateFilter | undefined,
  projectionTarget: string,
  _output: any,
): Graph {
  return physicalCurrentSourceForIndexScanIn(
    st,
    "Global",
    table,
    schemaVersion,
    X.BranchKey.default(),
    column,
    orderColumn,
    reverse,
    prefix,
    intersections,
    maintained,
    sourceLimit,
    candidateFilter,
    projectionTarget,
  );
}

function tableMapping(st: NodeState, schemaVersion: any, table: string, message: string): any {
  const mapping = st.catalogue.physicalMappings.get(schemaVersion)?.tables.get(table);
  if (!mapping) throw nodeError("InvalidStoredValue", message);
  return mapping;
}

function columnId(mapping: any, column: string, message: string): any {
  const id = mapping.columns.get(column);
  if (id === undefined) throw nodeError("InvalidStoredValue", message);
  return id;
}

/** How many rows an index-prefix read of `class` starts from, counting no further than `maxItems`. */
export async function physicalCurrentIndexPrefixRowsUpTo(
  st: NodeState,
  cls: any,
  table: any,
  schemaVersion: any,
  branchKey: any,
  column: string,
  orderColumn: string | undefined,
  prefix: any[],
  maxItems: number,
): Promise<number> {
  const mapping = tableMapping(
    st,
    schemaVersion,
    table.name,
    "physical current index table mapping missing",
  );
  const col = (c: string) => columnId(mapping, c, "physical current index column mapping missing");
  const index =
    orderColumn !== undefined
      ? X.physicalCurrentCompositeIndexName([col(column), col(orderColumn)])
      : X.physicalCurrentIndexName(col(column));
  const storageTable = st.physicalCurrentTableForSchema(schemaVersion, table.name, cls);
  const scanPrefix = [Value.Bytes(branchKey.canonicalBytes()), ...prefix];
  return await st.database.indexPrefixEntriesUpTo(storageTable, index, scanPrefix, maxItems);
}

/** Read one physical current class under an index prefix of one branch key. */
export function physicalCurrentSourceForIndexScanIn(
  st: NodeState,
  cls: any,
  table: any,
  schemaVersion: any,
  branchKey: any,
  column: string,
  orderColumn: string | undefined,
  reverse: boolean,
  prefix: any[],
  intersectionsIn: [string, any[]][],
  maintained: boolean,
  sourceLimit: number | undefined,
  candidateFilter: CurrentIndexCandidateFilter | undefined,
  projectionTarget: string,
): Graph {
  const mapping = tableMapping(
    st,
    schemaVersion,
    table.name,
    "physical current index table mapping missing",
  );
  const colId = columnId(mapping, column, "physical current index column mapping missing");
  const storageTable = st.physicalCurrentTableForSchema(schemaVersion, table.name, cls);
  // Physical current indexes lead with the branch key so branch-local keys cannot alias the shared domain.
  const indexPrefix = (p: any[]) =>
    [Value.Bytes(branchKey.canonicalBytes()), ...p].map((v) => X.LiteralValue.from(v));
  const scanPrefix = indexPrefix(prefix);
  const scan =
    sourceLimit !== undefined
      ? {
          kind: reverse ? "ReversePrefixLimit" : "PrefixLimit",
          prefix: scanPrefix,
          maxItems: sourceLimit,
        }
      : { kind: "Prefix", prefix: scanPrefix };
  const intersections = intersectionsIn.map(([c, p]): [any, any] => [
    X.physicalCurrentIndexName(
      columnId(mapping, c, "physical current intersected index column mapping missing"),
    ),
    { kind: "Prefix", prefix: indexPrefix(p) },
  ]);
  const primaryIndex =
    orderColumn !== undefined
      ? X.physicalCurrentCompositeIndexName([
          colId,
          columnId(mapping, orderColumn, "physical current ordered index column mapping missing"),
        ])
      : X.physicalCurrentIndexName(colId);
  if (maintained) {
    // Model each equality as an index source and express the intersection in IVM.
    let graph = GraphBuilder.variantIndexScan(storageTable, primaryIndex, projectionTarget, scan);
    for (const [index, s] of intersections) {
      graph = GraphBuilder.semiJoin(
        graph,
        GraphBuilder.variantIndexScan(storageTable, index, projectionTarget, s),
        ["row_uuid"],
        ["row_uuid"],
      );
    }
    return graph;
  }
  if (candidateFilter && !intersections.length && sourceLimit === undefined) {
    const filter = candidateFilter;
    const candidateMapping = tableMapping(
      st,
      schemaVersion,
      filter.table,
      "candidate index table mapping missing",
    );
    const cc = (c: string) =>
      columnId(candidateMapping, c, "candidate index column mapping missing");
    const candidateColumnId = cc(filter.column);
    const candidateIndex =
      filter.orderColumn !== undefined
        ? X.physicalCurrentCompositeIndexName([candidateColumnId, cc(filter.orderColumn)])
        : X.physicalCurrentIndexName(candidateColumnId);
    const sourceColumnId = columnId(
      mapping,
      filter.sourceColumn,
      "candidate-filtered source column mapping missing",
    );
    return GraphBuilder.variantIndexCandidateScan(
      storageTable,
      primaryIndex,
      scan,
      {
        table: X.physicalGlobalCurrentTableName(candidateMapping.tableId),
        index: candidateIndex,
        scan: { kind: "Prefix", prefix: indexPrefix(filter.prefix) },
        sourceColumn: X.physicalUserColumnField(sourceColumnId),
        candidateColumn: "row_uuid",
      },
      projectionTarget,
    );
  }
  return GraphBuilder.variantIndexIntersectionScan(
    storageTable,
    primaryIndex,
    scan,
    intersections,
    projectionTarget,
  );
}

/** Conservative equality restrictions safe to push into a physical source. */
function normalizedProgramEqualities(
  shape: any,
  nodeId: string,
  binding: any,
  policy: any,
): Map<SourceId, Map<string, any>> | undefined {
  const node = shape.nodes.get(nodeId);
  if (!node) throw nodeError("InvalidStoredValue", "normalized access-path node missing");
  switch (node.kind) {
    case "Filter": {
      const equalities = normalizedProgramEqualities(shape, node.input, binding, policy);
      if (!equalities) return undefined;
      collectNormalizedProgramEqualities(node.predicate, binding, policy, equalities);
      return equalities;
    }
    case "Distinct":
    case "OrderBy":
    case "Project":
    case "Slice":
      return normalizedProgramEqualities(shape, node.input, binding, policy);
    case "Source":
      return new Map([[node.source, new Map()]]);
    default:
      return undefined;
  }
}

function collectNormalizedProgramEqualities(
  predicate: any,
  binding: any,
  policy: any,
  equalities: Map<SourceId, Map<string, any>>,
): void {
  if (predicate.kind === "And") {
    for (const p of predicate.predicates)
      collectNormalizedProgramEqualities(p, binding, policy, equalities);
  } else if (predicate.kind === "Compare" && predicate.op === "Eq") {
    // Do not descend into Or: an equality within one branch does not restrict the other.
    const hit =
      normalizedProgramEquality(predicate.left, predicate.right, binding, policy) ??
      normalizedProgramEquality(predicate.right, predicate.left, binding, policy);
    if (hit) {
      const [source, field, value] = hit;
      let m = equalities.get(source);
      if (!m) equalities.set(source, (m = new Map()));
      if (!m.has(field)) m.set(field, value);
    }
  }
}

function normalizedProgramEquality(
  field: any,
  value: any,
  binding: any,
  policy: any,
): [SourceId, string, any] | undefined {
  const sf = normalizedProgramSourceField(field);
  if (!sf) return undefined;
  const v = normalizedBoundValue(value, binding, policy);
  // The physical current index stores its own nullable envelope.
  if (v === undefined || v.kind === "Nullable") return undefined;
  return [sf[0], sf[1], v];
}

function normalizedProgramSourceField(value: any): [SourceId, string] | undefined {
  if (value.kind === "SourceField") return [value.source, value.field];
  if (value.kind === "RowId" && value.ref.kind === "Source") return [value.ref.source, "id"];
  return undefined;
}

export function normalizedBoundValue(value: any, binding: any, policy: any): any | undefined {
  switch (value.kind) {
    case "Param":
      return binding.values.get(value.name);
    case "Literal":
      try {
        return X.decodeValue(value.bytes);
      } catch (err) {
        throw nodeError("QueryLowering", `literal decoding failed: ${err}`);
      }
    case "Claim":
      // System reads have no identity to bind a claim; keep the ordinary scan.
      if (policy.kind === "System") return undefined;
      return X.preparedClaimValue(value.path, policy);
    default:
      return undefined;
  }
}

export function currentRowFields(table: any): string[] {
  return X.currentRowFieldNames(table);
}

export function globalCurrentStorageFields(
  table: any,
  includeVersion: boolean,
  includeSettlePosition: boolean,
): string[] {
  const fields = ["row_uuid"];
  if (includeVersion) fields.push("schema_version", "authored_columns");
  fields.push(...table.columns.map((c: any) => X.userColumnField(c.name)));
  fields.push("created_by", "created_at", "updated_by", "updated_at", "tx_time", "tx_node_id");
  if (includeSettlePosition) fields.push("global_time");
  return fields;
}

function currentRowDescriptor(table: any): any {
  return currentRowDescriptorWithHiddenSourceFields(table, new Map());
}

export function emptyAuthorizedRowIdGraph(): Graph {
  return GraphBuilder.inlineRecords(RecordDescriptor.new([["row_uuid", VT.Uuid]]), []);
}

function provenanceOf(row: any): any {
  const p = row.provenance();
  if (!p)
    throw nodeError("InvalidStoredValue", "physical current source row is missing provenance");
  return p;
}

function txAliasValues(row: any): any[] {
  const [txTime, txNode] = row.projectedTxAlias() ?? [0, 0];
  return [Value.U64(txTime), Value.U64(txNode)];
}

function inlineCurrentRecord(table: any, descriptor: any, row: any): Uint8Array {
  const values: any[] = [Value.Uuid(row.rowUuid())];
  for (const c of table.columns) values.push(Value.Nullable(row.cell(table, c.name)));
  const p = provenanceOf(row);
  values.push(
    rowAuthorValue(p.createdBy),
    Value.U64(p.createdAt),
    rowAuthorValue(p.updatedBy),
    Value.U64(p.updatedAt),
    ...txAliasValues(row),
  );
  appendAuthorProjectionValues(values, descriptor, p.createdBy, p.updatedBy);
  return descriptor.create(values);
}

export function inlineCurrentGraph(table: any, rows: any[]): Graph {
  const descriptor = currentRowDescriptor(table);
  return GraphBuilder.inlineRecords(
    descriptor,
    rows.map((r) => inlineCurrentRecord(table, descriptor, r)),
  );
}

type InlineGraph = [Graph, any, Map<any, any>];

export function inlineCurrentGraphWithSourceMetadata(
  table: any,
  rows: any[],
  alias: any,
  coverage: string,
  requirements: any,
): InlineGraph {
  return inlineCurrentGraphWithSourceMetadataAndBranchWitness(
    table,
    rows,
    alias,
    coverage,
    requirements,
    undefined,
  );
}

export function inlineCurrentGraphWithSourceMetadataAndBranchWitness(
  table: any,
  rows: any[],
  alias: any,
  coverage: string,
  requirements: any,
  branchWitness: [string, any] | undefined,
): InlineGraph {
  const metadata = inlineSourceMetadata(requirements, branchWitness?.[0]);
  const descriptor = currentRowDescriptorWithHiddenSourceFieldsForBranch(
    table,
    metadata,
    branchWitness !== undefined,
  );
  const records = rows.map((r) =>
    inlineCurrentRecordWithSourceMetadata(table, descriptor, r, alias, coverage, branchWitness),
  );
  return [GraphBuilder.inlineRecords(descriptor, records), descriptor, metadata];
}

export function inlineSourceMetadata(
  requirements: any,
  branchWitnessField: string | undefined,
): Map<any, any> {
  const metadata = new Map<any, any>();
  const reqs: any[] = [...requirements.metadata];
  const has = (kind: string) => reqs.some((r) => r.kind === kind);
  // Provenance-only requirements still need the full version witness.
  if (has("VersionWitnesses") || has("Provenance")) {
    metadata.set(
      { kind: "VersionWitnesses" },
      {
        kind: "VersionWitnesses",
        schemaVersionField: "schema_version",
        txTimeField: "tx_time",
        txNodeField: "tx_node_id",
        branchOrPrefixField: branchWitnessField,
      },
    );
  }
  if (has("Coverage"))
    metadata.set({ kind: "Coverage" }, { kind: "Coverage", coverageField: "coverage" });
  if (has("SettlePosition"))
    metadata.set(
      { kind: "SettlePosition" },
      { kind: "SettlePosition", settlePositionField: "settle_position" },
    );
  for (const r of reqs) {
    if (r.kind === "AuthorPath") metadata.set(r, { kind: "Provenance", field: r.name });
    if (r.kind === "Provenance")
      metadata.set(r, { kind: "Provenance", field: sourceProvenanceField(r.field) });
  }
  return metadata;
}

function inlineCurrentRecordWithSourceMetadata(
  table: any,
  descriptor: any,
  row: any,
  alias: any,
  coverage: string,
  branchWitness: [string, any] | undefined,
): Uint8Array {
  return inlineCurrentRecordWithSourceMetadataAndDeletion(
    table,
    descriptor,
    row,
    alias,
    coverage,
    branchWitness,
    undefined,
  );
}

/** Preserve the supplying physical branch in covered version witnesses. */
export function coveredInputSourceMetadata(requirements: any, table: any): Map<any, any> {
  return inlineSourceMetadata(
    requirements,
    table.branchBy.length ? "supplying_branch_key" : undefined,
  );
}

/** Encode one already-authorized current row for a receiver-owned covered input. */
export function coveredInputRecord(
  table: any,
  descriptor: any,
  row: any,
  alias: any,
  sourceBranch: any,
): Uint8Array {
  return inlineCurrentRecordWithSourceMetadata(
    table,
    descriptor,
    row,
    alias,
    "authority-covered-input",
    descriptor.fieldIndex("supplying_branch_key") !== undefined
      ? ["supplying_branch_key", sourceBranch]
      : undefined,
  );
}

function inlineCurrentRecordWithSourceMetadataAndDeletion(
  table: any,
  descriptor: any,
  row: any,
  alias: any,
  coverage: string,
  branchWitness: [string, any] | undefined,
  deletionMarker: boolean | undefined,
): Uint8Array {
  return descriptor.create(
    inlineCurrentValuesWithSourceMetadataAndDeletion(
      table,
      descriptor,
      row,
      alias,
      coverage,
      branchWitness,
      deletionMarker,
    ),
  );
}

function inlineCurrentValuesWithSourceMetadataAndDeletion(
  table: any,
  descriptor: any,
  row: any,
  alias: any,
  coverage: string,
  branchWitness: [string, any] | undefined,
  deletionMarker: boolean | undefined,
): any[] {
  const values: any[] = [Value.Uuid(row.rowUuid())];
  table.columns.forEach((column: any, i: number) => {
    const value = row.cell(table, column.name);
    if (
      branchWitness &&
      table.branchBy.includes(column.name) &&
      descriptor.fields()[i + 1].valueType.kind !== "Nullable"
    ) {
      if (value === undefined)
        throw nodeError("InvalidStoredValue", "frozen branch row is missing a branch column value");
      values.push(value);
    } else values.push(Value.Nullable(value));
  });
  const p = provenanceOf(row);
  values.push(
    rowAuthorValue(p.createdBy),
    Value.U64(p.createdAt),
    rowAuthorValue(p.updatedBy),
    Value.U64(p.updatedAt),
    ...txAliasValues(row),
  );
  appendAuthorProjectionValues(values, descriptor, p.createdBy, p.updatedBy);
  if (descriptor.fieldIndex("table") !== undefined) {
    values.push(
      Value.String(table.name),
      Value.String("content"),
      Value.U64(alias),
      Value.Nullable(undefined),
      rowAuthorValue(p.createdBy),
      Value.U64(p.createdAt),
      rowAuthorValue(p.updatedBy),
      Value.U64(p.updatedAt),
    );
  }
  if (branchWitness) values.push(Value.Bytes(branchWitness[1].canonicalBytes()));
  if (descriptor.fieldIndex("settle_position") !== undefined)
    values.push(Value.Nullable(undefined));
  if (descriptor.fieldIndex("coverage") !== undefined) values.push(Value.String(coverage));
  if (deletionMarker !== undefined) values.push(Value.Bool(deletionMarker));
  return values;
}

/** Encode one current row as a record of `output`, filling fields by name and adjusting nullable wrapping only. */
export function inlineCurrentRecordForOutput(
  table: any,
  output: any,
  row: any,
  alias: any,
  coverage: string,
  requirements: any,
): Uint8Array {
  const complete = {
    ...requirements,
    metadata: [
      ...requirements.metadata,
      { kind: "VersionWitnesses" },
      { kind: "SettlePosition" },
      { kind: "Coverage" },
    ],
  };
  const metadata = inlineSourceMetadata(complete, undefined);
  const inline = currentRowDescriptorWithHiddenSourceFieldsForBranch(table, metadata, false);
  const values = inlineCurrentValuesWithSourceMetadataAndDeletion(
    table,
    inline,
    row,
    alias,
    coverage,
    undefined,
    undefined,
  );
  const missing = () => nodeError("InvalidStoredValue", "overlay row cannot supply a source field");
  const fitted = output.fields().map((field: any) => {
    if (field.name === undefined) throw missing();
    const index = inline.fields().findIndex((c: any) => c.name === field.name);
    if (index < 0 || values[index] === undefined) throw missing();
    const value = values[index];
    const wantNullable = field.valueType.kind === "Nullable";
    if (wantNullable) return value.kind === "Nullable" ? value : Value.Nullable(value);
    return value.kind === "Nullable" && value.value !== undefined ? value.value : value;
  });
  return output.create(fitted);
}

export function inlineSnapshotIncludeDeletedCurrentGraphWithSourceMetadata(
  table: any,
  rows: [any, boolean][],
  alias: any,
  coverage: string,
  requirements: any,
): InlineGraph {
  const metadata = inlineSourceMetadata(requirements, undefined);
  const descriptor = currentRowDescriptorWithHiddenSourceFieldsForBranchAndDeletion(
    table,
    metadata,
    false,
    true,
  );
  const records = rows.map(([row, deleted]) =>
    inlineCurrentRecordWithSourceMetadataAndDeletion(
      table,
      descriptor,
      row,
      alias,
      coverage,
      undefined,
      deleted,
    ),
  );
  return [GraphBuilder.inlineRecords(descriptor, records), descriptor, metadata];
}

export function includeDeletedCurrentRowDescriptor(table: any): any {
  return RecordDescriptor.new([
    ["row_uuid", VT.Uuid],
    ...table.columns.map((c: any) => [X.userColumnField(c.name), VT.Nullable(c.columnType)]),
    ["$createdBy", X.RowAuthor.valueType()],
    ["$createdAt", VT.U64],
    ["$updatedBy", X.RowAuthor.valueType()],
    ["$updatedAt", VT.U64],
    ["tx_time", VT.U64],
    ["tx_node_id", VT.U64],
    ["__jazz_deleted", VT.Bool],
  ]);
}

const withDeletedFlag = (
  table: any,
  graph: Graph,
  deleted: boolean,
  rename?: (f: string) => any,
): Graph =>
  graph.projectFields([
    ...currentRowFields(table).map((f) => (rename ? rename(f) : ProjectField.named(f))),
    ProjectField.literal("__jazz_deleted", Value.Bool(deleted)),
  ]);

export function includeDeletedBranchGraph(
  table: any,
  head: any,
  content: Graph,
  deletions: Graph,
): Graph {
  const c = content
    .projectFields(branchViewStorageSourceFields(table, head))
    .projectFields(storageToCanonicalCurrentSourceFields(table, false, false));
  const deletedWinners = deletions
    .filter(PredicateExpr.eq("_deletion", Value.EnumTag(0)))
    .projectFields([
      ProjectField.named("row_uuid"),
      ProjectField.named("tx_time"),
      ProjectField.named("tx_node_id"),
      ProjectField.renamed("updated_by", "$updatedBy"),
      ProjectField.renamed("updated_at", "$updatedAt"),
    ]);
  const undeleted = withDeletedFlag(
    table,
    GraphBuilder.antiJoin(c, deletedWinners, ["row_uuid"], ["row_uuid"]),
    false,
  );
  const fromRight = new Set(["$updatedBy", "$updatedAt", "tx_time", "tx_node_id"]);
  const deleted = withDeletedFlag(
    table,
    GraphBuilder.join(c, deletedWinners, ["row_uuid"], ["row_uuid"]),
    true,
    (f) => ProjectField.renamed(fromRight.has(f) ? X.rightField(f) : X.leftField(f), f),
  );
  return GraphBuilder.union([undeleted, deleted]);
}

export function includeDeletedCurrentGraph(table: any, tier: any): Graph {
  const userFields: string[] = table.columns.map((c: any) => X.userColumnField(c.name));
  const contentStorageFields = [
    "row_uuid",
    "schema_version",
    "authored_columns",
    ...userFields,
    "created_by",
    "created_at",
    "updated_by",
    "updated_at",
    "tx_time",
    "tx_node_id",
    "_deletion",
  ];
  const normalize = (g: Graph) =>
    g.projectFields([
      ...["row_uuid", ...userFields].map((f) => ProjectField.named(f)),
      ProjectField.renamed("created_by", "$createdBy"),
      ProjectField.renamed("created_at", "$createdAt"),
      ProjectField.renamed("updated_by", "$updatedBy"),
      ProjectField.renamed("updated_at", "$updatedAt"),
      ProjectField.named("tx_time"),
      ProjectField.named("tx_node_id"),
      ProjectField.named("_deletion"),
    ]);
  const global = normalize(
    GraphBuilder.table(X.globalCurrentTableName(table.name)).project(contentStorageFields),
  );
  // The winning image carries its own deletion state and provenance.
  const current =
    tier === "Global"
      ? global
      : GraphBuilder.argMaxBy(
          GraphBuilder.union([
            global,
            normalize(
              GraphBuilder.table(X.aheadCurrentTableName(table.name)).project(contentStorageFields),
            ),
          ]),
          ["row_uuid"],
          ["tx_time", "tx_node_id"],
        );
  return includeDeletedFromImages(table, current);
}

/** Split winning row images into the IncludeDeleted carrier. */
export function includeDeletedFromImages(table: any, current: Graph): Graph {
  return GraphBuilder.union([
    withDeletedFlag(table, current.clone().filter(X.notDeletedPredicate()), false),
    withDeletedFlag(table, current.filter(PredicateExpr.eq("_deletion", Value.EnumTag(0))), true),
  ]);
}

export function maintainedViewHistoryStorageFieldNames(table: any): string[] {
  return [
    "row_uuid",
    "tx_time",
    "tx_node_id",
    "schema_version",
    "created_by",
    "created_at",
    "updated_by",
    "updated_at",
    ...table.columns.map((c: any) => X.userColumnField(c.name)),
    "authored_columns",
  ];
}

/** A first-result equality conjunction can use both columns of a declared two-column composite index as one prefix. */
function selectCompositeEqualityAccessPath(
  table: any,
  equalities: Map<string, any>,
): CurrentAccessPath | undefined {
  if (equalities.has("id")) return undefined;
  for (const columns of table.compositeIndexes) {
    if (columns.length !== 2) continue;
    const [first, second] = columns;
    const fv = equalities.get(first);
    const sv = equalities.get(second);
    if (fv === undefined || sv === undefined) continue;
    const intersections: [string, any[]][] = [];
    for (const column of table.globalCurrentIndexedColumns()) {
      if (column === first || column === second) continue;
      const v = equalities.get(column);
      if (v !== undefined)
        intersections.push([column, [physicalCurrentIndexValue(table, column, v)]]);
    }
    return {
      kind: "Index",
      column: first,
      orderColumn: second,
      reverse: false,
      prefix: [
        physicalCurrentIndexValue(table, first, fv),
        physicalCurrentIndexValue(table, second, sv),
      ],
      intersections,
      maintained: false,
      candidateFilter: undefined,
      sourceLimit: undefined,
    };
  }
  return undefined;
}

function rowAuthorValue(author: any): any {
  let ra;
  try {
    ra = X.RowAuthor.fromPersistedSubject(author);
  } catch {
    throw nodeError("UnadmittedWriteAuthor");
  }
  return ra.toValue();
}

function appendAuthorProjectionValues(
  values: any[],
  descriptor: any,
  createdBy: any,
  updatedBy: any,
): void {
  for (const field of descriptor.fields()) {
    if (field.name === undefined) continue;
    const mp = X.AuthorSubject.metadataPath(field.name);
    if (!mp) continue;
    const [root, path] = mp;
    let value = rowAuthorValue(root === "$createdBy" ? createdBy : updatedBy);
    for (const member of path) {
      if (value.kind !== "Record") throw new Error("validated author record");
      value = value.record.get(member);
    }
    values.push(value);
  }
}

/** Attach a covered-key candidate filter to an admitted junction index path. */
function attachCoveredJoinKeyFilter(
  joinTable: any,
  onColumn: string,
  rootTable: string,
  rootPath: CurrentAccessPath | undefined,
  joinPath: CurrentAccessPath,
): void {
  if (rootPath?.kind !== "Index" || rootPath.maintained || rootPath.sourceLimit !== undefined)
    return;
  if (
    joinPath.kind !== "Index" ||
    joinPath.reverse ||
    joinPath.maintained ||
    joinPath.sourceLimit !== undefined
  )
    return;
  if (
    joinPath.prefix.length !== 1 ||
    joinPath.intersections.length ||
    (joinPath.orderColumn !== undefined && joinPath.orderColumn !== onColumn) ||
    !joinTable.compositeIndexes.some(
      (cols: string[]) => cols.length === 2 && cols[0] === joinPath.column && cols[1] === onColumn,
    )
  ) {
    return;
  }
  joinPath.orderColumn = onColumn;
  joinPath.candidateFilter = {
    table: rootTable,
    column: rootPath.column,
    orderColumn: rootPath.orderColumn,
    prefix: rootPath.prefix,
    sourceColumn: onColumn,
  };
}

/** The normalized source occurrence of a query's only root-level `join_via`. */
function singleRootJoinSource(request: any, joinTable: string): SourceId | undefined {
  const rootJoins = request.input.shape.joinContributions.filter(
    (c: any) => c.parent === undefined,
  );
  return rootJoins.length === 1 && rootJoins[0].source.table === joinTable
    ? rootJoins[0].source
    : undefined;
}

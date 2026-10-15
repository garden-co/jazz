import { SortedMap } from "../../../support/index.js";
import type {
  NormalizedRowSetShape,
  SourceId,
  SchemaVersionId,
  DurabilityTier,
  BindingViewKey,
  AuthorityResultKey,
  GlobalTime,
  Snapshot,
  OpenTransactionId,
  ReadViewSpec,
  JazzSchema,
  SourceExpr,
  RequestedReadSet,
} from "../../../_extern/node-qe-b.js";
import { Err, cmpSourceId, QueryReadSet, branchViewStack } from "../../../_extern/node-qe-b.js";

const canonicalProjection = (schemaVersion: SchemaVersionId) => ({
  schemaFamily: { kind: "Current" },
  storage: { kind: "Single", version: schemaVersion },
  lens: { kind: "Canonical" },
});

function nodeSources(shape: NormalizedRowSetShape): SourceId[] {
  const out: SourceId[] = [];
  for (const [, node] of shape.nodes) if (node.kind === "Source") out.push(node.source);
  return out;
}

const allSources = (shape: NormalizedRowSetShape): SourceId[] => [
  ...nodeSources(shape),
  ...shape.auxiliarySources,
];

function uniformReadSet(
  readSchema: SchemaVersionId,
  policySchema: SchemaVersionId,
  sources: SourceId[],
  expr: () => SourceExpr,
): RequestedReadSet {
  const map = new SortedMap<SourceId, SourceExpr>(cmpSourceId);
  for (const source of sources) map.set(source, expr());
  return QueryReadSet.primary({ readSchema, policySchema, sources: map });
}

export function currentQueryReadSet(
  shape: NormalizedRowSetShape,
  readSchema: SchemaVersionId,
  policySchema: SchemaVersionId,
  tier: DurabilityTier,
  settledBindingView: BindingViewKey | undefined,
  settledAuthorityResultKey: AuthorityResultKey | undefined,
  settledCurrentDefault: boolean,
): RequestedReadSet {
  const projection = canonicalProjection(readSchema);
  const expr = (): SourceExpr =>
    settledBindingView !== undefined
      ? {
          kind: "SettledBindingView",
          currentDefault: settledCurrentDefault,
          projection,
          bindingView: settledBindingView,
          authorityResultKey: settledAuthorityResultKey,
        }
      : { kind: "VisibleCurrent", projection, data: { kind: "Current" }, tier };
  return uniformReadSet(readSchema, policySchema, allSources(shape), expr);
}

export function historicalQueryReadSet(
  shape: NormalizedRowSetShape,
  schemaVersion: SchemaVersionId,
  position: GlobalTime,
): RequestedReadSet {
  const projection = canonicalProjection(schemaVersion);
  return uniformReadSet(schemaVersion, schemaVersion, nodeSources(shape), () => ({
    kind: "HistoryCut",
    projection,
    data: { kind: "Current" },
    position,
  }));
}

export function snapshotQueryReadSet(
  shape: NormalizedRowSetShape,
  schemaVersion: SchemaVersionId,
  snapshot: Snapshot,
): RequestedReadSet {
  const projection = canonicalProjection(schemaVersion);
  return uniformReadSet(schemaVersion, schemaVersion, allSources(shape), () => ({
    kind: "SnapshotRef",
    projection,
    data: { kind: "Current" },
    snapshot,
  }));
}

export function txQueryReadSet(
  shape: NormalizedRowSetShape,
  schemaVersion: SchemaVersionId,
  txId: OpenTransactionId,
  snapshot: Snapshot,
): RequestedReadSet {
  const projection = canonicalProjection(schemaVersion);
  return uniformReadSet(schemaVersion, schemaVersion, allSources(shape), () => ({
    kind: "WithOverlays",
    input: { kind: "SnapshotRef", projection, data: { kind: "Current" }, snapshot },
    overlays: { entries: [{ kind: "OpenTransaction", txId }] },
  }));
}

export function queryReadSetForReadView(
  shape: NormalizedRowSetShape,
  readSchema: SchemaVersionId,
  policySchema: SchemaVersionId,
  tier: DurabilityTier,
  readView: ReadViewSpec,
  settledBindingView: BindingViewKey | undefined,
  settledAuthorityResultKey: AuthorityResultKey | undefined,
  schema: JazzSchema,
): RequestedReadSet {
  if (settledBindingView !== undefined)
    return currentQueryReadSet(
      shape,
      readSchema,
      policySchema,
      tier,
      settledBindingView,
      settledAuthorityResultKey,
      readView.source.kind === "Current",
    );
  const source = readView.source;
  switch (source.kind) {
    case "Current":
      return currentQueryReadSet(
        shape,
        readSchema,
        policySchema,
        tier,
        undefined,
        undefined,
        false,
      );
    case "Snapshot": {
      const s = source.snapshot;
      return snapshotQueryReadSet(shape, readSchema, {
        owner: s.owner,
        globalBase: s.globalBase,
        localBase: s.localBase,
        dots: s.dots,
      });
    }
    case "BranchView": {
      const projection = canonicalProjection(readSchema);
      const sources = new SortedMap<SourceId, SourceExpr>(cmpSourceId);
      for (const sourceId of allSources(shape)) {
        const table = schema.tables.find((t: any) => t.name === sourceId.table);
        if (!table) throw Err.InvalidStoredValue("branch view source table missing");
        let stack;
        try {
          stack = branchViewStack(schema, table, source.head, source.base);
        } catch (e) {
          throw Err.InvalidBranchKey(e);
        }
        const [head, base] = stack;
        sources.set(sourceId, { kind: "BranchView", projection, head, base, tier });
      }
      return QueryReadSet.primary({ readSchema, policySchema, sources });
    }
  }
  throw Err.InvalidStoredValue("unknown read view source");
}

import type { SortedMap } from "../../../support/collections.js";
import { AuthorSubject } from "../../../_extern/engine.js";
import {
  dbg,
  structEq,
  structMap,
  structSet,
  type NormalizedValueRef,
  type PredicateExpr,
  type ProvenanceField,
  type QueryProgramRequest,
  type SourceId,
} from "../input.js";
import type { AppPathProjection, FieldProjection } from "../output.js";
import {
  CapabilityError,
  UnsupportedError,
  type FieldRequirement,
  type SourceRequirements,
} from "../resolver.js";
import { closurePathSegments } from "./closure.js";
import { flatJoinPayloadFields } from "./terminals.js";
import {
  collectPlanFragments,
  planRootSource,
  programSources,
  type AnalyzedQueryPlan,
  type LinearStep,
} from "./planning.js";

const PROVENANCE_BY_NAME: Record<string, ProvenanceField> = {
  $createdAt: "CreatedAt",
  $createdBy: "CreatedBy",
  $updatedAt: "UpdatedAt",
  $updatedBy: "UpdatedBy",
};

const emptyRequirements = (): SourceRequirements => ({
  appFields: { kind: "None" },
  metadata: structSet(),
});

export function sourceRequirements(
  request: QueryProgramRequest,
  plan: AnalyzedQueryPlan,
): SortedMap<SourceId, SourceRequirements> {
  const output = request.output;
  const requirements = structMap<SourceId, SourceRequirements>();
  for (const source of programSources(request, plan)) requirements.set(source, emptyRequirements());

  for (const path of request.input.shape.closurePaths) {
    for (const segment of closurePathSegments(path)) {
      const reqs = requirements.get(segment.parent);
      if (!reqs)
        throw CapabilityError.runtime(
          `closure parent source ${dbg(segment.parent)} was not initialized`,
        );
      addRequiredAppField(reqs, segment.sourceField);
    }
  }

  const insertAll = (
    ...kinds: ("VersionWitnesses" | "VersionPayloads" | "Coverage" | "SettlePosition")[]
  ) => {
    for (const reqs of requirements.valuesArray())
      for (const kind of kinds) reqs.metadata.add({ kind });
  };

  if (flatJoinPayloadFields(plan).length > 0) insertAll("VersionWitnesses");

  const root = () => requirements.get(planRootSource(plan))!;
  const appRows = output.appRows;
  if (appRows) {
    if (!(plan.kind === "Linear" || plan.kind === "Union" || plan.kind === "CorrelatedPath")) {
      throw CapabilityError.explained(
        {
          kind: "Operator",
          message: "app row materialization for recursive relation projections is not lowered yet",
        },
        "recursive relation app rows are not lowered",
      );
    }
    const rootReqs = root();
    const projection = appRows.projection;
    const treeFields =
      projection.kind === "Tree" && projection.tree.fields.kind === "Fields"
        ? [...projection.tree.fields.fields]
        : [];
    if (appRows.publicTerminal && treeFields.some((f) => f in PROVENANCE_BY_NAME)) {
      for (const field of Object.values(PROVENANCE_BY_NAME))
        rootReqs.metadata.add({ kind: "Provenance", field });
    }
    mergeFieldRequirement(
      rootReqs,
      projection.kind === "Tree" ? fieldRequirementFrom(projection.tree.fields) : { kind: "All" },
    );
    if (projection.kind === "Tree") {
      for (const field of treeFields) {
        const provenance = PROVENANCE_BY_NAME[field];
        if (provenance) rootReqs.metadata.add({ kind: "Provenance", field: provenance });
      }
      collectAppPathProjectionRequirements(projection.tree.paths, requirements);
    }
  }

  for (const fact of output.facts) {
    switch (fact.kind) {
      case "AuthorizedRows":
        break;
      case "ResultMembership": {
        const rootReqs = root();
        rootReqs.metadata.add({ kind: "VersionWitnesses" });
        rootReqs.metadata.add({ kind: "SettlePosition" });
        for (const contribution of request.input.shape.joinContributions) {
          const reqs = requirements.get(contribution.source);
          if (reqs) {
            reqs.metadata.add({ kind: "VersionWitnesses" });
            reqs.metadata.add({ kind: "SettlePosition" });
          }
        }
        break;
      }
      case "VersionWitnesses":
      case "ReplacementWitnesses":
        insertAll("VersionWitnesses", "VersionPayloads");
        break;
      case "ProgramSourceCoverage": {
        const scope = fact.scope;
        if (scope.kind === "Program") insertAll("Coverage");
        else if (scope.kind === "Source") {
          const reqs = requirements.get(scope.source);
          if (!reqs) throw CapabilityError.single({ kind: "Source", gap: { kind: "Coverage" } });
          reqs.metadata.add({ kind: "Coverage" });
        } else root().metadata.add({ kind: "Coverage" });
        break;
      }
      case "RelationEdges":
      case "PathCorrelationCoverage":
        insertAll("VersionWitnesses");
        break;
      default:
        throw CapabilityError.explained(
          { kind: "Output", fact },
          "requested fact is not lowered yet",
        );
    }
  }

  collectPlanRequirements(plan, requirements);
  return requirements;
}

function collectAppPathProjectionRequirements(
  paths: AppPathProjection[],
  requirements: SortedMap<SourceId, SourceRequirements>,
): void {
  for (const path of paths) {
    const reqs = requirements.get(path.path.child);
    if (!reqs) {
      throw CapabilityError.runtime(
        `app projection path child source ${dbg(path.path.child)} was not initialized`,
      );
    }
    mergeFieldRequirement(reqs, fieldRequirementFrom(path.fields));
    collectAppPathProjectionRequirements(path.children, requirements);
  }
}

const fieldRequirementFrom = (value: FieldProjection): FieldRequirement =>
  value.kind === "All" ? { kind: "All" } : { kind: "Fields", fields: structSet(value.fields) };

function mergeFieldRequirement(target: SourceRequirements, incoming: FieldRequirement): void {
  const existing = target.appFields;
  if (incoming.kind === "None") return;
  if (incoming.kind === "All") target.appFields = incoming;
  else if (existing.kind === "None") target.appFields = incoming;
  else if (existing.kind === "Fields") for (const f of incoming.fields) existing.fields.add(f);
}

function collectPlanRequirements(
  plan: AnalyzedQueryPlan,
  requirements: SortedMap<SourceId, SourceRequirements>,
): void {
  const fragments = collectPlanFragments(plan);
  for (const fragment of fragments.linears) {
    for (const step of fragment.steps) {
      for (const [source, reqs] of requirements) collectStepRequirements(step, source, reqs);
    }
  }
  for (const correlation of fragments.correlations) {
    for (const [source, reqs] of requirements) {
      explainGap("path correlation requirements are not lowered", () =>
        collectPredicateRequirements(correlation, source, reqs),
      );
    }
  }
  const skip = (key: NormalizedValueRef) =>
    key.kind === "FrontierColumn" ||
    (key.kind === "RowId" && key.row.kind === "Frontier") ||
    key.kind === "Param" ||
    key.kind === "Literal";
  for (const relation of fragments.recursives) {
    for (const key of [relation.frontierKey, ...relation.dedupeKeys]) {
      if (skip(key)) continue;
      for (const [source, reqs] of requirements) {
        explainGap("relation key requirements are not lowered", () =>
          collectValueRequirements(key, source, reqs),
        );
      }
    }
  }
}

function explainGap(capability: string, f: () => void): void {
  try {
    f();
  } catch (err) {
    if (err instanceof UnsupportedError) throw CapabilityError.explained(err.gap, capability);
    throw err;
  }
}

function collectStepRequirements(
  step: LinearStep,
  source: SourceId,
  reqs: SourceRequirements,
): void {
  explainGap("operator source requirements are not lowered", () => {
    const values = (vs: NormalizedValueRef[]) =>
      vs.forEach((v) => collectValueRequirements(v, source, reqs));
    switch (step.kind) {
      case "Filter":
        return collectPredicateRequirements(step.predicate, source, reqs);
      case "Join":
        return collectPredicateRequirements(step.on, source, reqs);
      case "Project":
        return values(step.columns.map((c) => c.value));
      case "OrderBy":
        return values(step.keys.map((k) => k.value));
      case "Slice":
        return values([...step.partitionBy, ...step.tieBreaker]);
      case "Aggregate":
        values(step.groupBy);
        return values(step.outputs.flatMap((a) => (a.input ? [a.input] : [])));
    }
  });
}

function collectPredicateRequirements(
  predicate: PredicateExpr,
  source: SourceId,
  reqs: SourceRequirements,
): void {
  switch (predicate.kind) {
    case "True":
    case "False":
      return;
    case "Compare":
      collectValueRequirements(predicate.left, source, reqs);
      return collectValueRequirements(predicate.right, source, reqs);
    case "In":
      collectValueRequirements(predicate.value, source, reqs);
      for (const option of predicate.options) collectValueRequirements(option, source, reqs);
      return;
    case "ArrayContains":
    case "TextContains":
      collectValueRequirements(predicate.value, source, reqs);
      return collectValueRequirements(predicate.needle, source, reqs);
    case "IsNull":
    case "IsNotNull":
    case "EnumMatch":
      return collectValueRequirements(predicate.value, source, reqs);
    case "And":
    case "Or":
      for (const child of predicate.children) collectPredicateRequirements(child, source, reqs);
      return;
    case "Not":
      return collectPredicateRequirements(predicate.child, source, reqs);
  }
}

function collectValueRequirements(
  value: NormalizedValueRef,
  source: SourceId,
  reqs: SourceRequirements,
): void {
  if (value.kind === "SourceField" && structEq(value.source, source))
    addRequiredAppField(reqs, value.field);
  else if (value.kind === "Provenance" && structEq(value.source, source)) {
    reqs.metadata.add({ kind: "Provenance", field: value.field });
  }
}

function addRequiredAppField(reqs: SourceRequirements, field: string): void {
  const authorPath = AuthorSubject.metadataPath(field);
  if (authorPath) {
    reqs.metadata.add({ kind: "AuthorPath", path: field });
    reqs.metadata.add({
      kind: "Provenance",
      field: authorPath[0] === "$createdBy" ? "CreatedBy" : "UpdatedBy",
    });
    return;
  }
  if (reqs.appFields.kind === "None")
    reqs.appFields = { kind: "Fields", fields: structSet([field]) };
  else if (reqs.appFields.kind === "Fields") reqs.appFields.fields.add(field);
}

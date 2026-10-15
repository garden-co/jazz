import { SortedSet, cmpStr, type SortedMap } from "../../../support/collections.js";
import {
  CollectByField,
  CollectBySlotBuilder,
  FieldRef,
  MAX_COLLECT_BY_TREE_DEPTH,
  NIL_UUID,
  ProjectField,
  RecordDescriptor,
  TopByLimit,
  type GraphBuilder,
  type ValueType,
} from "../../../_extern/engine.js";
import {
  currentRowFieldNames,
  descriptorPublicName,
  leftField,
  rightField,
  routeParamFromField,
  userColumnField,
} from "../fields.js";
import { dbg, structEq, type ProgramPathId, type SourceId } from "../input.js";
import type { AppPathProjection, AppProjectionTree } from "../output.js";
import {
  CapabilityError,
  lookupResolvedSource,
  orReport,
  type ResolvedSource,
} from "../resolver.js";
import type { ParameterDomain } from "../lowering.js";
import type { LoweringContext } from "./arguments.js";
import type { AnalyzedQueryPlan, CorrelatedPathPlan } from "./planning.js";
import { lowerCorrelatedPathRelationGraphFromParent, sourceFieldType } from "./graph_lowering.js";
import { findNestedCorrelatedPath, rootJoinOccurrenceFields } from "./terminals.js";

export type CollectFieldOrigin = "SourceRow" | "Derived";

export type CollectFlatField = {
  input: string;
  output: string;
  valueType: ValueType;
  outputValueType: ValueType;
  sourceField?: string;
  sourcePublicName?: string;
  origin: CollectFieldOrigin;
  isRowId: boolean;
  isPresence: boolean;
  isOutput: boolean;
};

export type CollectSlotLayout = {
  path: ProgramPathId;
  collectionField: string;
  fields: CollectFlatField[];
  rowIdInput: string;
  presenceInput: string;
  referenceArrayInput?: string;
  referenceOrder: boolean;
  orderCols: any[];
  tieCols: string[];
  offset: number;
  limit: any;
  children: CollectSlotLayout[];
};

export type CollectLayout = {
  rootFields: CollectFlatField[];
  rootOccurrenceInputs: string[];
  rootOrderCols: any[];
  rootTieCols: string[];
  rootOffset: number;
  rootLimit: any;
  slots: CollectSlotLayout[];
};

const PROVENANCE_OUTPUTS: Record<string, string> = {
  $createdAt: "$createdAt",
  $createdBy: "$createdBy",
  $updatedAt: "$updatedAt",
  $updatedBy: "$updatedBy",
  created_at: "$createdAt",
  created_by: "$createdBy",
  updated_at: "$updatedAt",
  updated_by: "$updatedBy",
};

const isMagic = (field: string) =>
  field === "$createdAt" ||
  field === "$createdBy" ||
  field === "$updatedAt" ||
  field === "$updatedBy";

function resolvedSourceFieldIndexByName(source: ResolvedSource, field: string): number | undefined {
  const index = source.rowShape.descriptor.fields().findIndex((c: any) => c.name === field);
  return index < 0 ? undefined : index;
}

function resolvedSourceFieldName(source: ResolvedSource, field: string): string | undefined {
  const index = source.rowShape.descriptor.fieldIndex(field);
  return index === undefined ? undefined : source.rowShape.descriptor.fields()[index]?.name;
}

function resolvedSourcePublicName(source: ResolvedSource, field: string): string | undefined {
  const found = source.rowShape.descriptor.fields().find((c: any) => c.name === field);
  return found && descriptorPublicName(found);
}

export function collectLayout(
  projection: AppProjectionTree,
  plan: AnalyzedQueryPlan,
  rootSource: ResolvedSource,
  resolvedSources: SortedMap<SourceId, ResolvedSource>,
  request: LoweringContext,
  routingParamFields: SortedSet<string>,
  parameterDomain: ParameterDomain,
  availableFields: SortedSet<string>,
): CollectLayout {
  const explicitRootProjection = projection.fields.kind === "Fields";
  const rowUuid = rootSource.rowShape.rowUuidField;
  const selectedRoot = new Set([rowUuid]);
  if (projection.fields.kind === "All") {
    // Shape-default app rows are CurrentRows: keep provenance and version fields.
    currentRowFieldNames(rootSource.tableSchema).forEach((f) => selectedRoot.add(f));
  } else {
    for (const field of projection.fields.fields) {
      if (field !== "id") selectedRoot.add(collectProjectionSourceField(rootSource, field));
    }
  }
  const rootFields: CollectFlatField[] = [];
  for (const field of rootSource.rowShape.descriptor.fields()) {
    const name: string | undefined = field.name;
    if (name === undefined) continue;
    rootFields.push({
      input: `__collect_root_${name}`,
      output: name === rowUuid ? name : collectProjectionOutputField(name),
      valueType: field.valueType,
      outputValueType: explicitRootProjection
        ? collectUnwrappedOutputType(rootSource, name, field.valueType)
        : field.valueType,
      sourceField: name,
      sourcePublicName: descriptorPublicName(field),
      origin: "SourceRow",
      isRowId: name === rowUuid,
      isPresence: false,
      isOutput: selectedRoot.has(name),
    });
  }
  const rootOccurrenceInputs: string[] = [];
  for (const [name, valueType] of rootJoinOccurrenceFields(plan, resolvedSources, request)) {
    if (!availableFields.has(name)) continue;
    const input = `__collect_root_${name}`;
    rootFields.push({
      input,
      output: name,
      valueType,
      outputValueType: valueType,
      sourceField: name,
      origin: "Derived",
      isRowId: false,
      isPresence: false,
      isOutput: false,
    });
    rootOccurrenceInputs.push(input);
  }
  for (const routeField of routingParamFields) {
    const param = routeParamFromField(routeField);
    const valueType =
      param !== undefined
        ? (parameterDomain.userParams.get(param) ?? request.input.binding.paramTypes.get(param))
        : parameterDomain.claimParams.get(routeField)?.ty;
    if (valueType === undefined) {
      throw CapabilityError.runtime(
        `collector route field ${dbg(routeField)} has no parameter type`,
      );
    }
    rootFields.push({
      input: routeField,
      output: routeField,
      valueType,
      outputValueType: valueType,
      sourceField: routeField,
      sourcePublicName: routeField,
      origin: "Derived",
      isRowId: false,
      isPresence: false,
      isOutput: true,
    });
  }
  const nextSlot = { value: 0 };
  return {
    rootFields,
    rootOccurrenceInputs,
    rootOrderCols: [],
    rootTieCols: [],
    rootOffset: 0,
    rootLimit: TopByLimit.Unbounded,
    slots: collectSlotLayouts(projection.paths, resolvedSources, 1, nextSlot),
  };
}

export function collectUnwrappedOutputType(
  source: ResolvedSource,
  sourceField: string,
  fallback: ValueType,
): ValueType {
  if (sourceField === source.rowShape.rowUuidField) return fallback;
  const logical = resolvedSourcePublicName(source, sourceField) ?? sourceField;
  const column = source.tableSchema.columns.find((c: any) => c.name === logical);
  if (!column) return fallback;
  // The collector unwraps the current-row presence cell, not the storage representation.
  const unwrapped = fallback.kind === "Nullable" ? fallback.inner : fallback;
  // Indirect JSON storage has one null for absence and NULL; a nullable output must still admit NULL.
  return column.columnType.kind === "Nullable" && unwrapped.kind !== "Nullable"
    ? { kind: "Nullable", inner: unwrapped }
    : unwrapped;
}

/** A collector child input carries exactly one anchor wrapper around its logical output. */
export const collectSlotInputType = (outputValueType: ValueType): ValueType => ({
  kind: "Nullable",
  inner: outputValueType,
});

function collectSlotLayouts(
  paths: AppPathProjection[],
  resolvedSources: SortedMap<SourceId, ResolvedSource>,
  depth: number,
  nextSlot: { value: number },
): CollectSlotLayout[] {
  if (depth > MAX_COLLECT_BY_TREE_DEPTH) {
    throw CapabilityError.operator(
      `association projection depth ${depth} exceeds Groove MAX_COLLECT_BY_TREE_DEPTH (${MAX_COLLECT_BY_TREE_DEPTH})`,
    );
  }
  return paths.map((path) => {
    const source = lookupResolvedSource(
      resolvedSources,
      path.path.child,
      "app projection path child",
    );
    const slotId = nextSlot.value++;
    const prefix = `__collect_path_${slotId}`;
    const rowUuid = source.rowShape.rowUuidField;
    const selected = new Set([rowUuid]);
    if (path.fields.kind === "All") {
      for (const column of source.tableSchema.columns) selected.add(userColumnField(column.name));
    } else {
      for (const field of path.fields.fields) {
        if (field !== "id") selected.add(collectProjectionSourceField(source, field));
      }
    }
    const fields: CollectFlatField[] = [];
    for (const descriptorField of source.rowShape.descriptor.fields()) {
      const sourceField: string | undefined = descriptorField.name;
      if (sourceField === undefined || !selected.has(sourceField)) continue;
      const sourceValueType = sourceFieldType(source, sourceField);
      if (sourceValueType === undefined) {
        throw CapabilityError.operator(
          `association projection source ${dbg(source.rowShape.source)} does not provide field ${dbg(sourceField)}`,
        );
      }
      const isRowId = sourceField === rowUuid;
      const outputValueType = collectUnwrappedOutputType(source, sourceField, sourceValueType);
      fields.push({
        input: `${prefix}_${sourceField}`,
        output: isRowId ? sourceField : collectNestedProjectionOutputField(source, sourceField),
        valueType: isRowId ? sourceValueType : collectSlotInputType(outputValueType),
        outputValueType,
        sourcePublicName: isRowId ? undefined : resolvedSourcePublicName(source, sourceField),
        origin: "SourceRow",
        sourceField,
        isRowId,
        isPresence: false,
        isOutput: true,
      });
    }
    const rowIdInput = fields.find((f) => f.isRowId)!.input;
    return {
      path: path.path,
      collectionField: path.field,
      fields,
      rowIdInput,
      presenceInput: `${prefix}_present`,
      referenceOrder: false,
      orderCols: [],
      tieCols: [],
      offset: 0,
      limit: TopByLimit.Unbounded,
      children: collectSlotLayouts(path.children, resolvedSources, depth + 1, nextSlot),
    };
  });
}

function collectProjectionSourceField(source: ResolvedSource, field: string): string {
  return isMagic(field)
    ? field
    : (resolvedSourceFieldName(source, field) ?? userColumnField(field));
}

/** Root records stay in the canonical current-row codec namespace. */
const collectProjectionOutputField = (field: string): string => PROVENANCE_OUTPUTS[field] ?? field;

/** Nested records are public tree payloads, so user columns keep logical names. */
function collectNestedProjectionOutputField(source: ResolvedSource, field: string): string {
  return PROVENANCE_OUTPUTS[field] ?? resolvedSourcePublicName(source, field) ?? field;
}

function collectRootFieldProjection(
  source: ResolvedSource,
  field: CollectFlatField,
  output: string,
  sourcePositionsAreStable: boolean,
): any {
  const sourceField = field.sourceField!;
  if (field.origin === "SourceRow" && sourcePositionsAreStable) {
    const index = resolvedSourceFieldIndexByName(source, sourceField);
    if (index === undefined)
      throw new Error("source-row collector fields are present in the source descriptor");
    return ProjectField.renamedResolved(index, output);
  }
  // Route parameters and occurrence keys are appended by graph lowering.
  return ProjectField.renamed(sourceField, output);
}

export function rootCollectContextGraph(
  graph: GraphBuilder,
  source: ResolvedSource,
  layout: CollectLayout,
  sourcePositionsAreStable: boolean,
): GraphBuilder {
  return graph.projectFields(
    layout.rootFields.flatMap((field) => [
      collectRootFieldProjection(source, field, field.sourceField!, sourcePositionsAreStable),
      collectRootFieldProjection(source, field, field.input, sourcePositionsAreStable),
    ]),
  );
}

export function collectAnchorGraph(
  graph: GraphBuilder,
  rootSource: ResolvedSource,
  layout: CollectLayout,
  sourcePositionsAreStable: boolean,
): GraphBuilder {
  return graph.projectFields(
    collectFlatProjection(layout, undefined, new Set(), rootSource, sourcePositionsAreStable),
  );
}

export function lowerCollectSlotGraphs(
  slot: CollectSlotLayout,
  path: CorrelatedPathPlan,
  parentGraph: GraphBuilder,
  parentSource: ResolvedSource,
  rootSource: ResolvedSource,
  layout: CollectLayout,
  inheritedFlatFields: Set<string>,
  resolvedSources: SortedMap<SourceId, ResolvedSource>,
  request: LoweringContext,
): GraphBuilder[] {
  const joined = orReport(() =>
    lowerCorrelatedPathRelationGraphFromParent(
      path,
      parentGraph,
      parentSource,
      resolvedSources,
      request,
      false,
    ),
  ).graph;
  const childSource = lookupResolvedSource(resolvedSources, slot.path.child, "collector child");
  const association = joined.projectFields(
    collectFlatProjection(layout, [slot, childSource], inheritedFlatFields, undefined, true),
  );
  const context = joined.projectFields(
    collectChildContextProjection(layout, slot, childSource, inheritedFlatFields),
  );
  const graphs = [association];
  const childInherited = new Set([...inheritedFlatFields, ...collectSlotFlatFieldNames(slot)]);
  for (const nestedSlot of slot.children) {
    const nestedPath = findNestedCorrelatedPath(path, nestedSlot.path);
    if (!nestedPath) {
      throw CapabilityError.operator(
        `app projection path ${dbg(nestedSlot.path)} is not nested below ${dbg(slot.path)}`,
      );
    }
    graphs.push(
      ...lowerCollectSlotGraphs(
        nestedSlot,
        nestedPath,
        context,
        childSource,
        rootSource,
        layout,
        childInherited,
        resolvedSources,
        request,
      ),
    );
  }
  return graphs;
}

function collectFlatProjection(
  layout: CollectLayout,
  currentSlot: [CollectSlotLayout, ResolvedSource] | undefined,
  inheritedFlatFields: Set<string>,
  rootSource: ResolvedSource | undefined,
  sourcePositionsAreStable: boolean,
): any[] {
  const fields = layout.rootFields.map((field) =>
    currentSlot
      ? ProjectField.renamed(leftField(field.input), field.input)
      : collectRootFieldProjection(rootSource!, field, field.input, sourcePositionsAreStable),
  );
  for (const slot of collectAllSlots(layout.slots)) {
    const childSource =
      currentSlot && structEq(currentSlot[0].path, slot.path) ? currentSlot[1] : undefined;
    const isCurrent = childSource !== undefined;
    for (const field of slot.fields) {
      if (isCurrent && slot.referenceArrayInput === field.input) {
        fields.push(ProjectField.nullableFlat(leftField(field.sourceField!), field.input));
      } else if (childSource) {
        const source = rightField(field.sourceField!);
        if (field.isRowId) fields.push(ProjectField.renamed(source, field.input));
        // Indirect JSON cells arrive as exactly their logical output and need a distinct wrapper.
        else if (
          structEq(sourceFieldType(childSource, field.sourceField!), field.outputValueType)
        ) {
          fields.push(ProjectField.nullable(source, field.input));
        } else fields.push(ProjectField.nullableFlat(source, field.input));
      } else if (inheritedFlatFields.has(field.input)) {
        fields.push(ProjectField.renamed(leftField(field.input), field.input));
      } else fields.push(collectFlatDefault(field));
    }
    fields.push(
      isCurrent
        ? ProjectField.literal(slot.presenceInput, { kind: "Bool", value: true })
        : inheritedFlatFields.has(slot.presenceInput)
          ? ProjectField.renamed(leftField(slot.presenceInput), slot.presenceInput)
          : ProjectField.literal(slot.presenceInput, { kind: "Bool", value: false }),
    );
  }
  return fields;
}

function collectChildContextProjection(
  layout: CollectLayout,
  currentSlot: CollectSlotLayout,
  childSource: ResolvedSource,
  inheritedFlatFields: Set<string>,
): any[] {
  const fields = childSource.rowShape.descriptor
    .fields()
    .filter((f: any) => f.name !== undefined)
    .map((f: any) => ProjectField.renamed(rightField(f.name), f.name));
  fields.push(
    ...collectFlatProjection(
      layout,
      [currentSlot, childSource],
      inheritedFlatFields,
      undefined,
      true,
    ),
  );
  return fields;
}

function collectFlatDefault(field: CollectFlatField): any {
  if (field.isRowId) return ProjectField.literal(field.input, { kind: "Uuid", value: NIL_UUID });
  if (field.isPresence) return ProjectField.literal(field.input, { kind: "Bool", value: false });
  return ProjectField.nullTyped(field.input, field.valueType);
}

function collectAllSlots(slots: CollectSlotLayout[]): CollectSlotLayout[] {
  return slots.flatMap((slot) => [slot, ...collectAllSlots(slot.children)]);
}

const collectSlotFlatFieldNames = (slot: CollectSlotLayout): string[] => [
  ...slot.fields.map((f) => f.input),
  slot.presenceInput,
];

export function collectByFieldFor(field: CollectFlatField): any {
  return field.isRowId || structEq(field.valueType, field.outputValueType)
    ? CollectByField.renamed(field.input, field.output)
    : CollectByField.renamedUnwrapNullable(field.input, field.output);
}

export function collectSlotBuilder(
  slot: CollectSlotLayout,
  parentRowId: string,
  routeFields: SortedSet<string>,
): any {
  const builder = new CollectBySlotBuilder(
    [parentRowId, ...routeFields],
    slot.fields.filter((f) => f.isOutput).map(collectByFieldFor),
    slot.collectionField,
    slot.children.map((child) => collectSlotBuilder(child, slot.rowIdInput, routeFields)),
    slot.orderCols,
    slot.tieCols,
    slot.offset,
    slot.limit,
  )
    // Route fields are execution owner keys only, not nested application fields.
    .withOwnerKeyCols([...routeFields])
    .withPresenceCol(slot.presenceInput);
  builder.referenceArrayCol = slot.referenceArrayInput && FieldRef.name(slot.referenceArrayInput);
  builder.referenceOrder = slot.referenceOrder;
  return builder;
}

export function collectOutputDescriptor(layout: CollectLayout): any {
  return new RecordDescriptor([
    ...layout.rootFields.filter((f) => f.isOutput).map((f) => [f.output, f.outputValueType]),
    ...layout.slots.map(collectSlotOutputField),
  ]);
}

function collectSlotOutputField(slot: CollectSlotLayout): [string, ValueType] {
  return [
    slot.collectionField,
    { kind: "Array", inner: { kind: "Record", descriptor: collectSlotOutputDescriptor(slot) } },
  ];
}

function collectSlotOutputDescriptor(slot: CollectSlotLayout): any {
  return new RecordDescriptor([
    ...slot.fields.filter((f) => f.isOutput).map((f) => [f.output, f.outputValueType]),
    ...slot.children.map(collectSlotOutputField),
  ]);
}

export const emptyStringSet = () => new SortedSet<string>(cmpStr);

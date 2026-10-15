// Runtime schema registration, variant projection, and snapshot reads.
import {
  type EnumSchema,
  type IndexSchema,
  type IvmRuntime,
  type OrderedKvStorage,
  type RecordDeltas,
  type TableSchema,
  type ColumnSchema,
  type TableVariant,
  type VariantRecord,
  HydrationMode,
  IvmRuntimeError,
  NodeState,
  OwnedRecord,
  OwnedWriteOperation,
  RecordDescriptor,
  RootIndirectValues,
  ScanRequest,
  EphemeralGraphInstall,
  applyPersistDelta,
  applyPersistDeltaWith,
  builderContainsBindingSource,
  collectScan,
  countBuilderNodes,
  variantCaseCanRefreshRegistriesTo,
} from "../../../_extern/groove-ivm-plan.js";
import {
  type GraphBuilder,
  type ProjectField,
  ProjectField as ProjectFieldCtor,
  projectExprSource,
} from "../graph.js";
import { type MapProjectOp, type VariantProjectionTarget, structEq } from "../op_types.js";
import { addDedupGraph, addDedupSchemaIndex } from "./compilation.js";
import {
  projectDescriptor,
  projectFieldExpr,
  rawProjectionFields,
  resolveFieldRef,
  schemaIndexInputDescriptor,
  schemaIndexInputFields,
  variantProjectionTargetName,
} from "./record_projection.js";

type VariantProjectionCase =
  | {
      kind: "Project";
      source: RecordDescriptor;
      project: MapProjectOp;
      rawProjection: any;
      omitUnrepresentableEnumRows: boolean;
    }
  | {
      kind: "Enum";
      source: RecordDescriptor;
      tag: number;
      payload: RecordDescriptor;
      project: MapProjectOp;
      rawProjection: any;
    }
  | { kind: "Ignore"; source: RecordDescriptor };

interface VariantProjection {
  output: RecordDescriptor;
  cases: Map<number, VariantProjectionCase>;
}

const projectionKey = (table: string, target: VariantProjectionTarget) =>
  `${table}\0${target.kind}\0${target.name}`;
const named = (name: string): VariantProjectionTarget => ({ kind: "Named", name });
const schemaIndexTarget = (name: string): VariantProjectionTarget => ({
  kind: "SchemaIndex",
  name,
});

export function declaredIndexRepairIsIdle(rt: IvmRuntime): boolean {
  return (
    !rt.multisinkSubscriptions.size &&
    !rt.preparedShapes.size &&
    !rt.pendingIncremental.isPending() &&
    !rt.pendingIncrementalPolling &&
    !rt.pendingBindingRetractions.length &&
    !rt.deferredNotifications.length &&
    !rt.persistenceIndeterminate
  );
}

function persistNodeParts(rt: IvmRuntime, persist: bigint): [any, bigint] {
  const node = rt.graph.node(persist);
  if (!node) throw IvmRuntimeError.GraphNodeNotFound(persist);
  const op = node.descriptor.operator;
  if (op.kind !== "Persist") throw IvmRuntimeError.UnsupportedOperator();
  if (node.descriptor.inputs.length !== 1) throw IvmRuntimeError.GraphInputArityMismatch(persist);
  return [op.op, node.descriptor.inputs[0]];
}

/** Replay only schema-owned TableSource -> IndexBy -> Persist graphs. Startup callers must exclude publications and queries. */
export async function rebuildDeclaredIndexes(
  rt: IvmRuntime,
  storage: OrderedKvStorage,
): Promise<void> {
  const BATCH_SIZE = 1024;
  for (const table of [...rt.schema.tables] as TableSchema[]) {
    for (const index of table.indices as IndexSchema[]) {
      const [op, input] = persistNodeParts(rt, addDedupSchemaIndex(rt, table, index));
      // Repeated bounded prefix scans avoid retaining all old keys and never touch other prefixes.
      for (;;) {
        const entries = await collectScan(
          await storage.scan(
            ScanRequest.prefix(op.storage.columnFamily, op.storage.keyPrefix).withMaxItems(
              BATCH_SIZE,
            ),
          ),
        );
        if (!entries.length) break;
        await storage.writeMany(
          entries.map(([key]: [Uint8Array]) =>
            OwnedWriteOperation.Delete(op.storage.columnFamily, key),
          ),
        );
      }
      invalidateTableInputs(rt, table.name);
      const snapshot = await rt.hydrationSnapshot(input, storage, HydrationMode.Ordinary);
      // Bound write batches independently of the hydration read machinery.
      for (let i = 0; i < snapshot.deltas.length; i += BATCH_SIZE) {
        await applyPersistDelta(storage, op.storage, op.keyFields, op.unique, {
          descriptor: snapshot.descriptor,
          deltas: snapshot.deltas.slice(i, i + BATCH_SIZE),
        });
      }
    }
    // Repair hydration is temporary startup work: release its snapshots before recovery or user reads begin.
    if (table.indices.length) invalidateTableInputs(rt, table.name);
  }
}

export function registerTable(rt: IvmRuntime, table: TableSchema): void {
  if (rt.schema.table(table.name)) throw IvmRuntimeError.TableAlreadyExists(table.name);
  rt.tableStorageDescriptors.set(table.name, table.recordSchema());
  if (!table.hasVariants()) {
    rt.tableDescriptors.set(table.name, table.recordSchema());
  } else {
    let descriptors = rt.variantDescriptors.get(table.name);
    if (!descriptors) rt.variantDescriptors.set(table.name, (descriptors = new Map()));
    for (const variant of table.variants) {
      const descriptor = table.recordSchemaForVariant(variant.tag);
      if (descriptor) descriptors.set(variant.tag, descriptor);
    }
  }
  rt.schema.tables.push(table);
  defineSchemaIndexVariantProjections(rt);
  rt.addDedupSchemaIndices();
}

function findTable(rt: IvmRuntime, table: string): TableSchema {
  const t = rt.schema.tables.find((c: TableSchema) => c.name === table);
  if (!t) throw IvmRuntimeError.TableNotFound(table);
  return t;
}

export function registerTableVariantWithColumns(
  rt: IvmRuntime,
  table: string,
  columns: ColumnSchema[],
  variantTag: TableVariant,
): void {
  const tableSchema = findTable(rt, table);
  if (tableSchema.variant(variantTag.tag))
    throw IvmRuntimeError.DuplicateTableVariant(table, variantTag.tag);
  for (const column of columns) {
    if (tableSchema.columns.some((e: ColumnSchema) => e.name === column.name))
      throw IvmRuntimeError.TableFieldAlreadyExists(table, column.name);
    tableSchema.columns.push(column);
  }
  const version = variantTag.tag;
  for (const field of variantTag.payloadFields)
    field.valueType.collectVariantRegistries(tableSchema.valueVariantRegistries);
  tableSchema.variants.push(variantTag);
  rt.tableStorageDescriptors.set(table, tableSchema.recordSchema());
  const descriptor = tableSchema.recordSchemaForVariant(version);
  if (!descriptor) throw new Error("the newly registered variant exists");
  let descriptors = rt.variantDescriptors.get(table);
  if (!descriptors) rt.variantDescriptors.set(table, (descriptors = new Map()));
  descriptors.set(version, descriptor);
  const snapshot = tableSchema.clone();
  for (const index of snapshot.indices)
    registerSchemaIndexVariantCase(rt, snapshot, index, version);
  invalidateTableInputs(rt, table);
}

export function evolveTableVariantRegistries(
  rt: IvmRuntime,
  table: string,
  columns: ColumnSchema[],
): void {
  const tableSchema = findTable(rt, table);
  for (const desired of columns) {
    const existing = tableSchema.columns.find((c: ColumnSchema) => c.name === desired.name);
    if (!existing) continue;
    if (!existing.columnType.canEvolveRegistryTo(desired.columnType))
      throw IvmRuntimeError.TableFieldAlreadyExists(table, desired.name);
    for (const variant of tableSchema.variants)
      for (const field of variant.payloadFields) {
        if (field.sharedColumn !== desired.name) continue;
        if (!field.valueType.canEvolveRegistryTo(desired.columnType))
          throw IvmRuntimeError.TableFieldAlreadyExists(table, desired.name);
      }
  }
  for (const desired of columns) {
    const existing = tableSchema.columns.find((c: ColumnSchema) => c.name === desired.name);
    if (!existing) continue;
    existing.columnType = desired.columnType;
    for (const variant of tableSchema.variants)
      for (const field of variant.payloadFields)
        if (field.sharedColumn === desired.name) field.valueType = desired.columnType;
    desired.columnType.collectVariantRegistries(tableSchema.valueVariantRegistries);
  }
  rt.tableStorageDescriptors.set(table, tableSchema.recordSchema());
  // Registry evolution changes column types; the plain-table descriptor must follow.
  if (!tableSchema.hasVariants()) rt.tableDescriptors.set(table, tableSchema.recordSchema());
  const descriptors = new Map<number, RecordDescriptor>();
  for (const variant of tableSchema.variants) {
    const descriptor = tableSchema.recordSchemaForVariant(variant.tag);
    if (!descriptor) throw new Error("registered variants have valid descriptors");
    descriptors.set(variant.tag, descriptor);
  }
  rt.variantDescriptors.set(table, descriptors);
  invalidateTableInputs(rt, table);
}

export async function registerTableIndex(
  rt: IvmRuntime,
  table: string,
  index: IndexSchema,
  storage: OrderedKvStorage,
) {
  const staged = rt.clone();
  await registerTableIndexStaged(staged, table, index, storage);
  Object.assign(rt, staged);
}

async function registerTableIndexStaged(
  rt: IvmRuntime,
  table: string,
  index: IndexSchema,
  storage: OrderedKvStorage,
) {
  findTable(rt, table).indices.push(index);
  const tableSchema: TableSchema = findTable(rt, table).clone();
  // A runtime (re)creation always backfills under a fresh durable id.
  rt.indexIds.retire(table, index.name);
  if (tableSchema.hasVariants()) {
    defineVariantProjectionTarget(
      rt,
      table,
      schemaIndexTarget(index.name),
      schemaIndexInputDescriptor(tableSchema, index),
    );
    for (const variant of tableSchema.variants)
      registerSchemaIndexVariantCase(rt, tableSchema, index, variant.tag);
  }
  const persist = addDedupSchemaIndex(rt, tableSchema, index);
  invalidateTableInputs(rt, table);
  const [persistOp, input] = persistNodeParts(rt, persist);
  const snapshot = await rt.hydrationSnapshot(input, storage, HydrationMode.Ordinary);
  // The new index's id registration becomes durable in the same atomic write as its backfilled entries.
  const registrations = rt.indexIds.pending();
  await applyPersistDeltaWith(
    storage,
    persistOp.storage,
    persistOp.keyFields,
    persistOp.unique,
    snapshot,
    [...registrations.operations],
  );
  rt.indexIds.markPersisted(registrations);
}

export function defineVariantProjection(
  rt: IvmRuntime,
  table: string,
  target: string,
  output: RecordDescriptor,
): void {
  defineVariantProjectionTarget(rt, table, named(target), output);
}

function defineVariantProjectionTarget(
  rt: IvmRuntime,
  table: string,
  target: VariantProjectionTarget,
  output: RecordDescriptor,
): void {
  if (!rt.schema.table(table)) throw IvmRuntimeError.TableNotFound(table);
  const projections: Map<string, VariantProjection> = rt.variantProjections;
  const key = projectionKey(table, target);
  const existing = projections.get(key);
  if (!existing) projections.set(key, { output, cases: new Map() });
  else if (existing.output.registryCompatibleWith(output)) existing.output = output;
  else
    throw IvmRuntimeError.VariantProjectionOutputMismatch(
      table,
      variantProjectionTargetName(target),
    );
}

export const registerVariantProjectionCase = (
  rt: IvmRuntime,
  table: string,
  target: string,
  tag: number,
  fields: ProjectField[],
) => registerVariantProjectionTargetCase(rt, table, named(target), tag, fields, false, false);

/** Refresh a raw variant source descriptor after append-only enum registry evolution. */
export const refreshVariantProjectionCaseForRegistryEvolution = (
  rt: IvmRuntime,
  table: string,
  target: string,
  tag: number,
  fields: ProjectField[],
) => registerVariantProjectionTargetCase(rt, table, named(target), tag, fields, false, true);

/** Register a schema-read case whose unrepresentable enum values are source-local row exclusions. */
export const registerVariantProjectionCaseOmittingUnrepresentableEnums = (
  rt: IvmRuntime,
  table: string,
  target: string,
  tag: number,
  fields: ProjectField[],
) => registerVariantProjectionTargetCase(rt, table, named(target), tag, fields, true, false);

export const registerVariantProjectionIgnoreCase = (
  rt: IvmRuntime,
  table: string,
  target: string,
  tag: number,
) => registerVariantProjectionTargetCase(rt, table, named(target), tag, undefined, false, false);

function variantSource(rt: IvmRuntime, table: string, variantTag: number): RecordDescriptor {
  const t = rt.schema.table(table);
  if (!t) throw IvmRuntimeError.TableNotFound(table);
  const source = t.recordSchemaForVariant(variantTag);
  if (!source) throw IvmRuntimeError.UnknownTableVariant(table, variantTag);
  return source;
}

function variantProjectionOf(
  rt: IvmRuntime,
  table: string,
  target: VariantProjectionTarget,
): VariantProjection {
  const projection = rt.variantProjections.get(projectionKey(table, target));
  if (!projection)
    throw IvmRuntimeError.VariantProjectionNotFound(table, variantProjectionTargetName(target));
  return projection;
}

function buildMapProject(source: RecordDescriptor, fields: ProjectField[]): MapProjectOp {
  const mapping: [number, number][] = [];
  for (const field of fields) {
    const s = projectExprSource(field.expression);
    if (s) mapping.push([0, resolveFieldRef(source, s)]);
  }
  return {
    expressions: fields.map((field) => ({
      expression: projectFieldExpr(source, field),
      outputName: field.outputName,
      outputIdentity: field.outputIdentity,
    })),
    mapping,
  };
}

/** Append a physical source case that constructs one stable logical enum value. */
export function registerVariantProjectionEnumCase(
  rt: IvmRuntime,
  table: string,
  target: string,
  variantTag: number,
  enumField: string,
  enumSchema: EnumSchema,
  enumCase: string,
  fields: ProjectField[],
): void {
  const source = variantSource(rt, table, variantTag);
  const projection = variantProjectionOf(rt, table, named(target));
  const outputFields = projection.output.fields();
  const unionIdx = projection.output.fieldIndex(enumField);
  if (unionIdx === undefined)
    throw IvmRuntimeError.VariantProjectionEnumFieldNotFound(table, target, enumField);
  if (outputFields.length !== 1)
    throw IvmRuntimeError.VariantProjectionEnumOutputMustBeSingleField(table, target);
  const outputType = outputFields[unionIdx].valueType;
  if (outputType.kind !== "Enum")
    throw IvmRuntimeError.VariantProjectionEnumFieldTypeMismatch(table, target, enumField);
  if (!structEq(outputType.schema, enumSchema))
    throw IvmRuntimeError.VariantProjectionEnumSchemaMismatch(table, target, enumField);
  const tag = enumSchema.tag(enumCase);
  const payload = enumSchema.case(tag).payload;
  if (projectDescriptor(source, fields) !== payload)
    throw IvmRuntimeError.VariantProjectionEnumPayloadMismatch(table, target, enumCase);
  const project = buildMapProject(source, fields);
  const rawProjection = rawProjectionFields(project, source, payload);
  const kase: VariantProjectionCase = {
    kind: "Enum",
    source,
    tag,
    payload,
    project,
    rawProjection,
  };
  // Neither outcome replaces a case, so no retained result is stale.
  const existing = projection.cases.get(variantTag);
  if (!existing) projection.cases.set(variantTag, kase);
  else if (!structEq(existing, kase))
    throw IvmRuntimeError.VariantProjectionCaseAlreadyRegistered(table, target, variantTag);
}

export function projectVariantRecord(
  rt: IvmRuntime,
  table: string,
  target: string,
  record: VariantRecord,
): OwnedRecord | undefined {
  const projection = variantProjectionOf(rt, table, named(target));
  const tag = record.variantTag();
  const kase = projection.cases.get(tag);
  if (!kase) throw IvmRuntimeError.VariantProjectionCaseNotFound(table, target, tag);
  if (!kase.source.registryCompatibleWith(record.record().descriptor()))
    throw IvmRuntimeError.VariantProjectionSourceMismatch(table, target, tag);
  const input: RecordDeltas = {
    descriptor: record.record().descriptor(),
    deltas: [{ record: record.record().raw().slice(), weight: 1 }],
  };
  let projected: RecordDeltas;
  switch (kase.kind) {
    case "Project":
      projected = NodeState.updateMapProject(
        kase.project,
        projection.output,
        input,
        kase.rawProjection,
        kase.omitUnrepresentableEnumRows,
      );
      break;
    case "Enum":
      projected = NodeState.updateVariantEnumProject(
        kase.tag,
        kase.payload,
        kase.project,
        projection.output,
        input,
        kase.rawProjection,
      );
      break;
    case "Ignore":
      return undefined;
  }
  const first = projected.deltas[0];
  return first ? OwnedRecord.new(first.record.slice(), projection.output) : undefined;
}

function registerVariantProjectionTargetCase(
  rt: IvmRuntime,
  table: string,
  target: VariantProjectionTarget,
  variantTag: number,
  fields: ProjectField[] | undefined,
  omitUnrepresentableEnumRows: boolean,
  allowRegistryRefresh: boolean,
): void {
  const source = variantSource(rt, table, variantTag);
  const projection = variantProjectionOf(rt, table, target);
  const targetName = variantProjectionTargetName(target);
  // An explicitly typed literal is also a descriptor boundary: it copies no source cell, so no physical enum
  // registry leaks through it.
  const allowRecursiveReplacement =
    fields?.some(
      (f) => f.expression.kind === "RecursiveEnumRemap" || f.expression.kind === "TypedLiteral",
    ) ?? false;
  let kase: VariantProjectionCase;
  if (fields) {
    const projected = projectDescriptor(source, fields);
    // A recursive enum projector is an explicit descriptor boundary; values are re-encoded at execution time.
    if (!allowRecursiveReplacement && !projected.registryCompatibleWith(projection.output))
      throw IvmRuntimeError.VariantProjectionOutputMismatch(table, targetName);
    const project = buildMapProject(source, fields);
    const rawProjection = rawProjectionFields(project, source, projection.output);
    kase = { kind: "Project", source, project, rawProjection, omitUnrepresentableEnumRows };
  } else {
    kase = { kind: "Ignore", source };
  }
  // Only replacing a case can make a retained result stale; adding a case for a new tag leaves them valid (#3797).
  const existing = projection.cases.get(variantTag);
  let replaced = false;
  if (!existing) projection.cases.set(variantTag, kase);
  else if (structEq(existing, kase)) replaced = false;
  else if (
    allowRecursiveReplacement ||
    (allowRegistryRefresh && variantCaseCanRefreshRegistriesTo(existing, kase))
  ) {
    // The physical registry can append while this authored target stays fixed.
    projection.cases.set(variantTag, kase);
    replaced = true;
  } else
    throw IvmRuntimeError.VariantProjectionCaseAlreadyRegistered(table, targetName, variantTag);
  if (replaced) invalidateTableInputs(rt, table);
}

export function defineSchemaIndexVariantProjections(rt: IvmRuntime): void {
  for (const table of [...rt.schema.tables] as TableSchema[]) {
    if (!table.hasVariants()) continue;
    for (const index of table.indices) {
      defineVariantProjectionTarget(
        rt,
        table.name,
        schemaIndexTarget(index.name),
        schemaIndexInputDescriptor(table, index),
      );
      for (const variant of table.variants)
        registerSchemaIndexVariantCase(rt, table, index, variant.tag);
    }
  }
}

function registerSchemaIndexVariantCase(
  rt: IvmRuntime,
  table: TableSchema,
  index: IndexSchema,
  variantTag: number,
): void {
  const version = table.variant(variantTag);
  if (!version) throw IvmRuntimeError.UnknownTableVariant(table.name, variantTag);
  let fields: ProjectField[] | undefined = [];
  for (const shared of schemaIndexInputFields(table, index)) {
    const local = version.payloadNameForShared(shared);
    if (local === undefined) {
      fields = undefined;
      break;
    }
    fields.push(ProjectFieldCtor.renamed(local, shared));
  }
  registerVariantProjectionTargetCase(
    rt,
    table.name,
    schemaIndexTarget(index.name),
    variantTag,
    fields,
    false,
    false,
  );
}

export function invalidateTableInputs(rt: IvmRuntime, table: string): void {
  rt.tableFrontiers.set(table, (rt.tableFrontiers.get(table) ?? 0) + 1);
  const invalidated = [...rt.evalMemoKeysByNode.keys()].filter((node) =>
    rt.nodeMeta.get(node)?.inputSignature?.tables.some((c: string) => c === table),
  );
  rt.removeRetainedEvalMemosForNodes(new Set(invalidated));
}

export function index(rt: IvmRuntime, table: string, indexName: string): IndexSchema | undefined {
  return rt.table(table)?.indices.find((i: IndexSchema) => i.name === indexName);
}

export const directRecordStore = (rt: IvmRuntime, store: string) =>
  rt.schema.directRecordStore(store);

export function querySnapshot(
  rt: IvmRuntime,
  graph: GraphBuilder,
  storage: OrderedKvStorage,
): Promise<RecordDeltas> {
  return querySnapshotWithRootValues(rt, graph, storage, RootIndirectValues.Materialize);
}

/** Like querySnapshot, choosing how the result presents indirect root values. */
export async function querySnapshotWithRootValues(
  rt: IvmRuntime,
  graph: GraphBuilder,
  storage: OrderedKvStorage,
  rootIndirectValues: any,
): Promise<RecordDeltas> {
  await rt.flushPendingBindingRetractions(storage);
  if (builderContainsBindingSource(graph)) throw IvmRuntimeError.BindingSourceRequiresPrepare();
  const query = new EphemeralGraphInstall(rt);
  try {
    const runtime = query.runtime();
    runtime.logicalNodesRequested += countBuilderNodes(graph);
    const { output, node } = addDedupGraph(runtime, graph);
    const records = await runtime.hydrationSnapshotWithRootValues(
      node,
      storage,
      HydrationMode.Ordinary,
      rootIndirectValues,
    );
    if (!records.descriptor.registryCompatibleWith(output))
      throw IvmRuntimeError.GraphOutputMismatch();
    return records;
  } finally {
    query.drop();
  }
}

export async function querySnapshots(
  rt: IvmRuntime,
  sinks: Iterable<[string, GraphBuilder]>,
  storage: OrderedKvStorage,
): Promise<any> {
  const list = [...sinks];
  await rt.flushPendingBindingRetractions(storage);
  if (!list.length) throw IvmRuntimeError.EmptyMultisinkSubscription();
  const names = new Set<string>();
  for (const [sink, graph] of list) {
    if (names.has(sink)) throw IvmRuntimeError.DuplicateMultisinkSink(sink);
    names.add(sink);
    if (builderContainsBindingSource(graph))
      throw IvmRuntimeError.MultisinkSinkRequiresPrepare(sink);
  }
  const query = new EphemeralGraphInstall(rt);
  try {
    const runtime = query.runtime();
    runtime.logicalNodesRequested += list.reduce((n, [, g]) => n + countBuilderNodes(g), 0);
    const outputs = new Map<string, any>();
    for (const [sink, graph] of list.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
      outputs.set(sink, addDedupGraph(runtime, graph));
    return await runtime.hydrationSnapshots(outputs, storage, HydrationMode.Ordinary);
  } finally {
    query.drop();
  }
}

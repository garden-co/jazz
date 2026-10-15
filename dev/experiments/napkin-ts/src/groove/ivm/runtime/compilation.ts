// Hash-consed GraphBuilder compilation into executable IVM nodes.
import {
  type CompiledNode,
  type IndexSchema,
  type IvmRuntime,
  type TableSchema,
  IvmRuntimeError,
  RecordDescriptor,
  builderContainsRecursive,
  collectByOrderedScalar,
  joinOutputMapping,
  planExprNames,
  recursiveReadTables,
  resolveAggregateExpr,
  validateArgByPrimaryKeyIndices,
} from "../../../_extern/groove-ivm-plan.js";
import {
  type CollectByBuilder,
  type GraphBuilder,
  type NodeId,
  type ProjectExpr,
  FieldRef,
  NodeDescriptor,
  NodeOutput,
  type NodeDurability,
  type OpType,
} from "../graph.js";
import {
  type IndexSourceOp,
  type JoinOp,
  type PlanExpr,
  type ProjectionExpr,
  type StaticScanSpec,
  type TopByDirection,
  type TopByOrderField,
  type ValueComparison,
  PlanExpr as Plan,
  predicateHasTemplateArguments,
  structEq,
} from "../op_types.js";
import type { IndexCandidateFilter } from "../graph.js";
import { CompilationCache, compilationOrder } from "./compilation_cache.js";
import { projectionPlan } from "./typed_projection.js";
import { installTypedTemplate } from "./typed_template.js";
import { durableIndexKeyPrefix } from "./index_ids.js";
import { joinDescriptor, primaryKeyFieldIndices } from "./key_encoding.js";
import {
  collectByProjections,
  collectBySlots,
  fieldNameAt,
  fieldRefName,
  indexRecordDescriptor,
  resolveFieldName,
  resolveFieldRef,
  schemaIndexInputDescriptor,
  validateCollectByKeyTypes,
  validateCollectByTerminality,
} from "./record_projection.js";

type Memo<T> = Map<GraphBuilder, T>;
const EPHEMERAL: NodeDurability = { kind: "Ephemeral" };

function dedup(
  rt: IvmRuntime,
  operator: OpType,
  inputs: NodeId[],
  output: RecordDescriptor,
): NodeId {
  const node = rt.graph.dedupNode(
    new NodeDescriptor(operator, inputs, NodeOutput.records(output)),
    EPHEMERAL,
  );
  rt.initializeNodeRuntime(node);
  return node;
}

function addArrangementNode(
  rt: IvmRuntime,
  input: NodeId,
  records: RecordDescriptor,
  fields: string[],
  comparison: ValueComparison,
): NodeId {
  rt.logicalNodesRequested += 1;
  const node = rt.graph.dedupNode(
    new NodeDescriptor(
      { kind: "Arrange", op: { fields, comparison } },
      [input],
      NodeOutput.arrangement(records),
    ),
    EPHEMERAL,
  );
  rt.initializeNodeRuntime(node);
  return node;
}

/** Consumer-local column pruning. */
function compileSelectedColumns(
  rt: IvmRuntime,
  input: CompiledNode,
  selected: number[],
): CompiledNode {
  if (selected.length === input.output.fields().length) return input;
  const names = selected.map((i) => `__narrow_field_${i}`);
  const output = RecordDescriptor.new(
    selected.map((i, n) => [names[n], input.output.fields()[i].valueType]),
  );
  const expressions: ProjectionExpr[] = selected.map((i, n) => ({
    expression: { kind: "Field", source: FieldRef.resolved(i) },
    outputName: names[n],
    outputIdentity: { kind: "Name", name: names[n] },
  }));
  const mapping = selected.map((i): [number, number] => [0, i]);
  const node = dedup(
    rt,
    { kind: "MapProject", op: { expressions, mapping } },
    [input.node],
    output,
  );
  return { output, node, rootOrderingNode: input.rootOrderingNode };
}

const sortedUnique = (xs: number[]) => [...new Set(xs)].sort((a, b) => a - b);

/** Semi/anti joins observe right-key multiplicities, not right payloads. */
function compileExistenceInput(
  rt: IvmRuntime,
  input: CompiledNode,
  keys: FieldRef[],
): [CompiledNode, PlanExpr[]] {
  const indices = keys.map((k) => resolveFieldRef(input.output, k));
  const selected = sortedUnique(indices);
  const narrowed = compileSelectedColumns(rt, input, selected);
  return [
    narrowed,
    indices.map((i) =>
      Plan.field(fieldRefName(narrowed.output, FieldRef.resolved(selected.indexOf(i)))),
    ),
  ];
}

/** Project(Join) needs only consumer fields plus matching keys on each input. */
function narrowProjectedJoin(rt: IvmRuntime, input: NodeId, expressions: ProjectionExpr[]): NodeId {
  const node = rt.graph.node(input);
  if (!node) throw IvmRuntimeError.GraphNodeNotFound(input);
  const descriptor = node.descriptor;
  if (descriptor.operator.kind !== "Join") return input;
  const join = descriptor.operator.op;
  if (join.residualPredicate || join.kind !== "Inner") return input;
  const original = descriptor.output.records;
  const inputMapping: [number, number][] = joinOutputMapping(
    join.leftDescriptor,
    join.rightDescriptor,
    original,
  );
  const demanded: [number, number][] = [];
  for (const e of expressions) {
    const field = projectionSourceRef(e.expression);
    if (field) demanded.push(inputMapping[resolveFieldRef(original, field)]);
  }
  const leftKeys = planExprNames(join.leftKey).map((n: string) =>
    resolveFieldRef(join.leftDescriptor, FieldRef.name(n)),
  );
  const rightKeys = planExprNames(join.rightKey).map((n: string) =>
    resolveFieldRef(join.rightDescriptor, FieldRef.name(n)),
  );
  const leftFields = sortedUnique([
    ...demanded.filter(([s]) => s === 0).map(([, i]) => i),
    ...leftKeys,
  ]);
  const rightFields = sortedUnique([
    ...demanded.filter(([s]) => s === 1).map(([, i]) => i),
    ...rightKeys,
  ]);
  if (
    leftFields.length === join.leftDescriptor.fields().length &&
    rightFields.length === join.rightDescriptor.fields().length
  )
    return input;
  // Prune record inputs, then build consumer-specific arrangements; never mutate a shared one.
  const compileSide = (
    arrangement: NodeId,
    output: RecordDescriptor,
    fields: number[],
    keys: number[],
  ) => {
    const arr = rt.graph.node(arrangement);
    if (!arr) throw IvmRuntimeError.GraphNodeNotFound(arrangement);
    const compiled = compileSelectedColumns(
      rt,
      { node: arr.descriptor.inputs[0], output, rootOrderingNode: undefined },
      fields,
    );
    const key = keys.map((i) =>
      Plan.field(fieldRefName(compiled.output, FieldRef.resolved(fields.indexOf(i)))),
    );
    const arranged = addArrangementNode(
      rt,
      compiled.node,
      compiled.output,
      planExprNames(key),
      join.comparison,
    );
    return [compiled.output, arranged, key] as const;
  };
  const [leftDescriptor, left, leftKey] = compileSide(
    descriptor.inputs[0],
    join.leftDescriptor,
    leftFields,
    leftKeys,
  );
  const [rightDescriptor, right, rightKey] = compileSide(
    descriptor.inputs[1],
    join.rightDescriptor,
    rightFields,
    rightKeys,
  );
  const output = joinDescriptor(leftDescriptor, rightDescriptor);
  const outputPositions = new Map<string, number>();
  (joinOutputMapping(leftDescriptor, rightDescriptor, output) as [number, number][]).forEach(
    ([s, i], position) => outputPositions.set(`${s}:${i}`, position),
  );
  for (const e of expressions) {
    const field = projectionSourceRef(e.expression);
    if (!field) continue;
    const [side, old] = inputMapping[resolveFieldRef(original, field)];
    const physical = (side === 0 ? leftFields : rightFields).indexOf(old);
    (e as any).expression = {
      ...e.expression,
      source: FieldRef.resolved(outputPositions.get(`${side}:${physical}`)!),
    };
  }
  return dedup(
    rt,
    { kind: "Join", op: { ...join, leftDescriptor, rightDescriptor, leftKey, rightKey } },
    [left, right],
    output,
  );
}

export function addDedupGraph(rt: IvmRuntime, graph: GraphBuilder): CompiledNode {
  return addDedupGraphInner(rt, graph, false);
}

/** Only the isolated template compiler accepts unbound scalar arguments. */
export function addDedupTemplateGraph(rt: IvmRuntime, graph: GraphBuilder): CompiledNode {
  return addDedupGraphInner(rt, graph, true);
}

function addDedupGraphInner(rt: IvmRuntime, graph: GraphBuilder, template: boolean): CompiledNode {
  validateCollectByTerminality(graph);
  const outputMemo: Memo<RecordDescriptor> = new Map();
  const compiledMemo: Memo<CompiledNode> = new Map();
  const cache: CompilationCache = rt.compilationCache;
  for (const [builder, owner] of compilationOrder(graph)) {
    if (
      !template &&
      ((builder.kind === "Filter" && predicateHasTemplateArguments(builder.predicate)) ||
        (builder.kind === "Project" &&
          builder.fields.some((f) => f.expression.kind === "TemplateArgument")))
    )
      throw IvmRuntimeError.UnsupportedOperator();
    const hit = owner && cache.get(owner, compiledMemo, outputMemo, rt.graph);
    if (hit) {
      rt.logicalNodesRequested += hit[2];
      outputMemo.set(builder, hit[1]);
      compiledMemo.set(builder, hit[0]);
      continue;
    }
    // Inputs are already inferred and compiled, so source checks run before reuse.
    const inferred = rt.inferBuilderOutputUncached(builder, outputMemo);
    outputMemo.set(builder, inferred);
    const before = rt.logicalNodesRequested;
    const compiled = addDedupGraphCached(rt, builder, outputMemo, compiledMemo);
    if (owner)
      cache.insert(
        owner,
        compiledMemo,
        outputMemo,
        compiled,
        inferred,
        rt.logicalNodesRequested - before,
      );
  }
  const result = compiledMemo.get(graph);
  if (!result) throw IvmRuntimeError.UnsupportedOperator();
  return result;
}

function addDedupGraphCached(
  rt: IvmRuntime,
  graph: GraphBuilder,
  outputMemo: Memo<RecordDescriptor>,
  compiledMemo: Memo<CompiledNode>,
): CompiledNode {
  const memo = compiledMemo.get(graph);
  if (memo) return memo;
  const inferred: RecordDescriptor = rt.inferBuilderOutputCached(graph, outputMemo);
  const sub = (g: GraphBuilder) => addDedupGraphCached(rt, g, outputMemo, compiledMemo);
  let compiled: CompiledNode;
  switch (graph.kind) {
    case "TypedTemplate": {
      const inputs = graph.inputs.map((input) => {
        const c = compiledMemo.get(input);
        if (!c) throw IvmRuntimeError.UnsupportedOperator();
        return c;
      });
      compiled = installTypedTemplate(
        rt,
        graph.program,
        inputs,
        graph.inputs,
        graph.predicates,
        graph.scalars,
      );
      break;
    }
    case "TemplateInput": {
      if (!graph.input) throw IvmRuntimeError.UnsupportedOperator();
      compiled = sub(graph.input);
      if (compiled.output !== graph.output) throw IvmRuntimeError.GraphOutputMismatch();
      break;
    }
    case "ArgMaxBy":
    case "ArgMinBy":
    case "TopBy":
      compiled = addDedupOrderingGraph(rt, graph, inferred, sub);
      break;
    case "Aggregate":
    case "Filter":
    case "Project":
    case "StreamingChecksum":
    case "UnwrapNullable":
    case "Unnest":
    case "VariantProject":
    case "Union":
      compiled = addDedupUnaryGraph(rt, graph, inferred, sub);
      break;
    case "Join":
    case "SemiJoin":
    case "AntiJoin":
      compiled = addDedupJoinGraph(rt, graph, inferred, sub);
      break;
    case "Lookup":
      compiled = addDedupLookupGraph(rt, graph, inferred, sub);
      break;
    default:
      compiled = addDedupSourceGraph(rt, graph, inferred, sub);
  }
  compiledMemo.set(graph, compiled);
  return compiled;
}

type Sub = (g: GraphBuilder) => CompiledNode;

/** Compile a keyed lookup: one node over its key relation that probes the table. */
function addDedupLookupGraph(
  rt: IvmRuntime,
  graph: GraphBuilder & { kind: "Lookup" },
  inferred: RecordDescriptor,
  sub: Sub,
): CompiledNode {
  const { keys, keysOn, table, scan, variantProjection } = graph;
  let prefixLen = 0;
  if (scan) {
    if (scan.kind !== "Prefix" && scan.kind !== "Point")
      throw IvmRuntimeError.UnsupportedLookup("a lookup's static scan must be a point or prefix");
    prefixLen = scan.values.length;
  }
  if (!keysOn.length)
    throw IvmRuntimeError.UnsupportedLookup("a lookup needs at least one key field");
  const compiledKeys = sub(keys);
  const keyDescriptor = compiledKeys.output;
  const keyFields = keysOn.map((f) => resolveFieldRef(keyDescriptor, f));
  const tableSchema = rt.schema.table(table);
  if (!tableSchema) throw IvmRuntimeError.TableNotFound(table);
  const primaryKey = primaryKeyFieldIndices(tableSchema, inferred);
  if (primaryKey.length < prefixLen + keyFields.length)
    throw IvmRuntimeError.UnsupportedLookup(
      "a lookup's prefix and keys exceed the table's primary key",
    );
  // The probe is the storage key encoding of the key values.
  keyFields.forEach((key, i) => {
    const column = primaryKey[prefixLen + i];
    if (!typesEqual(keyDescriptor.fields()[key].valueType, inferred.fields()[column].valueType))
      throw IvmRuntimeError.UnsupportedLookup(
        "a lookup key field must have its primary-key column's type",
      );
  });
  const node = dedup(
    rt,
    {
      kind: "Lookup",
      op: {
        source: {
          table,
          scan,
          variantProjection:
            variantProjection === undefined
              ? undefined
              : { kind: "Named", name: variantProjection },
        },
        keyDescriptor,
        keyFields,
      },
    },
    [compiledKeys.node],
    inferred,
  );
  return { output: inferred, node, rootOrderingNode: undefined };
}

const typesEqual = structEq;
const leaf = (output: RecordDescriptor, node: NodeId): CompiledNode => ({
  output,
  node,
  rootOrderingNode: undefined,
});

function addDedupSourceGraph(
  rt: IvmRuntime,
  graph: GraphBuilder,
  inferred: RecordDescriptor,
  sub: Sub,
): CompiledNode {
  switch (graph.kind) {
    case "Table":
      return leaf(
        inferred,
        dedup(
          rt,
          {
            kind: "TableSource",
            op: {
              table: graph.table,
              scan: graph.scan,
              variantProjection:
                graph.variantProjection === undefined
                  ? undefined
                  : { kind: "Named", name: graph.variantProjection },
            },
          },
          [],
          inferred,
        ),
      );
    case "InlineRecords":
      if (!inferred.registryCompatibleWith(graph.output))
        throw IvmRuntimeError.GraphOutputMismatch();
      return leaf(
        graph.output,
        dedup(rt, { kind: "InlineRecords", op: { records: graph.records } }, [], graph.output),
      );
    case "InputSource": {
      const { id, output } = graph;
      if (!id.belongsTo(rt.inputSourceRuntimeNamespace)) throw IvmRuntimeError.ForeignInputSource();
      if (!inferred.registryCompatibleWith(output)) throw IvmRuntimeError.GraphOutputMismatch();
      const key = id.bindingKey();
      const source = rt.bindingSources.get(key);
      if (!source) throw IvmRuntimeError.InputSourceRetired();
      if (source.descriptor !== output)
        throw IvmRuntimeError.BindingSourceDescriptorMismatch(id.diagnosticName());
      // A mutable input uses the same runtime delta machinery as a prepared binding source.
      return leaf(output, dedup(rt, { kind: "BindingSource", op: { key } }, [], output));
    }
    case "Index": {
      const table: TableSchema = rt.schema.table(graph.table);
      if (!table) throw IvmRuntimeError.TableNotFound(graph.table);
      const index = table.indices.find((c: IndexSchema) => c.name === graph.index);
      if (!index) throw IvmRuntimeError.IndexNotFound(graph.index);
      const source = indexSourceOp(
        rt,
        table,
        index,
        graph.scan,
        graph.intersections,
        graph.candidateFilter,
        graph.rowProjection,
      );
      return leaf(inferred, dedup(rt, { kind: "IndexSource", op: source }, [], inferred));
    }
    case "FrontierSource":
      return leaf(
        inferred,
        dedup(rt, { kind: "FrontierSource", op: { binding: graph.binding } }, [], graph.output),
      );
    case "BindingSource":
      return leaf(
        inferred,
        dedup(
          rt,
          { kind: "BindingSource", op: { key: { kind: "Prepared", shape: graph.shape } } },
          [],
          graph.output,
        ),
      );
    case "Recursive": {
      const { seed, step, stepWitness } = graph;
      if (
        builderContainsRecursive(seed) ||
        builderContainsRecursive(step) ||
        (stepWitness && builderContainsRecursive(stepWitness))
      )
        throw IvmRuntimeError.UnsupportedNestedRecursion();
      const cSeed = sub(seed);
      const cStep = sub(step);
      const cWitness = stepWitness && sub(stepWitness);
      if (
        !cSeed.output.registryCompatibleWith(cStep.output) ||
        !cSeed.output.registryCompatibleWith(inferred)
      )
        throw IvmRuntimeError.GraphOutputMismatch();
      const node = dedup(
        rt,
        {
          kind: "Recursive",
          op: {
            frontier: graph.frontier,
            maxIters: graph.maxIters,
            truncateAtMaxIters: graph.truncateAtMaxIters,
            stepWitnessOutput: cWitness?.output,
            readTables: recursiveReadTables(rt.graph, cSeed.node, cStep.node, cWitness?.node),
          },
        },
        cWitness ? [cSeed.node, cStep.node, cWitness.node] : [cSeed.node, cStep.node],
        inferred,
      );
      return leaf(inferred, node);
    }
    case "RecursiveStepWitness": {
      if (graph.recursive.kind !== "Recursive" || !graph.recursive.stepWitness)
        throw IvmRuntimeError.UnsupportedOperator();
      const rec = sub(graph.recursive);
      const recNode = rt.graph.node(rec.node);
      if (!recNode) throw IvmRuntimeError.GraphNodeNotFound(rec.node);
      const inputs = recNode.descriptor.inputs;
      if (inputs.length !== 3) throw IvmRuntimeError.GraphInputArityMismatch(rec.node);
      const witness = rt.graph.node(inputs[2]);
      if (!witness) throw IvmRuntimeError.GraphNodeNotFound(inputs[2]);
      const output = witness.descriptor.output.records;
      return leaf(output, dedup(rt, { kind: "RecursiveStepWitness", op: {} }, [rec.node], output));
    }
    case "CollectBy":
      return addCollectByGraph(rt, graph.input, graph.collect, inferred, sub);
    default:
      throw new Error("dispatcher routes only source graph builders here");
  }
}

function addDedupOrderingGraph(
  rt: IvmRuntime,
  graph: GraphBuilder,
  output: RecordDescriptor,
  sub: Sub,
): CompiledNode {
  if (graph.kind === "ArgMaxBy" || graph.kind === "ArgMinBy") {
    const compiledInput = sub(graph.input);
    const groupFieldIndices = graph.groupCols.map((f) => resolveFieldRef(output, f));
    const orderFieldIndices = graph.orderCols.map((f) => resolveFieldRef(output, f));
    let comparisonFieldIndices: number[];
    if (graph.input.kind === "Table") {
      const table = graph.input.table;
      const tableSchema = rt.schema.table(table);
      if (!tableSchema) throw IvmRuntimeError.TableNotFound(table);
      const primaryKey = tableSchema.primaryKey;
      if (!primaryKey) throw IvmRuntimeError.MissingPrimaryKey(table);
      comparisonFieldIndices = primaryKey.columns.map((c: any) => {
        const idx = output.fieldIndex(c.column);
        if (idx === undefined) throw IvmRuntimeError.GraphFieldNotFound(c.column);
        return idx;
      });
      validateArgByPrimaryKeyIndices(
        graph.kind === "ArgMaxBy" ? "arg_max_by" : "arg_min_by",
        tableSchema,
        groupFieldIndices,
        orderFieldIndices,
        comparisonFieldIndices,
      );
    } else comparisonFieldIndices = [...groupFieldIndices, ...orderFieldIndices];
    const groupFields = groupFieldIndices.map((i) => fieldNameAt(output, i));
    const orderFields = orderFieldIndices.map((i) => fieldNameAt(output, i));
    const arrangement = addArrangementNode(rt, compiledInput.node, output, groupFields, "Exact");
    const node = dedup(
      rt,
      {
        kind: graph.kind,
        op: { groupFields, orderFields, groupFieldIndices, comparisonFieldIndices },
      },
      [arrangement],
      output,
    );
    return { output, node, rootOrderingNode: compiledInput.rootOrderingNode };
  }
  if (graph.kind !== "TopBy")
    throw new Error("dispatcher routes only ordering graph builders here");
  const compiledInput = sub(graph.input);
  const groupFieldIndices = graph.groupCols.map((f) => resolveFieldRef(output, f));
  const orderFieldIndices = graph.orderCols.map((o) => resolveFieldRef(output, o.field));
  const tieFieldIndices = graph.tieCols.map((f) => resolveFieldRef(output, f));
  for (const idx of [...orderFieldIndices, ...tieFieldIndices]) {
    const field = output.fields()[idx];
    if (!field) throw IvmRuntimeError.GraphFieldIndexOutOfBounds(idx);
    if (!collectByOrderedScalar(field.valueType))
      throw IvmRuntimeError.InvalidTopBy(
        `sort field ${fieldNameAt(output, idx)} must be an orderable scalar`,
      );
  }
  const groupFields = groupFieldIndices.map((i) => fieldNameAt(output, i));
  const orderFields: TopByOrderField[] = graph.orderCols.map((o, i) => ({
    field: fieldNameAt(output, orderFieldIndices[i]),
    direction: o.direction,
  }));
  const arrangement = addArrangementNode(rt, compiledInput.node, output, groupFields, "Exact");
  const node = dedup(
    rt,
    {
      kind: "TopBy",
      op: {
        groupFields,
        groupFieldIndices,
        orderFields,
        tieFields: tieFieldIndices.map((i) => fieldNameAt(output, i)),
        sortFieldIndices: [...orderFieldIndices, ...tieFieldIndices],
        sortDirections: [
          ...graph.orderCols.map((o) => o.direction),
          ...tieFieldIndices.map((): TopByDirection => "Asc"),
        ],
        offset: graph.offset,
        limit: graph.limit,
      },
    },
    [arrangement],
    output,
  );
  return { output, node, rootOrderingNode: node };
}

function addDedupUnaryGraph(
  rt: IvmRuntime,
  graph: GraphBuilder,
  output: RecordDescriptor,
  sub: Sub,
): CompiledNode {
  switch (graph.kind) {
    case "Aggregate": {
      const input = sub(graph.input);
      const inOut = input.output;
      const groupFieldIndices = graph.groupCols.map((f) => resolveFieldRef(inOut, f));
      const groupKey = groupFieldIndices.map((field) => {
        const carrier = FieldRef.storedName(fieldNameAt(inOut, field));
        if (resolveFieldRef(inOut, carrier) !== field)
          throw IvmRuntimeError.AmbiguousGraphFieldReference(FieldRef.displayName(carrier));
        // Only use a spelling that resolves back to this group slot.
        return Plan.field(fieldRefName(inOut, carrier));
      });
      const aggregates = graph.aggregates.map((a) => resolveAggregateExpr(inOut, a));
      const arrangement = addArrangementNode(
        rt,
        input.node,
        inOut,
        planExprNames(groupKey),
        "Exact",
      );
      const node = dedup(
        rt,
        { kind: "Aggregate", op: { groupKey, groupFieldIndices, aggregates } },
        [arrangement],
        output,
      );
      return { output, node, rootOrderingNode: input.rootOrderingNode };
    }
    case "Filter": {
      const input = sub(graph.input);
      const node = dedup(
        rt,
        { kind: "Filter", op: { predicate: graph.predicate, comparison: graph.comparison } },
        [input.node],
        output,
      );
      return { output, node, rootOrderingNode: input.rootOrderingNode };
    }
    case "Project": {
      const input = sub(graph.input);
      let inputNode = input.node;
      const expressions = projectionPlan(rt, input.output, graph.fields)
        .expressions()
        .map((e) => ({ ...e }));
      // Compose only total field selections; never cross semantic operators.
      while (expressions.every((e) => e.expression.kind === "Field")) {
        const parent = rt.graph.node(inputNode);
        if (!parent) throw IvmRuntimeError.GraphNodeNotFound(inputNode);
        const op = parent.descriptor.operator;
        if (op.kind !== "MapProject") break;
        if (
          !op.op.expressions.length ||
          !op.op.expressions.every((e) => e.expression.kind === "Field")
        )
          break;
        for (const e of expressions) {
          const index = resolveFieldRef(
            parent.descriptor.output.records,
            (e.expression as any).source,
          );
          e.expression = op.op.expressions[index].expression;
        }
        inputNode = parent.descriptor.inputs[0];
      }
      inputNode = narrowProjectedJoin(rt, inputNode, expressions);
      const sourceNode = rt.graph.node(inputNode);
      if (!sourceNode) throw IvmRuntimeError.GraphNodeNotFound(inputNode);
      const sourceOutput = sourceNode.descriptor.output.records;
      const mapping = expressions.flatMap((e): [number, number][] =>
        e.expression.kind === "Field"
          ? [[0, resolveFieldRef(sourceOutput, e.expression.source)]]
          : [],
      );
      const node = dedup(
        rt,
        { kind: "MapProject", op: { expressions, mapping } },
        [inputNode],
        output,
      );
      return { output, node, rootOrderingNode: input.rootOrderingNode };
    }
    case "StreamingChecksum": {
      if (graph.windowBytes === 0 || graph.maxBytesPerTurn === 0)
        throw IvmRuntimeError.InvalidStreamingChecksumBudget();
      const input = sub(graph.input);
      const node = dedup(
        rt,
        {
          kind: "StreamingChecksum",
          op: {
            field: fieldRefName(input.output, graph.field),
            fieldIdx: resolveFieldRef(input.output, graph.field),
            outputField: graph.outputField,
            windowBytes: graph.windowBytes,
            maxBytesPerTurn: graph.maxBytesPerTurn,
          },
        },
        [input.node],
        output,
      );
      return { output, node, rootOrderingNode: input.rootOrderingNode };
    }
    case "UnwrapNullable": {
      const input = sub(graph.input);
      const node = dedup(
        rt,
        {
          kind: "UnwrapNullable",
          op: {
            field: fieldRefName(input.output, graph.field),
            fieldIdx: resolveFieldRef(input.output, graph.field),
          },
        },
        [input.node],
        output,
      );
      return { output, node, rootOrderingNode: input.rootOrderingNode };
    }
    case "Unnest": {
      const input = sub(graph.input);
      const node = dedup(
        rt,
        {
          kind: "Unnest",
          op: {
            arrayField: fieldRefName(input.output, graph.arrayField),
            arrayFieldIdx: resolveFieldRef(input.output, graph.arrayField),
            elementField: graph.elementField,
          },
        },
        [input.node],
        output,
      );
      return { output, node, rootOrderingNode: input.rootOrderingNode };
    }
    case "VariantProject": {
      const input = sub(graph.input);
      const fieldIdx = resolveFieldRef(input.output, graph.field);
      const valueType = input.output.fields()[fieldIdx].valueType;
      if (valueType.kind !== "Enum")
        throw IvmRuntimeError.VariantProjectFieldTypeMismatch(
          fieldRefName(input.output, graph.field),
        );
      const tag = valueType.schema.tag(graph.case);
      const node = dedup(
        rt,
        {
          kind: "VariantProject",
          op: { field: fieldRefName(input.output, graph.field), fieldIdx, tag },
        },
        [input.node],
        output,
      );
      return { output, node, rootOrderingNode: input.rootOrderingNode };
    }
    case "Union": {
      const inputNodes: NodeId[] = [];
      let publicRootOrdering: NodeId | undefined;
      for (const g of graph.inputs) {
        const input = sub(g);
        // Structured lowering places the public/root anchor first.
        if (!inputNodes.length) publicRootOrdering = input.rootOrderingNode;
        if (!output.registryCompatibleWith(input.output))
          throw IvmRuntimeError.GraphOutputMismatch();
        inputNodes.push(input.node);
      }
      const node = dedup(rt, { kind: "Union" }, inputNodes, output);
      return { output, node, rootOrderingNode: publicRootOrdering };
    }
    default:
      throw new Error("dispatcher routes only unary graph builders here");
  }
}

function addDedupJoinGraph(
  rt: IvmRuntime,
  graph: GraphBuilder,
  output: RecordDescriptor,
  sub: Sub,
): CompiledNode {
  if (graph.kind !== "Join" && graph.kind !== "SemiJoin" && graph.kind !== "AntiJoin")
    throw new Error("dispatcher routes only join graph builders here");
  const left = sub(graph.left);
  let right = sub(graph.right);
  let rightKey: PlanExpr[];
  if (graph.kind === "Join")
    rightKey = graph.rightOn.map((f) => Plan.field(fieldRefName(right.output, f)));
  else [right, rightKey] = compileExistenceInput(rt, right, graph.rightOn);
  const leftKey = graph.leftOn.map((f) => Plan.field(fieldRefName(left.output, f)));
  const leftArr = addArrangementNode(
    rt,
    left.node,
    left.output,
    planExprNames(leftKey),
    graph.comparison,
  );
  const rightArr = addArrangementNode(
    rt,
    right.node,
    right.output,
    planExprNames(rightKey),
    graph.comparison,
  );
  const op: JoinOp = {
    kind: "Inner",
    leftKey,
    rightKey,
    leftDescriptor: left.output,
    rightDescriptor: right.output,
    comparison: graph.comparison,
  };
  const node = dedup(rt, { kind: graph.kind, op }, [leftArr, rightArr], output);
  // Prefer the public (left) side so a nested TopBy cannot become root ordering.
  return { output, node, rootOrderingNode: left.rootOrderingNode ?? right.rootOrderingNode };
}

function addCollectByGraph(
  rt: IvmRuntime,
  input: GraphBuilder,
  collect: CollectByBuilder,
  output: RecordDescriptor,
  sub: Sub,
): CompiledNode {
  const compiledInput = sub(input);
  const inOut = compiledInput.output;
  const resolve = (fs: FieldRef[]) => fs.map((f) => resolveFieldRef(inOut, f));
  const names = (idx: number[]) => idx.map((i) => fieldNameAt(inOut, i));
  const groupFieldIndices = resolve(collect.groupCols);
  const parentFields = collectByProjections(inOut, collect.parentFields);
  const direction = (i: number): TopByDirection => collect.orderCols[i]?.direction ?? "Asc";
  const finish = (op: any) => {
    const arrangement = addArrangementNode(rt, compiledInput.node, inOut, op.groupFields, "Exact");
    const node = dedup(rt, { kind: "CollectBy", op }, [arrangement], output);
    // CollectBy changes representation, not the order of public roots.
    return { output, node, rootOrderingNode: compiledInput.rootOrderingNode };
  };
  if (
    (collect.mode === "Collect" || collect.mode === "Root") &&
    (collect.slots.length || collect.mode === "Root")
  ) {
    if (!parentFields.length)
      throw IvmRuntimeError.InvalidCollectBy("tree collect requires a parent projection");
    validateCollectByKeyTypes(inOut, groupFieldIndices);
    const slots = collectBySlots(inOut, collect.slots, groupFieldIndices, 1);
    const orderFieldIndices = collect.orderCols.length
      ? collect.orderCols.map((o) => resolveFieldRef(inOut, o.field))
      : groupFieldIndices;
    const tieFieldIndices = collect.tieCols.length ? resolve(collect.tieCols) : groupFieldIndices;
    validateCollectByKeyTypes(inOut, orderFieldIndices);
    validateCollectByKeyTypes(inOut, tieFieldIndices);
    return finish({
      mode: collect.mode,
      groupFields: names(groupFieldIndices),
      groupFieldIndices,
      parentFields,
      childFields: [],
      childDescriptor: RecordDescriptor.new([]),
      collectionField: "",
      collectionFieldIndex: 0,
      slots,
      tupleFields: [],
      occurrenceIdFields: [],
      occurrenceIdFieldIndices: [],
      orderFields: orderFieldIndices.map((idx, i) => ({
        field: fieldNameAt(inOut, idx),
        direction: direction(i),
      })),
      tieFields: names(tieFieldIndices),
      sortFieldIndices: [...orderFieldIndices, ...tieFieldIndices],
      sortDirections: [
        ...orderFieldIndices.map((_, i) => direction(i)),
        ...tieFieldIndices.map(() => "Asc"),
      ],
      offset: collect.offset,
      limit: collect.limit,
    });
  }
  const childFields = collectByProjections(inOut, collect.childFields);
  const tupleFields = collectByProjections(inOut, collect.tupleFields);
  const occurrenceIdFieldIndices = resolve(collect.occurrenceIdCols);
  // Parent values must be determined by the group.
  if (
    collect.mode === "Collect" &&
    parentFields.some((f: any) => !groupFieldIndices.includes(f.fieldIdx))
  )
    throw IvmRuntimeError.InvalidCollectBy("parent projection fields must be grouping fields");
  validateCollectByKeyTypes(inOut, groupFieldIndices);
  validateCollectByKeyTypes(inOut, occurrenceIdFieldIndices);
  const orderFieldIndices = collect.orderCols.map((o) => resolveFieldRef(inOut, o.field));
  const tieFieldIndices = resolve(collect.tieCols);
  if (!orderFieldIndices.length || !tieFieldIndices.length)
    throw IvmRuntimeError.InvalidCollectBy(
      "order and tie fields must both be complete and non-empty",
    );
  validateCollectByKeyTypes(inOut, orderFieldIndices);
  validateCollectByKeyTypes(inOut, tieFieldIndices);
  const childDescriptor = RecordDescriptor.new(
    childFields.map((f: any) => [f.outputName, inOut.fields()[f.fieldIdx].valueType]),
  );
  if (collect.mode === "Expand" && (!tupleFields.length || !occurrenceIdFieldIndices.length))
    throw IvmRuntimeError.InvalidCollectBy(
      "expand mode requires a tuple projection and ordered occurrence-id fields",
    );
  return finish({
    mode: collect.mode,
    groupFields: names(groupFieldIndices),
    groupFieldIndices,
    parentFields,
    childFields,
    childDescriptor,
    collectionField: collect.collectionField,
    collectionFieldIndex: parentFields.length,
    slots: [],
    tupleFields,
    occurrenceIdFields: names(occurrenceIdFieldIndices),
    occurrenceIdFieldIndices,
    orderFields: collect.orderCols.map((o, i) => ({
      field: fieldNameAt(inOut, orderFieldIndices[i]),
      direction: o.direction,
    })),
    tieFields: names(tieFieldIndices),
    sortFieldIndices: [...orderFieldIndices, ...tieFieldIndices],
    sortDirections: [
      ...collect.orderCols.map((o) => o.direction),
      ...tieFieldIndices.map(() => "Asc"),
    ],
    offset: collect.offset,
    limit: collect.limit,
  });
}

export function addDedupSchemaIndex(
  rt: IvmRuntime,
  table: TableSchema,
  index: IndexSchema,
): NodeId {
  rt.logicalNodesRequested += 3;
  const [tableDescriptor, variantProjection] = table.hasVariants()
    ? [schemaIndexInputDescriptor(table, index), { kind: "SchemaIndex" as const, name: index.name }]
    : [table.recordSchema(), undefined];
  const input = dedup(
    rt,
    { kind: "TableSource", op: { table: table.name, variantProjection } },
    [],
    tableDescriptor,
  );
  const { output: indexDescriptor, node: indexBy } = addDedupIndexByFromInput(
    rt,
    table,
    index,
    input,
    tableDescriptor,
  );
  const storage = {
    columnFamily: "indices",
    keyPrefix: durableIndexKeyPrefix(rt.indexStorageId(table.name, index)),
    name: `${table.name}.${index.name}`,
  };
  const persist = rt.graph.dedupNode(
    new NodeDescriptor(
      { kind: "Persist", op: { name: index.name, storage, keyFields: [0], unique: index.unique } },
      [indexBy],
      NodeOutput.records(indexDescriptor),
    ),
    { kind: "Durable", storage },
  );
  rt.addRetainer(persist, { kind: "DurableSchemaObject", id: `${table.name}.${index.name}` });
  rt.initializeNodeRuntime(persist);
  return persist;
}

function indexSourceOp(
  rt: IvmRuntime,
  table: TableSchema,
  index: IndexSchema,
  scan: StaticScanSpec | undefined,
  intersections: [string, StaticScanSpec][],
  candidateFilter: IndexCandidateFilter | undefined,
  rowProjection: string | undefined,
): IndexSourceOp {
  const findIndex = (t: TableSchema, name: string) =>
    t.indices.find((i: IndexSchema) => i.name === name);
  for (const [name] of intersections)
    if (!findIndex(table, name)) throw IvmRuntimeError.IndexNotFound(name);
  if (candidateFilter) {
    if (
      rowProjection === undefined ||
      intersections.length ||
      scan?.kind !== "Prefix" ||
      candidateFilter.scan.kind !== "Prefix"
    )
      throw IvmRuntimeError.UnsupportedIndexCandidateFilter();
    const candidateTable = rt.schema.table(candidateFilter.table);
    if (!candidateTable) throw IvmRuntimeError.TableNotFound(candidateFilter.table);
    const candidateIndex = findIndex(candidateTable, candidateFilter.index);
    if (!candidateIndex) throw IvmRuntimeError.IndexNotFound(candidateFilter.index);
    const covered = (t: TableSchema, i: IndexSchema, column: string) =>
      i.columns.includes(column) || !!t.primaryKey?.columns.some((p: any) => p.column === column);
    if (!covered(table, index, candidateFilter.sourceColumn))
      throw IvmRuntimeError.GraphFieldNotFound(candidateFilter.sourceColumn);
    if (!covered(candidateTable, candidateIndex, candidateFilter.candidateColumn))
      throw IvmRuntimeError.GraphFieldNotFound(candidateFilter.candidateColumn);
  }
  const [tableDescriptor, variantProjection] = table.hasVariants()
    ? [schemaIndexInputDescriptor(table, index), { kind: "SchemaIndex" as const, name: index.name }]
    : [table.recordSchema(), undefined];
  const resolve = (column: string) => {
    const idx = resolveFieldName(tableDescriptor, column);
    if (idx === undefined) throw IvmRuntimeError.GraphFieldNotFound(column);
    return idx;
  };
  const keyFields = index.columns.map(resolve);
  const primaryKey = table.primaryKey;
  if (!primaryKey) throw IvmRuntimeError.MissingPrimaryKey(table.name);
  // Primary-key columns already in the index key are not repeated in the suffix.
  const valueFields = primaryKey.columns
    .filter((c: any) => !index.columns.includes(c.column))
    .map((c: any) => resolve(c.column));
  const covers = valueFields.length === 0;
  const candidate =
    candidateFilter &&
    rt.schema.table(candidateFilter.table) &&
    findIndex(rt.schema.table(candidateFilter.table), candidateFilter.index);
  if (candidateFilter && !candidate) throw IvmRuntimeError.IndexNotFound(candidateFilter.index);
  return {
    table: table.name,
    index: index.name,
    durableIds: {
      index: rt.indexStorageId(table.name, index),
      intersections: intersections.map(([name]) =>
        rt.indexStorageId(table.name, findIndex(table, name)),
      ),
      candidate: candidateFilter && rt.indexStorageId(candidateFilter.table, candidate),
    },
    intersections,
    candidateFilter,
    inputDescriptor: tableDescriptor,
    variantProjection,
    rowProjection: rowProjection === undefined ? undefined : { kind: "Named", name: rowProjection },
    keyFields,
    valueFields,
    unique: index.unique || covers,
    appendValueToKey: !index.unique && !covers,
    storeValue: index.unique && !covers,
    scan,
  };
}

function addDedupIndexByFromInput(
  rt: IvmRuntime,
  table: TableSchema,
  index: IndexSchema,
  input: NodeId,
  tableDescriptor: RecordDescriptor,
  scan?: StaticScanSpec,
): CompiledNode {
  const indexDescriptor = indexRecordDescriptor();
  const resolve = (column: string) => {
    const idx = resolveFieldName(tableDescriptor, column);
    if (idx === undefined) throw IvmRuntimeError.GraphFieldNotFound(column);
    return idx;
  };
  const keyFields = index.columns.map(resolve);
  const primaryKey = table.primaryKey;
  if (!primaryKey) throw IvmRuntimeError.MissingPrimaryKey(table.name);
  const suffixColumns: string[] = primaryKey.columns
    .map((c: any) => c.column)
    .filter((c: string) => !index.columns.includes(c));
  const covers = suffixColumns.length === 0;
  const node = dedup(
    rt,
    {
      kind: "IndexBy",
      op: {
        keyExpressions: index.columns.map(Plan.field),
        valueExpressions: suffixColumns.map(Plan.field),
        explicitIndex: index,
        keyFields,
        valueFields: suffixColumns.map(resolve),
        unique: index.unique || covers,
        appendValueToKey: !index.unique && !covers,
        storeValue: index.unique && !covers,
        scan,
      },
    },
    [input],
    indexDescriptor,
  );
  return { output: indexDescriptor, node, rootOrderingNode: undefined };
}

function projectionSourceRef(expression: ProjectExpr): FieldRef | undefined {
  return "source" in expression ? expression.source : undefined;
}

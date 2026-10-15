// Compile a whole typed family; bind sources and predicates without re-lowering.
import {
  type CompiledNode,
  type IvmRuntime,
  type RecordDescriptor,
  DatabaseSchema,
  IvmRuntimeError,
  IvmRuntime as IvmRuntimeCtor,
  graphBuilderFingerprint,
  graphBuildersEqual,
  predicateReferencedFields,
} from "../../../_extern/groove-ivm-plan.js";
import {
  type GraphBuilder,
  type NodeId,
  GraphBuilder as G,
  NodeDescriptor,
  graphInputs,
  graphMapInputs,
} from "../graph.js";
import { type PredicateExpr, predicateAcceptsTemplateArgument, structEq } from "../op_types.js";
import {
  type CompiledForest,
  type TemplateNode,
  type TemplateNodeRef,
  type TemplateSlice,
  type TemplateScalarArgument,
  TemplateGraphInput,
  TypedGraphTemplate,
  bindTemplateGraphsInner,
} from "../template.js";
import { bindProjectExpression } from "../template/arguments.js";
import { addDedupGraph, addDedupTemplateGraph } from "./compilation.js";
import { validateCollectByTerminality } from "./record_projection.js";

let NEXT_TEMPLATE = 1;

/** All terminal families of one lowered program; compiled together to preserve fragment sharing. */
interface Forest {
  roots: GraphBuilder[];
  inputs: GraphBuilder[];
  contracts: RecordDescriptor[];
  predicates: PredicateExpr[];
  markers: PredicateExpr[];
}

/** One terminal's view of a forest: its local fallback graph and the forest slots it owns. */
interface Slice {
  fallback: GraphBuilder;
  inputs: number[];
  predicates: number[];
}

// Preserve dependency information while distinguishing equal-valued arguments.
function predicateMarker(slot: number, predicate: PredicateExpr): PredicateExpr {
  const fields = new Set<string>();
  predicateReferencedFields(predicate, fields);
  return { kind: "TemplateArgument", slot, fields: [...fields].sort() };
}

const markerSlot = (p: PredicateExpr) => (p.kind === "TemplateArgument" ? p.slot : undefined);

const predicateMarkerWithSlot = (slot: number, marker: PredicateExpr): PredicateExpr =>
  marker.kind === "TemplateArgument"
    ? { kind: "TemplateArgument", slot, fields: marker.fields }
    : marker;

/** Post-order walk shared by forest and slice: `leaf` may replace a node outright. */
function rewrite(
  root: GraphBuilder,
  rewritten: Map<GraphBuilder, GraphBuilder>,
  leaf: (node: GraphBuilder) => GraphBuilder | undefined,
  inner: (bound: GraphBuilder) => GraphBuilder,
): GraphBuilder {
  const pending: [GraphBuilder, boolean][] = [[root, false]];
  while (pending.length) {
    const [node, expanded] = pending.pop()!;
    if (rewritten.has(node)) continue;
    const replaced = leaf(node);
    if (replaced) {
      rewritten.set(node, replaced);
      continue;
    }
    if (!expanded) {
      pending.push([node, true]);
      for (const child of graphInputs(node)) pending.push([child, false]);
      continue;
    }
    rewritten.set(node, inner(graphMapInputs(node, (child) => rewritten.get(child)!)));
  }
  return rewritten.get(root)!;
}

function forest(roots: GraphBuilder[], describe: (g: GraphBuilder) => RecordDescriptor): Forest {
  const f: Forest = { roots: [], inputs: [], contracts: [], predicates: [], markers: [] };
  // One identity memo across roots: a shared fragment keeps one slot numbering and one rewritten node.
  const rewritten = new Map<GraphBuilder, GraphBuilder>();
  const leaf = (node: GraphBuilder): GraphBuilder | undefined => {
    let output: RecordDescriptor | undefined;
    switch (node.kind) {
      case "TemplateInput":
        if (node.input) return undefined;
        output = node.output;
        break;
      case "BindingSource":
      case "InlineRecords":
      case "InputSource":
      case "FrontierSource":
        output = node.output;
        break;
      case "ArgMaxBy":
      case "ArgMinBy":
        if (node.input.kind !== "Table") return undefined;
        output = describe(node);
        break;
      case "Table":
      case "Index":
      case "Lookup":
      case "Recursive":
      case "RecursiveStepWitness":
      case "VariantProject":
        output = describe(node);
        break;
      case "TypedTemplate":
      case "CollectBy":
        throw IvmRuntimeError.UnsupportedOperator();
      default:
        return undefined;
    }
    const slot = f.inputs.length;
    f.inputs.push(node);
    f.contracts.push(output);
    return { kind: "TemplateInput", slot, output };
  };
  const inner = (bound: GraphBuilder): GraphBuilder => {
    if (bound.kind !== "Filter") return bound;
    const marker = predicateMarker(f.predicates.length, bound.predicate);
    f.predicates.push(bound.predicate);
    f.markers.push(marker);
    return { ...bound, predicate: marker };
  };
  for (const root of roots) f.roots.push(rewrite(root, rewritten, leaf, inner));
  return f;
}

/** Renumber one terminal's forest slots densely. */
function slice(root: GraphBuilder, markers: PredicateExpr[]): Slice {
  const inputs = new Map<number, number>();
  const predicates = new Map<number, number>();
  const inputOrder: number[] = [];
  const predicateOrder: number[] = [];
  const localOf = (map: Map<number, number>, order: number[], global: number) => {
    let local = map.get(global);
    if (local === undefined) {
      order.push(global);
      map.set(global, (local = order.length - 1));
    }
    return local;
  };
  const fallback = rewrite(
    root,
    new Map(),
    (node) =>
      node.kind === "TemplateInput" && !node.input
        ? {
            kind: "TemplateInput",
            slot: localOf(inputs, inputOrder, node.slot),
            output: node.output,
          }
        : undefined,
    (bound) => {
      if (bound.kind !== "Filter") return bound;
      const global = markerSlot(bound.predicate);
      if (global === undefined || !structEq(markers[global], bound.predicate))
        throw IvmRuntimeError.UnsupportedOperator();
      const local = localOf(predicates, predicateOrder, global);
      return { ...bound, predicate: predicateMarkerWithSlot(local, bound.predicate) };
    },
  );
  return { fallback, inputs: inputOrder, predicates: predicateOrder };
}

function forestFingerprint(roots: GraphBuilder[]): bigint {
  let h = 0xcbf29ce484222325n ^ BigInt(roots.length);
  for (const root of roots)
    h =
      ((h ^ BigInt.asUintN(64, graphBuilderFingerprint(root))) * 0x100000001b3n) &
      0xffffffffffffffffn;
  return h;
}

/** Process-local cache of typed programs keyed by operator structure, not sources, rows or argument values. */
export class TypedGraphTemplateCache {
  entries: [bigint, GraphBuilder[], CompiledForest][] = [];
  compilations = 0;
  reuses = 0;

  /** Cumulative compiler work, retained across entry invalidation. */
  counters(): [number, number] {
    return [this.compilations, this.reuses];
  }

  clear(): void {
    this.entries = [];
  }

  /** One exact whole-program lookup; each returned graph owns fresh source/predicate arguments. */
  compile(graphs: GraphBuilder[]): GraphBuilder[] {
    return this.compileWithSources(graphs, () => {
      throw IvmRuntimeError.UnsupportedOperator();
    });
  }

  /** Extend families through source projections; catalogue-sensitive leaves stay on the validated install path. */
  compileWithSources(
    graphs: GraphBuilder[],
    describe: (g: GraphBuilder) => RecordDescriptor,
  ): GraphBuilder[] {
    // Binding-route filters must remain below an explicit terminal.
    const roots = graphs.map((graph) => {
      validateCollectByTerminality(graph);
      if (graph.kind !== "CollectBy") return graph;
      validateCollectByTerminality(graph.input);
      return graph.input;
    });
    const f = forest(roots, describe);
    const fingerprint = forestFingerprint(f.roots);
    let compiled: CompiledForest;
    const hit = this.entries.find(
      ([hash, rs]) =>
        hash === fingerprint &&
        rs.length === f.roots.length &&
        rs.every((a, i) => graphBuildersEqual(a, f.roots[i])),
    );
    if (hit) {
      this.reuses++;
      compiled = hit[2];
    } else {
      const compiler: IvmRuntime = IvmRuntimeCtor.new(DatabaseSchema.new([]));
      compiled = compileTypedForest(compiler, f);
      this.compilations++;
      if (this.entries.length === 128) this.entries.shift();
      this.entries.push([fingerprint, f.roots, compiled]);
    }
    return graphs.map((graph, i) => {
      const [program, s] = compiled[i];
      const typed: GraphBuilder = {
        kind: "TypedTemplate",
        program,
        inputs: s.inputs.map((slot) => f.inputs[slot]),
        predicates: s.predicates.map((slot) => f.predicates[slot]),
        scalars: [],
      };
      return graph.kind === "CollectBy" ? graphMapInputs(graph, () => typed) : typed;
    });
  }
}

function compileTypedForest(rt: IvmRuntime, f: Forest): CompiledForest {
  const sources = new Map<NodeId, number>();
  const replacements: TemplateGraphInput[] = [];
  f.contracts.forEach((output, slot) => {
    const replacement = G.inputSource(rt.allocateInputSource(output), output);
    sources.set(addDedupGraph(rt, replacement).node, slot);
    replacements.push(TemplateGraphInput.withOutputContract(replacement, output));
  });
  // One forest bind and one scratch graph: shared fragments keep one builder identity.
  let bound: GraphBuilder[];
  try {
    bound = bindTemplateGraphsInner(f.roots, replacements, true);
  } catch {
    throw IvmRuntimeError.GraphOutputMismatch();
  }
  return f.roots.map((root, i) => {
    const compiled = addDedupTemplateGraph(rt, bound[i]);
    const s = slice(root, f.markers);
    const program = extractTypedTemplate(rt, compiled, sources, s, f);
    const templateSlice: TemplateSlice = { inputs: s.inputs, predicates: s.predicates };
    return [program, templateSlice];
  });
}

function extractTypedTemplate(
  rt: IvmRuntime,
  compiled: CompiledNode,
  sources: Map<NodeId, number>,
  s: Slice,
  f: Forest,
): TypedGraphTemplate {
  const localInputs = new Map(s.inputs.map((g, l) => [g, l]));
  const localPredicates = new Map(s.predicates.map((g, l) => [g, l]));
  const references = new Map<NodeId, TemplateNodeRef>();
  const nodes: TemplateNode[] = [];
  const pending: [NodeId, boolean][] = [[compiled.node, false]];
  if (compiled.rootOrderingNode !== undefined) pending.push([compiled.rootOrderingNode, false]);
  while (pending.length) {
    const [id, expanded] = pending.pop()!;
    if (references.has(id)) continue;
    const global = sources.get(id);
    if (global !== undefined) {
      // Input source ids are unique per forest slot, so an outside source cannot be reachable.
      const local = localInputs.get(global);
      if (local === undefined) throw IvmRuntimeError.UnsupportedOperator();
      references.set(id, { kind: "Input", index: local });
      continue;
    }
    const node = rt.graph.node(id);
    if (!node) throw IvmRuntimeError.GraphNodeNotFound(id);
    const descriptor: NodeDescriptor = node.descriptor;
    if (!expanded) {
      pending.push([id, true], ...descriptor.inputs.map((i): [NodeId, boolean] => [i, false]));
      continue;
    }
    const inputs = descriptor.inputs.map((i) => references.get(i)!);
    let operator = descriptor.operator;
    let predicate: number | undefined;
    if (operator.kind === "Filter") {
      const slot = markerSlot(operator.op.predicate);
      const local =
        slot !== undefined && structEq(f.markers[slot], operator.op.predicate)
          ? localPredicates.get(slot)
          : undefined;
      if (local === undefined) throw IvmRuntimeError.UnsupportedOperator();
      operator = {
        kind: "Filter",
        op: { ...operator.op, predicate: { kind: "And", children: [] } },
      };
      predicate = local;
    }
    references.set(id, { kind: "Node", index: nodes.length });
    nodes.push({
      descriptor: new NodeDescriptor(operator, [], descriptor.output),
      inputs,
      predicate,
    });
  }
  return new TypedGraphTemplate(
    NEXT_TEMPLATE++,
    nodes,
    s.inputs.map((slot) => f.contracts[slot]),
    compiled.output,
    references.get(compiled.node)!,
    compiled.rootOrderingNode !== undefined ? references.get(compiled.rootOrderingNode) : undefined,
    false,
    s.fallback,
    s.predicates.map((global, local) => predicateMarkerWithSlot(local, f.markers[global])),
  );
}

export function installTypedTemplate(
  rt: IvmRuntime,
  program: TypedGraphTemplate,
  inputs: CompiledNode[],
  graphs: GraphBuilder[],
  predicates: PredicateExpr[],
  scalars: TemplateScalarArgument[],
): CompiledNode {
  if (
    inputs.length !== program.inputs.length ||
    program.predicateMarkers.some(
      (m, i) => i < predicates.length && !predicateAcceptsTemplateArgument(m, predicates[i]),
    ) ||
    predicates.length !== program.predicateMarkers.length ||
    inputs.some((input, i) => i < program.inputs.length && input.output !== program.inputs[i])
  )
    throw IvmRuntimeError.GraphOutputMismatch();
  // Until inherited root ordering has an explicit relocation, preserve the full compiler.
  if (inputs.some((input) => input.rootOrderingNode !== undefined)) {
    let bound: GraphBuilder;
    try {
      bound = program.bindDeclarativeWithArguments(graphs, predicates, scalars);
    } catch {
      throw IvmRuntimeError.GraphOutputMismatch();
    }
    return addDedupGraph(rt, bound);
  }
  const installed: NodeId[] = [];
  const resolve = (r: TemplateNodeRef): NodeId =>
    r.kind === "Input" ? inputs[r.index].node : installed[r.index];
  for (const instruction of program.nodes) {
    const nodeInputs = instruction.inputs.map(resolve);
    let operator = instruction.descriptor.operator;
    // Only argument-consuming operators need an instance copy.
    if (instruction.predicate !== undefined && operator.kind === "Filter")
      operator = {
        kind: "Filter",
        op: { ...operator.op, predicate: predicates[instruction.predicate] },
      };
    if (
      operator.kind === "MapProject" &&
      operator.op.expressions.some((e) => e.expression.kind === "TemplateArgument")
    ) {
      try {
        operator = {
          kind: "MapProject",
          op: {
            ...operator.op,
            expressions: operator.op.expressions.map((e) => ({
              ...e,
              expression: bindProjectExpression(e.expression, scalars),
            })),
          },
        };
      } catch {
        throw IvmRuntimeError.GraphOutputMismatch();
      }
    }
    rt.logicalNodesRequested += 1;
    const id = rt.graph.dedupNodeParts(operator, nodeInputs, instruction.descriptor.output, {
      kind: "Ephemeral",
    });
    rt.initializeNodeRuntime(id);
    installed.push(id);
  }
  return {
    node: resolve(program.root),
    output: program.output,
    rootOrderingNode: program.ordering ? resolve(program.ordering) : undefined,
  };
}

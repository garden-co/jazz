// Reuse compilation of the *same immutable fragment*, never its evaluation.
import type { CompiledNode, RecordDescriptor } from "../../../_extern/groove-ivm-plan.js";
import { type GraphBuilder, type IvmGraph, graphInputs } from "../graph.js";

interface Entry {
  inputs: [CompiledNode, RecordDescriptor][];
  inferred: RecordDescriptor;
  compiled: CompiledNode;
  logicalNodes: number;
}

const sameCompiled = (a: CompiledNode | undefined, b: CompiledNode) =>
  a !== undefined &&
  a.node === b.node &&
  a.output === b.output &&
  a.rootOrderingNode === b.rootOrderingNode;

function eligible(graph: GraphBuilder): boolean {
  switch (graph.kind) {
    // Source validation must run for every install.
    case "Table":
    case "TemplateInput":
    case "Index":
    case "Lookup":
    case "InlineRecords":
    case "InputSource":
    case "BindingSource":
    case "FrontierSource":
    case "Recursive":
    case "RecursiveStepWitness":
    case "CollectBy":
      return false;
    // This case validates the current table primary-key contract too.
    case "ArgMaxBy":
    case "ArgMinBy":
      return graph.input.kind !== "Table";
    default:
      return true;
  }
}

/** Weakly keyed by fragment identity; never retains graph definitions. */
export class CompilationCache {
  private slots = new WeakMap<GraphBuilder, Entry>();

  get(
    owner: GraphBuilder,
    compiled: Map<GraphBuilder, CompiledNode>,
    outputs: Map<GraphBuilder, RecordDescriptor>,
    graph: IvmGraph,
  ): [CompiledNode, RecordDescriptor, number] | undefined {
    const entry = this.slots.get(owner);
    if (!entry) return undefined;
    if (
      !graph.node(entry.compiled.node) ||
      (entry.compiled.rootOrderingNode !== undefined &&
        !graph.node(entry.compiled.rootOrderingNode))
    )
      return undefined;
    const inputs = graphInputs(owner);
    if (inputs.length !== entry.inputs.length) return undefined;
    const matches = inputs.every((input, i) => {
      const [expected, output] = entry.inputs[i];
      return sameCompiled(compiled.get(input), expected) && outputs.get(input) === output;
    });
    return matches ? [entry.compiled, entry.inferred, entry.logicalNodes] : undefined;
  }

  insert(
    owner: GraphBuilder,
    compiled: Map<GraphBuilder, CompiledNode>,
    outputs: Map<GraphBuilder, RecordDescriptor>,
    result: CompiledNode,
    inferred: RecordDescriptor,
    logicalNodes: number,
  ): void {
    if (!eligible(owner)) return;
    const inputs = graphInputs(owner).map((i): [CompiledNode, RecordDescriptor] => [
      compiled.get(i)!,
      outputs.get(i)!,
    ]);
    // Bound retained metadata (notably wide Union).
    if (inputs.length > 64) return;
    this.slots.set(owner, { inputs, inferred, compiled: result, logicalNodes });
  }
}

/** One visit per immutable DAG fragment, children before parents, iteratively. */
export function compilationOrder(graph: GraphBuilder): [GraphBuilder, GraphBuilder | undefined][] {
  const pending: [GraphBuilder, GraphBuilder | undefined, boolean][] = [[graph, undefined, false]];
  const seen = new Set<GraphBuilder>();
  const ordered: [GraphBuilder, GraphBuilder | undefined][] = [];
  while (pending.length) {
    const [g, owner, visited] = pending.pop()!;
    if (visited) ordered.push([g, owner]);
    else if (!seen.has(g)) {
      seen.add(g);
      pending.push([g, owner, true]);
      for (const input of graphInputs(g)) pending.push([input, input, false]);
    }
  }
  return ordered;
}

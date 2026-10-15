// Source-keyed tick programs. Only immutable topology is retained.
import type { BindingSourceKey } from "./op_types.js";
import { structKey } from "./op_types.js";
import type { IvmGraph, NodeId } from "./graph.js";
import { type ExecutionLayout, sortedUniqueNodeIds } from "./execution_layout.js";

export class ActivationPlan {
  constructor(
    readonly sources: NodeId[],
    readonly affected: Set<NodeId>,
    readonly relevant: Set<NodeId>,
    readonly layout: ExecutionLayout,
    readonly durable: NodeId[],
    readonly ephemeral: NodeId[],
    readonly tables: string[],
    readonly bindings: BindingSourceKey[],
    readonly routed: NodeId[],
  ) {}

  static compile(graph: IvmGraph, sources: NodeId[]): ActivationPlan {
    const affected = new Set<NodeId>();
    const pending = sources.slice();
    const routes = graph.routes();
    while (pending.length) {
      const id = pending.pop()!;
      // A route barrier is reached only through its shared terminal.
      if (routes.isBarrier(id) && !sources.includes(id) && !reachesDurable(graph, id)) continue;
      if (!affected.has(id)) {
        affected.add(id);
        pending.push(...graph.nodeOrThrow(id).children);
      }
    }
    const routed = sortedUniqueNodeIds([...affected].filter((id) => routes.hasTable(id)));
    const layout = graph.executionLayout(affected);
    const relevant = new Set(layout.nodes);
    const durable: NodeId[] = [];
    const ephemeral: NodeId[] = [];
    const tables = new Set<string>();
    const bindings = new Map<string, BindingSourceKey>();
    for (const id of layout.nodes) {
      const node = graph.nodeOrThrow(id);
      if (affected.has(id)) (node.isDurable() ? durable : ephemeral).push(id);
      const op = node.descriptor.operator;
      if (op.kind === "TableSource") tables.add(op.op.table);
      else if (op.kind === "Lookup") tables.add(op.op.source.table);
      else if (op.kind === "BindingSource") bindings.set(structKey(op.op.key), op.op.key);
    }
    return new ActivationPlan(
      sources,
      affected,
      relevant,
      layout,
      durable,
      ephemeral,
      [...tables],
      [...bindings.values()],
      routed,
    );
  }

  weight(): number {
    return (
      this.sources.length +
      this.affected.size +
      this.relevant.size +
      this.layout.weight() +
      this.durable.length +
      this.ephemeral.length +
      this.tables.length +
      this.bindings.length +
      this.routed.length
    );
  }
}

/** Durable nodes are ticked before the routed frame. */
function reachesDurable(graph: IvmGraph, barrier: NodeId): boolean {
  const seen = new Set<NodeId>();
  const pending = [barrier];
  while (pending.length) {
    const id = pending.pop()!;
    if (seen.has(id)) continue;
    seen.add(id);
    const node = graph.nodeOrThrow(id);
    if (node.isDurable()) return true;
    pending.push(...node.children);
  }
  return false;
}

export class ActivationCache {
  private plans: ActivationPlan[] = [];
  private weight = 0;
  private builds = 0;
  private hits = 0;

  get(graph: IvmGraph, sourcesIn: NodeId[]): ActivationPlan {
    const sources = sortedUniqueNodeIds(sourcesIn);
    const index = this.plans.findIndex(
      (p) => p.sources.length === sources.length && p.sources.every((s, i) => s === sources[i]),
    );
    if (index >= 0) {
      const [plan] = this.plans.splice(index, 1);
      this.plans.push(plan);
      this.hits++;
      return plan;
    }
    const plan = ActivationPlan.compile(graph, sources);
    this.builds++;
    const MAX_PLANS = 16;
    const MAX_WEIGHT = 1_000_000;
    const weight = plan.weight();
    if (weight <= MAX_WEIGHT) {
      while (this.plans.length >= MAX_PLANS || this.weight + weight > MAX_WEIGHT) {
        this.weight -= this.plans.shift()!.weight();
      }
      this.weight += weight;
      this.plans.push(plan);
    }
    return plan;
  }

  added(inputs: NodeId[]): void {
    this.retain((plan) => !inputs.some((id) => plan.affected.has(id)));
  }
  removed(node: NodeId): void {
    this.retain((plan) => !plan.relevant.has(node));
  }
  clear(): void {
    this.retain(() => false);
  }
  private retain(keep: (p: ActivationPlan) => boolean): void {
    this.plans = this.plans.filter(keep);
    this.weight = this.plans.reduce((s, p) => s + p.weight(), 0);
  }
}

// Reusable graph wiring, deliberately independent of evaluation state.
import { type IvmGraph, type NodeId, MissingNodeError } from "./graph.js";
import { pipelineSupportsNode } from "../../_extern/groove-ivm-plan.js";

export class ExecutionLayout {
  constructor(
    readonly nodes: NodeId[],
    readonly slots: Map<NodeId, number>,
    readonly roots: NodeId[],
    readonly inputCounts: number[],
    private readonly inputOffsets: number[],
    private readonly inputSlots: number[],
    readonly sourceSlots: number[],
    readonly pipelinePredecessors: (number | undefined)[],
    private readonly dependentOffsets: number[],
    private readonly dependentSlots: number[],
  ) {}

  static compile(graph: IvmGraph, roots: NodeId[]): ExecutionLayout {
    const nodes: NodeId[] = [];
    const slots = new Map<NodeId, number>();
    const inputCounts: number[] = [];
    const sourceSlots: number[] = [];
    const pending = roots.slice();
    for (let head = 0; head < pending.length; head++) {
      const id = pending[head];
      if (slots.has(id)) continue;
      const node = graph.nodeOrThrow(id);
      const slot = nodes.length;
      slots.set(id, slot);
      nodes.push(id);
      inputCounts.push(node.descriptor.inputs.length);
      const op = node.descriptor.operator.kind;
      if (op === "TableSource" || op === "IndexSource") sourceSlots.push(slot);
      pending.push(...node.descriptor.inputs);
    }
    // Compressed adjacency, with repeated inputs preserved.
    const dependentOffsets = new Array<number>(nodes.length + 1).fill(0);
    const inputOffsets: number[] = [];
    const inputSlots: number[] = [];
    for (const id of nodes) {
      inputOffsets.push(inputSlots.length);
      for (const input of graph.nodeOrThrow(id).descriptor.inputs) {
        const s = slots.get(input)!;
        inputSlots.push(s);
        dependentOffsets[s + 1]++;
      }
    }
    inputOffsets.push(inputSlots.length);
    for (let slot = 0; slot < nodes.length; slot++)
      dependentOffsets[slot + 1] += dependentOffsets[slot];
    const positions = dependentOffsets.slice();
    const dependentSlots = new Array<number>(dependentOffsets[dependentOffsets.length - 1]).fill(0);
    nodes.forEach((id, slot) => {
      for (const input of graph.nodeOrThrow(id).descriptor.inputs) {
        dependentSlots[positions[slots.get(input)!]++] = slot;
      }
    });
    const unary = nodes.map((id) => pipelineSupportsNode(graph, id));
    const rootSet = new Set(roots);
    const pipelinePredecessors = nodes.map((id, slot) => {
      if (!unary[slot]) return undefined;
      const input = graph.node(id)?.descriptor.inputs[0];
      if (input === undefined) return undefined;
      const predecessor = slots.get(input)!;
      return unary[predecessor] &&
        !rootSet.has(input) &&
        dependentOffsets[predecessor + 1] - dependentOffsets[predecessor] === 1
        ? predecessor
        : undefined;
    });
    return new ExecutionLayout(
      nodes,
      slots,
      roots,
      inputCounts,
      inputOffsets,
      inputSlots,
      sourceSlots,
      pipelinePredecessors,
      dependentOffsets,
      dependentSlots,
    );
  }

  dependents(slot: number): number[] {
    return this.dependentSlots.slice(this.dependentOffsets[slot], this.dependentOffsets[slot + 1]);
  }
  inputs(slot: number): number[] {
    return this.inputSlots.slice(this.inputOffsets[slot], this.inputOffsets[slot + 1]);
  }
  weight(): number {
    return this.nodes.length + this.dependentSlots.length + this.inputSlots.length;
  }
}

export function sortedUniqueNodeIds(ids: Iterable<NodeId>): NodeId[] {
  return [...new Set(ids)].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
}
const sameIds = (a: NodeId[], b: NodeId[]) =>
  a.length === b.length && a.every((x, i) => x === b[i]);

/** Bounded, graph-owned acceleration only. */
export class ExecutionLayoutCache {
  private layouts: ExecutionLayout[] = [];
  private weight = 0;
  private builds = 0;
  private hits = 0;

  get(graph: IvmGraph, rootsIn: Iterable<NodeId>): ExecutionLayout {
    const roots = sortedUniqueNodeIds(rootsIn);
    const index = this.layouts.findIndex((l) => sameIds(l.roots, roots));
    if (index >= 0) {
      const [layout] = this.layouts.splice(index, 1);
      this.layouts.push(layout);
      this.hits++;
      return layout;
    }
    const layout = ExecutionLayout.compile(graph, roots);
    this.builds++;
    const MAX_LAYOUTS = 16;
    const MAX_WEIGHT = 65_536;
    const weight = layout.weight();
    if (weight <= MAX_WEIGHT) {
      while (this.layouts.length >= MAX_LAYOUTS || this.weight + weight > MAX_WEIGHT) {
        this.weight -= this.layouts.shift()!.weight();
      }
      this.weight += weight;
      this.layouts.push(layout);
    }
    return layout;
  }

  invalidate(node?: NodeId): void {
    this.layouts = this.layouts.filter((l) => node !== undefined && !l.slots.has(node));
    this.weight = this.layouts.reduce((s, l) => s + l.weight(), 0);
  }

  counters(): [number, number] {
    return [this.builds, this.hits];
  }
}

export { MissingNodeError };

// Physical unary tasks: private stateless edges lose their batch boundary.
import {
  BorrowedRecord,
  resolveFieldName,
  type IvmGraph,
  type NodeId,
  type RecordDelta,
  type RecordDeltas,
  type RecordDescriptor,
} from "../../../_extern/groove-ivm-ops.js";
import {
  type FrameInputs,
  type NodeMemoLookup,
  PENDING,
  type Poll,
  type ReadyNodeEvaluation,
  type TickEvaluator,
} from "./evaluator.js";
import { NodeState } from "./operator_updates.js";
import { FieldRoutes } from "./pipeline/fields.js";

export function supportsNode(graph: IvmGraph, id: NodeId): boolean {
  const node = graph.node(id);
  if (!node || node.descriptor.inputs.length !== 1) return false;
  const operator = node.descriptor.operator;
  if (operator.kind === "MapProject") return true;
  if (operator.kind !== "Filter") return false;
  const input = graph.node(node.descriptor.inputs[0]);
  if (!input) return false;
  const descriptor = input.descriptor.output.records();
  const fields = new Set<string>();
  operator.value.predicate.referencedFields(fields);
  return [...fields].every((field) => {
    const index = resolveFieldName(descriptor, field);
    return index !== undefined && !descriptor.fields()[index].valueType.mayContainStoredScalar();
  });
}

type Stage =
  /** A total projection composed into later routes; keeps its budget slot. */
  | { kind: "VirtualProject" }
  | { kind: "RoutedFilter"; filter: any; routes: FieldRoutes }
  | { kind: "Materialize"; routes: FieldRoutes }
  | { kind: "Filter"; filter: any; descriptor: RecordDescriptor }
  | {
      kind: "Project";
      project: any;
      input: RecordDescriptor;
      output: RecordDescriptor;
      prepared: any;
      omitUnrepresentable: boolean;
    };

export interface PreparedPipeline {
  nodes: NodeId[];
  input: NodeId;
  output: RecordDescriptor;
  stages: Stage[];
}

/** Owns all state across a CPU yield. */
export class PendingPipeline {
  next = 0;
  stage = 0;
  location = 0;
  scratch: [Uint8Array, Uint8Array] = [new Uint8Array(), new Uint8Array()];
  output: Uint8Array[] = [];
  borrowed: RecordDelta[] = [];
  error?: [number, unknown];

  constructor(
    readonly plan: PreparedPipeline,
    readonly lookup: NodeMemoLookup,
    readonly input: RecordDeltas,
  ) {}

  row(index: number, budget: { left: number }): boolean {
    const delta = this.input.deltas[index];
    // 0 = source, 1/2 = reusable scratch.
    let location = this.location;
    const stages = this.plan.stages;
    const current = () => (location === 0 ? delta.record : this.scratch[location - 1]);
    const write = (produce: (raw: Uint8Array) => Uint8Array | undefined): boolean => {
      const out = produce(current());
      if (!out) return false;
      this.scratch[location === 1 ? 1 : 0] = out;
      location = location === 1 ? 2 : 1;
      return true;
    };
    for (let stageIndex = this.stage; stageIndex < stages.length; stageIndex++) {
      if (this.error && stageIndex >= this.error[0]) return true;
      if (budget.left === 0) {
        this.location = location;
        return false;
      }
      budget.left--;
      this.stage = stageIndex + 1;
      const stage = stages[stageIndex];
      const last = stageIndex + 1 === stages.length;
      const raw = current();
      let keep: boolean;
      try {
        switch (stage.kind) {
          case "VirtualProject":
            continue;
          case "RoutedFilter":
            keep = stage.filter.predicate.matches(
              stage.routes.record(raw),
              stage.filter.comparison,
            );
            break;
          case "Materialize": {
            if (stage.routes.reusesInput) continue;
            const project = (r: Uint8Array) => {
              const out: number[] = [];
              stage.routes.append(r, out);
              return Uint8Array.from(out);
            };
            if (last) {
              this.output.push(project(raw));
              this.borrowed.push({
                record: this.output[this.output.length - 1],
                weight: delta.weight,
              });
              return true;
            }
            keep = write(project);
            break;
          }
          case "Filter":
            keep = stage.filter.predicate.matches(
              new BorrowedRecord(raw, stage.descriptor),
              stage.filter.comparison,
            );
            break;
          case "Project": {
            if (stage.prepared?.reusesInput) continue;
            const project = (r: Uint8Array) =>
              NodeState.projectRow(
                stage.project,
                stage.output,
                stage.input,
                r,
                stage.prepared,
                stage.omitUnrepresentable,
              );
            if (last) {
              const record = project(raw);
              if (record) this.borrowed.push({ record, weight: delta.weight });
              return true;
            }
            keep = write(project);
            break;
          }
        }
      } catch (error) {
        // Earlier stages outrank later ones regardless of row (stage-major first error).
        this.error = [stageIndex, error];
        this.output = [];
        this.borrowed = [];
        return true;
      }
      if (!keep) return true;
    }
    this.borrowed.push({
      record: location === 0 ? delta.record : current().slice(),
      weight: delta.weight,
    });
    return true;
  }

  finish(): [NodeMemoLookup, RecordDeltas] {
    if (this.error) throw this.error[1];
    return [this.lookup, { descriptor: this.plan.output, deltas: this.borrowed }];
  }
}

function preparedPipeline(ev: TickEvaluator, nodes: NodeId[]): PreparedPipeline | undefined {
  const tail = nodes[nodes.length - 1];
  if (tail === undefined) return undefined;
  const cached = ev.nodeMeta.get(tail)?.pipeline;
  if (
    cached &&
    cached.nodes.length === nodes.length &&
    cached.nodes.every((n, i) => n === nodes[i])
  )
    return cached;
  const head = ev.graph.node(nodes[0]);
  if (!head) return undefined;
  const input: NodeId = head.descriptor.inputs[0];
  const inputNode = ev.graph.node(input);
  if (!inputNode) return undefined;
  const stages: Stage[] = [];
  let descriptor = inputNode.descriptor.output.records();
  let routes = FieldRoutes.identity(descriptor);
  let virtualRows = false;
  for (const node of nodes) {
    const graphNode = ev.graph.node(node);
    if (!graphNode) return undefined;
    const output = graphNode.descriptor.output.records();
    const operator = graphNode.descriptor.operator;
    let stage: Stage;
    if (operator.kind === "Filter") {
      stage = virtualRows
        ? { kind: "RoutedFilter", filter: operator.value, routes }
        : { kind: "Filter", filter: operator.value, descriptor };
    } else if (operator.kind === "MapProject") {
      let prepared;
      try {
        prepared = ev.rawProjectionFields(node, operator.value, descriptor, output);
      } catch {
        return undefined;
      }
      const composed = prepared ? routes.compose(output, prepared) : undefined;
      if (composed) {
        routes = composed;
        virtualRows = true;
        descriptor = output;
        stages.push({ kind: "VirtualProject" });
        continue;
      }
      if (virtualRows) stages.push({ kind: "Materialize", routes });
      routes = FieldRoutes.identity(output);
      virtualRows = false;
      stage = {
        kind: "Project",
        project: operator.value,
        input: descriptor,
        output,
        prepared,
        omitUnrepresentable: operator.value.expressions.some(
          (e: any) =>
            e.expression.kind === "RecursiveEnumRemap" && e.expression.omitUnrepresentable,
        ),
      };
    } else {
      return undefined;
    }
    stages.push(stage);
    descriptor = output;
  }
  if (virtualRows) stages.push({ kind: "Materialize", routes });
  const plan: PreparedPipeline = { nodes: nodes.slice(), input, output: descriptor, stages };
  ev.meta(tail).pipeline = plan;
  return plan;
}

export function pollPipeline(
  ev: TickEvaluator,
  nodes: NodeId[],
  pending: Map<NodeId, PendingPipeline>,
  frameInputs: FrameInputs,
): Poll<ReadyNodeEvaluation> {
  const tail = nodes[nodes.length - 1];
  if (!pending.has(tail)) {
    if (!ev.nodeMeta.get(tail)?.inputSignature) {
      // Never turn metadata preparation back into recursive traversal of a deep chain.
      for (const node of nodes) ev.inputSignature(node);
    }
    const lookup = ev.prepareMemoLookup(tail);
    const cached = ev.cachedNodeRecords(lookup);
    if (cached) return cached;
    if (ev.context.subTick > 1 && !lookup.dependsOnContext) return ev.computeNode(tail, lookup);
    const plan = preparedPipeline(ev, nodes);
    if (!plan) return ev.computeNode(tail, lookup);
    const resident = ev.resolveRegisterInputs(frameInputs)?.pop();
    const input = resident ?? ev.cachedNodeRecords(ev.prepareMemoLookup(plan.input));
    if (!input) return ev.computeNode(tail, lookup);
    if (ev.context.evalMode === "Hydrate") {
      ev.metrics.hydrationMemoComputes++;
      ev.metrics.hydrationMemoComputedNodes.add(tail);
    }
    pending.set(tail, new PendingPipeline(plan, lookup, input));
  }
  const batch = pending.get(tail)!;
  const budget = { left: 256 };
  while (batch.next < batch.input.deltas.length && budget.left > 0) {
    if (!batch.row(batch.next, budget)) break;
    batch.next++;
    batch.stage = 0;
    batch.location = 0;
    if (batch.error?.[0] === 0) break;
  }
  if (batch.next < batch.input.deltas.length && batch.error?.[0] !== 0) return PENDING;
  pending.delete(tail);
  const [lookup, output] = batch.finish();
  return ev.memoizeResult(lookup, output);
}

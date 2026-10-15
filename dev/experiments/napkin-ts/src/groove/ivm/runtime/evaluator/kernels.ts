// Synchronous batch kernels. Inputs are already resolved by the execution driver.
import {
  AsOf,
  IvmRuntimeError,
  emptyDeltas,
  resolveFieldName,
  type NodeId,
  type RecordDeltas,
} from "../../../../_extern/groove-ivm-ops.js";
import { type TickEvaluator, planExprFields } from "../evaluator.js";
import { ArrangementState, JoinInput, prepareArrangement } from "../join.js";
import { NodeState } from "../operator_updates.js";
import { updateLookup } from "./lookup.js";

const sortedUnique = (fields: number[]) => [...new Set(fields)].sort((a, b) => a - b);

function firstInput(inputs: RecordDeltas[], node: NodeId): RecordDeltas {
  const input = inputs[0];
  if (!input) throw new IvmRuntimeError("GraphInputMissing", node);
  return input;
}

export function computeBatch(
  ev: TickEvaluator,
  node: NodeId,
  inputs: RecordDeltas[],
): RecordDeltas {
  const graphNode = ev.graphNode(node);
  const outputDesc = graphNode.descriptor.output.records();
  const operator = graphNode.descriptor.operator;
  const op = operator.value;
  const hydrateInputs = ev.context.evalMode === "Hydrate" ? ev.evaluationInputs : undefined;
  const arity = (n: number) => {
    if (graphNode.descriptor.inputs.length !== n)
      throw new IvmRuntimeError("GraphInputArityMismatch", node);
    return graphNode.descriptor.inputs as NodeId[];
  };
  switch (operator.kind) {
    case "TableSource":
      return hydrateInputs
        ? NodeState.updateTableSourceFromInputs(
            op,
            ev.schema,
            ev.variantProjections,
            outputDesc,
            hydrateInputs,
          )
        : NodeState.updateTableSource(
            op,
            ev.schema,
            ev.variantProjections,
            outputDesc,
            ev.tableDeltas,
          );
    case "IndexSource":
      if (hydrateInputs) {
        return NodeState.updateIndexSourceFromInputs(
          op,
          ev.schema,
          ev.variantProjections,
          outputDesc,
          hydrateInputs,
        );
      }
      throw new IvmRuntimeError("UnsupportedOperator");
    case "InlineRecords":
      if (ev.context.evalMode !== "Hydrate") return emptyDeltas(outputDesc);
      return {
        descriptor: outputDesc,
        deltas: op.records.map((record: Uint8Array) => ({ record, weight: 1 })),
      };
    case "BindingSource":
      return NodeState.updateBindingSource(
        op,
        outputDesc,
        ev.bindingDeltas,
        ev.bindingSnapshots,
        ev.context.arrangementUpdateMode,
      );
    case "Arrange": {
      arity(1);
      const input = inputs[0];
      if (!ev.arrangementNeedsIndex(node)) return input;
      const materialized = ev.materializeIndirectFields(input, op.fields);
      const key = ev.arrangementKey(node, outputDesc, op.fields, op.comparison);
      const stamp = ev.arrangementSubTick(key);
      const state = ev.arrangementStates.get(key) ?? new AsOf(new ArrangementState());
      ev.arrangementStates.delete(key);
      prepareArrangement(
        state,
        outputDesc,
        op.fields,
        op.comparison,
        JoinInput.snapshot(materialized),
        stamp,
        ev.context.arrangementUpdateMode,
      );
      ev.insertArrangement(key, state);
      return materialized;
    }
    case "FrontierSource":
      return ev.frontierSource(op, outputDesc);
    case "Filter":
      return ev.computeFilter(node, op, outputDesc, firstInput(inputs, node));
    case "MapProject":
      return ev.computeProjection(node, op, outputDesc, firstInput(inputs, node));
    case "UnwrapNullable":
      return NodeState.updateUnwrapNullable(op, outputDesc, firstInput(inputs, node));
    case "Unnest":
      return NodeState.updateUnnest(op, outputDesc, firstInput(inputs, node));
    case "VariantProject":
      return NodeState.updateVariantProject(op, outputDesc, firstInput(inputs, node));
    case "ArgMaxBy":
    case "ArgMinBy": {
      const input = ev.materializeIndirectFieldIndices(
        firstInput(inputs, node),
        op.comparisonFieldIndices,
      );
      return ev.updateArgBy(
        node,
        {
          groupFieldIndices: op.groupFieldIndices,
          comparisonFieldIndices: op.comparisonFieldIndices,
          direction: operator.kind === "ArgMaxBy" ? "Max" : "Min",
        },
        outputDesc,
        input,
      );
    }
    case "TopBy": {
      const fields = sortedUnique([...op.groupFieldIndices, ...op.sortFieldIndices]);
      const input = ev.materializeIndirectFieldIndices(firstInput(inputs, node), fields);
      return ev.updateTopBy(node, op, outputDesc, input);
    }
    case "CollectBy": {
      const canonical = firstInput(inputs, node);
      const input = ev.materializeIndirectFieldIndices(canonical, op.readFieldIndices());
      return ev.updateCollectBy(node, op, outputDesc, input, canonical);
    }
    case "Aggregate": {
      const canonical = firstInput(inputs, node);
      // COUNT(*) without grouping observes only row weights: no chunk requests.
      const needsValues =
        op.groupKey.length > 0 ||
        op.aggregates.some(
          (e: any) => e.function !== "Count" || e.expression !== undefined || e.distinct,
        );
      let input = canonical;
      if (needsValues) {
        const fields = [...op.groupFieldIndices];
        const expressions = op.aggregates
          .map((a: any) => a.expression)
          .filter((e: any) => e !== undefined);
        for (const field of planExprFields(expressions)) {
          const index = resolveFieldName(canonical.descriptor, field);
          if (index === undefined) throw new IvmRuntimeError("GraphFieldNotFound", field);
          fields.push(index);
        }
        input = ev.materializeIndirectFieldIndices(canonical, sortedUnique(fields));
      }
      return ev.updateAggregate(node, op, outputDesc, input, canonical);
    }
    case "IndexBy": {
      const fields = [...op.keyFields];
      if (op.appendValueToKey) fields.push(...op.valueFields);
      const input = ev.materializeIndirectFieldIndices(
        firstInput(inputs, node),
        sortedUnique(fields),
      );
      const trace = (globalThis as any).GROOVE_TRACE_INDEX_BY !== undefined;
      const start = trace ? performance.now() : 0;
      const result = NodeState.updateIndexBy(op, outputDesc, input);
      if (trace && input.deltas.length > 0) {
        const indexName = op.explicitIndex?.name ?? "<derived>";
        const keyFields = op.keyExpressions.map((e: any) => JSON.stringify(e)).join(",");
        console.error(
          `GROOVE_TRACE_INDEX_BY node=${node} index=${indexName} input=${input.deltas.length} output=${result.deltas.length} unique=${op.unique} append_value_to_key=${op.appendValueToKey} store_value=${op.storeValue} scan=${op.scan !== undefined} key_fields=[${keyFields}] elapsed_ms=${(performance.now() - start).toFixed(3)}`,
        );
      }
      return result;
    }
    case "Union":
      return NodeState.updateUnion(outputDesc, inputs.slice());
    case "Join": {
      const [left, right] = arity(2);
      // Arrange already materialized join keys.
      return ev.updateJoin(node, op, outputDesc, left, right);
    }
    case "SemiJoin":
    case "AntiJoin": {
      const [leftInput, rightInput] = arity(2);
      ev.materializeIndirectFields(inputs[0], planExprFields(op.leftKey));
      ev.materializeIndirectFields(inputs[1], planExprFields(op.rightKey));
      return ev.updateThresholdJoin(
        node,
        op,
        outputDesc,
        leftInput,
        rightInput,
        operator.kind === "SemiJoin",
      );
    }
    case "Lookup":
      return updateLookup(ev, node, op, outputDesc, firstInput(inputs, node));
    case "RecursiveStepWitness": {
      const [recursive] = arity(1);
      // Drive the owner first; never re-evaluate a recursive step independently.
      return ev.updateRecursiveStepWitness(recursive, outputDesc);
    }
    default:
      // Durable writes are an async preparation boundary driven outside this frame.
      throw new IvmRuntimeError("UnsupportedOperator");
  }
}

// Demand-driven narrowing of flattened inner-join chains.
import {
  type FieldRef,
  type GraphBuilder,
  type ProjectField,
  projectExprSource,
} from "../graph.js";
import { predicateReferencedFields } from "../../../_extern/groove-ivm-plan.js";

const LEFT = "left.";
const RIGHT = "right.";

type Demand = Set<string>;
/** Narrowed rebuilds by input node and demand. */
type Narrowed = Map<GraphBuilder, Map<string, GraphBuilder | undefined>>;
const demandKey = (d: Demand) => [...d].sort().join("\u0000");

/** Drop fields nothing consumes from the projections between the inner joins below this projection. */
export function narrowProjectedJoinChain(self: GraphBuilder): GraphBuilder {
  if (self.kind !== "Project") return self;
  const narrowed: Narrowed = new Map();
  const demand = demandedNames(self.fields);
  const input = demand && narrowChain(self.input, demand, narrowed);
  return { kind: "Project", input: input ?? self.input, fields: self.fields };
}

function narrowChain(
  node: GraphBuilder,
  demand: Demand,
  narrowed: Narrowed,
): GraphBuilder | undefined {
  switch (node.kind) {
    case "Join": {
      const leftDemand = joinSideDemand(demand, LEFT, node.leftOn);
      const rightDemand = leftDemand && joinSideDemand(demand, RIGHT, node.rightOn);
      if (!leftDemand || !rightDemand) return undefined;
      const left = narrowJoinInput(node.left, leftDemand, narrowed);
      const right = narrowJoinInput(node.right, rightDemand, narrowed);
      if (!left && !right) return undefined;
      return { ...node, left: left ?? node.left, right: right ?? node.right };
    }
    // A semi-join outputs its left record unchanged and reads only the left keys.
    case "SemiJoin": {
      const leftDemand = new Set(demand);
      for (const key of node.leftOn) {
        const name = fieldRefName(key);
        if (name === undefined) return undefined;
        leftDemand.add(name);
      }
      const left = narrowJoinInput(node.left, leftDemand, narrowed);
      return left && { ...node, left };
    }
    case "UnwrapNullable": {
      const name = fieldRefName(node.field);
      if (name === undefined) return undefined;
      const input = narrowJoinInput(node.input, new Set(demand).add(name), narrowed);
      return input && { ...node, input };
    }
    case "Filter": {
      const inputDemand = new Set(demand);
      predicateReferencedFields(node.predicate, inputDemand);
      const input = narrowJoinInput(node.input, inputDemand, narrowed);
      return input && { ...node, input };
    }
    default:
      return undefined;
  }
}

function narrowJoinInput(
  node: GraphBuilder,
  demand: Demand,
  narrowed: Narrowed,
): GraphBuilder | undefined {
  let byDemand = narrowed.get(node);
  if (!byDemand) narrowed.set(node, (byDemand = new Map()));
  const key = demandKey(demand);
  if (byDemand.has(key)) return byDemand.get(key);
  const rebuilt = narrowInputUncached(node, demand, narrowed);
  byDemand.set(key, rebuilt);
  return rebuilt;
}

function narrowInputUncached(
  node: GraphBuilder,
  demand: Demand,
  narrowed: Narrowed,
): GraphBuilder | undefined {
  if (node.kind !== "Project") return narrowChain(node, demand, narrowed);
  const { input, fields } = node;
  if (input.kind !== "Join" && input.kind !== "SemiJoin") return undefined;
  const kept = fields.filter((field) => [...demand].some((name) => provides(field, name)));
  // If a demanded name no longer resolves, keep the projection whole rather than guess.
  if (![...demand].every((name) => kept.some((field) => provides(field, name)))) return undefined;
  const keptDemand = demandedNames(kept);
  const narrowedInput = keptDemand && narrowChain(input, keptDemand, narrowed);
  if (kept.length === fields.length && !narrowedInput) return undefined;
  return { kind: "Project", input: narrowedInput ?? input, fields: kept };
}

function joinSideDemand(demand: Demand, prefix: string, keys: FieldRef[]): Demand | undefined {
  const side = new Set<string>();
  for (const name of demand) if (name.startsWith(prefix)) side.add(name.slice(prefix.length));
  for (const key of keys) {
    const name = fieldRefName(key);
    if (name === undefined) return undefined;
    side.add(name);
  }
  return side;
}

/** The input names a projection reads, or undefined if one is positional. */
function demandedNames(fields: ProjectField[]): Demand | undefined {
  const names = new Set<string>();
  for (const field of fields) {
    const source = projectExprSource(field.expression);
    if (!source) continue;
    const name = fieldRefName(source);
    if (name === undefined) return undefined;
    names.add(name);
  }
  return names;
}

function fieldRefName(field: FieldRef): string | undefined {
  return field.kind === "Resolved" ? undefined : field.name;
}

function provides(field: ProjectField, name: string): boolean {
  const id = field.outputIdentity;
  const logical = id.kind === "Name" || id.kind === "NamedSlot" ? id.name : field.outputName;
  return field.outputName === name || logical === name;
}

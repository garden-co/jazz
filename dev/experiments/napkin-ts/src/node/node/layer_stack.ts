// The layer-stack overlay engine (SPEC 4 §4.7, SPEC 11 §11.3): the top layer holding a row supplies it.
import { GraphBuilder } from "../../_extern/node-state.js";
import { type Ref, refStack } from "./refs.js";

/** How a layer hides the rows of the layers below it. */
export type Mask = { kind: "OwnRows" } | { kind: "Presence"; rows: any };

export class Layer {
  constructor(
    readonly rows: any,
    readonly mask: Mask = { kind: "OwnRows" },
  ) {}

  static maskedBy(rows: any, mask: Mask): Layer {
    return new Layer(rows, mask);
  }

  /** The `row_uuid`s this layer hides in every layer below it. */
  hiddenBelow(): any {
    return (this.mask.kind === "OwnRows" ? this.rows.clone() : this.mask.rows.clone()).project([
      "row_uuid",
    ]);
  }
}

/** Compose a stack, top first: each layer's rows anti-joined against the masks above it. */
export function compose(layers: Iterable<Layer>): any {
  const stack = [...layers];
  const bottom = stack.pop();
  if (!bottom) throw new Error("a layer stack has at least one layer");
  return stack
    .reverse()
    .reduce(
      (below, layer) =>
        GraphBuilder.union([
          layer.rows,
          GraphBuilder.antiJoin(below, layer.hiddenBelow(), ["row_uuid"], ["row_uuid"]),
        ]),
      bottom.rows,
    );
}

/** One term of `compose`: the rows of a layer that the layers above leave exposed. */
export function exposed(rows: any, above: Iterable<Layer>): any {
  let out = rows;
  for (const layer of above)
    out = GraphBuilder.antiJoin(out, layer.hiddenBelow(), ["row_uuid"], ["row_uuid"]);
  return out;
}

/** The layers of one coordinate's ref stack at `tier`, top first. Errors propagate. */
export function refLayers(tier: "Global" | "Local" | "None", layer: (r: Ref) => Layer): Layer[] {
  return refStack(tier).map(layer);
}
export const tryRefLayers = refLayers;

/** `compose` over already materialized layers: the value of the top layer holding `key`. */
export function topWins<K, V>(
  layers: Iterable<{ get(k: K): V | undefined }>,
  key: K,
): V | undefined {
  for (const layer of layers) {
    const v = layer.get(key);
    if (v !== undefined) return v;
  }
  return undefined;
}

/** The head key and (non-collapsed) base of a branch view over one table. */
export function branchViewStack(
  schema: any,
  table: any,
  head: any,
  base: any | undefined,
): [
  any,
  { kind: "Current"; key: any } | { kind: "Snapshot"; key: any; snapshot: any } | undefined,
] {
  const [headKey] = schema.projectBranchViewSelector(table, head);
  let projected: any;
  if (base) {
    if (base.kind === "Current")
      projected = {
        kind: "Current",
        key: schema.projectBranchViewSelector(table, base.selector)[0],
      };
    else
      projected = {
        kind: "Snapshot",
        key: schema.projectBranchViewSelector(table, base.branch)[0],
        snapshot: base.snapshot,
      };
    if (headKey.equals(projected.key)) projected = undefined;
  }
  return [headKey, projected];
}

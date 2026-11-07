import type { WasmSchema } from "../drivers/types.js";
import type { BuiltRelation } from "./query-builder-shape.js";

/** Resolve the row scope shared by query guards and result preparation. */
export function resolveQueryOutputTable(schema: WasmSchema, relation: BuiltRelation): string {
  if (relation.union) {
    const first = relation.union.inputs[0];
    if (!first) throw new Error("union(...) requires at least one relation.");
    const firstTable = resolveQueryOutputTable(schema, first.input);
    for (const input of relation.union.inputs.slice(1)) {
      if (resolveQueryOutputTable(schema, input.input) !== firstTable)
        throw new Error("union(...) requires all relations to output the same table.");
    }
    return firstTable;
  }
  const seedTable = relation.gather?.seed
    ? resolveQueryOutputTable(schema, relation.gather.seed)
    : relation.table;
  if (!seedTable) throw new Error("gather(...) seed relation is missing table metadata.");
  let table: string = seedTable;
  if (relation.hops?.length) {
    for (const hop of relation.hops) {
      const target = schema[table]?.relations?.[hop];
      if (!target) throw new Error(`Unknown relation "${hop}" on table "${table}"`);
      table = target.table;
    }
  }
  return table;
}

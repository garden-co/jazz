/**
 * Translate QueryBuilder JSON to WASM Query format.
 *
 * QueryBuilder produces a compact JSON structure:
 * { table, conditions, includes, orderBy, limit, offset, hops?, gather? }
 *
 * Runtime semantics are driven by `relation_ir`. The wire payload keeps only
 * fields required for execution (`table`, `relation_ir`, and `array_subqueries`).
 */
import type { WasmSchema } from "../drivers/types.js";
import type { RelExpr } from "../ir.js";
/**
 * Translate QueryBuilder JSON to relation IR.
 *
 * This emits the canonical compositional form:
 * - hopTo => Join + Project
 * - gather => Gather with step Join + Project
 */
export declare function translateBuilderToRelationIr(
  builderJson: string,
  schema: WasmSchema,
): RelExpr;
/**
 * Translate QueryBuilder JSON to WASM Query JSON.
 *
 * @param builderJson JSON string from QueryBuilder._build()
 * @param schema WasmSchema for relation analysis
 * @returns JSON string for WASM runtime query()
 */
export declare function translateQuery(builderJson: string, schema: WasmSchema): string;
//# sourceMappingURL=query-adapter.d.ts.map

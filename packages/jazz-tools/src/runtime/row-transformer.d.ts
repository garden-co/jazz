/**
 * Transform WASM row results to typed TypeScript objects.
 */
import type { Value as WasmValue, WasmRow, WasmSchema } from "../drivers/types.js";
import type { ColumnType } from "../drivers/types.js";
export type { WasmValue };
export interface IncludeSpec {
  [relationName: string]: boolean | IncludeSpec;
}
export declare function unwrapValue(v: WasmValue, columnType?: ColumnType): unknown;
/**
 * Transform WasmRow[] to typed objects using schema column order.
 *
 * @param rows Array of WasmRow results from query
 * @param schema WasmSchema containing table definitions
 * @param tableName Name of the table being queried
 * @param includes Include tree from QueryBuilder._build() (if any)
 * @returns Array of typed objects with named properties
 */
export declare function transformRows<T>(
  rows: WasmRow[],
  schema: WasmSchema,
  tableName: string,
  includes?: IncludeSpec,
): T[];
//# sourceMappingURL=row-transformer.d.ts.map

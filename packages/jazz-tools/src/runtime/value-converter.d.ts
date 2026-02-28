/**
 * Convert JS values to WasmValue types for mutations.
 *
 * Used by Db.insert() and Db.update() to convert typed Init objects
 * into the Value[] format expected by JazzClient.
 */
import type { WasmSchema, ColumnType, Value as WasmValue } from "../drivers/types.js";
/**
 * Convert a JS value to WasmValue based on column type.
 */
export declare function toValue(value: unknown, columnType: ColumnType): WasmValue;
/**
 * Convert Init object to Value[] in schema column order.
 *
 * @param data The Init object with field values
 * @param schema WasmSchema containing table definitions
 * @param tableName Name of the table to insert into
 * @returns Array of WasmValue in column order
 */
export declare function toValueArray(
  data: Record<string, unknown>,
  schema: WasmSchema,
  tableName: string,
): WasmValue[];
/**
 * Convert partial update object to Record<string, WasmValue>.
 *
 * Only includes fields that are present in the data object.
 * Undefined values are skipped.
 *
 * @param data Partial object with fields to update
 * @param schema WasmSchema containing table definitions
 * @param tableName Name of the table being updated
 * @returns Record mapping column names to WasmValues
 */
export declare function toUpdateRecord(
  data: Record<string, unknown>,
  schema: WasmSchema,
  tableName: string,
): Record<string, WasmValue>;
//# sourceMappingURL=value-converter.d.ts.map

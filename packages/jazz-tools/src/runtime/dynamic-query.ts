import type { WasmSchema } from "../drivers/types.js";
import type { QueryBuilder } from "./db.js";

export type DynamicTableRow = {
  id: string;
  [columnName: string]: unknown;
};

export function allRowsInTableQuery<T extends { id: string } = DynamicTableRow>(
  tableName: string,
  schema: WasmSchema,
): QueryBuilder<T> {
  return {
    _table: tableName,
    _schema: schema,
    _rowType: undefined as unknown as T,
    _build() {
      return JSON.stringify({ table: tableName });
    },
  };
}

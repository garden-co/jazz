// Durable physical table / field / projection-target spellings.
import { RecordDescriptor, VT, userColumnField } from "../../../_extern/node-physical-ingest.js";

export const physicalHistoryTableName = (tableId: number) => `jazz_physical_${tableId}_history`;
export const physicalGlobalCurrentTableName = (tableId: number) =>
  `jazz_physical_${tableId}_global_current`;
export const physicalAheadCurrentTableName = (tableId: number) =>
  `jazz_physical_${tableId}_ahead_current`;
export const physicalRejectedVersionsTableName = (tableId: number) =>
  `jazz_physical_${tableId}_rejected_versions`;

/** Split `jazz_physical_{id}_{suffix}`; only the exact constructor spelling is accepted. */
export function splitPhysicalTableName(name: string): [number, string] | undefined {
  if (!name.startsWith("jazz_physical_")) return undefined;
  const rest = name.slice("jazz_physical_".length);
  const sep = rest.indexOf("_");
  if (sep < 0) return undefined;
  const digits = rest.slice(0, sep);
  if (!/^(0|[1-9][0-9]*)$/.test(digits)) return undefined;
  const id = Number(digits);
  return Number.isSafeInteger(id) ? [id, rest.slice(sep + 1)] : undefined;
}

export function physicalVersionTableId(name: string): number | undefined {
  const split = splitPhysicalTableName(name);
  return split && split[1] === "history" ? split[0] : undefined;
}

// Durable key/catalogue vocabulary: never rename a persisted index namespace.
export const physicalCurrentIndexName = (columnId: number) => `by_physical_app_v1_${columnId}`;
export const physicalCurrentCompositeIndexName = (columnIds: number[]) =>
  `by_physical_composite_v1_${columnIds.join("_")}`;
export const physicalUserColumnField = (columnId: number) => `_app_${columnId}`;
export const physicalHistoryProjectionTarget = (alias: number, table: string) =>
  `schema_${alias}_${table}_history`;
export const physicalCurrentProjectionTarget = (alias: number, table: string) =>
  `schema_${alias}_${table}_current`;

/** A schema-agnostic current-row target used only until the Global/Ahead winner is selected. */
export const physicalCurrentWinnerProjectionTarget = (tableId: number, physicalFields: string[]) =>
  `physical_${tableId}_current_winner_${physicalFields.join("_")}`;

/** A query-local current-source target which typed-nulls unneeded enum cells. */
export const physicalCurrentProjectionTargetForEnumColumns = (
  alias: number,
  table: string,
  enumColumns: Iterable<number>,
) => `${physicalCurrentProjectionTarget(alias, table)}_enum_fields_${[...enumColumns].join("_")}`;

/** The physical current tables of one table, named by the refs they store. */
export type PhysicalCurrentClass = "Global" | "Ahead";
export type ContentProjectionShape = "History" | "Current";

/** A query-local history source crosses into the reader's authored descriptor. */
export function authoredHistoryProjectionDescriptor(table: any): any {
  return RecordDescriptor.new_(
    table
      .historyStorageTable()
      .recordSchema()
      .fields()
      .map((field: any) => {
        if (field.name === undefined) throw new Error("Jazz history storage fields are named");
        const column = table.columns.find((c: any) => userColumnField(c.name) === field.name);
        return [field.name, column ? VT.nullable(column.columnType) : field.valueType];
      }),
  );
}

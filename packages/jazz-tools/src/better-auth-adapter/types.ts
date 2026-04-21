export type JazzSortBy = { field: string; direction: "asc" | "desc" };
export type JazzBuiltCondition = { column: string; op: string; value?: unknown };
export type JazzRowRecord = Record<string, unknown> & { id: string };

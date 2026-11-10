// Where the data explorer reads rows and sends writes. The overlay and the
// standalone Inspector read through a Jazz Db (the default below); the
// Inspector a backend serves reads through that backend's Inspector API
// (`../backend/api-data.ts`). The grid only sees this interface.

import type { QueryOptions, TableProxy } from "jazz-tools";
import { useAll, useDb } from "jazz-tools/react";
import { createContext, useContext, type PropsWithChildren } from "react";
import type { DynamicTableRow, GenericQueryBuilder } from "../utility/generic-query-builder.js";

export interface InspectorRows {
  data: DynamicTableRow[] | undefined;
  isLoading: boolean;
  error: Error | null;
}

/** One table's queued changes, written in one transaction. */
export interface InspectorChanges {
  updates: Array<{ rowId: string; updates: Record<string, unknown> }>;
  deletes: string[];
  inserts: Array<{ id: string; values: Record<string, unknown> }>;
}

export interface InspectorWrite {
  /** Settles once the write is durable at `tier`, or rejects when it was refused. */
  wait(options: { tier: "local" | "global" }): Promise<unknown>;
}

export type InspectorTable = TableProxy<DynamicTableRow, Record<string, unknown>>;

export interface InspectorDataSource {
  /** A React hook: the rows `query` matches, kept current. */
  useRows(query: GenericQueryBuilder, options: QueryOptions): InspectorRows;
  /** A React hook: the function that writes `changes` to `table`. */
  useWrite(): (table: InspectorTable, changes: InspectorChanges) => Promise<InspectorWrite>;
}

function useJazzWrite() {
  const db = useDb();
  return async (table: InspectorTable, changes: InspectorChanges): Promise<InspectorWrite> =>
    db.transaction((tx) => {
      for (const { rowId, updates } of changes.updates) tx.update(table, rowId, updates);
      for (const rowId of changes.deletes) tx.delete(table, rowId);
      for (const insert of changes.inserts) tx.insert(table, insert.values, { id: insert.id });
    });
}

/** Rows and writes through the Jazz Db of the surrounding `JazzClientProvider`. */
export const jazzInspectorData: InspectorDataSource = {
  useRows: (query, options) => useAll<DynamicTableRow>(query, options),
  useWrite: useJazzWrite,
};

const InspectorDataContext = createContext<InspectorDataSource>(jazzInspectorData);

/**
 * Use `source` for everything inside. It must stay the same object for the
 * provider's lifetime: its hooks are called in place of each other.
 */
export function InspectorDataProvider({
  source,
  children,
}: PropsWithChildren<{ source: InspectorDataSource }>) {
  return <InspectorDataContext.Provider value={source}>{children}</InspectorDataContext.Provider>;
}

export function useInspectorData(): InspectorDataSource {
  return useContext(InspectorDataContext);
}

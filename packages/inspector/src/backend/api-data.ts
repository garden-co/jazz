// The data explorer's rows and writes through the backend's Inspector API.
// A read is one `query` call, kept current by asking again every few seconds
// while the page is visible, and right after any write from this page. The
// API has no subscriptions: a backend may be many short-lived instances.

import type { ColumnDescriptor, QueryOptions, WasmSchema } from "jazz-tools";
import {
  fromJsonForColumn,
  toJsonValue,
  type InspectAs,
  type InspectQuery,
  type InspectRowsAnswer,
  type InspectWhere,
  type InspectWriteOp,
  isLargeValueSummary,
} from "jazz-tools/inspect";
import { useEffect, useState } from "react";
import type {
  InspectorChanges,
  InspectorDataSource,
  InspectorRows,
  InspectorTable,
  InspectorWrite,
} from "../contexts/inspector-data.js";
import type { DynamicTableRow, GenericQueryBuilder } from "../utility/generic-query-builder.js";
import type { InspectorCall } from "./api.js";

export const REFRESH_INTERVAL_MS = 3_000;

const PROVENANCE_TIMESTAMPS = new Set(["$createdAt", "$updatedAt"]);

/** The API's query for what the data explorer built. */
export function toInspectQuery(query: GenericQueryBuilder): InspectQuery {
  const built = query.parts();
  const where: InspectWhere = {};
  for (const { column, op, value } of built.conditions) {
    const ops = (where[column] ??= {}) as Record<string, unknown>;
    if (op in ops) {
      throw new Error(
        `Two "${op}" filters on ${column} are not supported yet: combine them into one.`,
      );
    }
    ops[op] = toJsonValue(value);
  }
  return {
    table: built.table,
    ...(built.conditions.length ? { where } : {}),
    ...(built.select.length ? { select: built.select } : {}),
    ...(built.orderBy.length ? { orderBy: built.orderBy } : {}),
    ...(built.limit !== undefined ? { limit: built.limit } : {}),
    ...(built.offset !== undefined ? { offset: built.offset } : {}),
  };
}

/** A row from the API, with its values as the grid holds them (dates, bigints, bytes). */
export function decodeRow(
  columns: ReadonlyMap<string, ColumnDescriptor>,
  row: Record<string, unknown>,
): DynamicTableRow {
  const decoded: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(row)) {
    if (isLargeValueSummary(value)) {
      // Not the value: a summary of one too large to list. Shown as such.
      decoded[key] = value;
    } else if (PROVENANCE_TIMESTAMPS.has(key) && typeof value === "string") {
      decoded[key] = new Date(value);
    } else {
      decoded[key] = fromJsonForColumn(columns.get(key)?.column_type, value);
    }
  }
  return decoded as DynamicTableRow;
}

/** The API's write for one table's queued changes. */
export function toWriteOps(table: string, changes: InspectorChanges): InspectWriteOp[] {
  const encode = (values: Record<string, unknown>) =>
    Object.fromEntries(Object.entries(values).map(([key, value]) => [key, toJsonValue(value)]));
  return [
    ...changes.updates.map(
      ({ rowId, updates }): InspectWriteOp => ({
        kind: "update",
        table,
        id: rowId,
        values: encode(updates),
      }),
    ),
    ...changes.deletes.map((id): InspectWriteOp => ({ kind: "delete", table, id })),
    ...changes.inserts.map(
      ({ id, values }): InspectWriteOp => ({
        kind: "insert",
        table,
        id,
        values: encode(values),
      }),
    ),
  ];
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

export interface ApiDataSource extends InspectorDataSource {
  /** Ask every visible query again now. */
  refresh(): void;
}

export function createApiDataSource(options: {
  call: InspectorCall;
  schema: WasmSchema;
  /** Whose permissions reads and writes run under; undefined for the backend's own. */
  as: () => InspectAs | undefined;
  /** The backend's warning about the latest rows (e.g. no claims given), or null. */
  onWarning?: (warning: string | null) => void;
  refreshIntervalMs?: number;
}): ApiDataSource {
  const listeners = new Set<() => void>();
  const refresh = () => {
    for (const listener of listeners) listener();
  };
  const columnsFor = (table: string) =>
    new Map((options.schema[table]?.columns ?? []).map((column) => [column.name, column]));

  function useRows(query: GenericQueryBuilder, _options: QueryOptions): InspectorRows {
    // The query as the API takes it, or why it can't: either way, what identifies it.
    let request: InspectQuery | null = null;
    let unsupported: Error | null = null;
    try {
      request = toInspectQuery(query);
    } catch (error) {
      unsupported = asError(error);
    }
    const key = request ? JSON.stringify(request) : `unsupported:${unsupported!.message}`;
    const [state, setState] = useState<InspectorRows & { key: string }>({
      key,
      data: undefined,
      isLoading: true,
      error: null,
    });

    useEffect(() => {
      let stopped = false;
      let inFlight = false;
      let again = false;
      if (!request) {
        setState({ key, data: undefined, isLoading: false, error: unsupported });
        return;
      }
      const query = request;
      const columns = columnsFor(query.table);
      setState((current) =>
        current.key === key ? current : { key, data: undefined, isLoading: true, error: null },
      );

      const load = async () => {
        if (inFlight) {
          again = true;
          return;
        }
        inFlight = true;
        try {
          const as = options.as();
          const answer = await options.call<InspectRowsAnswer>({
            op: "query",
            query,
            ...(as !== undefined ? { as } : {}),
          });
          if (!stopped) {
            options.onWarning?.(answer.warning ?? null);
            setState({
              key,
              data: answer.rows.map((row) => decodeRow(columns, row)),
              isLoading: false,
              error: null,
            });
          }
        } catch (error) {
          // Keep the rows shown so far: a refresh that fails says so without blanking the grid.
          if (!stopped)
            setState((current) =>
              current.key === key
                ? { ...current, isLoading: false, error: asError(error) }
                : { key, data: undefined, isLoading: false, error: asError(error) },
            );
        } finally {
          inFlight = false;
          if (again && !stopped) {
            again = false;
            void load();
          }
        }
      };

      void load();
      listeners.add(load);
      const interval = setInterval(() => {
        if (typeof document === "undefined" || document.visibilityState === "visible") void load();
      }, options.refreshIntervalMs ?? REFRESH_INTERVAL_MS);
      return () => {
        stopped = true;
        listeners.delete(load);
        clearInterval(interval);
      };
      // `key` captures everything about the query.
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [key]);

    return state.key === key ? state : { data: undefined, isLoading: true, error: null };
  }

  function useWrite() {
    return async (table: InspectorTable, changes: InspectorChanges): Promise<InspectorWrite> => {
      const as = options.as();
      const done = options
        .call({
          op: "write",
          ops: toWriteOps(table._table, changes),
          ...(as !== undefined ? { as } : {}),
        })
        .finally(refresh);
      // The grid waits on it; a refusal surfaces there.
      done.catch(() => {});
      return { wait: () => done };
    };
  }

  return { useRows, useWrite, refresh };
}

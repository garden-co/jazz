import "react-data-grid/lib/styles.css";
import {
  Cell,
  DataGrid,
  Row,
  type Column,
  type RenderEditCellProps,
  type Renderers,
  type RowsChangeData,
  type SortColumn,
} from "react-data-grid";
import { Group, Panel, Separator } from "react-resizable-panels";
import type { ColumnDescriptor, ColumnType, DynamicTableRow, TableProxy } from "jazz-tools";
import { useAll, useDb } from "jazz-tools/react";
import {
  Profiler,
  useEffect,
  useMemo,
  useRef,
  useState,
  type Dispatch,
  type SetStateAction,
} from "react";
import { Link, Navigate, useParams, useSearchParams } from "react-router";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import { GenericQueryBuilder } from "../../utility/generic-query-builder.js";
import { RowMutationSidebar } from "./RowMutationSidebar.js";
import {
  TableFilterBuilder,
  type TableFilterBuilderHandle,
  type TableFilterClause,
} from "./TableFilterBuilder.js";
import {
  formatMutationFieldValue,
  getFieldReadOnlyReason,
  parseMutationFieldValue,
} from "./row-mutation-form.js";
import { buildRelationFilterHref } from "./relation-navigation.js";
import styles from "./TableDataGrid.module.css";

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

const RELATION_LABEL_COLUMN_PRIORITY = [
  "name",
  "title",
  "label",
  "displayName",
  "display_name",
  "username",
  "handle",
  "slug",
  "email",
] as const;

const PAGE_SIZE_OPTIONS = [10, 25, 50] as const;
const DEFAULT_PAGE_SIZE = 25;
const EMPTY_ROWS: DynamicTableRow[] = [];
const CELL_UPDATE_ANIMATION_MS = 1_200;
const ROW_ADDED_ANIMATION_MS = 2_000;
const ROW_REMOVED_ANIMATION_MS = 650;
const DATA_COLUMN_MAX_WIDTH = 360;
const POINTER_SIDEBAR_OPEN_DELAY_MS = 180;

interface MutationState {
  mode: "insert";
}

interface TableDataGridProfilerEntry {
  id: string;
  phase: "mount" | "update" | "nested-update";
  actualDuration: number;
  baseDuration: number;
  startTime: number;
  commitTime: number;
  table: string;
  pageIndex: number;
  pageSize: number;
  sortColumn: string;
  sortDirection: "asc" | "desc";
  filterCount: number;
  rowCount: number;
  visibleRowCount: number;
  queryOffset: number;
  queryLimit: number;
  hasNextPage: boolean;
  query: string;
}

interface TableDataGridDebugEvent {
  type: string;
  timestampMs: number;
  table: string;
  pageIndex: number;
  pageSize: number;
  sortColumn: string;
  sortDirection: "asc" | "desc";
  filterCount: number;
  rowCount: number;
  visibleRowCount: number;
  queryOffset: number;
  queryLimit: number;
  hasNextPage: boolean;
  query: string;
  details?: Record<string, unknown>;
}

type TableDataGridInspectorWindow = Window & {
  __inspectorProfiler?: TableDataGridProfilerEntry[];
  __inspectorGridEvents?: TableDataGridDebugEvent[];
};

interface GridColumn {
  id: string;
  accessorKey: string;
  header: string;
  enableSorting: boolean;
}

type RowChangeState = "added" | "removed";

interface AnimatedGridRow {
  row: DynamicTableRow;
  rowChangeState?: RowChangeState;
  changedCellIds?: Record<string, true>;
}

interface QueuedCellEdit {
  text: string;
}

type QueuedRowEdits = Record<string, QueuedCellEdit>;

interface EditableGridRow extends AnimatedGridRow {
  row: DynamicTableRow;
  sourceRow: DynamicTableRow;
  queuedEdits?: QueuedRowEdits;
}

function isColumnSortable(columnType: ColumnType): boolean {
  switch (columnType.type) {
    case "Integer":
    case "BigInt":
    case "Double":
    case "Boolean":
    case "Text":
    case "Enum":
    case "Timestamp":
    case "Uuid":
      return true;
    default:
      return false;
  }
}

function recordTableDataGridProfilerEntry(entry: TableDataGridProfilerEntry): void {
  const profilerWindow = globalThis.window as TableDataGridInspectorWindow | undefined;
  if (!profilerWindow?.__inspectorProfiler) {
    return;
  }
  profilerWindow.__inspectorProfiler.push(entry);
}

function getGridRowId(row: DynamicTableRow): string {
  return String(row.id);
}

function clearAnimationTimeouts(timeouts: Map<string, ReturnType<typeof setTimeout>>): void {
  for (const timeout of timeouts.values()) {
    clearTimeout(timeout);
  }
  timeouts.clear();
}

function mergeChangedCellIds(
  existingCellIds: Record<string, true> | undefined,
  nextCellIds: string[],
): Record<string, true> | undefined {
  if (!existingCellIds && nextCellIds.length === 0) {
    return undefined;
  }

  const mergedCellIds = { ...existingCellIds };
  for (const cellId of nextCellIds) {
    mergedCellIds[cellId] = true;
  }

  return Object.keys(mergedCellIds).length > 0 ? mergedCellIds : undefined;
}

function getChangedCellIds(
  previousRow: DynamicTableRow,
  nextRow: DynamicTableRow,
  gridColumns: GridColumn[],
): string[] {
  const changedCellIds: string[] = [];

  for (const column of gridColumns) {
    const previousValue = formatCellValue(previousRow[column.accessorKey]);
    const nextValue = formatCellValue(nextRow[column.accessorKey]);
    if (previousValue !== nextValue) {
      changedCellIds.push(column.id);
    }
  }

  return changedCellIds;
}

function createAnimatedGridRow(row: DynamicTableRow): AnimatedGridRow {
  return { row };
}

function isDisplayFriendlyColumn(column: ColumnDescriptor): boolean {
  if (column.name === "id" || column.references) {
    return false;
  }

  switch (column.column_type.type) {
    case "Text":
    case "Enum":
    case "Timestamp":
    case "Integer":
    case "BigInt":
    case "Double":
    case "Boolean":
      return true;
    default:
      return false;
  }
}

function getRelationDisplayColumn(
  schema: Record<string, { columns: ColumnDescriptor[] }>,
  table: string,
) {
  const tableColumns = schema[table]?.columns ?? [];

  for (const columnName of RELATION_LABEL_COLUMN_PRIORITY) {
    const matchingColumn = tableColumns.find(
      (column) => column.name === columnName && isDisplayFriendlyColumn(column),
    );
    if (matchingColumn) {
      return matchingColumn;
    }
  }

  const firstTextColumn = tableColumns.find(
    (column) => column.column_type.type === "Text" && isDisplayFriendlyColumn(column),
  );
  if (firstTextColumn) {
    return firstTextColumn;
  }

  return tableColumns.find(isDisplayFriendlyColumn);
}

function getQueuedEditText(value: unknown): string {
  return formatMutationFieldValue(value);
}

function parseQueuedEditValue(column: ColumnDescriptor, text: string): unknown {
  const trimmed = text.trim();

  if (column.nullable) {
    switch (column.column_type.type) {
      case "Uuid":
      case "Boolean":
      case "Integer":
      case "BigInt":
      case "Double":
      case "Timestamp":
      case "Enum":
      case "Json":
      case "Array":
      case "Row":
        if (trimmed.length === 0) {
          return null;
        }
        break;
      default:
        break;
    }
  }

  return parseMutationFieldValue(column.column_type, text);
}

function applyQueuedEditsToRow(
  row: DynamicTableRow,
  queuedEdits?: QueuedRowEdits,
): DynamicTableRow {
  if (!queuedEdits || Object.keys(queuedEdits).length === 0) {
    return row;
  }

  const nextRow = { ...row };
  for (const [columnId, queuedEdit] of Object.entries(queuedEdits)) {
    nextRow[columnId] = queuedEdit.text;
  }

  return nextRow;
}

function removeQueuedCellEdit(
  queuedEdits: Record<string, QueuedRowEdits>,
  rowId: string,
  columnId: string,
): Record<string, QueuedRowEdits> {
  const rowEdits = queuedEdits[rowId];
  if (!rowEdits?.[columnId]) {
    return queuedEdits;
  }

  const nextRowEdits = { ...rowEdits };
  delete nextRowEdits[columnId];

  if (Object.keys(nextRowEdits).length === 0) {
    const nextQueuedEdits = { ...queuedEdits };
    delete nextQueuedEdits[rowId];
    return nextQueuedEdits;
  }

  return {
    ...queuedEdits,
    [rowId]: nextRowEdits,
  };
}

function useAnimatedGridRows(
  rows: DynamicTableRow[],
  gridColumns: GridColumn[],
  animationScopeKey: string,
): AnimatedGridRow[] {
  const [renderedRows, setRenderedRows] = useState<AnimatedGridRow[]>(() =>
    rows.map(createAnimatedGridRow),
  );
  const previousRowsRef = useRef(rows);
  const previousScopeKeyRef = useRef(animationScopeKey);
  const hasEstablishedScopeBaselineRef = useRef(rows.length > 0);
  const cellAnimationTimeoutsRef = useRef(new Map<string, ReturnType<typeof setTimeout>>());
  const rowAnimationTimeoutsRef = useRef(new Map<string, ReturnType<typeof setTimeout>>());

  useEffect(() => {
    return () => {
      clearAnimationTimeouts(cellAnimationTimeoutsRef.current);
      clearAnimationTimeouts(rowAnimationTimeoutsRef.current);
    };
  }, []);

  useEffect(() => {
    if (previousScopeKeyRef.current !== animationScopeKey) {
      previousScopeKeyRef.current = animationScopeKey;
      previousRowsRef.current = rows;
      hasEstablishedScopeBaselineRef.current = rows.length > 0;
      clearAnimationTimeouts(cellAnimationTimeoutsRef.current);
      clearAnimationTimeouts(rowAnimationTimeoutsRef.current);
      setRenderedRows(rows.map(createAnimatedGridRow));
      return;
    }

    const previousRows = previousRowsRef.current;
    previousRowsRef.current = rows;

    if (!hasEstablishedScopeBaselineRef.current) {
      if (rows.length === 0) {
        setRenderedRows([]);
        return;
      }

      hasEstablishedScopeBaselineRef.current = true;
      setRenderedRows(rows.map(createAnimatedGridRow));
      return;
    }

    const previousRowById = new Map(previousRows.map((row) => [getGridRowId(row), row]));
    const nextRowById = new Map(rows.map((row) => [getGridRowId(row), row]));
    const addedRowIds = new Set(
      rows.map(getGridRowId).filter((rowId) => !previousRowById.has(rowId)),
    );
    const removedRows = previousRows.filter((row) => !nextRowById.has(getGridRowId(row)));
    const changedCellIdsByRowId = new Map<string, string[]>();

    for (const row of rows) {
      const rowId = getGridRowId(row);
      const previousRow = previousRowById.get(rowId);
      if (!previousRow) {
        continue;
      }
      const changedCellIds = getChangedCellIds(previousRow, row, gridColumns);
      if (changedCellIds.length > 0) {
        changedCellIdsByRowId.set(rowId, changedCellIds);
      }
    }

    setRenderedRows((currentRenderedRows) => {
      const currentRenderedRowById = new Map(
        currentRenderedRows.map((entry) => [getGridRowId(entry.row), entry]),
      );
      const nextRenderedRows: AnimatedGridRow[] = rows.map((row) => {
        const rowId = getGridRowId(row);
        const currentRenderedRow = currentRenderedRowById.get(rowId);
        const rowChangeState: RowChangeState | undefined =
          addedRowIds.has(rowId) || currentRenderedRow?.rowChangeState === "added"
            ? "added"
            : undefined;

        return {
          row,
          rowChangeState,
          changedCellIds: mergeChangedCellIds(
            currentRenderedRow?.changedCellIds,
            changedCellIdsByRowId.get(rowId) ?? [],
          ),
        };
      });

      const ghostInsertions = new Map<string, { entry: AnimatedGridRow; index: number }>();

      for (const [index, entry] of currentRenderedRows.entries()) {
        const rowId = getGridRowId(entry.row);
        if (entry.rowChangeState === "removed" && !nextRowById.has(rowId)) {
          ghostInsertions.set(rowId, { entry, index });
        }
      }

      for (const removedRow of removedRows) {
        const rowId = getGridRowId(removedRow);
        ghostInsertions.set(rowId, {
          entry: { row: removedRow, rowChangeState: "removed" },
          index: previousRows.findIndex((row) => getGridRowId(row) === rowId),
        });
      }

      for (const { entry, index } of [...ghostInsertions.values()].sort((left, right) => {
        return left.index - right.index;
      })) {
        const rowId = getGridRowId(entry.row);
        if (nextRenderedRows.some((renderedRow) => getGridRowId(renderedRow.row) === rowId)) {
          continue;
        }

        nextRenderedRows.splice(Math.max(0, Math.min(index, nextRenderedRows.length)), 0, entry);
      }

      return nextRenderedRows;
    });

    for (const rowId of addedRowIds) {
      const existingTimeout = rowAnimationTimeoutsRef.current.get(rowId);
      if (existingTimeout) {
        clearTimeout(existingTimeout);
      }
      rowAnimationTimeoutsRef.current.set(
        rowId,
        setTimeout(() => {
          rowAnimationTimeoutsRef.current.delete(rowId);
          setRenderedRows((currentRenderedRows) =>
            currentRenderedRows.map((entry) =>
              getGridRowId(entry.row) === rowId && entry.rowChangeState === "added"
                ? { ...entry, rowChangeState: undefined }
                : entry,
            ),
          );
        }, ROW_ADDED_ANIMATION_MS),
      );
    }

    for (const removedRow of removedRows) {
      const rowId = getGridRowId(removedRow);
      const existingTimeout = rowAnimationTimeoutsRef.current.get(rowId);
      if (existingTimeout) {
        clearTimeout(existingTimeout);
      }
      rowAnimationTimeoutsRef.current.set(
        rowId,
        setTimeout(() => {
          rowAnimationTimeoutsRef.current.delete(rowId);
          setRenderedRows((currentRenderedRows) =>
            currentRenderedRows.filter((entry) => getGridRowId(entry.row) !== rowId),
          );
        }, ROW_REMOVED_ANIMATION_MS),
      );
    }

    for (const [rowId, changedCellIds] of changedCellIdsByRowId) {
      for (const cellId of changedCellIds) {
        const timeoutKey = `${rowId}:${cellId}`;
        const existingTimeout = cellAnimationTimeoutsRef.current.get(timeoutKey);
        if (existingTimeout) {
          clearTimeout(existingTimeout);
        }
        cellAnimationTimeoutsRef.current.set(
          timeoutKey,
          setTimeout(() => {
            cellAnimationTimeoutsRef.current.delete(timeoutKey);
            setRenderedRows((currentRenderedRows) =>
              currentRenderedRows.map((entry) => {
                if (getGridRowId(entry.row) !== rowId || !entry.changedCellIds?.[cellId]) {
                  return entry;
                }

                const nextChangedCellIds = { ...entry.changedCellIds };
                delete nextChangedCellIds[cellId];

                return {
                  ...entry,
                  changedCellIds:
                    Object.keys(nextChangedCellIds).length > 0 ? nextChangedCellIds : undefined,
                };
              }),
            );
          }, CELL_UPDATE_ANIMATION_MS),
        );
      }
    }
  }, [animationScopeKey, gridColumns, rows]);

  return renderedRows;
}

export function TableDataGrid() {
  const { table } = useParams();

  if (!table) {
    return <Navigate to="/data-explorer" replace />;
  }

  const { wasmSchema: schema, queryPropagation, runtime } = useDevtoolsContext();
  const db = useDb();
  const [searchParams, setSearchParams] = useSearchParams();

  const sorting = useMemo<readonly SortColumn[]>(() => {
    const col = searchParams.get("sort");
    const dir = searchParams.get("dir");
    if (col) {
      return [{ columnKey: col, direction: dir === "DESC" ? "DESC" : "ASC" }];
    }
    return [{ columnKey: "id", direction: "ASC" }];
  }, [searchParams]);

  const pageSize = useMemo(() => {
    const raw = searchParams.get("pageSize");
    if (raw) {
      const n = Number(raw);
      if (PAGE_SIZE_OPTIONS.includes(n as (typeof PAGE_SIZE_OPTIONS)[number])) return n;
    }
    return DEFAULT_PAGE_SIZE;
  }, [searchParams]);

  const pageIndex = useMemo(() => {
    const raw = searchParams.get("page");
    if (raw) {
      const n = Number(raw);
      if (Number.isInteger(n) && n >= 0) return n;
    }
    return 0;
  }, [searchParams]);

  const filters = useMemo<TableFilterClause[]>(() => {
    const raw = searchParams.get("filters");
    if (raw) {
      try {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) return parsed;
      } catch {
        // ignore malformed
      }
    }
    return [];
  }, [searchParams]);

  const setPageSize = (next: number) => {
    setSearchParams(
      (prev) => {
        const p = new URLSearchParams(prev);
        if (next !== DEFAULT_PAGE_SIZE) {
          p.set("pageSize", String(next));
        } else {
          p.delete("pageSize");
        }
        p.delete("page");
        return p;
      },
      { replace: true },
    );
  };

  const setPageIndex = (next: number | ((current: number) => number)) => {
    setSearchParams(
      (prev) => {
        const p = new URLSearchParams(prev);
        const currentPage = Number(p.get("page") ?? 0);
        const resolved = typeof next === "function" ? next(currentPage) : next;
        if (resolved > 0) {
          p.set("page", String(resolved));
        } else {
          p.delete("page");
        }
        return p;
      },
      { replace: true },
    );
  };

  const setFilters = (next: TableFilterClause[]) => {
    setSearchParams(
      (prev) => {
        const p = new URLSearchParams(prev);
        if (next.length > 0) {
          p.set("filters", JSON.stringify(next));
        } else {
          p.delete("filters");
        }
        p.delete("page");
        return p;
      },
      { replace: true },
    );
  };
  const [mutationState, setMutationState] = useState<MutationState | null>(null);
  const [selectedRowId, setSelectedRowId] = useState<string | null>(null);
  const [isSidebarMutationPending, setIsSidebarMutationPending] = useState(false);
  const [queuedEdits, setQueuedEdits] = useState<Record<string, QueuedRowEdits>>({});
  const [isQueuedSavePending, setIsQueuedSavePending] = useState(false);
  const [queuedSaveError, setQueuedSaveError] = useState<string | null>(null);
  const [queuedDeletes, setQueuedDeletes] = useState<Set<string>>(new Set());
  const filterBuilderRef = useRef<TableFilterBuilderHandle | null>(null);
  const schemaColumns = schema[table]?.columns ?? [];
  const schemaColumnById = useMemo(
    () => new Map(schemaColumns.map((column) => [column.name, column])),
    [schemaColumns],
  );
  const activeSort = sorting[0] ?? { columnKey: "id", direction: "ASC" };
  const sortColumn = activeSort.columnKey;
  const sortDirection = activeSort.direction === "DESC" ? "desc" : "asc";
  const queryOffset = pageIndex * pageSize;
  const queryLimit = pageSize + 1;
  const queryBuilder = useMemo(() => {
    let builder = new GenericQueryBuilder(table, schema);
    for (const filter of filters) {
      if (filter.operator === "eq") {
        builder = builder.where({ [filter.column]: filter.value });
      } else {
        builder = builder.where({
          [filter.column]: {
            [filter.operator]: filter.value,
          },
        });
      }
    }
    return builder.orderBy(sortColumn, sortDirection).limit(queryLimit).offset(queryOffset);
  }, [table, schema, filters, sortColumn, sortDirection, queryLimit, queryOffset]);
  const builtQuery = useMemo(() => queryBuilder._build(), [queryBuilder]);
  const mutationDurabilityTier = runtime === "standalone" ? "edge" : "worker";
  const queryOptions = useMemo(
    () =>
      ({
        propagation: queryPropagation,
        visibility: "hidden_from_live_query_list",
      }) as const,
    [queryPropagation],
  );

  const rows = useAll<DynamicTableRow>(queryBuilder, queryOptions) ?? EMPTY_ROWS;

  const gridColumns = useMemo<GridColumn[]>(
    () => [
      {
        id: "id",
        accessorKey: "id",
        header: "ID",
        enableSorting: true,
      },
      ...schemaColumns.map(
        (column): GridColumn => ({
          id: column.name,
          accessorKey: column.name,
          header: column.name,
          enableSorting: isColumnSortable(column.column_type),
        }),
      ),
    ],
    [schemaColumns],
  );
  const hasNextPage = rows.length > pageSize;
  const visibleRows = hasNextPage ? rows.slice(0, pageSize) : rows;
  const queuedEditCount = useMemo(() => {
    return Object.values(queuedEdits).reduce(
      (total, rowEdits) => total + Object.keys(rowEdits).length,
      0,
    );
  }, [queuedEdits]);
  const queuedEditedRowCount = useMemo(() => Object.keys(queuedEdits).length, [queuedEdits]);
  const hasQueuedEdits = queuedEditCount > 0;
  const hasQueuedChanges = hasQueuedEdits || queuedDeletes.size > 0;
  const isAnyMutationPending = isSidebarMutationPending || isQueuedSavePending;
  const filterButtonLabel = filters.length > 0 ? `Filter (${filters.length})` : "Filter";
  const gridAnimationScopeKey = useMemo(
    () => `${table}:${builtQuery}:${gridColumns.map((column) => column.id).join("|")}`,
    [builtQuery, gridColumns, table],
  );
  const rowById = useMemo(() => {
    return new Map(visibleRows.map((row) => [row.id, row]));
  }, [visibleRows]);
  const selectedRowValues = useMemo(() => {
    if (!selectedRowId) {
      return null;
    }

    const selectedRow = rowById.get(selectedRowId);
    if (!selectedRow) {
      return null;
    }

    return applyQueuedEditsToRow(selectedRow, queuedEdits[selectedRowId]);
  }, [queuedEdits, rowById, selectedRowId]);
  const insertRowValues = useMemo(() => {
    const values: Record<string, unknown> = {};
    for (const column of schemaColumns) {
      values[column.name] = undefined;
    }
    return values;
  }, [schemaColumns]);
  const tableProxy = useMemo(
    () =>
      ({
        _table: table,
        _schema: schema,
        _rowType: undefined,
        _initType: undefined,
      }) as unknown as TableProxy<DynamicTableRow, Record<string, unknown>>,
    [table, schema],
  );
  const startRow = pageIndex * pageSize;
  const endRow = startRow + visibleRows.length;
  const profileEntryBase = {
    table,
    pageIndex,
    pageSize,
    sortColumn,
    sortDirection,
    filterCount: filters.length,
    rowCount: rows.length,
    visibleRowCount: visibleRows.length,
    queryOffset,
    queryLimit,
    hasNextPage,
    query: builtQuery,
  } as const;
  const recordGridEvent = (type: string, details?: Record<string, unknown>): void => {
    const inspectorWindow = globalThis.window as TableDataGridInspectorWindow | undefined;
    if (!inspectorWindow?.__inspectorGridEvents) {
      return;
    }
    const event = {
      type,
      timestampMs: globalThis.performance?.now?.() ?? Date.now(),
      ...profileEntryBase,
      details,
    };
    inspectorWindow.__inspectorGridEvents.push(event);
    globalThis.console?.debug?.("[inspector-grid]", JSON.stringify(event));
  };
  const handleSortColumnsChange = (nextSortColumns: SortColumn[]): void => {
    const nextSort =
      nextSortColumns.length === 0
        ? [{ columnKey: "id", direction: "ASC" as const }]
        : [nextSortColumns.at(-1)!];
    recordGridEvent("sort-click", {
      columnId: nextSort[0]?.columnKey,
      nextSortDirection: nextSort[0]?.direction === "DESC" ? "desc" : "asc",
    });
    setSearchParams(
      (prev) => {
        const p = new URLSearchParams(prev);
        const col = nextSort[0];
        if (col && (col.columnKey !== "id" || col.direction !== "ASC")) {
          p.set("sort", col.columnKey);
          p.set("dir", col.direction);
        } else {
          p.delete("sort");
          p.delete("dir");
        }
        p.delete("page");
        return p;
      },
      { replace: true },
    );
  };
  const handleSaveSelectedRow = async (updates: Record<string, unknown>): Promise<void> => {
    if (!selectedRowId) {
      return;
    }

    try {
      setIsSidebarMutationPending(true);
      await db.updateDurable(tableProxy, selectedRowId, updates, {
        tier: mutationDurabilityTier,
      });
      setQueuedEdits((currentQueuedEdits) => {
        if (!currentQueuedEdits[selectedRowId]) {
          return currentQueuedEdits;
        }

        const nextQueuedEdits = { ...currentQueuedEdits };
        delete nextQueuedEdits[selectedRowId];
        return nextQueuedEdits;
      });
      setQueuedSaveError(null);
    } finally {
      setIsSidebarMutationPending(false);
    }
  };
  const handleDiscardQueuedEdits = (): void => {
    setQueuedEdits({});
    setQueuedDeletes(new Set());
    setQueuedSaveError(null);
  };
  const handleSaveQueuedEdits = async (): Promise<void> => {
    if (!hasQueuedChanges) {
      return;
    }

    try {
      setIsQueuedSavePending(true);
      setQueuedSaveError(null);

      const rowUpdates = Object.entries(queuedEdits)
        .filter(([rowId]) => !queuedDeletes.has(rowId))
        .map(([rowId, rowEdits]) => {
          const updates: Record<string, unknown> = {};
          for (const [columnId, queuedEdit] of Object.entries(rowEdits)) {
            const schemaColumn = schemaColumnById.get(columnId);
            if (!schemaColumn || getFieldReadOnlyReason(schemaColumn) !== null) {
              continue;
            }
            updates[columnId] = parseQueuedEditValue(schemaColumn, queuedEdit.text);
          }
          return { rowId, updates };
        });

      await Promise.all([
        ...rowUpdates.map(({ rowId, updates }) =>
          db.updateDurable(tableProxy, rowId, updates, {
            tier: mutationDurabilityTier,
          }),
        ),
        ...[...queuedDeletes].map((rowId) =>
          db.deleteDurable(tableProxy, rowId, {
            tier: mutationDurabilityTier,
          }),
        ),
      ]);

      if (selectedRowId && queuedDeletes.has(selectedRowId)) {
        setSelectedRowId(null);
      }
      setQueuedEdits({});
      setQueuedDeletes(new Set());
    } catch (error) {
      setQueuedSaveError(
        error instanceof Error ? error.message : "Could not persist queued cell edits.",
      );
    } finally {
      setIsQueuedSavePending(false);
    }
  };

  useEffect(() => {
    recordGridEvent("state-commit");
  }, [
    table,
    pageIndex,
    pageSize,
    sortColumn,
    sortDirection,
    filters.length,
    rows.length,
    visibleRows.length,
    queryOffset,
    queryLimit,
    hasNextPage,
    builtQuery,
  ]);

  useEffect(() => {
    if (selectedRowId && !rowById.has(selectedRowId)) {
      setSelectedRowId(null);
    }
  }, [rowById, selectedRowId]);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key !== "Escape") return;
      if (mutationState) {
        setMutationState(null);
      } else if (selectedRowId) {
        setSelectedRowId(null);
      }
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [mutationState, selectedRowId]);

  return (
    <Profiler
      id="TableDataGrid"
      onRender={(id, phase, actualDuration, baseDuration, startTime, commitTime) => {
        recordTableDataGridProfilerEntry({
          id,
          phase,
          actualDuration,
          baseDuration,
          startTime,
          commitTime,
          ...profileEntryBase,
        });
      }}
    >
      <section className={styles.container}>
        <header className={styles.header}>
          <h2 className={styles.title}>{table}</h2>
          <div className={styles.headerActions}>
            <Link to={`/data-explorer/${table}/schema`} className={styles.secondaryButton}>
              Schema
            </Link>
            <button
              type="button"
              className={styles.secondaryButton}
              onClick={() => {
                filterBuilderRef.current?.open();
              }}
            >
              {filterButtonLabel}
            </button>
            <button
              type="button"
              className={styles.secondaryButton}
              onClick={() => {
                setMutationState({ mode: "insert" });
              }}
              disabled={hasQueuedChanges || isAnyMutationPending}
            >
              Insert
            </button>
          </div>
        </header>
        <TableFilterBuilder
          ref={filterBuilderRef}
          schemaColumns={schemaColumns}
          clauses={filters}
          showTrigger={false}
          onClausesChange={(nextFilters) => {
            recordGridEvent("filters-change", { nextFilterCount: nextFilters.length });
            setFilters(nextFilters);
          }}
        />
        <div className={styles.contentArea}>
          <Group className={styles.contentPanels} orientation="horizontal">
            <Panel className={styles.gridPanel} defaultSize="68%" minSize="35%">
              <div className={styles.gridFrame}>
                <PlainTableView
                  rows={visibleRows}
                  gridColumns={gridColumns}
                  sorting={sorting}
                  schema={schema}
                  queryOptions={queryOptions}
                  schemaColumnById={schemaColumnById}
                  queuedEdits={queuedEdits}
                  queuedDeletes={queuedDeletes}
                  animationScopeKey={gridAnimationScopeKey}
                  onSortColumnsChange={handleSortColumnsChange}
                  onQueuedEditsChange={setQueuedEdits}
                  onQueuedSaveErrorChange={setQueuedSaveError}
                  onSelectedRowIdChange={setSelectedRowId}
                  onQueuedDeletesChange={setQueuedDeletes}
                />
              </div>
            </Panel>
            <Separator className={styles.resizeHandle} />
            <Panel className={styles.detailsPanel} defaultSize="32%" minSize="22%" maxSize="45%">
              <RowMutationSidebar
                key={`edit:${selectedRowId ?? "empty"}`}
                mode="edit"
                tableName={table}
                schemaColumns={schemaColumns}
                targetRowId={selectedRowId}
                rowValues={selectedRowValues}
                onSave={handleSaveSelectedRow}
                onDelete={async () => {
                  if (!selectedRowId) {
                    return;
                  }
                  await db.deleteDurable(tableProxy, selectedRowId, {
                    tier: mutationDurabilityTier,
                  });
                  setSelectedRowId(null);
                }}
              />
            </Panel>
          </Group>
        </div>
        <div className={styles.bottomRail}>
          {hasQueuedChanges || queuedSaveError ? (
            <div
              className={styles.queuedBanner}
              role={queuedSaveError ? "alert" : "status"}
              aria-live="polite"
            >
              <div className={styles.queuedBannerCopy}>
                <span className={styles.queuedBannerLabel}>Queued</span>
                {hasQueuedEdits ? (
                  <span>
                    {queuedEditCount} edit{queuedEditCount === 1 ? "" : "s"} across{" "}
                    {queuedEditedRowCount} row{queuedEditedRowCount === 1 ? "" : "s"}
                  </span>
                ) : null}
                {queuedDeletes.size > 0 ? (
                  <span>
                    {queuedDeletes.size} row{queuedDeletes.size === 1 ? "" : "s"} will be deleted
                  </span>
                ) : null}
                {queuedSaveError ? (
                  <span className={styles.queuedBannerError}>{queuedSaveError}</span>
                ) : null}
              </div>
              <div className={styles.queuedBannerActions}>
                <button
                  type="button"
                  className={`${styles.secondaryButton} ${styles.queuedBannerButton}`}
                  onClick={handleDiscardQueuedEdits}
                  disabled={isQueuedSavePending}
                >
                  Discard
                </button>
                <button
                  type="button"
                  className={`${styles.primaryButton} ${styles.queuedBannerButton}`}
                  onClick={() => {
                    void handleSaveQueuedEdits();
                  }}
                  disabled={isQueuedSavePending}
                >
                  {isQueuedSavePending ? "Saving..." : "Save changes"}
                </button>
              </div>
            </div>
          ) : null}
          <footer className={styles.footer}>
            <div className={styles.paginationInfo}>
              Showing {visibleRows.length === 0 ? 0 : startRow + 1}-{endRow}
            </div>
            <div className={styles.paginationControls}>
              <label className={styles.pageSizeLabel}>
                Rows per page
                <select
                  className={styles.pageSizeSelect}
                  value={pageSize}
                  onChange={(event) => {
                    const nextPageSize = Number(event.target.value);
                    recordGridEvent("page-size-change", { nextPageSize });
                    setPageSize(nextPageSize);
                  }}
                >
                  {PAGE_SIZE_OPTIONS.map((sizeOption) => (
                    <option key={sizeOption} value={sizeOption}>
                      {sizeOption}
                    </option>
                  ))}
                </select>
              </label>
              <span className={styles.pageIndicator}>Page {pageIndex + 1}</span>
              <button
                type="button"
                className={styles.secondaryButton}
                onClick={() => {
                  recordGridEvent("previous-page-click", {
                    nextPageIndex: Math.max(0, pageIndex - 1),
                  });
                  setPageIndex((current) => Math.max(0, current - 1));
                }}
                disabled={pageIndex === 0}
              >
                Previous
              </button>
              <button
                type="button"
                className={styles.secondaryButton}
                onClick={() => {
                  recordGridEvent("next-page-click", {
                    nextPageIndex: pageIndex + 1,
                  });
                  setPageIndex((current) => current + 1);
                }}
                disabled={!hasNextPage}
              >
                Next
              </button>
            </div>
          </footer>
        </div>
        {mutationState ? (
          <div
            className={styles.sidebarOverlay}
            data-testid="row-mutation-overlay"
            onClick={() => {
              if (isSidebarMutationPending) return;
              setMutationState(null);
            }}
          >
            <div
              className={styles.sidebarPanel}
              onClick={(event) => {
                event.stopPropagation();
              }}
            >
              <RowMutationSidebar
                key="insert:new"
                mode="insert"
                tableName={table}
                schemaColumns={schemaColumns}
                targetRowId={null}
                rowValues={insertRowValues}
                onCancel={() => {
                  if (isSidebarMutationPending) return;
                  setMutationState(null);
                }}
                onSave={async (updates) => {
                  try {
                    setIsSidebarMutationPending(true);
                    await db.insertDurable(tableProxy, updates, {
                      tier: mutationDurabilityTier,
                    });
                    setMutationState(null);
                  } finally {
                    setIsSidebarMutationPending(false);
                  }
                }}
              />
            </div>
          </div>
        ) : null}
      </section>
    </Profiler>
  );
}

function QueuedCellEditor({
  row,
  onRowChange,
  onClose,
  schemaColumn,
}: RenderEditCellProps<EditableGridRow> & {
  schemaColumn: ColumnDescriptor;
}) {
  const value = getQueuedEditText(row.row[schemaColumn.name]);

  const updateValue = (nextValue: string) => {
    onRowChange(
      {
        ...row,
        row: {
          ...row.row,
          [schemaColumn.name]: nextValue,
        },
      },
      false,
    );
  };
  const commit = () => {
    onClose(true, false);
  };

  if (schemaColumn.column_type.type === "Enum") {
    return (
      <select
        aria-label={`Edit ${schemaColumn.name}`}
        className={styles.inlineEditorSelect}
        autoFocus
        value={value}
        onChange={(event) => {
          updateValue(event.target.value);
        }}
        onBlur={commit}
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            onClose(false, false);
          }
        }}
      >
        {schemaColumn.nullable ? <option value="">null</option> : null}
        {schemaColumn.column_type.variants.map((variant) => (
          <option key={variant} value={variant}>
            {variant}
          </option>
        ))}
      </select>
    );
  }

  if (
    schemaColumn.column_type.type === "Json" ||
    schemaColumn.column_type.type === "Array" ||
    schemaColumn.column_type.type === "Row"
  ) {
    return (
      <textarea
        aria-label={`Edit ${schemaColumn.name}`}
        className={styles.inlineEditorTextarea}
        autoFocus
        value={value}
        onChange={(event) => {
          updateValue(event.target.value);
        }}
        onBlur={commit}
        onKeyDown={(event) => {
          if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
            commit();
          }
          if (event.key === "Escape") {
            onClose(false, false);
          }
        }}
      />
    );
  }

  return (
    <input
      aria-label={`Edit ${schemaColumn.name}`}
      className={styles.inlineEditorInput}
      autoFocus
      value={value}
      onChange={(event) => {
        updateValue(event.target.value);
      }}
      onBlur={commit}
      onKeyDown={(event) => {
        if (event.key === "Enter") {
          commit();
        }
        if (event.key === "Escape") {
          onClose(false, false);
        }
      }}
    />
  );
}

function BooleanCellCheckbox({
  checked,
  indeterminate,
  label,
  onToggle,
}: {
  checked: boolean;
  indeterminate: boolean;
  label: string;
  onToggle: (checked: boolean) => void;
}) {
  const checkboxRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (!checkboxRef.current) {
      return;
    }

    checkboxRef.current.indeterminate = indeterminate;
  }, [indeterminate]);

  return (
    <input
      ref={checkboxRef}
      type="checkbox"
      className={styles.booleanCellCheckbox}
      aria-label={label}
      checked={checked}
      onMouseDown={(event) => {
        event.stopPropagation();
      }}
      onClick={(event) => {
        event.stopPropagation();
      }}
      onDoubleClick={(event) => {
        event.stopPropagation();
      }}
      onChange={(event) => {
        event.stopPropagation();
        onToggle(event.target.checked);
      }}
    />
  );
}

function RelationCell({
  schema,
  relationTable,
  relationId,
  queryOptions,
}: {
  schema: Record<string, { columns: ColumnDescriptor[] }>;
  relationTable: string;
  relationId: string;
  queryOptions: { propagation: "full" | "local-only"; visibility: "hidden_from_live_query_list" };
}) {
  const queryBuilder = useMemo(
    () => new GenericQueryBuilder(relationTable, schema).where({ id: relationId }).limit(1),
    [relationId, relationTable, schema],
  );
  const relationRows = useAll<DynamicTableRow>(queryBuilder, queryOptions) ?? EMPTY_ROWS;
  const relationRow = relationRows[0];
  const displayColumn = useMemo(
    () => getRelationDisplayColumn(schema, relationTable),
    [relationTable, schema],
  );
  const displayValue =
    relationRow && displayColumn
      ? formatCellValue(relationRow[displayColumn.name])
      : formatCellValue(relationId);
  const href = buildRelationFilterHref(relationTable, relationId);

  return (
    <div className={styles.relationCell} title={`${relationTable}.${relationId}`}>
      <span className={styles.cellContent}>{displayValue}</span>
      <Link
        to={href}
        className={styles.relationLink}
        aria-label={`Open ${displayValue} in ${relationTable}`}
        onClick={(event) => {
          event.stopPropagation();
        }}
        onMouseDown={(event) => {
          event.stopPropagation();
        }}
      >
        <svg
          className={styles.relationLinkIcon}
          viewBox="0 0 16 16"
          aria-hidden="true"
          focusable="false"
        >
          <path
            d="M6 3h7v7h-1.5V5.56l-6.97 6.97-1.06-1.06 6.97-6.97H6V3zm-2 2H2.5v8h8V12H4V5z"
            fill="currentColor"
          />
        </svg>
      </Link>
    </div>
  );
}

function PlainTableView({
  rows,
  gridColumns,
  sorting,
  schema,
  queryOptions,
  schemaColumnById,
  queuedEdits,
  queuedDeletes,
  animationScopeKey,
  onSortColumnsChange,
  onQueuedEditsChange,
  onQueuedSaveErrorChange,
  onSelectedRowIdChange,
  onQueuedDeletesChange,
}: {
  rows: DynamicTableRow[];
  gridColumns: GridColumn[];
  sorting: readonly SortColumn[];
  schema: Record<string, { columns: ColumnDescriptor[] }>;
  queryOptions: { propagation: "full" | "local-only"; visibility: "hidden_from_live_query_list" };
  schemaColumnById: Map<string, ColumnDescriptor>;
  queuedEdits: Record<string, QueuedRowEdits>;
  queuedDeletes: Set<string>;
  animationScopeKey: string;
  onSortColumnsChange: (sortColumns: SortColumn[]) => void;
  onQueuedEditsChange: Dispatch<SetStateAction<Record<string, QueuedRowEdits>>>;
  onQueuedSaveErrorChange: (value: string | null) => void;
  onSelectedRowIdChange: (rowId: string | null) => void;
  onQueuedDeletesChange: Dispatch<SetStateAction<Set<string>>>;
}) {
  const suppressNextSelectedCellChangeRef = useRef(false);
  const pointerSidebarOpenTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const animatedRows = useAnimatedGridRows(rows, gridColumns, animationScopeKey);
  const clearPendingSidebarOpen = (): void => {
    const timeout = pointerSidebarOpenTimeoutRef.current;
    if (timeout) {
      clearTimeout(timeout);
      pointerSidebarOpenTimeoutRef.current = null;
    }
  };

  useEffect(() => {
    return () => {
      clearPendingSidebarOpen();
    };
  }, []);

  const editableRows = useMemo<EditableGridRow[]>(() => {
    return animatedRows.map((entry) => {
      const rowId = getGridRowId(entry.row);
      const rowQueuedEdits = queuedEdits[rowId];

      return {
        ...entry,
        sourceRow: entry.row,
        row: applyQueuedEditsToRow(entry.row, rowQueuedEdits),
        queuedEdits: rowQueuedEdits,
      };
    });
  }, [animatedRows, queuedEdits]);
  const rowClass = (row: EditableGridRow): string | undefined => {
    if (row.rowChangeState === "added") {
      return styles.rowAdded;
    }

    if (row.rowChangeState === "removed") {
      return styles.rowRemoved;
    }

    if (queuedDeletes.has(getGridRowId(row.sourceRow))) {
      return styles.rowQueuedDelete;
    }

    return undefined;
  };
  const renderers = useMemo<Renderers<EditableGridRow, unknown>>(
    () => ({
      renderCell(key, props) {
        const columnKey = String(props.column.key);
        return (
          <Cell
            key={key}
            {...props}
            data-cell-change-state={props.row.changedCellIds?.[columnKey] ? "updated" : undefined}
          />
        );
      },
      renderRow(key, props) {
        return <Row key={key} {...props} data-row-change-state={props.row.rowChangeState} />;
      },
      noRowsFallback: <div className={styles.emptyState}>No rows</div>,
    }),
    [],
  );
  const columns = useMemo<readonly Column<EditableGridRow>[]>(() => {
    const dataColumns = gridColumns.map((column): Column<EditableGridRow> => {
      const isIdColumn = column.id === "id";
      const schemaColumn = schemaColumnById.get(column.id);
      const isEditable =
        schemaColumn &&
        getFieldReadOnlyReason(schemaColumn) === null &&
        schemaColumn.column_type.type !== "Boolean";

      return {
        key: column.id,
        name: column.header,
        sortable: column.enableSorting,
        resizable: true,
        editable: isEditable,
        minWidth: isIdColumn ? 148 : 120,
        maxWidth: isIdColumn ? 220 : DATA_COLUMN_MAX_WIDTH,
        width: isIdColumn ? 180 : undefined,
        headerCellClass: column.enableSorting ? styles.sortableHeaderCell : styles.gridHeaderCell,
        cellClass: (row) =>
          row.changedCellIds?.[column.id]
            ? `${styles.dataGridCell} ${styles.cellUpdated}`
            : styles.dataGridCell,
        renderCell: ({ row }) => {
          const rawValue = row.row[column.accessorKey];
          const value = formatCellValue(rawValue);

          if (schemaColumn?.column_type.type === "Boolean") {
            const rowId = getGridRowId(row.sourceRow);
            return (
              <div className={styles.booleanCell}>
                <BooleanCellCheckbox
                  label={`Toggle ${column.accessorKey} for ${rowId}`}
                  checked={rawValue === true || rawValue === "true"}
                  indeterminate={schemaColumn.nullable && value.length === 0}
                  onToggle={(checked) => {
                    onQueuedSaveErrorChange(null);
                    onQueuedEditsChange((currentQueuedEdits) => {
                      const nextValueText = checked ? "true" : "false";
                      const sourceValueText = getQueuedEditText(row.sourceRow[column.accessorKey]);

                      if (nextValueText === sourceValueText) {
                        return removeQueuedCellEdit(currentQueuedEdits, rowId, column.accessorKey);
                      }

                      return {
                        ...currentQueuedEdits,
                        [rowId]: {
                          ...currentQueuedEdits[rowId],
                          [column.accessorKey]: { text: nextValueText },
                        },
                      };
                    });
                  }}
                />
              </div>
            );
          }

          if (
            schemaColumn?.references &&
            typeof rawValue === "string" &&
            rawValue.trim().length > 0
          ) {
            return (
              <RelationCell
                schema={schema}
                relationTable={schemaColumn.references}
                relationId={rawValue}
                queryOptions={queryOptions}
              />
            );
          }

          return (
            <div className={styles.cellContent} title={value}>
              {value}
            </div>
          );
        },
        renderEditCell:
          schemaColumn && isEditable
            ? (props) => <QueuedCellEditor {...props} schemaColumn={schemaColumn} />
            : undefined,
      };
    });

    return dataColumns;
  }, [
    gridColumns,
    onQueuedEditsChange,
    onQueuedSaveErrorChange,
    queryOptions,
    schema,
    schemaColumnById,
  ]);
  const handleRowsChange = (
    nextRows: EditableGridRow[],
    data: RowsChangeData<EditableGridRow>,
  ): void => {
    const columnId = String(data.column.key);

    onQueuedSaveErrorChange(null);
    onQueuedEditsChange((currentQueuedEdits) => {
      let nextQueuedEdits = currentQueuedEdits;

      for (const rowIndex of data.indexes) {
        const nextRow = nextRows[rowIndex];
        if (!nextRow) {
          continue;
        }

        const rowId = getGridRowId(nextRow.sourceRow);
        const nextValueText = getQueuedEditText(nextRow.row[columnId]);
        const sourceValueText = getQueuedEditText(nextRow.sourceRow[columnId]);

        if (nextValueText === sourceValueText) {
          nextQueuedEdits = removeQueuedCellEdit(nextQueuedEdits, rowId, columnId);
          continue;
        }

        nextQueuedEdits = {
          ...nextQueuedEdits,
          [rowId]: {
            ...nextQueuedEdits[rowId],
            [columnId]: { text: nextValueText },
          },
        };
      }

      return nextQueuedEdits;
    });
  };

  return (
    <DataGrid
      className={`${styles.dataGrid} rdg-dark`}
      columns={columns}
      rows={editableRows}
      rowKeyGetter={(row) => getGridRowId(row.sourceRow)}
      sortColumns={sorting}
      onSortColumnsChange={onSortColumnsChange}
      onRowsChange={handleRowsChange}
      onCellMouseDown={() => {
        suppressNextSelectedCellChangeRef.current = true;
        clearPendingSidebarOpen();
      }}
      onCellKeyDown={(args, event) => {
        if (args.mode === "EDIT") {
          return;
        }

        if (event.key === "Backspace" || event.key === "Delete") {
          const rowId = args.row ? getGridRowId(args.row.sourceRow) : null;
          if (rowId) {
            event.preventGridDefault();
            onQueuedDeletesChange((current) => {
              const next = new Set(current);
              if (next.has(rowId)) {
                next.delete(rowId);
              } else {
                next.add(rowId);
              }
              return next;
            });
          }
        }
      }}
      onCellClick={(args, event) => {
        clearPendingSidebarOpen();
        const rowId = args.row ? getGridRowId(args.row.sourceRow) : null;
        if (rowId === null) {
          onSelectedRowIdChange(null);
          return;
        }

        if (event.detail > 1) {
          return;
        }

        pointerSidebarOpenTimeoutRef.current = setTimeout(() => {
          pointerSidebarOpenTimeoutRef.current = null;
          onSelectedRowIdChange(rowId);
        }, POINTER_SIDEBAR_OPEN_DELAY_MS);
      }}
      onSelectedCellChange={(args) => {
        if (suppressNextSelectedCellChangeRef.current) {
          suppressNextSelectedCellChangeRef.current = false;
          return;
        }

        clearPendingSidebarOpen();
        onSelectedRowIdChange(args.row ? getGridRowId(args.row.sourceRow) : null);
      }}
      onCellDoubleClick={(args, event) => {
        clearPendingSidebarOpen();
        const schemaColumn = schemaColumnById.get(String(args.column.key));
        if (!schemaColumn || getFieldReadOnlyReason(schemaColumn) !== null) {
          event.preventGridDefault();
          return;
        }

        suppressNextSelectedCellChangeRef.current = true;
        args.selectCell(true);
        event.preventGridDefault();
      }}
      rowClass={rowClass}
      renderers={renderers}
      rowHeight={38}
      headerRowHeight={40}
      enableVirtualization={false}
    />
  );
}

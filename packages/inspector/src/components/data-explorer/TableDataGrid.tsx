import {
  flexRender,
  getCoreRowModel,
  type ColumnDef,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table";
import type { ColumnType, DynamicTableRow, TableProxy } from "jazz-tools";
import { useAll, useDb } from "jazz-tools/react";
import { Profiler, useEffect, useMemo, useState } from "react";
import { Navigate, useParams } from "react-router";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import { GenericQueryBuilder } from "../../utility/generic-query-builder.js";
import { RowMutationSidebar } from "./RowMutationSidebar.js";
import { TableFilterBuilder, type TableFilterClause } from "./TableFilterBuilder.js";
import type { MutationFormMode } from "./row-mutation-form.js";
import styles from "./TableDataGrid.module.css";

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

const PAGE_SIZE_OPTIONS = [10, 25, 50] as const;
const EMPTY_ROWS: DynamicTableRow[] = [];

interface MutationState {
  mode: MutationFormMode;
  rowId: string | null;
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

export function TableDataGrid() {
  const { table } = useParams();

  if (!table) {
    return <Navigate to="/data-explorer" replace />;
  }

  const { wasmSchema: schema, queryPropagation, runtime } = useDevtoolsContext();
  const db = useDb();
  const [sorting, setSorting] = useState<SortingState>([{ id: "id", desc: false }]);
  const [pageSize, setPageSize] = useState<number>(PAGE_SIZE_OPTIONS[0]);
  const [pageIndex, setPageIndex] = useState(0);
  const [filters, setFilters] = useState<TableFilterClause[]>([]);
  const [mutationState, setMutationState] = useState<MutationState | null>(null);
  const [isSidebarMutationPending, setIsSidebarMutationPending] = useState(false);
  const [deletingRowId, setDeletingRowId] = useState<string | null>(null);
  const schemaColumns = schema[table]?.columns ?? [];
  const activeSort = sorting[0] ?? { id: "id", desc: false };
  const sortColumn = activeSort.id;
  const sortDirection = activeSort.desc ? "desc" : "asc";
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
  const rowById = useMemo(() => {
    return new Map(visibleRows.map((row) => [row.id, row]));
  }, [visibleRows]);
  const editingRow =
    mutationState?.mode === "edit" && mutationState.rowId
      ? (rowById.get(mutationState.rowId) ?? null)
      : null;
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
  const handleSort = (columnId: string, canSort: boolean): void => {
    if (!canSort) {
      return;
    }
    const nextSort =
      sortColumn === columnId && sortDirection === "asc"
        ? [{ id: columnId, desc: true }]
        : [{ id: columnId, desc: false }];
    recordGridEvent("sort-click", {
      columnId,
      nextSortDirection: nextSort[0]?.desc ? "desc" : "asc",
    });
    setSorting(nextSort);
    setPageIndex(0);
  };
  const handleDeleteRow = async (rowId: string): Promise<void> => {
    const confirmed = globalThis.confirm(`Delete row "${rowId}" from "${table}"?`);
    if (!confirmed) return;

    try {
      setDeletingRowId(rowId);
      await db.deleteDurable(tableProxy, rowId, {
        tier: mutationDurabilityTier,
      });
      if (mutationState?.mode === "edit" && mutationState.rowId === rowId) {
        setMutationState(null);
      }
    } finally {
      setDeletingRowId(null);
    }
  };
  const handleEditRow = (rowId: string): void => {
    setMutationState({ mode: "edit", rowId });
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
          <div>
            <h2 className={styles.title}>{table}</h2>
            <p className={styles.stats}>
              {gridColumns.length} column{gridColumns.length === 1 ? "" : "s"} ·{" "}
              {visibleRows.length} row{visibleRows.length === 1 ? "" : "s"} on page ·{" "}
              {filters.length} filter{filters.length === 1 ? "" : "s"}
            </p>
          </div>
          <button
            type="button"
            className={styles.secondaryButton}
            onClick={() => {
              setMutationState({ mode: "insert", rowId: null });
            }}
            disabled={isSidebarMutationPending || deletingRowId !== null}
          >
            Insert
          </button>
        </header>
        <TableFilterBuilder
          schemaColumns={schemaColumns}
          clauses={filters}
          onClausesChange={(nextFilters) => {
            recordGridEvent("filters-change", { nextFilterCount: nextFilters.length });
            setFilters(nextFilters);
            setPageIndex(0);
          }}
        />
        <div className={styles.contentArea}>
          <div className={styles.gridFrame}>
            <TanStackTableView
              rows={rows}
              gridColumns={gridColumns}
              pageSize={pageSize}
              sorting={sorting}
              onSortingChange={setSorting}
              isSidebarMutationPending={isSidebarMutationPending}
              deletingRowId={deletingRowId}
              onSort={handleSort}
              onEditRow={handleEditRow}
              onDeleteRow={handleDeleteRow}
            />
          </div>
        </div>
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
                  setPageIndex(0);
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
                key={`${mutationState.mode}:${mutationState.rowId ?? "new"}`}
                mode={mutationState.mode}
                tableName={table}
                schemaColumns={schemaColumns}
                targetRowId={mutationState.mode === "edit" ? (editingRow?.id ?? null) : null}
                rowValues={mutationState.mode === "edit" ? editingRow : insertRowValues}
                onCancel={() => {
                  if (isSidebarMutationPending) return;
                  setMutationState(null);
                }}
                onSave={async (updates) => {
                  try {
                    setIsSidebarMutationPending(true);
                    if (mutationState.mode === "edit") {
                      if (!editingRow) return;
                      await db.updateDurable(tableProxy, editingRow.id, updates, {
                        tier: mutationDurabilityTier,
                      });
                    } else {
                      await db.insertDurable(tableProxy, updates, {
                        tier: mutationDurabilityTier,
                      });
                    }
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

function TanStackTableView({
  rows,
  gridColumns,
  pageSize,
  sorting,
  onSortingChange,
  isSidebarMutationPending,
  deletingRowId,
  onSort,
  onEditRow,
  onDeleteRow,
}: {
  rows: DynamicTableRow[];
  gridColumns: GridColumn[];
  pageSize: number;
  sorting: SortingState;
  onSortingChange: (updater: SortingState) => void;
  isSidebarMutationPending: boolean;
  deletingRowId: string | null;
  onSort: (columnId: string, canSort: boolean) => void;
  onEditRow: (rowId: string) => void;
  onDeleteRow: (rowId: string) => Promise<void>;
}) {
  const columns = useMemo<ColumnDef<DynamicTableRow>[]>(
    () =>
      gridColumns.map(
        (column): ColumnDef<DynamicTableRow> => ({
          id: column.id,
          accessorKey: column.accessorKey,
          header: column.header,
          enableSorting: column.enableSorting,
          cell: (cellContext) => formatCellValue(cellContext.getValue()),
        }),
      ),
    [gridColumns],
  );
  const tableState = useReactTable({
    data: rows,
    columns,
    state: { sorting },
    onSortingChange,
    getCoreRowModel: getCoreRowModel(),
  });

  return (
    <table className={styles.table}>
      <thead>
        {tableState.getHeaderGroups().map((headerGroup) => (
          <tr key={headerGroup.id}>
            {headerGroup.headers.map((header) => {
              const headerSortDirection = header.column.getIsSorted();
              const canSort = header.column.getCanSort();
              return (
                <th
                  key={header.id}
                  onClick={canSort ? () => onSort(header.column.id, canSort) : undefined}
                  className={canSort ? styles.sortableHeader : styles.headerCell}
                >
                  {header.isPlaceholder
                    ? null
                    : flexRender(header.column.columnDef.header, header.getContext())}
                  {headerSortDirection === "asc"
                    ? " ↑"
                    : headerSortDirection === "desc"
                      ? " ↓"
                      : ""}
                </th>
              );
            })}
            <th className={styles.actionsHeader}>Actions</th>
          </tr>
        ))}
      </thead>
      <tbody>
        {tableState
          .getRowModel()
          .rows.slice(0, pageSize)
          .map((row) => (
            <tr key={row.id}>
              {row.getVisibleCells().map((cell) => (
                <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>
              ))}
              <RowActionsCell
                rowId={String(row.original.id)}
                isSidebarMutationPending={isSidebarMutationPending}
                deletingRowId={deletingRowId}
                onEditRow={onEditRow}
                onDeleteRow={onDeleteRow}
              />
            </tr>
          ))}
      </tbody>
    </table>
  );
}

function RowActionsCell({
  rowId,
  isSidebarMutationPending,
  deletingRowId,
  onEditRow,
  onDeleteRow,
}: {
  rowId: string;
  isSidebarMutationPending: boolean;
  deletingRowId: string | null;
  onEditRow: (rowId: string) => void;
  onDeleteRow: (rowId: string) => Promise<void>;
}) {
  return (
    <td className={styles.actionsCell}>
      <div className={styles.rowActions}>
        <button
          type="button"
          className={styles.actionButton}
          disabled={isSidebarMutationPending || deletingRowId !== null}
          onClick={() => {
            onEditRow(rowId);
          }}
        >
          Edit
        </button>
        <button
          type="button"
          className={styles.dangerActionButton}
          disabled={isSidebarMutationPending || deletingRowId !== null}
          onClick={async () => {
            await onDeleteRow(rowId);
          }}
        >
          {deletingRowId === rowId ? "Deleting..." : "Delete"}
        </button>
      </div>
    </td>
  );
}

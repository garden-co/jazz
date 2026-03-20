import {
  flexRender,
  getCoreRowModel,
  type ColumnDef,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table";
import type { ColumnType, DynamicTableRow, TableProxy } from "jazz-tools";
import { useAll, useDb } from "jazz-tools/react";
import { useMemo, useState } from "react";
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

interface MutationState {
  mode: MutationFormMode;
  rowId: string | null;
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
  const mutationDurabilityTier = runtime === "standalone" ? "edge" : "worker";

  const rows =
    useAll<DynamicTableRow>(queryBuilder, {
      propagation: queryPropagation,
      visibility: "hidden_from_live_query_list",
    }) ?? [];

  const columnDefs = useMemo<ColumnDef<DynamicTableRow>[]>(
    () => [
      {
        id: "id",
        accessorKey: "id",
        header: "ID",
        cell: (cellContext) => formatCellValue(cellContext.getValue()),
      },
      ...schemaColumns.map(
        (column): ColumnDef<DynamicTableRow> => ({
          id: column.name,
          accessorKey: column.name,
          header: column.name,
          enableSorting: isColumnSortable(column.column_type),
          cell: (cellContext) => formatCellValue(cellContext.getValue()),
        }),
      ),
    ],
    [schemaColumns],
  );

  const tableState = useReactTable({
    data: rows,
    columns: columnDefs,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
  });

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

  return (
    <section className={styles.container}>
      <header className={styles.header}>
        <div>
          <h2 className={styles.title}>{table}</h2>
          <p className={styles.stats}>
            {columnDefs.length} column{columnDefs.length === 1 ? "" : "s"} · {visibleRows.length}{" "}
            row{visibleRows.length === 1 ? "" : "s"} on page · {filters.length} filter
            {filters.length === 1 ? "" : "s"}
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
          setFilters(nextFilters);
          setPageIndex(0);
        }}
      />
      <div className={styles.contentArea}>
        <div className={styles.gridFrame}>
          <table className={styles.table}>
            <thead>
              {tableState.getHeaderGroups().map((headerGroup) => (
                <tr key={headerGroup.id}>
                  {headerGroup.headers.map((header) => {
                    const sortDirection = header.column.getIsSorted();
                    const canSort = header.column.getCanSort();
                    return (
                      <th
                        key={header.id}
                        onClick={
                          canSort
                            ? () => {
                                const nextSort =
                                  sortDirection === "asc"
                                    ? [{ id: header.column.id, desc: true }]
                                    : [{ id: header.column.id, desc: false }];
                                setSorting(nextSort);
                                setPageIndex(0);
                              }
                            : undefined
                        }
                        className={canSort ? styles.sortableHeader : styles.headerCell}
                      >
                        {header.isPlaceholder
                          ? null
                          : flexRender(header.column.columnDef.header, header.getContext())}
                        {sortDirection === "asc" ? " ↑" : sortDirection === "desc" ? " ↓" : ""}
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
                      <td key={cell.id}>
                        {flexRender(cell.column.columnDef.cell, cell.getContext())}
                      </td>
                    ))}
                    <td className={styles.actionsCell}>
                      <div className={styles.rowActions}>
                        <button
                          type="button"
                          className={styles.actionButton}
                          disabled={isSidebarMutationPending || deletingRowId !== null}
                          onClick={() => {
                            setMutationState({ mode: "edit", rowId: String(row.original.id) });
                          }}
                        >
                          Edit
                        </button>
                        <button
                          type="button"
                          className={styles.dangerActionButton}
                          disabled={isSidebarMutationPending || deletingRowId !== null}
                          onClick={async () => {
                            const rowId = String(row.original.id);
                            const confirmed = globalThis.confirm(
                              `Delete row "${rowId}" from "${table}"?`,
                            );
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
                          }}
                        >
                          {deletingRowId === String(row.original.id) ? "Deleting..." : "Delete"}
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
            </tbody>
          </table>
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
                setPageSize(Number(event.target.value));
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
            onClick={() => setPageIndex((current) => Math.max(0, current - 1))}
            disabled={pageIndex === 0}
          >
            Previous
          </button>
          <button
            type="button"
            className={styles.secondaryButton}
            onClick={() => setPageIndex((current) => current + 1)}
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
  );
}

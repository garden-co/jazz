import {
  flexRender,
  getCoreRowModel,
  type ColumnDef,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table";
import type { ColumnType, DynamicTableRow } from "jazz-tools";
import { useDb } from "jazz-tools/react";
import { useEffect, useMemo, useState } from "react";
import { Navigate, useParams } from "react-router";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import { GenericQueryBuilder } from "../../utility/generic-query-builder.js";
import styles from "./TableDataGrid.module.css";

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

const PAGE_SIZE_OPTIONS = [10, 25, 50] as const;

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

  const { wasmSchema: schema, queryPropagation } = useDevtoolsContext();
  const db = useDb();
  const [rows, setRows] = useState<DynamicTableRow[]>([]);
  const [sorting, setSorting] = useState<SortingState>([{ id: "id", desc: false }]);
  const [pageSize, setPageSize] = useState<number>(PAGE_SIZE_OPTIONS[0]);
  const [pageIndex, setPageIndex] = useState(0);
  const schemaColumns = schema[table]?.columns ?? [];
  const activeSort = sorting[0] ?? { id: "id", desc: false };
  const sortColumn = activeSort.id;
  const sortDirection = activeSort.desc ? "desc" : "asc";
  const queryOffset = pageIndex * pageSize;
  const queryLimit = pageSize + 1;
  const queryBuilder = useMemo(
    () =>
      new GenericQueryBuilder(table, schema)
        .orderBy(sortColumn, sortDirection)
        .limit(queryLimit)
        .offset(queryOffset),
    [table, schema, sortColumn, sortDirection, queryLimit, queryOffset],
  );

  useEffect(() => {
    const unsubscribe = db.subscribeAll(
      queryBuilder,
      (delta) => {
        setRows(delta.all);
      },
      { propagation: queryPropagation },
    );

    return () => {
      unsubscribe();
    };
  }, [db, queryBuilder, queryPropagation]);

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
  const startRow = pageIndex * pageSize;
  const endRow = startRow + visibleRows.length;

  return (
    <section className={styles.container}>
      <header className={styles.header}>
        <div>
          <h2 className={styles.title}>{table}</h2>
          <p className={styles.stats}>
            {columnDefs.length} column{columnDefs.length === 1 ? "" : "s"} · {visibleRows.length}{" "}
            row{visibleRows.length === 1 ? "" : "s"} on page
          </p>
        </div>
      </header>
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
                </tr>
              ))}
          </tbody>
        </table>
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
    </section>
  );
}

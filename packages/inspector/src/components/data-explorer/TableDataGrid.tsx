import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  type ColumnDef,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table";
import { allRowsInTableQuery, type DynamicTableRow } from "jazz-tools";
import { useAll } from "jazz-tools/react";
import { useEffect, useMemo, useState } from "react";
import { Navigate, useParams } from "react-router";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import styles from "./TableDataGrid.module.css";

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

const PAGE_SIZE_OPTIONS = [10, 25, 50] as const;

export function TableDataGrid() {
  const { table } = useParams();

  if (!table) {
    return <Navigate to="/data-explorer" replace />;
  }

  const schema = useDevtoolsContext().wasmSchema;
  const queryBuilder = useMemo(
    () => allRowsInTableQuery<DynamicTableRow>(table, schema),
    [table, schema],
  );
  const rows = useAll(queryBuilder) ?? [];
  const [sorting, setSorting] = useState<SortingState>([]);
  const [pageSize, setPageSize] = useState<number>(PAGE_SIZE_OPTIONS[0]);
  const [pageIndex, setPageIndex] = useState(0);

  const schemaColumns = schema[table]?.columns ?? [];

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
    getSortedRowModel: getSortedRowModel(),
  });

  const sortedRows = tableState.getRowModel().rows;
  const totalRows = sortedRows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  const clampedPageIndex = Math.min(pageIndex, totalPages - 1);
  const startRow = clampedPageIndex * pageSize;
  const endRow = startRow + pageSize;
  const paginatedRows = sortedRows.slice(startRow, endRow);

  useEffect(() => {
    if (pageIndex !== clampedPageIndex) {
      setPageIndex(clampedPageIndex);
    }
  }, [pageIndex, clampedPageIndex]);

  return (
    <section className={styles.container}>
      <header className={styles.header}>
        <div>
          <h2 className={styles.title}>{table}</h2>
          <p className={styles.stats}>
            {columnDefs.length} column{columnDefs.length === 1 ? "" : "s"} · {rows.length} row
            {rows.length === 1 ? "" : "s"}
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
                  return (
                    <th
                      key={header.id}
                      onClick={header.column.getToggleSortingHandler()}
                      className={
                        header.column.getCanSort() ? styles.sortableHeader : styles.headerCell
                      }
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
            {paginatedRows.map((row) => (
              <tr key={row.id}>
                {row.getVisibleCells().map((cell) => (
                  <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <footer className={styles.footer}>
        <div className={styles.paginationInfo}>
          Showing {totalRows === 0 ? 0 : startRow + 1}-{Math.min(endRow, totalRows)} of {totalRows}
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
          <span className={styles.pageIndicator}>
            Page {clampedPageIndex + 1} / {totalPages}
          </span>
          <button
            type="button"
            className={styles.secondaryButton}
            onClick={() => setPageIndex((current) => Math.max(0, current - 1))}
            disabled={clampedPageIndex === 0}
          >
            Previous
          </button>
          <button
            type="button"
            className={styles.secondaryButton}
            onClick={() => setPageIndex((current) => Math.min(totalPages - 1, current + 1))}
            disabled={clampedPageIndex >= totalPages - 1}
          >
            Next
          </button>
        </div>
      </footer>
    </section>
  );
}

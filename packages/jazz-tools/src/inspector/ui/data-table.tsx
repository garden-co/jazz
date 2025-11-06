import { useEffect, useMemo, useState } from "react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "./table";
import { Button } from "./button";
import { Input } from "./input";

export type ColumnDef<T> = {
  id: string;
  header: string;
  accessor: (row: T) => React.ReactNode;
  sortable?: boolean;
  filterable?: boolean;
  sortFn?: (a: T, b: T) => number;
  filterFn?: (row: T, filterValue: string) => boolean;
};

export type SortConfig = {
  columnId: string;
  direction: "asc" | "desc";
} | null;

export type DataTableProps<T> = {
  columns: ColumnDef<T>[];
  data: T[];
  pageSize?: number;
  initialSort?: SortConfig;
  getRowKey: (row: T, index: number) => string;
  emptyMessage?: string;
};

export function DataTable<T>({
  columns,
  data,
  pageSize = 10,
  initialSort = null,
  getRowKey,
  emptyMessage = "No data available",
}: DataTableProps<T>) {
  const [currentPage, setCurrentPage] = useState(1);
  const [sortConfig, setSortConfig] = useState<SortConfig>(initialSort);
  const [filters, setFilters] = useState<Record<string, string>>({});

  // Apply filtering
  const filteredData = useMemo(() => {
    return data.filter((row) => {
      return Object.entries(filters).every(([columnId, filterValue]) => {
        if (!filterValue) return true;

        const column = columns.find((col) => col.id === columnId);
        if (!column?.filterable) return true;

        if (column.filterFn) {
          return column.filterFn(row, filterValue);
        }

        // Default filter: convert to string and check inclusion
        const cellValue = String(column.accessor(row));
        return cellValue.toLowerCase().includes(filterValue.toLowerCase());
      });
    });
  }, [data, filters, columns]);

  // Apply sorting
  const sortedData = useMemo(() => {
    if (!sortConfig) return filteredData;

    const column = columns.find((col) => col.id === sortConfig.columnId);
    if (!column?.sortable) return filteredData;

    const sorted = [...filteredData].sort((a, b) => {
      if (column.sortFn) {
        return column.sortFn(a, b);
      }

      // Default sort: compare string values
      const aValue = String(column.accessor(a));
      const bValue = String(column.accessor(b));
      return aValue.localeCompare(bValue);
    });

    return sortConfig.direction === "desc" ? sorted.reverse() : sorted;
  }, [filteredData, sortConfig, columns]);

  // Calculate pagination
  const totalPages = Math.ceil(sortedData.length / pageSize);
  const showPagination = sortedData.length > pageSize;
  const startIndex = (currentPage - 1) * pageSize;
  const endIndex = startIndex + pageSize;
  const paginatedData = sortedData.slice(startIndex, endIndex);

  // Reset to page 1 when filters change
  useEffect(() => {
    setCurrentPage(1);
  }, [filters]);

  const handleSort = (columnId: string) => {
    const column = columns.find((col) => col.id === columnId);
    if (!column?.sortable) return;

    setSortConfig((current) => {
      if (current?.columnId === columnId) {
        if (current.direction === "asc") {
          return { columnId, direction: "desc" };
        }
        return null; // Remove sorting
      }
      return { columnId, direction: "asc" };
    });
  };

  const handleFilterChange = (columnId: string, value: string) => {
    setFilters((current) => ({
      ...current,
      [columnId]: value,
    }));
  };

  const handlePageChange = (page: number) => {
    setCurrentPage(Math.max(1, Math.min(page, totalPages)));
  };

  return (
    <>
      <Table>
        <TableHead>
          <TableRow>
            {columns.map((column) => (
              <TableHeader key={column.id}>
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "8px",
                    cursor: column.sortable ? "pointer" : "default",
                  }}
                  onClick={() => handleSort(column.id)}
                >
                  <span>{column.header}</span>
                  {column.sortable && (
                    <span
                      style={{
                        fontSize: "12px",
                        opacity: 0.7,
                      }}
                    >
                      {sortConfig?.columnId === column.id
                        ? sortConfig.direction === "asc"
                          ? "↑"
                          : "↓"
                        : "↕"}
                    </span>
                  )}
                </div>
              </TableHeader>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {columns.some((column) => column.filterable) && (
            <TableRow>
              {columns.map((column) => (
                <TableCell key={column.id}>
                  {column.filterable && (
                    <Input
                      label="Filter"
                      hideLabel
                      type="search"
                      placeholder={`Filter ${column.header.toLowerCase()}`}
                      value={filters[column.id] || ""}
                      onChange={(e) =>
                        handleFilterChange(column.id, e.target.value)
                      }
                      onClick={(e) => e.stopPropagation()}
                    />
                  )}
                </TableCell>
              ))}
            </TableRow>
          )}
          {paginatedData.length === 0 ? (
            <TableRow>
              <TableCell colSpan={columns.length}>
                <div
                  style={{
                    textAlign: "center",
                    padding: "20px",
                    opacity: 0.6,
                  }}
                >
                  {emptyMessage}
                </div>
              </TableCell>
            </TableRow>
          ) : (
            paginatedData.map((row, index) => (
              <TableRow key={getRowKey(row, startIndex + index)}>
                {columns.map((column) => (
                  <TableCell key={column.id}>{column.accessor(row)}</TableCell>
                ))}
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>

      {showPagination && (
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginTop: "16px",
            padding: "8px 0",
          }}
        >
          <div style={{ fontSize: "14px", opacity: 0.7 }}>
            Showing {startIndex + 1} to {Math.min(endIndex, sortedData.length)}{" "}
            of {sortedData.length} entries
            {Object.keys(filters).some((key) => filters[key]) &&
              ` (filtered from ${data.length})`}
          </div>
          <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
            <Button
              variant="secondary"
              onClick={() => handlePageChange(1)}
              disabled={currentPage === 1}
            >
              ««
            </Button>
            <Button
              variant="secondary"
              onClick={() => handlePageChange(currentPage - 1)}
              disabled={currentPage === 1}
            >
              «
            </Button>
            <span style={{ fontSize: "14px" }}>
              Page {currentPage} of {totalPages}
            </span>
            <Button
              variant="secondary"
              onClick={() => handlePageChange(currentPage + 1)}
              disabled={currentPage === totalPages}
            >
              »
            </Button>
            <Button
              variant="secondary"
              onClick={() => handlePageChange(totalPages)}
              disabled={currentPage === totalPages}
            >
              »»
            </Button>
          </div>
        </div>
      )}
    </>
  );
}

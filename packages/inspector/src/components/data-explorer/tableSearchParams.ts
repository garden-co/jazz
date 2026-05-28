import { useNavigate, useSearch } from "@tanstack/react-router";
import type { SortColumn } from "react-data-grid";

import { appRoutes } from "#lib/navigation/appRoutes.ts";
import type { TableFilterClause } from "#data-explorer/TableFilterBuilder.tsx";
import { WHERE_OPERATORS, type WhereOperator } from "../../utility/where-operators.ts";

export const PAGE_SIZE_OPTIONS = [10, 25, 50] as const;
export const DEFAULT_PAGE_SIZE = 25;
const WHERE_OPERATOR_SET: ReadonlySet<string> = new Set(WHERE_OPERATORS);
const DEFAULT_SORT: SortColumn = { columnKey: "id", direction: "ASC" };

export interface TableDataSearch {
  dir?: "ASC" | "DESC";
  filters?: TableFilterClause[];
  page?: number;
  pageSize?: (typeof PAGE_SIZE_OPTIONS)[number];
  sort?: string;
}

export function validateTableDataSearch(search: Record<string, unknown>): TableDataSearch {
  return compactTableDataSearch({
    dir: search.dir === "DESC" ? "DESC" : search.dir === "ASC" ? "ASC" : undefined,
    filters: parseFilters(search.filters),
    page: parsePage(search.page),
    pageSize: parsePageSize(search.pageSize),
    sort: typeof search.sort === "string" && search.sort.length > 0 ? search.sort : undefined,
  });
}

export function buildRelationFilterSearch(id: string): TableDataSearch {
  return {
    filters: [{ id: `relation-id-${id}`, column: "id", operator: "eq", value: id }],
  };
}

export function useTableExplorerSearchParams() {
  const navigate = useNavigate({ from: appRoutes.tableData });
  const search = useSearch({ from: appRoutes.tableData });
  const sorting: readonly SortColumn[] = [
    { columnKey: search.sort ?? "id", direction: search.dir ?? "ASC" },
  ];

  const setSearch = (
    updater: (currentSearch: TableDataSearch) => TableDataSearch,
    options: { replace?: boolean } = {},
  ) => {
    void navigate({
      replace: options.replace ?? true,
      search: (currentSearch) =>
        compactTableDataSearch(
          updater(validateTableDataSearch(currentSearch as Record<string, unknown>)),
        ),
    });
  };

  return {
    filters: search.filters ?? [],
    pageIndex: search.page ?? 0,
    pageSize: search.pageSize ?? DEFAULT_PAGE_SIZE,
    setFilters: (filters: TableFilterClause[]) => {
      setSearch((currentSearch) => ({ ...currentSearch, filters, page: undefined }));
    },
    setPageIndex: (next: number | ((current: number) => number)) => {
      setSearch((currentSearch) => {
        const currentPage = currentSearch.page ?? 0;
        const page = typeof next === "function" ? next(currentPage) : next;
        return { ...currentSearch, page: Math.max(0, page) };
      });
    },
    setPageSize: (pageSize: number) => {
      setSearch((currentSearch) => ({
        ...currentSearch,
        page: undefined,
        pageSize: parsePageSize(pageSize) ?? DEFAULT_PAGE_SIZE,
      }));
    },
    setSorting: (nextSortColumns: SortColumn[]) => {
      const nextSort = nextSortColumns.at(-1) ?? DEFAULT_SORT;
      setSearch((currentSearch) => ({
        ...currentSearch,
        dir: nextSort.direction,
        page: undefined,
        sort: nextSort.columnKey,
      }));
    },
    sorting,
  };
}

function compactTableDataSearch(search: TableDataSearch): TableDataSearch {
  const nextSearch: TableDataSearch = {};
  if (search.sort !== undefined && (search.sort !== "id" || search.dir !== "ASC")) {
    nextSearch.sort = search.sort;
    nextSearch.dir = search.dir ?? "ASC";
  }
  if (search.pageSize !== undefined && search.pageSize !== DEFAULT_PAGE_SIZE) {
    nextSearch.pageSize = search.pageSize;
  }
  if (search.page !== undefined && search.page > 0) {
    nextSearch.page = search.page;
  }
  if (search.filters !== undefined && search.filters.length > 0) {
    nextSearch.filters = search.filters;
  }
  return nextSearch;
}

function parseFilters(value: unknown): TableFilterClause[] | undefined {
  const parsed = typeof value === "string" ? parseJson(value) : value;
  if (Array.isArray(parsed) === false) {
    return undefined;
  }

  const filters = parsed.filter(isTableFilterClause);
  return filters.length > 0 ? filters : undefined;
}

function parseJson(value: string): unknown {
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return undefined;
  }
}

function isTableFilterClause(value: unknown): value is TableFilterClause {
  if (typeof value !== "object" || value === null) {
    return false;
  }

  const candidate = value as TableFilterClause;
  return (
    typeof candidate.id === "string" &&
    typeof candidate.column === "string" &&
    isWhereOperator(candidate.operator) &&
    "value" in candidate
  );
}

function isWhereOperator(value: unknown): value is WhereOperator {
  return typeof value === "string" && WHERE_OPERATOR_SET.has(value);
}

function parsePage(value: unknown): number | undefined {
  const page = typeof value === "number" ? value : typeof value === "string" ? Number(value) : NaN;
  return Number.isInteger(page) && page > 0 ? page : undefined;
}

function parsePageSize(value: unknown): (typeof PAGE_SIZE_OPTIONS)[number] | undefined {
  const pageSize =
    typeof value === "number" ? value : typeof value === "string" ? Number(value) : NaN;
  return PAGE_SIZE_OPTIONS.includes(pageSize as (typeof PAGE_SIZE_OPTIONS)[number])
    ? (pageSize as (typeof PAGE_SIZE_OPTIONS)[number])
    : undefined;
}

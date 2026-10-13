"use client";

import { useState } from "react";
import {
  Badge,
  HStack,
  Pagination,
  Selector,
  TextInput,
  useTableSortable,
  type TablePlugin,
  type TableSortState,
} from "@astryxdesign/core";

/**
 * Search, sort and page state for a table backed by a bounded, ordered Jazz
 * query. Each page asks for one extra row to learn whether a next page exists,
 * so no count query is needed.
 */
export function useListControls<K extends string>(
  defaultSort: K,
  defaultDirection: "ascending" | "descending" = "ascending",
  pageSize = 20,
) {
  const [search, setSearchValue] = useState("");
  const [sort, setSortState] = useState<TableSortState<K>>([
    { sortKey: defaultSort, direction: defaultDirection },
  ]);
  const [page, setPage] = useState(1);
  const sortPlugin = useTableSortable<Record<string, unknown>, K>({
    sort,
    onSortChange: (next) => {
      setSortState(next);
      setPage(1);
    },
    allowUnsortedState: false,
  });
  const active = sort[0] ?? { sortKey: defaultSort, direction: defaultDirection };
  return {
    search,
    setSearch: (value: string) => {
      setSearchValue(value);
      setPage(1);
    },
    /** Lower-cased search text, matched against each row's `searchKey`. */
    searchWhere: search.trim() ? { searchKey: { contains: search.trim().toLowerCase() } } : {},
    sortKey: active.sortKey,
    direction: active.direction === "ascending" ? ("asc" as const) : ("desc" as const),
    page,
    setPage,
    pageSize,
    limit: pageSize + 1,
    offset: (page - 1) * pageSize,
    // The sort plugin only reads column keys, so it fits any row type.
    plugins: { sort: sortPlugin as TablePlugin<any> },
    resetPage: () => setPage(1),
  };
}

export function pageOf<T>(rows: T[] | undefined, pageSize: number) {
  return { rows: rows?.slice(0, pageSize) ?? [], hasMore: (rows?.length ?? 0) > pageSize };
}

/** Search field and an optional status filter above a table. */
export function ListToolbar({
  searchLabel,
  search,
  onSearch,
  statuses,
  status,
  onStatus,
}: {
  searchLabel: string;
  search: string;
  onSearch: (value: string) => void;
  statuses?: readonly string[];
  status?: string | null;
  onStatus?: (value: string | null) => void;
}) {
  return (
    <HStack gap={2} wrap="wrap" vAlign="end">
      <TextInput
        label={searchLabel}
        isLabelHidden
        placeholder={searchLabel}
        startIcon="search"
        value={search}
        onChange={onSearch}
        hasClear
        width={280}
      />
      {statuses && onStatus && (
        <Selector
          label="Status"
          isLabelHidden
          placeholder="Any status"
          options={statuses.map((value) => ({ value, label: sentence(value) }))}
          value={status ?? null}
          onChange={onStatus}
          hasClear
          width={180}
        />
      )}
    </HStack>
  );
}

export function ListPagination({
  page,
  onChange,
  hasMore,
}: {
  page: number;
  onChange: (page: number) => void;
  hasMore: boolean;
}) {
  if (page === 1 && !hasMore) return null;
  return <Pagination page={page} onChange={onChange} hasMore={hasMore} variant="compact" />;
}

const statusVariants: Record<string, "success" | "info" | "neutral" | "warning"> = {
  active: "success",
  developing: "info",
  "on hiatus": "warning",
  planning: "neutral",
  scheduled: "info",
  released: "success",
};

export function StatusBadge({ status }: { status: string }) {
  return <Badge variant={statusVariants[status] ?? "neutral"} label={sentence(status)} />;
}

export function sentence(value: string) {
  return value ? value[0]!.toUpperCase() + value.slice(1) : value;
}

export function formatDate(value: Date | string | number) {
  return new Date(value).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  });
}

/**
 * Jazz has no count query yet, so counts come from bounded reads. When a read
 * returns its full limit there may be more rows, and the count says so.
 */
export function formatCount(count: number, isCapped: boolean) {
  return isCapped ? `${count.toLocaleString()}+` : count.toLocaleString();
}

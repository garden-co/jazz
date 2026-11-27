"use client";

import { useRef, useState } from "react";
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

/** Where a page starts: the previous page's last row (its id and sort value). */
type Cursor = { id: string; value: string | number | null };

/**
 * Search, sort and page state for a table backed by a bounded, ordered Zero
 * query. Each page asks for one extra row to learn whether a next page exists,
 * so no count query is needed. ZQL pages with a cursor (`start`) rather than
 * an offset, so this remembers where each visited page starts.
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
  const [page, setPageValue] = useState(1);
  // starts[n - 1]: where page n starts (null: the beginning).
  const [starts, setStarts] = useState<(Cursor | null)[]>([null]);
  const lastRow = useRef<Record<string, unknown> | undefined>(undefined);
  const resetPage = () => {
    setPageValue(1);
    setStarts([null]);
  };
  const sortPlugin = useTableSortable<Record<string, unknown>, K>({
    sort,
    onSortChange: (next) => {
      setSortState(next);
      resetPage();
    },
    allowUnsortedState: false,
  });
  const active = sort[0] ?? { sortKey: defaultSort, direction: defaultDirection };
  const sortKey = active.sortKey;
  return {
    search,
    setSearch: (value: string) => {
      setSearchValue(value);
      resetPage();
    },
    sortKey,
    direction: active.direction === "ascending" ? ("asc" as const) : ("desc" as const),
    page,
    /** Next and previous only: the next page starts after this page's last row. */
    setPage: (next: number) => {
      if (next === page + 1 && lastRow.current) {
        const row = lastRow.current;
        setStarts((current) => {
          const copy = current.slice(0, page);
          copy[page] = { id: String(row.id), value: (row[sortKey] ?? null) as Cursor["value"] };
          return copy;
        });
      }
      if (next === page + 1 ? Boolean(lastRow.current) : next >= 1 && next <= starts.length) setPageValue(next);
    },
    pageSize,
    limit: pageSize + 1,
    /** The `start` argument of the page's query. */
    start: starts[page - 1] ?? null,
    /** The page's rows and whether there is a next page; remembers where the next one starts. */
    pageOf<T extends Record<string, unknown>>(rows: readonly T[] | undefined) {
      const pageRows = rows?.slice(0, pageSize) ?? [];
      lastRow.current = pageRows.at(-1);
      return { rows: pageRows, hasMore: (rows?.length ?? 0) > pageSize };
    },
    // The sort plugin only reads column keys, so it fits any row type.
    plugins: { sort: sortPlugin as TablePlugin<any> },
    resetPage,
  };
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
 * There is no count query, so counts come from bounded reads. When a read
 * returns its full limit there may be more rows, and the count says so.
 */
export function formatCount(count: number, isCapped: boolean) {
  return isCapped ? `${count.toLocaleString()}+` : count.toLocaleString();
}

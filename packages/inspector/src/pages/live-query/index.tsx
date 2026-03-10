import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  type ColumnDef,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table";
import { getActiveQuerySubscriptions, onActiveQuerySubscriptionsChange } from "jazz-tools";
import type { ActiveQuerySubscriptionTrace, DurabilityTier } from "jazz-tools";
import { useEffect, useMemo, useState } from "react";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import { LiveQueryFilters } from "./LiveQueryFilters.js";
import styles from "./index.module.css";

function getUserStackSummary(stack: string | undefined): string {
  if (!stack) {
    return "n/a";
  }

  const lines = stack.split("\n").slice(1);
  const firstUserFrame = lines.find((line) => line.includes("/src/"));
  return firstUserFrame?.trim() ?? lines[0]?.trim() ?? "n/a";
}

function formatStartedAt(value: string): string {
  return new Date(value).toLocaleTimeString();
}

function tierRank(tier: DurabilityTier): number {
  switch (tier) {
    case "worker":
      return 0;
    case "edge":
      return 1;
    case "global":
      return 2;
  }
}

function useActiveSubscriptions(runtime: "standalone" | "extension") {
  const [subscriptions, setSubscriptions] = useState(() => getActiveQuerySubscriptions());

  useEffect(() => {
    if (runtime !== "extension") {
      setSubscriptions([]);
      return;
    }

    setSubscriptions(getActiveQuerySubscriptions());
    return onActiveQuerySubscriptionsChange((nextSubscriptions) => {
      setSubscriptions([...nextSubscriptions]);
    });
  }, [runtime]);

  return subscriptions;
}

export function LiveQuery() {
  const { runtime, wasmSchema } = useDevtoolsContext();
  const subscriptions = useActiveSubscriptions(runtime);
  const [selectedTable, setSelectedTable] = useState("");
  const [selectedTier, setSelectedTier] = useState("");
  const [sorting, setSorting] = useState<SortingState>([
    { id: "createdAt", desc: true },
    { id: "tier", desc: false },
  ]);

  const availableTables = useMemo(() => Object.keys(wasmSchema ?? {}).sort(), [wasmSchema]);
  const filteredSubscriptions = useMemo(() => {
    return subscriptions.filter((subscription) => {
      if (selectedTable && subscription.table !== selectedTable) {
        return false;
      }
      if (selectedTier && subscription.tier !== selectedTier) {
        return false;
      }
      return true;
    });
  }, [selectedTable, selectedTier, subscriptions]);

  const columns = useMemo<ColumnDef<ActiveQuerySubscriptionTrace>[]>(
    () => [
      {
        accessorKey: "table",
        header: "Table",
        cell: (info) => info.getValue<string>(),
      },
      {
        accessorKey: "tier",
        header: "Tier",
        sortingFn: (left, right, columnId) =>
          tierRank(left.getValue<DurabilityTier>(columnId)) -
          tierRank(right.getValue<DurabilityTier>(columnId)),
        cell: (info) => info.getValue<string>(),
      },
      {
        id: "branches",
        header: "Branches",
        cell: ({ row }) => row.original.branches.join(", "),
      },
      {
        accessorKey: "createdAt",
        header: "Started",
        sortingFn: "datetime",
        cell: ({ row }) => formatStartedAt(row.original.createdAt),
      },
      {
        accessorKey: "query",
        header: "Query",
        enableSorting: false,
        cell: ({ row }) => <pre className={styles.codeBlock}>{row.original.query}</pre>,
      },
      {
        id: "stack",
        header: "Stack",
        enableSorting: false,
        cell: ({ row }) => (
          <details className={styles.stackDetails}>
            <summary className={styles.stackSummary}>
              {getUserStackSummary(row.original.stack)}
            </summary>
            <pre className={styles.codeBlock}>{row.original.stack ?? "n/a"}</pre>
          </details>
        ),
      },
    ],
    [],
  );

  const table = useReactTable({
    data: filteredSubscriptions,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  if (runtime !== "extension") {
    return null;
  }

  return (
    <section className={styles.container}>
      <header className={styles.header}>
        <div>
          <h1 className={styles.title}>Live Query</h1>
          <p className={styles.subtitle}>
            Active `Db.subscribeAll(...)` subscriptions captured from the inspected page runtime.
          </p>
        </div>
        <LiveQueryFilters
          availableTables={availableTables}
          selectedTable={selectedTable}
          selectedTier={selectedTier}
          onTableChange={setSelectedTable}
          onTierChange={setSelectedTier}
        />
      </header>
      {filteredSubscriptions.length === 0 ? (
        <section className={styles.emptyState}>
          <p className={styles.emptyTitle}>No active subscriptions</p>
          <p className={styles.emptyText}>
            Open a page with `devMode: true` and create a live query to see it here.
          </p>
        </section>
      ) : (
        <div className={styles.tableShell}>
          <table className={styles.table}>
            <thead>
              {table.getHeaderGroups().map((headerGroup) => (
                <tr key={headerGroup.id}>
                  {headerGroup.headers.map((header) => {
                    const sortDirection = header.column.getIsSorted();
                    const canSort = header.column.getCanSort();

                    return (
                      <th
                        key={header.id}
                        className={canSort ? styles.sortableHeader : undefined}
                        onClick={canSort ? header.column.getToggleSortingHandler() : undefined}
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
              {table.getRowModel().rows.map((row) => (
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
      )}
    </section>
  );
}

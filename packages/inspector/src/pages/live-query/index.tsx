import {
  fetchServerSubscriptions,
  getActiveQuerySubscriptions,
  onActiveQuerySubscriptionsChange,
} from "jazz-tools";
import type {
  ActiveQuerySubscriptionTrace,
  DurabilityTier,
  IntrospectionSubscriptionGroup,
} from "jazz-tools";
import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  type ColumnDef,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table";
import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import { useStandaloneContext } from "../../contexts/standalone-context.js";
import { LiveQueryFilters } from "./LiveQueryFilters.js";
import styles from "./index.module.css";

const SERVER_SUBSCRIPTIONS_POLL_MS = 20_000;

const IR_OP_TO_FILTER_OP: Record<string, string> = {
  Eq: "eq",
  Ne: "ne",
  Gt: "gt",
  Gte: "gte",
  Lt: "lt",
  Lte: "lte",
};

interface FilterClause {
  id: string;
  column: string;
  operator: string;
  value: unknown;
}

function extractFiltersFromIR(node: unknown): FilterClause[] {
  if (!node || typeof node !== "object") return [];
  const obj = node as Record<string, unknown>;

  if ("Cmp" in obj && obj.Cmp && typeof obj.Cmp === "object") {
    const cmp = obj.Cmp as Record<string, unknown>;
    const left = cmp.left as Record<string, unknown> | undefined;
    const right = cmp.right as Record<string, unknown> | undefined;
    const op = IR_OP_TO_FILTER_OP[cmp.op as string];
    const column = left?.column as string | undefined;
    const literal = right?.Literal as Record<string, unknown> | undefined;
    if (op && column && literal && "value" in literal) {
      return [{ id: `ir-${column}`, column, operator: op, value: literal.value }];
    }
    return [];
  }

  if ("And" in obj && Array.isArray(obj.And)) {
    return obj.And.flatMap((child: unknown) => extractFiltersFromIR(child));
  }

  const filters: FilterClause[] = [];
  for (const value of Object.values(obj)) {
    if (value && typeof value === "object") {
      filters.push(...extractFiltersFromIR(value));
    }
  }
  return filters;
}

function buildExplorerUrl(table: string, queryJson: string): string {
  const base = `/data-explorer/${table}/data`;
  try {
    const parsed = JSON.parse(queryJson);
    const filters = extractFiltersFromIR(parsed.relation_ir);
    if (filters.length > 0) {
      const params = new URLSearchParams();
      params.set("filters", JSON.stringify(filters));
      return `${base}?${params.toString()}`;
    }
  } catch {
    // ignore parse errors
  }
  return base;
}

function getUserStackSummary(stack: string | undefined): string {
  if (!stack) {
    return "n/a";
  }

  const lines = stack.split("\n").slice(1);
  const firstUserFrame = lines.find((line) => line.includes("/src/"));
  return firstUserFrame?.trim() ?? lines[0]?.trim() ?? "n/a";
}

function formatTime(value: string | number): string {
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

function useServerSubscriptionTelemetry(runtime: "standalone" | "extension") {
  const standaloneContext = useStandaloneContext();
  const [queries, setQueries] = useState<IntrospectionSubscriptionGroup[]>([]);
  const [generatedAt, setGeneratedAt] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(runtime === "standalone");

  useEffect(() => {
    if (runtime !== "standalone" || !standaloneContext) {
      setQueries([]);
      setGeneratedAt(null);
      setError(null);
      setIsLoading(false);
      return;
    }

    let cancelled = false;

    const load = async (showLoader: boolean) => {
      if (showLoader) {
        setIsLoading(true);
      }

      try {
        const response = await fetchServerSubscriptions(standaloneContext.connection.serverUrl, {
          adminSecret: standaloneContext.connection.adminSecret,
          appId: standaloneContext.connection.appId,
          pathPrefix: standaloneContext.connection.serverPathPrefix,
        });
        if (cancelled) {
          return;
        }
        setQueries(response.queries);
        setGeneratedAt(response.generatedAt);
        setError(null);
      } catch (err) {
        if (cancelled) {
          return;
        }
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!cancelled && showLoader) {
          setIsLoading(false);
        }
      }
    };

    load(true);
    const intervalId = window.setInterval(() => {
      load(false);
    }, SERVER_SUBSCRIPTIONS_POLL_MS);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [
    runtime,
    standaloneContext?.connection.adminSecret,
    standaloneContext?.connection.appId,
    standaloneContext?.connection.serverPathPrefix,
    standaloneContext?.connection.serverUrl,
  ]);

  return { queries, generatedAt, error, isLoading };
}

function ExtensionLiveQuery() {
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
        cell: ({ row }) => (
          <Link
            to={buildExplorerUrl(row.original.table, row.original.query)}
            className={styles.tableLink}
          >
            {row.original.table}
          </Link>
        ),
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
        accessorKey: "propagation",
        header: "Propagation",
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
        cell: ({ row }) => formatTime(row.original.createdAt),
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

function StandaloneLiveQuery() {
  const { queries, generatedAt, error, isLoading } = useServerSubscriptionTelemetry("standalone");
  const [selectedTable, setSelectedTable] = useState("");

  const availableTables = useMemo(() => {
    return [...new Set(queries.map((query) => query.table))].sort();
  }, [queries]);
  const filteredQueries = useMemo(() => {
    return queries.filter((query) => !selectedTable || query.table === selectedTable);
  }, [queries, selectedTable]);

  return (
    <section className={styles.container}>
      <header className={styles.header}>
        <div>
          <h1 className={styles.title}>Live Query</h1>
          <p className={styles.subtitle}>
            Grouped active server-managed subscriptions for the connected Jazz server.
          </p>
          <p className={styles.statusText}>
            {generatedAt
              ? `Polled every 20s. Last refresh ${formatTime(generatedAt)}.`
              : "Polled every 20s."}
          </p>
        </div>
        <LiveQueryFilters
          availableTables={availableTables}
          selectedTable={selectedTable}
          selectedTier=""
          onTableChange={setSelectedTable}
          onTierChange={() => undefined}
          showTierFilter={false}
        />
      </header>
      {error && queries.length === 0 ? (
        <section className={styles.emptyState}>
          <p className={styles.emptyTitle}>Unable to load subscription telemetry</p>
          <p className={styles.emptyText}>{error}</p>
        </section>
      ) : isLoading && queries.length === 0 ? (
        <section className={styles.emptyState}>
          <p className={styles.emptyTitle}>Loading subscriptions</p>
          <p className={styles.emptyText}>Fetching active server subscriptions.</p>
        </section>
      ) : filteredQueries.length === 0 ? (
        <section className={styles.emptyState}>
          <p className={styles.emptyTitle}>No active subscriptions</p>
          <p className={styles.emptyText}>
            The connected server is not currently tracking any downstream query subscriptions.
          </p>
        </section>
      ) : (
        <div className={styles.tableShell}>
          <table className={styles.table}>
            <thead>
              <tr>
                <th>Count</th>
                <th>Table</th>
                <th>Propagation</th>
                <th>Branches</th>
                <th>Query</th>
              </tr>
            </thead>
            <tbody>
              {filteredQueries.map((query) => (
                <tr key={query.groupKey}>
                  <td>{query.count}</td>
                  <td>
                    <Link
                      to={buildExplorerUrl(query.table, query.query)}
                      className={styles.tableLink}
                    >
                      {query.table}
                    </Link>
                  </td>
                  <td>{query.propagation}</td>
                  <td>{query.branches.join(", ")}</td>
                  <td>
                    <pre className={styles.codeBlock}>{query.query}</pre>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {error && queries.length > 0 ? <p className={styles.statusText}>{error}</p> : null}
    </section>
  );
}

export function LiveQuery() {
  const { runtime } = useDevtoolsContext();

  if (runtime === "standalone") {
    return <StandaloneLiveQuery />;
  }

  return <ExtensionLiveQuery />;
}

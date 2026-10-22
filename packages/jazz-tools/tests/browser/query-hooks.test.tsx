import * as React from "react";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { userEvent } from "vitest/browser";
import { createRoot, type Root } from "react-dom/client";
import type { QueryBuilder, QueryOptions } from "../../src/runtime/db.js";
import {
  createJazzClient as createPublicJazzClient,
  type JazzClient,
} from "../../src/react/create-jazz-client.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { JazzClientProvider as JazzProvider } from "../../src/react-core/provider.js";
import { useAll, useAllSuspense } from "../../src/react-core/use-all.js";
import { getSubscriptionStore } from "../../src/subscription-store-internal.js";

import {
  orgs,
  teams,
  users,
  todos,
  type Org,
  type Team,
  type User,
  type Todo,
  liveConditionCases,
  makeLocalQuery as makeQuery,
} from "../shared/local-query-scenarios.js";

function uniqueId(label: string): string {
  return `use-all-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

/** Exercise the public React client boundary with a manager-issued account. */
async function createBrowserTestJazzClient(
  config: Omit<Parameters<typeof createPublicJazzClient>[0], "account">,
): Promise<JazzClient> {
  return await createPublicJazzClient({
    ...config,
    account: await acquireBrowserTestAccount(config),
  });
}

const clients: JazzClient[] = [];
let root: Root | null = null;
let container: HTMLDivElement | null = null;

beforeEach(() => {
  container = document.createElement("div");
  document.body.appendChild(container);
  root = createRoot(container);
});

afterEach(async () => {
  // Keep React unmount and client shutdown in one hook so nested teardown
  // cannot close a Db while its consumers are still mounted.
  if (root) {
    root.unmount();
    root = null;
  }
  if (container) {
    container.remove();
    container = null;
  }
  for (const client of clients.splice(0).reverse()) {
    await client.shutdown();
  }
});

function renderRoot(node: React.ReactNode): void {
  if (!root) throw new Error("render called before root initialization");
  root.render(node);
}

function getText(testId: string): string {
  if (!container) return "";
  const node = container.querySelector(`[data-testid="${testId}"]`);
  return node?.textContent ?? "";
}

async function waitForCondition(
  check: () => boolean,
  timeoutMs: number,
  errorMessage: string,
  diagnostic?: () => string,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (check()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(diagnostic ? `${errorMessage}; ${diagnostic()}` : errorMessage);
}

function hasTestId(testId: string): boolean {
  return container?.querySelector(`[data-testid="${testId}"]`) != null;
}

function useSuspenseResult<T extends { id: string }>(
  query?: QueryBuilder<T>,
  options?: QueryOptions,
) {
  return { data: useAllSuspense(query, options), error: undefined, isLoading: false };
}

function useNonSuspenseResult<T extends { id: string }>(
  query?: QueryBuilder<T>,
  options?: QueryOptions,
) {
  return useAll(query, options);
}

describe.each([false, true])("query hooks (suspense=%s)", (suspense) => {
  const useQueryResult = suspense ? useSuspenseResult : useNonSuspenseResult;
  function render(node: React.ReactNode): void {
    renderRoot(
      suspense ? (
        <React.Suspense fallback={<div data-testid="rows-fallback">pending</div>}>
          {node}
        </React.Suspense>
      ) : (
        node
      ),
    );
  }
  function UseAllProbe<T extends { id: string }>({
    query,
    options,
    pick,
  }: {
    query?: QueryBuilder<T>;
    options?: QueryOptions;
    pick: (row: T) => string;
  }) {
    const result = useQueryResult(query, options);
    const text = result.error
      ? `error:${result.error.message}`
      : result.data
        ? result.data.map(pick).join("|")
        : result.isLoading
          ? "pending"
          : "no result";
    return <div data-testid="rows">{text}</div>;
  }

  let conditionsClient: JazzClient;
  // Full operator semantics are covered by the database suites. These cases
  // exercise hook delivery for scalar, array, text, and reference queries.
  const conditionCases = liveConditionCases.filter(({ name }) =>
    ["eq", "contains-array", "contains-text", "contains-text-empty", "in-reference"].includes(name),
  );

  function track(client: JazzClient): JazzClient {
    clients.push(client);
    return client;
  }

  beforeAll(async () => {
    conditionsClient = await createBrowserTestJazzClient({
      appId: uniqueId("operators"),
      driver: { type: "persistent", dbName: uniqueId("operators") },
    });
  });

  afterAll(async () => {
    await conditionsClient.shutdown();
  });

  for (const testCase of conditionCases) {
    it(`supports condition operator ${testCase.name}`, async () => {
      const preloadBeforeRender =
        testCase.name === "contains-text" || testCase.name === "contains-text-empty";
      if (preloadBeforeRender) {
        await conditionsClient.db.insert(todos, testCase.insert, { id: testCase.insertId });
      }

      render(
        <JazzProvider client={conditionsClient} key={testCase.name}>
          <UseAllProbe query={testCase.query} pick={(row) => row.title} />
        </JazzProvider>,
      );

      await waitForCondition(
        () => hasTestId("rows"),
        5000,
        `expected suspense rows mount for ${testCase.name}`,
      );

      if (!preloadBeforeRender) {
        await conditionsClient.db.insert(todos, testCase.insert, { id: testCase.insertId });
      }

      await waitForCondition(
        () => getText("rows").split("|").includes(testCase.insert.title),
        5000,
        `expected useAll rows to include ${testCase.name}`,
      );
    });
  }

  it("supports orderBy + limit + offset", async () => {
    const client = track(
      await createBrowserTestJazzClient({
        appId: uniqueId("order"),
        driver: { type: "persistent", dbName: uniqueId("order") },
        devMode: true,
      }),
    );

    const query = makeQuery<Todo>("todos", {
      orderBy: [["priority", "desc"]],
      offset: 1,
      limit: 1,
    });

    // This query has previously timed out only under the aggregate browser
    // workload. Keep a failure receipt local to this test: it distinguishes a
    // hook that never settled from a settled-but-wrong page, and records
    // whether the public maintained subscription was ever registered.
    let hookState = "not rendered";
    const subscriptionHistory: string[] = [];
    const stopSubscriptionTrace = client.db.onActiveQuerySubscriptionsChange((traces) => {
      if (subscriptionHistory.length === 16) subscriptionHistory.shift();
      subscriptionHistory.push(
        traces.map(({ id, table, tier }) => `${id}:${table}:${tier}`).join("|") || "none",
      );
    });

    function PaginationProbe() {
      const result = useQueryResult(query);
      hookState = result.error
        ? `error:${result.error.message}`
        : result.data
          ? `fulfilled:${result.data.map((row) => row.title).join("|")}`
          : result.isLoading
            ? "pending"
            : "no result";
      return (
        <div data-testid="rows">{result.data?.map((row) => row.title).join("|") ?? hookState}</div>
      );
    }

    try {
      render(
        <JazzProvider client={client}>
          <PaginationProbe />
        </JazzProvider>,
      );

      // An empty paginated query must settle without a write waking it up.
      await waitForCondition(
        () => hookState === "fulfilled:" && hasTestId("rows") && getText("rows") === "",
        5000,
        "expected the empty paginated query to settle before any writes",
        () =>
          `final DOM=${container?.innerHTML ?? "missing"}; hook=${hookState}; subscriptions=${subscriptionHistory.join(" -> ")}`,
      );

      await client.db.insert(todos, {
        title: "p1",
        done: false,
        priority: 1,
        owner_id: undefined,
        tags: ["x"],
      });
      await client.db.insert(todos, {
        title: "p2",
        done: false,
        priority: 2,
        owner_id: undefined,
        tags: ["x"],
      });
      await client.db.insert(todos, {
        title: "p3",
        done: false,
        priority: 3,
        owner_id: undefined,
        tags: ["x"],
      });

      await waitForCondition(
        () => getText("rows") === "p2",
        5000,
        "expected p2 in paginated useAll",
        () =>
          `final DOM=${container?.innerHTML ?? "missing"}; hook=${hookState}; subscriptions=${subscriptionHistory.join(" -> ")}`,
      );
    } finally {
      stopSubscriptionTrace();
    }
  });

  it("accepts core-supported QueryOptions for subscriptions", async () => {
    const client = track(
      await createBrowserTestJazzClient({
        appId: uniqueId("options"),
        driver: { type: "persistent", dbName: uniqueId("options") },
      }),
    );

    render(
      <JazzProvider client={client}>
        <UseAllProbe
          query={makeQuery<Todo>("todos", {})}
          options={{ tier: "local-first" }}
          pick={(row) => row.title}
        />
      </JazzProvider>,
    );

    await client.db.insert(todos, {
      title: "optioned-task",
      done: false,
      priority: 1,
      owner_id: undefined,
      tags: ["x"],
    });

    await waitForCondition(
      () => getText("rows").includes("optioned-task"),
      5000,
      "expected useAll with QueryOptions to receive rows",
    );
  });

  it("does not include rows for non-matching text contains", async () => {
    const client = track(
      await createBrowserTestJazzClient({
        appId: uniqueId("contains-text-miss"),
        driver: { type: "persistent", dbName: uniqueId("contains-text-miss") },
      }),
    );

    render(
      <JazzProvider client={client}>
        <UseAllProbe
          query={makeQuery<Todo>("todos", {
            conditions: [{ column: "title", op: "contains", value: "needle" }],
          })}
          pick={(row) => row.title}
        />
      </JazzProvider>,
    );

    await client.db.insert(todos, {
      title: "completely unrelated",
      done: false,
      priority: 1,
      owner_id: undefined,
      tags: ["x"],
    });

    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(getText("rows").includes("completely unrelated")).toBe(false);
  });

  it("supports include query execution path", async () => {
    const client = track(
      await createBrowserTestJazzClient({
        appId: uniqueId("include"),
        driver: { type: "persistent", dbName: uniqueId("include") },
      }),
    );

    const query = makeQuery<User>("users", {
      includes: { todosViaOwner: true },
    });

    render(
      <JazzProvider client={client}>
        <UseAllProbe query={query} pick={(row) => row.name} />
      </JazzProvider>,
    );

    const {
      value: { id: userId },
    } = await client.db.insert(users, { name: "Owner", team_id: undefined });
    await client.db.insert(todos, {
      title: "owned-todo",
      done: false,
      priority: 1,
      owner_id: userId,
      tags: ["x"],
    });

    await waitForCondition(
      () => getText("rows").includes("Owner"),
      5000,
      "expected include useAll row",
    );
  });

  it("supports hop queries", async () => {
    const client = track(
      await createBrowserTestJazzClient({
        appId: uniqueId("hops"),
        driver: { type: "persistent", dbName: uniqueId("hops") },
      }),
    );

    const query = makeQuery<Org>("users", {
      hops: ["team", "org"],
    });

    render(
      <JazzProvider client={client}>
        <UseAllProbe query={query} pick={(row) => row.name} />
      </JazzProvider>,
    );

    try {
      await waitForCondition(
        () => hasTestId("rows"),
        5000,
        "expected suspense rows mount for hop query",
      );
    } catch (error) {
      // #2677: Inspect only after failure; do not open another read that could wake
      // the subscription and hide the missing initial delivery.
      const store = getSubscriptionStore(client);
      const state = store.peekState<Org>(store.computeKey(query));
      console.error("[hop suspense failure]", {
        cacheStatus: state.status,
        rowCount: state.status === "fulfilled" ? state.data.length : undefined,
        rowsMounted: hasTestId("rows"),
        fallbackMounted: hasTestId("rows-fallback"),
        activeSubscriptions: client.db.getActiveQuerySubscriptions().map(({ table, tier }) => ({
          table,
          tier,
        })),
      });
      throw error;
    }

    const {
      value: { id: orgId },
    } = await client.db.insert(orgs, { name: "Hop Org" });
    const {
      value: { id: teamId },
    } = await client.db.insert(teams, {
      name: "Hop Team",
      org_id: orgId,
      parent_id: undefined,
    });
    await client.db.insert(users, { name: "Hop User", team_id: teamId });

    await waitForCondition(
      () => getText("rows").includes("Hop Org"),
      5000,
      "expected hop useAllSuspense row",
    );
  });

  it("supports gather queries", async () => {
    const client = track(
      await createBrowserTestJazzClient({
        appId: uniqueId("gather"),
        driver: { type: "persistent", dbName: uniqueId("gather") },
      }),
    );

    const query = makeQuery<Team>("teams", {
      conditions: [{ column: "name", op: "eq", value: "leaf" }],
      gather: {
        max_depth: 10,
        step_table: "teams",
        step_current_column: "id",
        step_conditions: [],
        step_hops: ["parent"],
      },
    });

    render(
      <JazzProvider client={client}>
        <UseAllProbe query={query} pick={(row) => row.name} />
      </JazzProvider>,
    );

    await waitForCondition(
      () => hasTestId("rows"),
      5000,
      "expected suspense rows mount for gather query",
    );

    const root = await client.db.insert(teams, {
      name: "root",
      org_id: undefined,
      parent_id: undefined,
    });
    const rootId = root.value.id;
    const mid = await client.db.insert(teams, {
      name: "mid",
      org_id: undefined,
      parent_id: rootId,
    });
    const midId = mid.value.id;
    const leaf = await client.db.insert(teams, {
      name: "leaf",
      org_id: undefined,
      parent_id: midId,
    });

    // A cold recursive gather may hydrate over several owner turns. Its work
    // must never starve unrelated local writes or their receipts.
    const waitForLocal = (label: string, wait: Promise<unknown>) =>
      Promise.race([
        wait,
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`${label} local acknowledgement timed out`)), 5_000),
        ),
      ]);
    await waitForLocal("root", root.wait({ tier: "local" }));
    await waitForLocal("mid", mid.wait({ tier: "local" }));
    await waitForLocal("leaf", leaf.wait({ tier: "local" }));

    await waitForCondition(
      () => {
        const values = getText("rows").split("|");
        return values.includes("root") && values.includes("mid") && values.includes("leaf");
      },
      5000,
      "expected gather useAllSuspense rows",
    );
  });

  it("reacts to query changes", async () => {
    const client = track(
      await createBrowserTestJazzClient({
        appId: uniqueId("query-change"),
        driver: { type: "persistent", dbName: uniqueId("query-change") },
      }),
    );

    await client.db.insert(todos, {
      title: "open-task",
      done: false,
      priority: 1,
      owner_id: undefined,
      tags: ["x"],
    });
    await client.db.insert(todos, {
      title: "done-task",
      done: true,
      priority: 2,
      owner_id: undefined,
      tags: ["x"],
    });

    function QuerySwitchProbe() {
      const [showDone, setShowDone] = React.useState(false);
      const query = makeQuery<Todo>("todos", {
        conditions: [{ column: "done", op: "eq", value: showDone }],
      });
      const { data: rows } = useQueryResult(query);
      return (
        <>
          <button data-testid="toggle-query" onClick={() => setShowDone((value) => !value)}>
            toggle
          </button>
          <div data-testid="rows">{rows ? rows.map((row) => row.title).join("|") : "pending"}</div>
        </>
      );
    }

    render(
      <JazzProvider client={client}>
        <QuerySwitchProbe />
      </JazzProvider>,
    );

    await waitForCondition(
      () => getText("rows").includes("open-task") && !getText("rows").includes("done-task"),
      5000,
      "expected initial query to show only open task",
    );

    const toggleQuery = container?.querySelector('[data-testid="toggle-query"]');
    expect(toggleQuery).toBeTruthy();
    await userEvent.click(toggleQuery as HTMLElement);

    await waitForCondition(
      () => getText("rows").includes("done-task") && !getText("rows").includes("open-task"),
      5000,
      "expected updated query to show only done task",
    );
  });

  it("unmounts the consumer before shutting down its client", async () => {
    const client = track(
      await createBrowserTestJazzClient({
        appId: uniqueId("cleanup"),
        driver: { type: "persistent", dbName: uniqueId("cleanup") },
      }),
    );
    let mounted = false;
    function CleanupProbe() {
      React.useEffect(() => {
        mounted = true;
        return () => {
          mounted = false;
        };
      }, []);
      return <UseAllProbe query={makeQuery<Todo>("todos", {})} pick={(row) => row.title} />;
    }
    const shutdown = client.shutdown.bind(client);
    vi.spyOn(client, "shutdown").mockImplementation(async (options) => {
      // This assertion runs in fixture teardown, making ordering deterministic
      // instead of depending on a pending React retry winning a timing race.
      try {
        expect(mounted, "consumer must unmount before client shutdown").toBe(false);
      } finally {
        await shutdown(options);
      }
    });
    render(
      <JazzProvider client={client}>
        <CleanupProbe />
      </JazzProvider>,
    );
    await waitForCondition(() => mounted && hasTestId("rows"), 5000, "expected committed consumer");
  });

  it("handles a missing query and resumes once one is provided", async () => {
    const client = track(
      await createBrowserTestJazzClient({
        appId: uniqueId("missing-query"),
        driver: { type: "persistent", dbName: uniqueId("missing-query") },
      }),
    );

    await client.db.insert(todos, {
      title: "late-query-task",
      done: false,
      priority: 1,
      owner_id: undefined,
      tags: ["x"],
    });

    function MissingThenSetSuspenseQueryProbe() {
      const [useQuery, setUseQuery] = React.useState(false);
      const query = useQuery ? makeQuery<Todo>("todos", {}) : undefined;

      return (
        <>
          <button data-testid="set-query" onClick={() => setUseQuery(true)}>
            set-query
          </button>
          <React.Suspense fallback={<div data-testid="rows-fallback">pending</div>}>
            <UseAllProbe query={query} pick={(row: Todo) => row.title} />
          </React.Suspense>
        </>
      );
    }

    render(
      <JazzProvider client={client}>
        <MissingThenSetSuspenseQueryProbe />
      </JazzProvider>,
    );

    await waitForCondition(
      () =>
        suspense
          ? hasTestId("rows-fallback") && !hasTestId("rows")
          : getText("rows") === "no result",
      5000,
      "expected fallback or no result while query is missing",
    );

    const setQueryButton = container?.querySelector('[data-testid="set-query"]');
    expect(setQueryButton).toBeTruthy();
    await userEvent.click(setQueryButton as HTMLElement);

    await waitForCondition(
      () => hasTestId("rows") && getText("rows").includes("late-query-task"),
      5000,
      "expected suspense hook to resolve after query is provided",
    );
  });
});

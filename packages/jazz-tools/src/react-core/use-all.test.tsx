import * as React from "react";
import { Component, Suspense, useState, type ReactNode } from "react";
import { afterEach, describe, expect, expectTypeOf, it, vi } from "vitest";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { renderToStaticMarkup } from "react-dom/server";
import type { Db, DbDeltaSubscriptionCallbacks, QueryBuilder } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";
import { attachSubscriptionStore } from "../subscription-store-internal.js";
import { JazzClientProvider } from "./provider.js";
import { useAll, useAllSuspense, type UseAllResult } from "./use-all.js";
import { unreadableRows, withUnreadable, type UnreadableRow } from "../encryption/unreadable.js";

type Todo = { id: string; title: string };
type NoQueryResult = {
  data: undefined;
  isLoading: false;
  error: null;
  db: Db;
  unreadable: readonly UnreadableRow[];
};
const keyPending = (id: string): UnreadableRow => ({
  table: "todos",
  id,
  column: "title",
  code: "key-pending",
});

function makeQuery(table = "todos"): QueryBuilder<Todo> {
  return {
    _table: table,
    _schema: {},
    _rowType: {} as Todo,
    _build: () => JSON.stringify({ table, conditions: [], includes: {}, orderBy: [] }),
  } as unknown as QueryBuilder<Todo>;
}

function delta(all: Todo[]): SubscriptionDelta<Todo> {
  return { all, delta: [] };
}

function makeHarness(appId: string, options?: { throwOnSubscribe?: Error }) {
  const subscribeCalls: Array<{
    callback: (d: SubscriptionDelta<Todo>) => void;
    unsubscribe: ReturnType<typeof vi.fn>;
  }> = [];

  const db = {
    getAuthState: () => ({ authMode: "device-account" as const, session: null }),
    onAuthChanged: () => () => {},
    updateAuthToken: () => {},
    getSyncStatus: () => ({
      link: { state: "local-only" as const },
      pendingUploads: 0,
      oldestPendingAt: null,
      rejected: 0,
      lastError: null,
      synced: true,
    }),
    onSyncStatusChange: () => () => {},
    subscribeDelta: (_query: QueryBuilder<Todo>, callbacks: DbDeltaSubscriptionCallbacks<Todo>) => {
      if (options?.throwOnSubscribe) {
        throw options.throwOnSubscribe;
      }
      const unsubscribe = vi.fn();
      subscribeCalls.push({
        callback: callbacks.onDelta,
        unsubscribe,
      });
      return unsubscribe;
    },
  };

  const manager = new SubscriptionsOrchestrator({ appId }, db as any);
  const client = attachSubscriptionStore({ db, session: null, shutdown: async () => {} }, manager);
  return { client, manager, subscribeCalls };
}

class ErrorBoundary extends Component<
  { fallback: ReactNode; children: ReactNode },
  { error: Error | null }
> {
  state: { error: Error | null } = { error: null };
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  render() {
    return this.state.error ? this.props.fallback : this.props.children;
  }
}

afterEach(() => {
  cleanup();
});

describe("react-core/useAll", () => {
  it("returns no result without subscribing when no query is provided", () => {
    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      expectTypeOf(useAll<Todo>()).toEqualTypeOf<NoQueryResult>();
      expectTypeOf(useAll<Todo>(undefined)).toEqualTypeOf<NoQueryResult>();

      const maybeQuery = makeQuery() as QueryBuilder<Todo> | undefined;
      expectTypeOf(useAll(maybeQuery)).toEqualTypeOf<UseAllResult<Todo> | NoQueryResult>();
    }

    const { client, subscribeCalls } = makeHarness("rc-all-00");
    let result: NoQueryResult | undefined;

    function Probe() {
      result = useAll<Todo>();
      return <span>{result.isLoading ? "loading" : "idle"}</span>;
    }

    const { container } = render(
      <JazzClientProvider client={client}>
        <Probe />
      </JazzClientProvider>,
    );

    expect(container.textContent).toBe("idle");
    expect(result).toEqual({
      data: undefined,
      isLoading: false,
      error: null,
      db: client.db,
      unreadable: [],
    });
    expect(subscribeCalls).toHaveLength(0);
  });

  it("returns the provider's db handle alongside the data in every state", () => {
    const { client, subscribeCalls } = makeHarness("rc-all-db-handle");
    const seen: UseAllResult<Todo>[] = [];

    function Probe() {
      seen.push(useAll(makeQuery()));
      return null;
    }

    render(
      <JazzClientProvider client={client}>
        <Probe />
      </JazzClientProvider>,
    );

    expect(seen.at(-1)!.isLoading).toBe(true);
    expect(seen.at(-1)!.db).toBe(client.db);

    act(() => subscribeCalls[0]!.callback(delta([{ id: "1", title: "first" }])));

    expect(seen.at(-1)!.data).toEqual([{ id: "1", title: "first" }]);
    expect(seen.at(-1)!.db).toBe(client.db);
  });

  it("useAllSuspense returns the rows and the provider's db handle", () => {
    const { client, subscribeCalls } = makeHarness("rc-all-suspense-db-handle");
    let result: { data: Todo[]; db: Db } | undefined;
    let showSuspense!: (show: boolean) => void;

    // A non-suspense reader keeps the entry subscribed and fulfilled, so the
    // suspense reader mounted afterwards reads it synchronously.
    function Warm() {
      useAll(makeQuery());
      return null;
    }

    function List() {
      result = useAllSuspense(makeQuery());
      return <span>{result.data.length}</span>;
    }

    function Root() {
      const [show, setShow] = useState(false);
      showSuspense = setShow;
      return (
        <>
          <Warm />
          <Suspense fallback={<span>loading</span>}>{show ? <List /> : null}</Suspense>
        </>
      );
    }

    const { container } = render(
      <JazzClientProvider client={client}>
        <Root />
      </JazzClientProvider>,
    );

    act(() => subscribeCalls[0]!.callback(delta([{ id: "1", title: "first" }])));
    act(() => showSuspense(true));

    expect(container.textContent).toBe("1");
    expect(result!.data).toEqual([{ id: "1", title: "first" }]);
    expect(result!.db).toBe(client.db);
  });

  it("reports the rows each delivery leaves out undecrypted", () => {
    const { client, subscribeCalls } = makeHarness("rc-all-unreadable");
    const seen: UseAllResult<Todo>[] = [];

    function Probe() {
      seen.push(useAll(makeQuery()));
      return null;
    }

    render(
      <JazzClientProvider client={client}>
        <Probe />
      </JazzClientProvider>,
    );

    expect(seen.at(-1)!.unreadable).toEqual([]);

    const rows = [{ id: "1", title: "first" }];
    act(() => subscribeCalls[0]!.callback(delta(withUnreadable([...rows], [keyPending("2")]))));
    expect(seen.at(-1)!.data).toEqual(rows);
    expect(seen.at(-1)!.unreadable).toEqual([keyPending("2")]);
    expect(unreadableRows(seen.at(-1)!.data!)).toEqual([keyPending("2")]);

    // A delivery that changes only the list still updates it.
    act(() =>
      subscribeCalls[0]!.callback(
        delta(withUnreadable([...rows], [keyPending("2"), keyPending("3")])),
      ),
    );
    expect(seen.at(-1)!.unreadable).toEqual([keyPending("2"), keyPending("3")]);

    // The key arrives: the row joins the result and leaves the list.
    act(() =>
      subscribeCalls[0]!.callback(
        delta(withUnreadable([...rows, { id: "2", title: "second" }], [keyPending("3")])),
      ),
    );
    expect(seen.at(-1)!.data).toEqual([...rows, { id: "2", title: "second" }]);
    expect(seen.at(-1)!.unreadable).toEqual([keyPending("3")]);

    act(() => subscribeCalls[0]!.callback(delta([...rows])));
    expect(seen.at(-1)!.unreadable).toEqual([]);
    expect(unreadableRows(seen.at(-1)!.data!)).toEqual([]);
  });

  it("useAllSuspense reports the rows its result leaves out undecrypted", () => {
    const { client, subscribeCalls } = makeHarness("rc-all-suspense-unreadable");
    let result: ReturnType<typeof useAllSuspense<Todo>> | undefined;
    let showSuspense!: (show: boolean) => void;

    function Warm() {
      useAll(makeQuery());
      return null;
    }

    function List() {
      result = useAllSuspense(makeQuery());
      return <span>{result.data.length}</span>;
    }

    function Root() {
      const [show, setShow] = useState(false);
      showSuspense = setShow;
      return (
        <>
          <Warm />
          <Suspense fallback={<span>loading</span>}>{show ? <List /> : null}</Suspense>
        </>
      );
    }

    render(
      <JazzClientProvider client={client}>
        <Root />
      </JazzClientProvider>,
    );

    const first = { id: "1", title: "first" };
    act(() => subscribeCalls[0]!.callback(delta(withUnreadable([first], [keyPending("2")]))));
    act(() => showSuspense(true));
    expect(result!.unreadable).toEqual([keyPending("2")]);
    expect(unreadableRows(result!.data)).toEqual([keyPending("2")]);

    act(() => subscribeCalls[0]!.callback(delta([first])));
    expect(result!.unreadable).toEqual([]);
  });

  it("keeps result object identity stable while the state is unchanged", () => {
    const { client, subscribeCalls } = makeHarness("rc-all-stable-identity");
    const noQueryResults: NoQueryResult[] = [];
    const queryResults: UseAllResult<Todo>[] = [];
    let force!: (n: number) => void;

    function Probe() {
      const [, setN] = useState(0);
      force = setN;
      noQueryResults.push(useAll<Todo>());
      queryResults.push(useAll(makeQuery()));
      return null;
    }

    render(
      <JazzClientProvider client={client}>
        <Probe />
      </JazzClientProvider>,
    );

    const initialNoQuery = noQueryResults[0]!;
    const initialLoading = queryResults[0]!;

    act(() => force(1));

    expect(noQueryResults.at(-1)).toBe(initialNoQuery);
    expect(queryResults.at(-1)).toBe(initialLoading);

    act(() => subscribeCalls[0]!.callback(delta([{ id: "1", title: "first" }])));

    const fulfilled = queryResults.at(-1)!;
    expect(fulfilled.data).toEqual([{ id: "1", title: "first" }]);

    act(() => force(2));

    expect(queryResults.at(-1)).toBe(fulfilled);
  });

  it("an inline query does not resubscribe across re-renders", () => {
    const { client, subscribeCalls } = makeHarness("rc-all-01");
    let force!: (n: number) => void;

    function Probe() {
      const [, setN] = useState(0);
      force = setN;
      useAll(makeQuery());
      return null;
    }

    render(
      <JazzClientProvider client={client}>
        <Probe />
      </JazzClientProvider>,
    );

    act(() => force(1));
    act(() => force(2));

    expect(subscribeCalls).toHaveLength(1);
  });

  it("renders rows and reflects later deltas", () => {
    const { client, subscribeCalls } = makeHarness("rc-all-02");

    function List() {
      const { data: todos } = useAll(makeQuery());
      return (
        <>
          {(todos ?? []).map((t) => (
            <span key={t.id}>{t.title}</span>
          ))}
        </>
      );
    }

    const { container } = render(
      <JazzClientProvider client={client}>
        <List />
      </JazzClientProvider>,
    );

    expect(container.textContent).toBe("");

    act(() => subscribeCalls[0]!.callback(delta([{ id: "1", title: "first" }])));
    expect(container.textContent).toBe("first");

    act(() =>
      subscribeCalls[0]!.callback(
        delta([
          { id: "1", title: "first" },
          { id: "2", title: "second" },
        ]),
      ),
    );
    expect(container.textContent).toBe("firstsecond");
  });

  it("StrictMode does not open a duplicate subscription", () => {
    const { client, subscribeCalls } = makeHarness("rc-all-03");

    function List() {
      const { data: todos } = useAll(makeQuery());
      return <span>{(todos ?? []).length}</span>;
    }

    render(
      <React.StrictMode>
        <JazzClientProvider client={client}>
          <List />
        </JazzClientProvider>
      </React.StrictMode>,
    );

    expect(subscribeCalls).toHaveLength(1);
  });

  it("server render reads the seeded snapshot without subscribing", () => {
    const { client, manager, subscribeCalls } = makeHarness("rc-all-04");
    const query = makeQuery();
    manager.makeQueryKey(query, undefined, [{ id: "1", title: "seeded" }]);

    function List() {
      const { data: todos } = useAll(query);
      return (
        <>
          {(todos ?? []).map((t) => (
            <span key={t.id}>{t.title}</span>
          ))}
        </>
      );
    }

    const html = renderToStaticMarkup(
      <JazzClientProvider client={client}>
        <List />
      </JazzClientProvider>,
    );

    expect(html).toContain("seeded");
    expect(subscribeCalls).toHaveLength(0);
  });

  it("a failed subscription surfaces a non-suspense useAll error and does not throw", async () => {
    const { client } = makeHarness("rc-all-05", {
      throwOnSubscribe: new Error("subscribe failed"),
    });

    function List() {
      const result = useAll(makeQuery());
      return (
        <span>
          {result.error
            ? result.error.message
            : result.isLoading
              ? "loading"
              : String(result.data.length)}
        </span>
      );
    }

    const { container } = render(
      <JazzClientProvider client={client}>
        <List />
      </JazzClientProvider>,
    );

    await waitFor(() => expect(container.textContent).toBe("subscribe failed"));
  });

  it("useAllSuspense throws a failed subscription to the error boundary", () => {
    const { client } = makeHarness("rc-all-06", {
      throwOnSubscribe: new Error("subscribe failed"),
    });

    function List() {
      const { data: todos } = useAllSuspense(makeQuery());
      return <span>{todos.length}</span>;
    }

    const { container } = render(
      <JazzClientProvider client={client}>
        <ErrorBoundary fallback={<span>caught</span>}>
          <Suspense fallback={<span>loading</span>}>
            <List />
          </Suspense>
        </ErrorBoundary>
      </JazzClientProvider>,
    );

    expect(container.textContent).toBe("caught");
  });
});

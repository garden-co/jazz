import * as React from "react";
import { Component, Suspense, useState, type ReactNode } from "react";
import { afterEach, describe, expect, expectTypeOf, it, vi } from "vitest";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { renderToStaticMarkup } from "react-dom/server";
import type { QueryBuilder } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";
import { JazzClientProvider } from "./provider.js";
import { useAll, useAllSuspense, type UseAllResult } from "./use-all.js";

type Todo = { id: string; title: string };
type NoQueryResult = { data: undefined; isLoading: false; error: null };

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
    getAuthState: () => ({ authMode: "local-first", session: null }),
    onAuthChanged: () => () => {},
    updateAuthToken: () => {},
    subscribeAll: (_query: any, callback: (d: SubscriptionDelta<any>) => void) => {
      if (options?.throwOnSubscribe) {
        throw options.throwOnSubscribe;
      }
      const unsubscribe = vi.fn();
      subscribeCalls.push({
        callback: callback as (d: SubscriptionDelta<Todo>) => void,
        unsubscribe,
      });
      return unsubscribe;
    },
  };

  const manager = new SubscriptionsOrchestrator({ appId }, db as any);
  const client = { db, manager, session: null, shutdown: async () => {} } as any;
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
  it("returns no result when no query is provided", () => {
    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      expectTypeOf(useAll<Todo>()).toEqualTypeOf<NoQueryResult>();
      expectTypeOf(useAll<Todo>(undefined)).toEqualTypeOf<NoQueryResult>();

      const maybeQuery = makeQuery() as QueryBuilder<Todo> | undefined;
      expectTypeOf(useAll(maybeQuery)).toEqualTypeOf<UseAllResult<Todo> | NoQueryResult>();
    }
  });

  it("returns no result without subscribing when no query is provided", () => {
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
    expect(result).toEqual({ data: undefined, isLoading: false, error: null });
    expect(subscribeCalls).toHaveLength(0);
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
      const todos = useAllSuspense(makeQuery());
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

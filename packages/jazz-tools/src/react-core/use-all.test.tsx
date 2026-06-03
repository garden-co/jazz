import * as React from "react";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { renderToStaticMarkup } from "react-dom/server";
import type { QueryBuilder } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";
import { JazzClientProvider } from "./provider.js";
import { useAll } from "./use-all.js";

type Todo = { id: string; title: string };

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

function makeHarness(appId: string) {
  const subscribeCalls: Array<{
    callback: (d: SubscriptionDelta<Todo>) => void;
    unsubscribe: ReturnType<typeof vi.fn>;
  }> = [];

  const db = {
    getAuthState: () => ({ authMode: "local-first", session: null }),
    onAuthChanged: () => () => {},
    updateAuthToken: () => {},
    subscribeAll: (_query: any, callback: (d: SubscriptionDelta<any>) => void) => {
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

afterEach(() => {
  cleanup();
});

describe("react-core/useAll", () => {
  it("RC-ALL-01: an inline query does not resubscribe across re-renders", () => {
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

  it("RC-ALL-02: renders rows and reflects later deltas", () => {
    const { client, subscribeCalls } = makeHarness("rc-all-02");

    function List() {
      const todos = useAll(makeQuery());
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

  it("RC-ALL-03: StrictMode does not open a duplicate subscription", () => {
    const { client, subscribeCalls } = makeHarness("rc-all-03");

    function List() {
      const todos = useAll(makeQuery());
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

  it("RC-ALL-04: server render reads the seeded snapshot without subscribing", () => {
    const { client, manager, subscribeCalls } = makeHarness("rc-all-04");
    const query = makeQuery();
    manager.makeQueryKey(query, undefined, [{ id: "1", title: "seeded" }]);

    function List() {
      const todos = useAll(query);
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
});

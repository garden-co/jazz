import * as React from "react";
import { Suspense, startTransition, useState } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Db, DbDeltaSubscriptionCallbacks, QueryBuilder } from "../runtime/db.js";
import { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";
import { attachSubscriptionStore } from "../subscription-store-internal.js";
import { JazzClientProvider } from "./provider.js";
import { useAllSuspense } from "./use-all.js";
import { useOneSuspense } from "./use-one.js";

// The suspense hooks suspend via `use()`. When the promise settles before React
// yields (here: the first result arrives in a microtask), React replays the
// suspended component instead of showing the fallback. A hook that runs after
// `use()` then changes the hook order on replay, which React reports as a
// hook-order warning and, in a concurrent render, as a recoverable error.

type Todo = { id: string; title: string };
const rows: Todo[] = [{ id: "1", title: "first" }];

function makeQuery(): QueryBuilder<Todo> {
  return {
    _table: "todos",
    _schema: {},
    _rowType: {} as Todo,
    _build: () => JSON.stringify({ table: "todos", conditions: [], includes: {}, orderBy: [] }),
  } as unknown as QueryBuilder<Todo>;
}

/** A client whose subscriptions deliver their first result in a microtask. */
function makeMicrotaskClient(appId: string) {
  const db = {
    getAuthState: () => ({ authMode: "local-first" as const, session: null }),
    onAuthChanged: () => () => {},
    updateAuthToken: () => {},
    getSyncStatus: () => ({
      link: { state: "local-only" as const },
      pendingUploads: 0,
      synced: true,
    }),
    onSyncStatusChange: () => () => {},
    subscribeDelta: (_query: QueryBuilder<Todo>, callbacks: DbDeltaSubscriptionCallbacks<Todo>) => {
      queueMicrotask(() => callbacks.onDelta({ all: rows, delta: [] }));
      return () => {};
    },
  };
  const manager = new SubscriptionsOrchestrator({ appId }, db as never);
  return attachSubscriptionStore({ db, session: null, shutdown: async () => {} }, manager);
}

let previousActEnvironment: unknown;
beforeEach(() => {
  // Real scheduling: `act()` would flush synchronously and hide the replay.
  previousActEnvironment = (globalThis as { IS_REACT_ACT_ENVIRONMENT?: unknown })
    .IS_REACT_ACT_ENVIRONMENT;
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: unknown }).IS_REACT_ACT_ENVIRONMENT = false;
});
afterEach(() => {
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: unknown }).IS_REACT_ACT_ENVIRONMENT =
    previousActEnvironment;
  vi.restoreAllMocks();
});

const hooks = {
  useAllSuspense: () => useAllSuspense(makeQuery()),
  useOneSuspense: () => useOneSuspense(makeQuery()),
};

describe("react-core suspense hooks replayed after use()", () => {
  for (const [hookName, useHook] of Object.entries(hooks)) {
    for (const transition of [false, true]) {
      it(`${hookName} keeps hook order when the first result arrives in a microtask${
        transition ? " (transition)" : ""
      }`, async () => {
        const client = makeMicrotaskClient(`suspense-replay-${hookName}-${transition}`);
        const consoleError = vi.spyOn(console, "error");
        const onRecoverableError = vi.fn();
        let result: { data: unknown; db: Db } | undefined;

        function Probe() {
          result = useHook();
          return <span>{JSON.stringify(result.data)}</span>;
        }

        const container = document.createElement("div");
        const root = createRoot(container, { onRecoverableError });
        const renderTree = () =>
          root.render(
            <JazzClientProvider client={client}>
              <Suspense fallback="loading">
                <Probe />
              </Suspense>
            </JazzClientProvider>,
          );
        if (transition) startTransition(renderTree);
        else renderTree();

        try {
          await vi.waitFor(() => expect(container.textContent).not.toBe("loading"));
          await vi.waitFor(() => expect(result).toBeDefined());
          expect(result!.db).toBe(client.db);
          expect(result!.data).toEqual(hookName === "useOneSuspense" ? rows[0] : rows);
          expect(consoleError).not.toHaveBeenCalled();
          expect(onRecoverableError).not.toHaveBeenCalled();
        } finally {
          root.unmount();
        }
      });
    }

    it(`${hookName} keeps its result identity across re-renders`, async () => {
      const client = makeMicrotaskClient(`suspense-identity-${hookName}`);
      const seen: Array<{ data: unknown; db: Db }> = [];
      let force!: (n: number) => void;

      function Probe() {
        const [, setN] = useState(0);
        force = setN;
        seen.push(useHook());
        return <span>ready</span>;
      }

      const container = document.createElement("div");
      const root = createRoot(container);
      root.render(
        <JazzClientProvider client={client}>
          <Suspense fallback="loading">
            <Probe />
          </Suspense>
        </JazzClientProvider>,
      );

      try {
        await vi.waitFor(() => expect(container.textContent).toBe("ready"));
        const before = seen.length;
        force(1);
        await vi.waitFor(() => expect(seen.length).toBeGreaterThan(before));
        expect(seen.at(-1)).toBe(seen[before - 1]);
      } finally {
        root.unmount();
      }
    });
  }
});

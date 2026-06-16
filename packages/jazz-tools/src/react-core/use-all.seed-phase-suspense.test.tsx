import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import { JazzProvider } from "./provider.js";
import { useAllSuspense } from "./use-all.js";
import type { QueryBuilder } from "../runtime/db.js";

type Todo = { id: string; title: string };

function makeQuery(): QueryBuilder<Todo> {
  return {
    _table: "todos",
    _schema: {},
    _rowType: {} as Todo,
    _build() {
      return JSON.stringify({ table: "todos", conditions: [], includes: {}, orderBy: [] });
    },
  };
}

function SuspenseProbe({ query }: { query: QueryBuilder<Todo> }) {
  const rows = useAllSuspense(query);
  return (
    <ul data-testid="rows">
      {rows.map((r) => (
        <li key={r.id}>{r.title}</li>
      ))}
    </ul>
  );
}

// A live client that never resolves keeps the provider in its seed phase — the
// same state the server renders in, where no live client ever attaches.
const neverConnects = () => new Promise<never>(() => {});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("useAllSuspense in the SSR seed phase", () => {
  it("renders empty instead of suspending for an unseeded query", async () => {
    await act(async () => {
      render(
        <JazzProvider
          config={{ appId: "app-ssr-unseeded", serverUrl: "https://jazz.example.com" }}
          createJazzClient={neverConnects as never}
          ssr
          fallback={<div data-testid="fallback">loading</div>}
        >
          <SuspenseProbe query={makeQuery()} />
        </JazzProvider>,
      );
      await Promise.resolve();
    });

    // Nothing can answer this in the seed phase (and on the server nothing ever
    // will), so it renders empty instead of suspending forever.
    const rows = screen.queryByTestId("rows");
    expect(rows).not.toBeNull();
    expect(rows!.textContent).toBe("");
    expect(screen.queryByTestId("fallback")).toBeNull();
  });
});

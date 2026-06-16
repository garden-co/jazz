import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import { JazzProvider } from "./provider.js";
import { useAllSuspense } from "./use-all.js";
import { computeSchemaFingerprint } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import type { QueryBuilder } from "../runtime/db.js";
import { computeQueryKey } from "../subscriptions-orchestrator.js";
import { sealSnapshot } from "../backend/snapshot-envelope.js";
import type { DehydratedSnapshot } from "../backend/ssr.js";

type Todo = { id: string; title: string };

const SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "id", column_type: { type: "Text" }, nullable: false },
      { name: "title", column_type: { type: "Text" }, nullable: false },
    ],
  },
};

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

function SuspenseProbe({
  query,
  snapshot,
}: {
  query: QueryBuilder<Todo>;
  snapshot?: DehydratedSnapshot;
}) {
  const rows = useAllSuspense(query, snapshot ? { snapshot } : undefined);
  return (
    <ul data-testid="rows">
      {rows.map((r) => (
        <li key={r.id}>{r.title}</li>
      ))}
    </ul>
  );
}

// A live client that never resolves keeps the provider in its seed phase
// (ctx.client === null) — the same state the server renders in, where no live
// client will ever attach.
const neverConnects = () => new Promise<never>(() => {});

async function renderSeedPhase(appId: string, child: React.ReactNode): Promise<void> {
  await act(async () => {
    render(
      <JazzProvider
        config={{ appId, serverUrl: "https://jazz.example.com" }}
        createJazzClient={neverConnects as never}
        ssr
        fallback={<div data-testid="fallback">loading</div>}
      >
        {child}
      </JazzProvider>,
    );
    await Promise.resolve();
  });
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("useAllSuspense in the SSR seed phase", () => {
  it("renders empty instead of suspending for an unseeded query", async () => {
    await renderSeedPhase("app-ssr-unseeded", <SuspenseProbe query={makeQuery()} />);

    // No live client can resolve this in the seed phase (and none ever will on
    // the server), so it degrades to empty rather than suspending forever.
    const rows = screen.queryByTestId("rows");
    expect(rows).not.toBeNull();
    expect(rows!.textContent).toBe("");
    expect(screen.queryByTestId("fallback")).toBeNull();
  });

  it("still renders the seeded rows for a snapshotted query", async () => {
    const appId = "app-ssr-seeded";
    const query = makeQuery();
    const snapshot = sealSnapshot({
      v: 1,
      appId,
      principalId: null,
      schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      payload: {
        kind: "rows",
        entries: [{ key: computeQueryKey(appId, query), result: [{ id: "1", title: "seeded" }] }],
      },
    });

    await renderSeedPhase(appId, <SuspenseProbe query={query} snapshot={snapshot} />);

    expect(screen.queryByTestId("rows")?.textContent).toBe("seeded");
  });
});

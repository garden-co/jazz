import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import { JazzProvider } from "./provider.js";
import { useAll } from "./use-all.js";
import { computeSchemaFingerprint } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import type { QueryBuilder } from "../runtime/db.js";
import { computeQueryKey } from "../subscriptions-orchestrator.js";
import { sealSnapshot } from "../backend/snapshot-envelope.js";
import type { DehydratedSnapshot } from "../backend/ssr.js";

type Todo = { id: string; title: string };

// Two schemas with distinct fingerprints — the snapshot is sealed under one and
// the client provider is told the other, modelling a client on a different build
// than the server that produced the snapshot.
const CLIENT_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "id", column_type: { type: "Text" }, nullable: false },
      { name: "title", column_type: { type: "Text" }, nullable: false },
    ],
  },
};
const SERVER_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "id", column_type: { type: "Text" }, nullable: false },
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
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

function sealRows(
  appId: string,
  query: QueryBuilder<Todo>,
  fingerprint: string,
): DehydratedSnapshot {
  return sealSnapshot({
    v: 1,
    appId,
    principalId: null,
    schemaFingerprint: fingerprint,
    payload: {
      kind: "rows",
      entries: [
        { key: computeQueryKey(appId, query), result: [{ id: "1", title: "from-snapshot" }] },
      ],
    },
  });
}

function TodoView({
  query,
  snapshot,
}: {
  query: QueryBuilder<Todo>;
  snapshot: DehydratedSnapshot;
}) {
  const todos = useAll(query, { snapshot }) ?? [];
  return (
    <ul>
      {todos.map((t) => (
        <li key={t.id}>{t.title}</li>
      ))}
    </ul>
  );
}

// A live client that never resolves holds the provider in its synchronous seed
// phase, so what renders is exactly what the per-hook snapshot seeded.
const neverConnects = () => new Promise<never>(() => {});

async function renderSeedPhase(opts: {
  appId: string;
  clientSchema: WasmSchema;
  snapshot: DehydratedSnapshot;
  query: QueryBuilder<Todo>;
}): Promise<void> {
  await act(async () => {
    render(
      <JazzProvider
        config={{ appId: opts.appId, serverUrl: "https://jazz.example.com" }}
        createJazzClient={neverConnects as never}
        ssr
        schema={opts.clientSchema}
      >
        <TodoView query={opts.query} snapshot={opts.snapshot} />
      </JazzProvider>,
    );
    await Promise.resolve();
  });
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("useAll seed — schema fingerprint guard", () => {
  it("seeds the snapshot rows when the client schema matches the snapshot fingerprint", async () => {
    const appId = "app-fp-match";
    const query = makeQuery();
    const snapshot = sealRows(appId, query, computeSchemaFingerprint(CLIENT_SCHEMA));

    await renderSeedPhase({ appId, clientSchema: CLIENT_SCHEMA, snapshot, query });

    expect(screen.queryByText("from-snapshot")).not.toBeNull();
  });

  it("discards the snapshot when the client schema differs from the snapshot fingerprint", async () => {
    const appId = "app-fp-mismatch";
    const query = makeQuery();
    const snapshot = sealRows(appId, query, computeSchemaFingerprint(SERVER_SCHEMA));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    await renderSeedPhase({ appId, clientSchema: CLIENT_SCHEMA, snapshot, query });

    expect(screen.queryByText("from-snapshot")).toBeNull();
    expect(warn.mock.calls.some((c) => String(c[0]).includes("schemaFingerprint mismatch"))).toBe(
      true,
    );
  });
});

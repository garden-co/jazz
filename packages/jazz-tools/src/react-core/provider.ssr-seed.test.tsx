import React from "react";
import { describe, it, expect } from "vitest";
import { renderToString } from "react-dom/server";
import { JazzProvider } from "./provider.js";
import { useAll } from "./use-all.js";
import { computeQueryKey } from "../subscriptions-orchestrator.js";
import { computeSchemaFingerprint } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import type { QueryBuilder } from "../runtime/db.js";
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
  } as unknown as QueryBuilder<Todo>;
}

const query = makeQuery();

function TodoList() {
  const todos = useAll<Todo>(query) ?? [];
  return (
    <ul>
      {todos.map((t) => (
        <li key={t.id}>{t.title}</li>
      ))}
    </ul>
  );
}

describe("JazzProvider — SSR seed", () => {
  it("server-renders seeded snapshot rows synchronously while the client never resolves", () => {
    const neverResolves = () => new Promise<never>(() => {});
    const key = computeQueryKey("ssr-app", query);
    const snapshot: DehydratedSnapshot = {
      appId: "ssr-app",
      principalId: null,
      schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      entries: [{ key, result: [{ id: "1", title: "seeded-row" }] }],
    };

    const html = renderToString(
      <JazzProvider
        config={{ appId: "ssr-app", serverUrl: "https://example.test" }}
        createJazzClient={neverResolves as never}
        snapshot={snapshot}
        fallback={<span>loading</span>}
      >
        <TodoList />
      </JazzProvider>,
    );

    expect(html).toContain("seeded-row");
  });

  it("server-renders a user-scoped snapshot without gating display on the principal", () => {
    const neverResolves = () => new Promise<never>(() => {});
    const key = computeQueryKey("ssr-app", query);
    const snapshot: DehydratedSnapshot = {
      appId: "ssr-app",
      principalId: "alice",
      schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      entries: [{ key, result: [{ id: "1", title: "alice-row" }] }],
    };

    const html = renderToString(
      <JazzProvider
        config={{ appId: "ssr-app", serverUrl: "https://example.test" }}
        createJazzClient={neverResolves as never}
        snapshot={snapshot}
        fallback={<span>loading</span>}
      >
        <TodoList />
      </JazzProvider>,
    );

    expect(html).toContain("alice-row");
  });
});

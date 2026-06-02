import { render } from "svelte/server";
import { describe, expect, it } from "vitest";
import type { DehydratedSnapshot } from "../backend/ssr.js";
import { computeSchemaFingerprint } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import type { QueryBuilder } from "../runtime/db.js";
import { computeQueryKey } from "../subscriptions-orchestrator.js";
import SsrSeedApp from "./__fixtures__/SsrSeedApp.svelte";

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

describe("JazzSvelteProvider — SSR seed", () => {
  it("server-renders seeded snapshot rows synchronously while the client never resolves", () => {
    const query = makeQuery();
    const key = computeQueryKey("ssr-app", query);
    const snapshot: DehydratedSnapshot = {
      appId: "ssr-app",
      principalId: null,
      schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      entries: [{ key, result: [{ id: "1", title: "seeded-row" }] }],
    };
    const neverResolves = new Promise<never>(() => {});

    const { body } = render(SsrSeedApp, {
      props: { client: neverResolves as never, snapshot, query },
    });

    expect(body).toContain("seeded-row");
  });
});

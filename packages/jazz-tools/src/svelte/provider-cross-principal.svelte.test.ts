import { flushSync, mount, unmount } from "svelte";
import { describe, expect, it } from "vitest";
import type { DehydratedSnapshot } from "../backend/ssr.js";
import { computeSchemaFingerprint } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import type { QueryBuilder } from "../runtime/db.js";
import { computeQueryKey, SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";
import CrossPrincipalApp from "./__fixtures__/CrossPrincipalApp.svelte";

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

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("JazzSvelteProvider — cross-principal live client", () => {
  it("throws into the error boundary when the live client's principal contradicts the snapshot", async () => {
    const query = makeQuery();
    const key = computeQueryKey("ssr-app", query);
    const snapshot: DehydratedSnapshot = {
      appId: "ssr-app",
      principalId: "alice",
      schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      entries: [{ key, result: [{ id: "1", title: "alice-row" }] }],
    };

    // A live client authenticated as a *different* principal (bob). The seeded
    // rows paint first (display isn't gated); the throw fires at the live-swap.
    const bobClient = {
      manager: new SubscriptionsOrchestrator({ appId: "ssr-app" }, {
        subscribeAll: () => () => {},
      } as never),
      db: { onAuthChanged: () => () => {} },
      session: { user_id: "bob", claims: {}, authMode: "external" },
      shutdown: async () => {},
    };

    const boundaryErrors: unknown[] = [];
    const target = document.createElement("div");
    document.body.appendChild(target);

    const component = mount(CrossPrincipalApp, {
      target,
      props: {
        client: Promise.resolve(bobClient) as unknown as Promise<never>,
        snapshot,
        query,
        onError: (error) => boundaryErrors.push(error),
      },
    });
    flushSync();
    await tick();
    flushSync();

    expect(boundaryErrors).toHaveLength(1);
    expect(String((boundaryErrors[0] as Error)?.message ?? boundaryErrors[0])).toContain(
      "refusing to seed",
    );

    unmount(component);
    target.remove();
  });
});

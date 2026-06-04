import { flushSync, mount, unmount } from "svelte";
import { describe, expect, it } from "vitest";
import type { DehydratedSnapshot } from "../backend/ssr.js";
import { computeSchemaFingerprint } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import type { QueryBuilder } from "../runtime/db.js";
import { computeQueryKey } from "../subscriptions-orchestrator.js";
import NoClientApp from "./__fixtures__/NoClientApp.svelte";

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

describe("JazzSvelteProvider — no client", () => {
  it("renders seeded rows and never tries to resolve an absent client", async () => {
    const query = makeQuery();
    const key = computeQueryKey("ssr-app", query);
    const snapshot: DehydratedSnapshot = {
      appId: "ssr-app",
      principalId: null,
      schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      entries: [{ key, result: [{ id: "1", title: "seeded-row" }] }],
    };

    const boundaryErrors: unknown[] = [];
    const target = document.createElement("div");
    document.body.appendChild(target);

    // No `client` prop. The provider must seed from the snapshot and the effect
    // must early-return — not resolve `undefined` and throw. The error boundary
    // captures any such throw (which is async), so we let the effect settle and
    // assert it stayed silent.
    const component = mount(NoClientApp, {
      target,
      props: { snapshot, query, onError: (error) => boundaryErrors.push(error) },
    });
    flushSync();
    await tick();
    flushSync();

    expect(boundaryErrors).toEqual([]);
    expect(target.textContent).toContain("seeded-row");
    expect(target.textContent).not.toContain("boundary-failed");

    unmount(component);
    target.remove();
  });
});

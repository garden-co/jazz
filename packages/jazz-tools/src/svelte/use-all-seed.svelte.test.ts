import { describe, expect, it, vi } from "vitest";
import "./test-helpers.svelte.js";
import { computeSchemaFingerprint } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import type { QueryBuilder } from "../runtime/db.js";
import { sealSnapshot } from "../backend/snapshot-envelope.js";
import type { DehydratedSnapshot } from "../backend/ssr.js";
import { computeQueryKey, type SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";
import { createDbLessOrchestrator } from "../ssr/seed-orchestrator.js";

type Todo = { id: string; title: string };

// Only the Svelte context plumbing is mocked: getJazzContext returns a real
// db-less orchestrator. applySnapshot, the snapshot envelope and the fingerprint
// are the real implementations, so this exercises the actual seed → fulfilled
// read path that gives SSR HTML its rows.
const ctx = vi.hoisted(() => ({ manager: null as SubscriptionsOrchestrator | null }));

vi.mock("./context.svelte.js", () => ({
  getJazzContext: () => ({ db: null, session: null, manager: ctx.manager }),
}));

const { QuerySubscription } = await import("./use-all.svelte.js");

const APP_ID = "svelte-seed";
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
    _schema: SCHEMA,
    _rowType: {} as Todo,
    _build() {
      return JSON.stringify({ table: "todos", conditions: [], includes: {}, orderBy: [] });
    },
  };
}

describe("svelte/QuerySubscription — SSR seed", () => {
  it("reads the snapshot rows synchronously in the constructor", () => {
    ctx.manager = createDbLessOrchestrator(APP_ID);
    const query = makeQuery();
    const snapshot: DehydratedSnapshot = sealSnapshot({
      v: 1,
      appId: APP_ID,
      principalId: null,
      schemaFingerprint: computeSchemaFingerprint(SCHEMA),
      payload: {
        kind: "rows",
        entries: [
          { key: computeQueryKey(APP_ID, query), result: [{ id: "1", title: "from-snapshot" }] },
        ],
      },
    });

    let ref!: InstanceType<typeof QuerySubscription<Todo>>;
    const cleanup = $effect.root(() => {
      ref = new QuerySubscription(query, { snapshot });
    });

    // The constructor seeds and reads the fulfilled entry before any $effect runs
    // (none fire on the server), so the rows are in the SSR HTML and first paint.
    expect(ref.current).toEqual([{ id: "1", title: "from-snapshot" }]);
    expect(ref.loading).toBe(false);

    cleanup();
  });

  it("discards a snapshot whose schema fingerprint differs from the query's", () => {
    ctx.manager = createDbLessOrchestrator(APP_ID);
    const query = makeQuery();
    const snapshot: DehydratedSnapshot = sealSnapshot({
      v: 1,
      appId: APP_ID,
      principalId: null,
      schemaFingerprint: "a-different-build",
      payload: {
        kind: "rows",
        entries: [
          { key: computeQueryKey(APP_ID, query), result: [{ id: "1", title: "from-snapshot" }] },
        ],
      },
    });

    let ref!: InstanceType<typeof QuerySubscription<Todo>>;
    const cleanup = $effect.root(() => {
      ref = new QuerySubscription(query, { snapshot });
    });

    expect(ref.current).toBeUndefined();

    cleanup();
  });
});

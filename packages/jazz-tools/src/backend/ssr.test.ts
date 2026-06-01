import { describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import type { Session } from "../runtime/context.js";
import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { computeQueryKey } from "../subscriptions-orchestrator.js";
import { computeSchemaFingerprint } from "../drivers/schema-wire.js";
import { createSnapshotBuilder } from "./ssr.js";

type Todo = { id: string; title: string };

function makeQuery(table = "todos"): QueryBuilder<Todo> {
  return {
    _table: table,
    _schema: {},
    _rowType: {} as Todo,
    _build() {
      return JSON.stringify({ table, conditions: [], includes: {}, orderBy: [] });
    },
  };
}

type FakeDb = {
  subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: (delta: SubscriptionDelta<T>) => void,
    options?: QueryOptions,
    session?: Session,
  ): () => void;
};

function makeFakeDb(payloadsByTable: Record<string, Todo[]>): {
  db: FakeDb;
  unsubscribes: ReturnType<typeof vi.fn>[];
} {
  const unsubscribes: ReturnType<typeof vi.fn>[] = [];
  const db: FakeDb = {
    subscribeAll<T extends { id: string }>(
      query: QueryBuilder<T>,
      callback: (delta: SubscriptionDelta<T>) => void,
    ) {
      const unsubscribe = vi.fn();
      unsubscribes.push(unsubscribe);
      queueMicrotask(() => {
        const all = (payloadsByTable[query._table] ?? []) as unknown as T[];
        callback({ all, delta: [] });
      });
      return unsubscribe;
    },
  };
  return { db, unsubscribes };
}

const TEST_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "id", column_type: { type: "Text" }, nullable: false },
      { name: "title", column_type: { type: "Text" }, nullable: false },
    ],
  },
};

describe("createSnapshotBuilder", () => {
  it("prefetches a query and dehydrates a snapshot keyed for the client orchestrator", async () => {
    const { db, unsubscribes } = makeFakeDb({
      todos: [{ id: "1", title: "from-server" }],
    });
    const builder = createSnapshotBuilder({
      appId: "test-app",
      schema: TEST_SCHEMA,
      principalId: "user-1",
    });

    const query = makeQuery();
    await builder.prefetch(db as any, query);

    const snapshot = builder.dehydrate();

    expect(snapshot.appId).toBe("test-app");
    expect(snapshot.principalId).toBe("user-1");
    expect(snapshot.schemaFingerprint).toBe(computeSchemaFingerprint(TEST_SCHEMA));
    expect(snapshot.entries).toHaveLength(1);
    expect(snapshot.entries[0]).toEqual({
      key: computeQueryKey("test-app", query),
      result: [{ id: "1", title: "from-server" }],
    });
    expect(unsubscribes[0]).toHaveBeenCalledTimes(1);
  });

  it("forwards options through to subscribeAll and embeds them in the key", async () => {
    const { db } = makeFakeDb({ todos: [] });
    const subscribeAll = vi.spyOn(db, "subscribeAll");
    const builder = createSnapshotBuilder({
      appId: "opts-app",
      schema: TEST_SCHEMA,
    });
    const query = makeQuery();
    const options: QueryOptions = { tier: "edge" };

    await builder.prefetch(db as any, query, options);

    expect(subscribeAll).toHaveBeenCalledWith(query, expect.any(Function), options, undefined);
    const snapshot = builder.dehydrate();
    expect(snapshot.entries[0]?.key).toBe(computeQueryKey("opts-app", query, options));
  });

  it("dedupes repeat prefetches by overwriting earlier results for the same key", async () => {
    const fakeFirst = makeFakeDb({ todos: [{ id: "1", title: "first" }] });
    const fakeSecond = makeFakeDb({ todos: [{ id: "1", title: "second" }] });
    const builder = createSnapshotBuilder({ appId: "dedupe", schema: TEST_SCHEMA });
    const query = makeQuery();

    await builder.prefetch(fakeFirst.db as any, query);
    await builder.prefetch(fakeSecond.db as any, query);

    const snapshot = builder.dehydrate();
    expect(snapshot.entries).toHaveLength(1);
    expect(snapshot.entries[0]?.result).toEqual([{ id: "1", title: "second" }]);
  });

  it("uses null principalId when none is provided", async () => {
    const { db } = makeFakeDb({ todos: [] });
    const builder = createSnapshotBuilder({ appId: "anon", schema: TEST_SCHEMA });
    await builder.prefetch(db as any, makeQuery());

    expect(builder.dehydrate().principalId).toBeNull();
  });

  it("accepts an App-like value (with .wasmSchema) and fingerprints the same as the raw schema", async () => {
    const app = { wasmSchema: TEST_SCHEMA };
    const { db } = makeFakeDb({ todos: [] });
    const builder = createSnapshotBuilder({ appId: "app-shape", schema: app });

    await builder.prefetch(db as any, makeQuery());

    expect(builder.dehydrate().schemaFingerprint).toBe(computeSchemaFingerprint(TEST_SCHEMA));
  });

  it("rejects prefetch if subscribeAll throws synchronously", async () => {
    const db: FakeDb = {
      subscribeAll() {
        throw new Error("boom");
      },
    };
    const builder = createSnapshotBuilder({ appId: "err", schema: TEST_SCHEMA });

    await expect(builder.prefetch(db as any, makeQuery())).rejects.toThrow("boom");
    expect(builder.dehydrate().entries).toHaveLength(0);
  });
});

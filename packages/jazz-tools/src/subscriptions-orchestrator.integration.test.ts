import { describe, expect, it } from "vitest";
import type { WasmSchema } from "./drivers/types.js";
import { createDb, type Db, type QueryBuilder, type TableProxy } from "./runtime/db.js";
import { applySubscriptionDelta, type SubscriptionDelta } from "./runtime/subscription-manager.js";
import { SubscriptionsOrchestrator } from "./subscriptions-orchestrator.js";

type Todo = {
  id: string;
  title: string;
  done: boolean;
};

type TodoInit = {
  title: string;
  done: boolean;
};

const schema: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

const todosTable: TableProxy<Todo, TodoInit> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as TodoInit,
};

const allTodosQuery: QueryBuilder<Todo> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _build() {
    return JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [],
    });
  },
};

async function waitForCondition(
  condition: () => boolean,
  timeoutMs: number,
  timeoutMessage: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (condition()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(timeoutMessage);
}

async function withRealManager<T>(
  label: string,
  run: (input: { appId: string; db: Db; manager: SubscriptionsOrchestrator }) => Promise<T>,
): Promise<T> {
  const appId = `orchestrator-int-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const db = await createDb({ appId });
  const manager = new SubscriptionsOrchestrator({ appId }, db);

  try {
    return await run({ appId, db, manager });
  } finally {
    await manager.shutdown();
    await db.shutdown();
  }
}

function reduceTodoDelta(current: Todo[], delta: SubscriptionDelta<Todo>): Todo[] {
  return [...applySubscriptionDelta(current, delta)];
}

describe("SubscriptionsOrchestrator integration coverage", () => {
  it("SO-I01 first mutation causes first fulfilled cache snapshot", async () => {
    await withRealManager("i01", async ({ manager, db }) => {
      const key = manager.makeQueryKey(allTodosQuery);
      const entry = manager.getCacheEntry<Todo>(key);
      const observedSnapshots: Todo[][] = [];
      const current: Todo[] = [];
      const unsubscribe = entry.subscribe({
        onfulfilled(data) {
          current.splice(0, current.length, ...data);
          observedSnapshots.push(data);
        },
        onDelta(delta) {
          observedSnapshots.push(reduceTodoDelta(current, delta));
        },
      });
      const { value: inserted } = await db.insert(todosTable, { title: "first", done: false });

      await waitForCondition(
        () => observedSnapshots.some((snapshot) => snapshot.some((row) => row.id === inserted.id)),
        5_000,
        "SO-I01 expected snapshot to include inserted row",
      );

      const latest = observedSnapshots[observedSnapshots.length - 1];
      expect(latest).toBeDefined();
      expect(latest!.some((row) => row.id === inserted.id)).toBe(true);
      expect(latest!.some((row) => row.title === "first")).toBe(true);
      expect(entry.status).toBe("fulfilled");

      unsubscribe();
    });
  });

  it("SO-I02 real delta ordering is reflected in entry all state", async () => {
    await withRealManager("i02", async ({ manager, db }) => {
      const key = manager.makeQueryKey(allTodosQuery);
      const entry = manager.getCacheEntry<Todo>(key);
      const snapshots: string[][] = [];
      const mismatches: Array<{ deltaOrder: string[]; stateOrder: string[] }> = [];
      const current: Todo[] = [];

      const recordSnapshot = (rows: Todo[]) => {
        const deltaOrder = rows.map((row) => row.title);
        snapshots.push(deltaOrder);
        if (entry.state.status !== "fulfilled") {
          return;
        }
        const stateOrder = entry.state.data.map((row) => row.title);
        if (JSON.stringify(stateOrder) !== JSON.stringify(deltaOrder)) {
          mismatches.push({ deltaOrder, stateOrder });
        }
      };

      const unsubscribe = entry.subscribe({
        onfulfilled(data) {
          current.splice(0, current.length, ...data);
          recordSnapshot(data);
        },
        onDelta(delta) {
          recordSnapshot(reduceTodoDelta(current, delta));
        },
      });

      await db.insert(todosTable, { title: "alpha", done: false });
      await waitForCondition(
        () => snapshots.some((rows) => rows.length === 1),
        5_000,
        "SO-I02 expected first snapshot",
      );

      await db.insert(todosTable, { title: "beta", done: false });
      await waitForCondition(
        () => snapshots.some((rows) => rows.length === 2),
        5_000,
        "SO-I02 expected second snapshot",
      );

      await db.insert(todosTable, { title: "gamma", done: false });
      await waitForCondition(
        () => snapshots.some((rows) => rows.length === 3),
        5_000,
        "SO-I02 expected third snapshot",
      );

      expect(mismatches).toEqual([]);
      expect(snapshots[snapshots.length - 1]).toHaveLength(3);

      unsubscribe();
    });
  });

  it("SO-I03 multiple listeners receive updates for same cache key", async () => {
    await withRealManager("i03", async ({ manager, db }) => {
      const key = manager.makeQueryKey(allTodosQuery);
      const entry = manager.getCacheEntry<Todo>(key);
      const listenerA: string[][] = [];
      const listenerB: string[][] = [];
      const currentA: Todo[] = [];
      const currentB: Todo[] = [];

      const offA = entry.subscribe({
        onfulfilled(data) {
          currentA.splice(0, currentA.length, ...data);
          listenerA.push(data.map((row) => row.id));
        },
        onDelta(delta) {
          listenerA.push(reduceTodoDelta(currentA, delta).map((row) => row.id));
        },
      });
      const offB = entry.subscribe({
        onfulfilled(data) {
          currentB.splice(0, currentB.length, ...data);
          listenerB.push(data.map((row) => row.id));
        },
        onDelta(delta) {
          listenerB.push(reduceTodoDelta(currentB, delta).map((row) => row.id));
        },
      });

      const { value: inserted } = await db.insert(todosTable, { title: "shared", done: false });
      await waitForCondition(
        () =>
          listenerA.some((rows) => rows.includes(inserted.id)) &&
          listenerB.some((rows) => rows.includes(inserted.id)),
        5_000,
        "SO-I03 expected both listeners to receive update",
      );

      expect(listenerA[listenerA.length - 1]).toEqual([inserted.id]);
      expect(listenerB[listenerB.length - 1]).toEqual([inserted.id]);

      offA();
      offB();
    });
  });

  it("SO-I04 unsubscribing one listener keeps shared subscription alive for others", async () => {
    await withRealManager("i04", async ({ manager, db }) => {
      const key = manager.makeQueryKey(allTodosQuery);
      const entry = manager.getCacheEntry<Todo>(key);
      const listenerA: string[][] = [];
      const listenerB: string[][] = [];
      const currentA: Todo[] = [];
      const currentB: Todo[] = [];

      const offA = entry.subscribe({
        onfulfilled(data) {
          currentA.splice(0, currentA.length, ...data);
          listenerA.push(data.map((row) => row.title));
        },
        onDelta(delta) {
          listenerA.push(reduceTodoDelta(currentA, delta).map((row) => row.title));
        },
      });
      const offB = entry.subscribe({
        onfulfilled(data) {
          currentB.splice(0, currentB.length, ...data);
          listenerB.push(data.map((row) => row.title));
        },
        onDelta(delta) {
          listenerB.push(reduceTodoDelta(currentB, delta).map((row) => row.title));
        },
      });

      offA();
      await db.insert(todosTable, { title: "remaining-1", done: false });
      await db.insert(todosTable, { title: "remaining-2", done: true });

      await waitForCondition(
        () => listenerB.some((rows) => rows.length === 2),
        5_000,
        "SO-I04 expected remaining listener updates",
      );

      expect(listenerA).toEqual([[]]);
      expect([...(listenerB[listenerB.length - 1] ?? [])].sort()).toEqual([
        "remaining-1",
        "remaining-2",
      ]);

      offB();
    });
  });

  it("SO-I05 manager shutdown after active subscriptions is clean", async () => {
    await withRealManager("i05", async ({ manager, db }) => {
      const key = manager.makeQueryKey(allTodosQuery);
      const entry = manager.getCacheEntry<Todo>(key);
      const events: number[] = [];
      const current: Todo[] = [];

      entry.subscribe({
        onfulfilled(data) {
          current.splice(0, current.length, ...data);
          events.push(data.length);
        },
        onDelta(delta) {
          events.push(reduceTodoDelta(current, delta).length);
        },
      });

      await db.insert(todosTable, { title: "before-shutdown", done: false });
      await waitForCondition(
        () => events.length > 0,
        5_000,
        "SO-I05 expected an active subscription before shutdown",
      );

      await expect(manager.shutdown()).resolves.toBeUndefined();
    });
  });
});

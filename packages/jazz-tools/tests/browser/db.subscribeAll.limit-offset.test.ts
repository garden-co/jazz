import { describe, expect, it } from "vitest";
import { createDb, type Db, type QueryBuilder, type TableProxy } from "../../src/runtime/db.js";
import type { SubscriptionDelta } from "../../src/runtime/subscription-manager.js";
import type { WasmSchema } from "../../src/drivers/types.js";

interface Todo {
  id: string;
  title: string;
  rank: number;
  done: boolean;
}

const schema: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "rank", column_type: { type: "Integer" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

const todos: TableProxy<Todo, Omit<Todo, "id">> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as Omit<Todo, "id">,
};

function makeTodosQuery(body: {
  orderBy?: Array<[string, "asc" | "desc"]>;
  limit?: number;
  offset?: number;
}): QueryBuilder<Todo> {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: {} as Todo,
    _build() {
      return JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: body.orderBy ?? [],
        limit: body.limit,
        offset: body.offset,
      });
    },
  };
}

const windowQuery = makeTodosQuery({ orderBy: [["rank", "asc"]], offset: 1, limit: 2 });

function uniqueDbName(label: string): string {
  return `db-subscribe-all-limit-offset-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function waitForCondition(
  check: () => boolean,
  timeoutMs: number,
  errorMessage: string,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (check()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(errorMessage);
}

async function withDb<T>(label: string, run: (db: Db) => Promise<T>): Promise<T> {
  const db = await createDb({ appId: uniqueDbName(label), dbName: uniqueDbName(label) });
  try {
    return await run(db);
  } finally {
    await db.shutdown();
  }
}

function latestRows(deltas: Array<SubscriptionDelta<Todo>>): Todo[] {
  return deltas[deltas.length - 1]?.all ?? [];
}

function latestIds(deltas: Array<SubscriptionDelta<Todo>>): string[] {
  return latestRows(deltas).map((row) => row.id);
}

describe("db.subscribeAll limit+offset browser integration", () => {
  it("moves a row into the window and pushes one out when deleting before the window", async () => {
    await withDb("delete-before-window", async (db) => {
      const deltas: Array<SubscriptionDelta<Todo>> = [];
      const unsubscribe = db.subscribeAll(windowQuery, (delta) => {
        deltas.push(delta);
      });

      try {
        const idA = await db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = await db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = await db.insert(todos, { title: "C", rank: 3, done: false });
        const idD = await db.insert(todos, { title: "D", rank: 4, done: false });

        await waitForCondition(
          () => latestRows(deltas).length === 2,
          10_000,
          "expected initial window rows",
        );
        expect(latestIds(deltas)).toEqual([idB, idC]);

        await db.deleteFrom(todos, idA);

        await waitForCondition(
          () => {
            const ids = latestIds(deltas);
            return ids.length === 2 && ids[0] === idC && ids[1] === idD;
          },
          10_000,
          "expected window to shift after delete before offset",
        );
      } finally {
        unsubscribe();
      }
    });
  });

  it("moves a row out of the window and pulls one in when inserting before the window", async () => {
    await withDb("insert-before-window", async (db) => {
      const deltas: Array<SubscriptionDelta<Todo>> = [];
      const unsubscribe = db.subscribeAll(windowQuery, (delta) => {
        deltas.push(delta);
      });

      try {
        const idA = await db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = await db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = await db.insert(todos, { title: "C", rank: 3, done: false });

        await waitForCondition(
          () => latestRows(deltas).length === 2,
          10_000,
          "expected initial window rows",
        );
        expect(latestIds(deltas)).toEqual([idB, idC]);

        await db.insert(todos, { title: "X", rank: 0, done: false });

        await waitForCondition(
          () => {
            const ids = latestIds(deltas);
            return ids.length === 2 && ids[0] === idA && ids[1] === idB;
          },
          10_000,
          "expected window to shift after insert before offset",
        );
      } finally {
        unsubscribe();
      }
    });
  });

  it("moves an in-window row out when its sort value moves before the window", async () => {
    await withDb("update-in-window-to-before-window", async (db) => {
      const deltas: Array<SubscriptionDelta<Todo>> = [];
      const unsubscribe = db.subscribeAll(windowQuery, (delta) => {
        deltas.push(delta);
      });

      try {
        const idA = await db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = await db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = await db.insert(todos, { title: "C", rank: 3, done: false });
        const idD = await db.insert(todos, { title: "D", rank: 4, done: false });

        await waitForCondition(
          () => latestRows(deltas).length === 2,
          10_000,
          "expected initial window rows",
        );
        expect(latestIds(deltas)).toEqual([idB, idC]);

        await db.update(todos, idB, { rank: 0 });

        await waitForCondition(
          () => {
            const ids = latestIds(deltas);
            return ids.length === 2 && ids[0] === idA && ids[1] === idC;
          },
          10_000,
          "expected in-window row to move out when rank crosses offset boundary",
        );
        expect(latestIds(deltas)).toEqual([idA, idC]);
        expect(latestRows(deltas).some((row) => row.id === idD)).toBe(false);
      } finally {
        unsubscribe();
      }
    });
  });

  it("moves an out-of-window row in when its sort value crosses into the window", async () => {
    await withDb("update-outside-window-to-inside", async (db) => {
      const deltas: Array<SubscriptionDelta<Todo>> = [];
      const unsubscribe = db.subscribeAll(windowQuery, (delta) => {
        deltas.push(delta);
      });

      try {
        await db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = await db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = await db.insert(todos, { title: "C", rank: 3, done: false });
        const idD = await db.insert(todos, { title: "D", rank: 4, done: false });

        await waitForCondition(
          () => latestRows(deltas).length === 2,
          10_000,
          "expected initial window rows",
        );
        expect(latestIds(deltas)).toEqual([idB, idC]);

        await db.update(todos, idD, { rank: 2 });

        await waitForCondition(
          () => {
            const ids = latestIds(deltas);
            return ids.length === 2 && ids[0] === idB && ids[1] === idD;
          },
          10_000,
          "expected out-of-window row to move into window after rank update",
        );
      } finally {
        unsubscribe();
      }
    });
  });
});

// ============================================================================
// Windowed index scan: limit+offset sorted by id
//
// When ORDER BY id (ASC or DESC) with limit, the query engine pushes
// pagination down to the storage index scan. These tests exercise that
// code path (and the non-windowed path for comparison).
// ============================================================================

describe("db.subscribeAll limit+offset sorted by id (windowed index scan)", () => {
  const idAscQuery = makeTodosQuery({ orderBy: [["id", "asc"]], offset: 1, limit: 2 });

  it("returns the correct page of rows sorted by id ASC", async () => {
    await withDb("id-asc-page", async (db) => {
      const deltas: Array<SubscriptionDelta<Todo>> = [];
      const unsubscribe = db.subscribeAll(idAscQuery, (delta) => {
        deltas.push(delta);
      });

      try {
        const idA = await db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = await db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = await db.insert(todos, { title: "C", rank: 3, done: false });
        const idD = await db.insert(todos, { title: "D", rank: 4, done: false });

        const sorted = [idA, idB, idC, idD].sort();

        await waitForCondition(
          () => latestRows(deltas).length === 2,
          10_000,
          "expected windowed page with 2 rows",
        );
        expect(latestIds(deltas)).toEqual([sorted[1], sorted[2]]);
      } finally {
        unsubscribe();
      }
    });
  });

  it("shifts window when deleting a row before the offset", async () => {
    await withDb("id-asc-delete-before", async (db) => {
      const deltas: Array<SubscriptionDelta<Todo>> = [];
      const unsubscribe = db.subscribeAll(idAscQuery, (delta) => {
        deltas.push(delta);
      });

      try {
        const idA = await db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = await db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = await db.insert(todos, { title: "C", rank: 3, done: false });
        const idD = await db.insert(todos, { title: "D", rank: 4, done: false });

        const sorted = [idA, idB, idC, idD].sort();

        await waitForCondition(
          () => latestRows(deltas).length === 2,
          10_000,
          "expected initial windowed page",
        );
        expect(latestIds(deltas)).toEqual([sorted[1], sorted[2]]);

        // Delete the first row in id order (before the window)
        await db.deleteFrom(todos, sorted[0]);

        await waitForCondition(
          () => {
            const ids = latestIds(deltas);
            return ids.length === 2 && ids[0] === sorted[2] && ids[1] === sorted[3];
          },
          10_000,
          "expected window to shift after deleting row before offset",
        );
      } finally {
        unsubscribe();
      }
    });
  });

  it("shifts window when inserting a row that sorts into the window", async () => {
    await withDb("id-asc-insert", async (db) => {
      const deltas: Array<SubscriptionDelta<Todo>> = [];
      const unsubscribe = db.subscribeAll(idAscQuery, (delta) => {
        deltas.push(delta);
      });

      try {
        const idA = await db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = await db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = await db.insert(todos, { title: "C", rank: 3, done: false });

        const sorted = [idA, idB, idC].sort();

        await waitForCondition(
          () => latestRows(deltas).length === 2,
          10_000,
          "expected initial windowed page",
        );
        expect(latestIds(deltas)).toEqual([sorted[1], sorted[2]]);

        const idD = await db.insert(todos, { title: "D", rank: 4, done: false });

        const newSorted = [idA, idB, idC, idD].sort();

        await waitForCondition(
          () => {
            const ids = latestIds(deltas);
            return ids.length === 2 && ids[0] === newSorted[1] && ids[1] === newSorted[2];
          },
          10_000,
          "expected window to reflect new sort order after insert",
        );
      } finally {
        unsubscribe();
      }
    });
  });

  it("returns the correct page of rows sorted by id DESC", async () => {
    const idDescQuery = makeTodosQuery({ orderBy: [["id", "desc"]], offset: 1, limit: 2 });

    await withDb("id-desc-page", async (db) => {
      const deltas: Array<SubscriptionDelta<Todo>> = [];
      const unsubscribe = db.subscribeAll(idDescQuery, (delta) => {
        deltas.push(delta);
      });

      try {
        const idA = await db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = await db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = await db.insert(todos, { title: "C", rank: 3, done: false });
        const idD = await db.insert(todos, { title: "D", rank: 4, done: false });

        // DESC: reversed id order, offset 1, limit 2 → 2nd and 3rd from the end
        const reversed = [idA, idB, idC, idD].sort().reverse();

        await waitForCondition(
          () => latestRows(deltas).length === 2,
          10_000,
          "expected windowed page with 2 rows (DESC)",
        );
        expect(latestIds(deltas)).toEqual([reversed[1], reversed[2]]);
      } finally {
        unsubscribe();
      }
    });
  });

  it("shifts DESC window when deleting a row within the window", async () => {
    const idDescQuery = makeTodosQuery({ orderBy: [["id", "desc"]], offset: 1, limit: 2 });

    await withDb("id-desc-delete-in-window", async (db) => {
      const deltas: Array<SubscriptionDelta<Todo>> = [];
      const unsubscribe = db.subscribeAll(idDescQuery, (delta) => {
        deltas.push(delta);
      });

      try {
        const idA = await db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = await db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = await db.insert(todos, { title: "C", rank: 3, done: false });
        const idD = await db.insert(todos, { title: "D", rank: 4, done: false });
        const idE = await db.insert(todos, { title: "E", rank: 5, done: false });

        // DESC reversed: [highest..lowest], offset 1, limit 2
        const reversed = [idA, idB, idC, idD, idE].sort().reverse();

        await waitForCondition(
          () => latestRows(deltas).length === 2,
          10_000,
          "expected initial windowed page (DESC)",
        );
        expect(latestIds(deltas)).toEqual([reversed[1], reversed[2]]);

        // Delete the first row in the window
        await db.deleteFrom(todos, reversed[1]);

        const remaining = [idA, idB, idC, idD, idE]
          .filter((id) => id !== reversed[1])
          .sort()
          .reverse();

        await waitForCondition(
          () => {
            const ids = latestIds(deltas);
            return ids.length === 2 && ids[0] === remaining[1] && ids[1] === remaining[2];
          },
          10_000,
          "expected DESC window to shift after deleting row in window",
        );
      } finally {
        unsubscribe();
      }
    });
  });
});

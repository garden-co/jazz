import { describe, expect, it } from "vitest";
import { createDb, type Db, type QueryBuilder, type TableProxy } from "../../src/runtime/db.js";
import type { SubscriptionDelta } from "../../src/runtime/subscription-manager.js";
import type { WasmSchema } from "../../src/drivers/types.js";

interface Todo {
  id: string;
  title: string;
  rank?: number;
  done: boolean;
}

const schema: WasmSchema = {
  tables: {
    todos: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "rank", column_type: { type: "Integer" }, nullable: true },
        { name: "done", column_type: { type: "Boolean" }, nullable: false },
      ],
    },
  },
};

const todos: TableProxy<Todo, Omit<Todo, "id">> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as Omit<Todo, "id">,
};

function makeTodosQuery(body: {
  conditions?: Array<{ column: string; op: string; value?: unknown }>;
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
        conditions: body.conditions ?? [],
        includes: {},
        orderBy: body.orderBy ?? [],
        limit: body.limit,
        offset: body.offset,
      });
    },
  };
}

function uniqueDbName(label: string): string {
  return `db-subscribe-all-limit-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function waitForCondition(
  check: () => boolean,
  timeoutMs: number,
  errorMessage: string,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (check()) {
      return;
    }
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

function latestRows(snapshots: Todo[][]): Todo[] {
  return snapshots[snapshots.length - 1] ?? [];
}

function latestIds(snapshots: Todo[][]): string[] {
  return latestRows(snapshots).map((row) => row.id);
}

function expectUniqueIds(rows: Todo[]): void {
  const ids = rows.map((row) => row.id);
  expect(new Set(ids).size).toBe(ids.length);
}

describe("db.subscribeAll limit browser integration", () => {
  it("applies where filtering before id windowing (initial load + update)", async () => {
    await withDb("where-before-id-window", async (db) => {
      let latest: Todo[] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({
          conditions: [{ column: "done", op: "eq", value: true }],
          orderBy: [["id", "asc"]],
          limit: 5,
        }),
        (delta: SubscriptionDelta<Todo>) => {
          latest = delta.all;
        },
      );

      try {
        const falseIds: string[] = [];
        const trueIds: string[] = [];
        for (let i = 0; i < 10; i += 1) {
          falseIds.push(db.insert(todos, { title: `F-${i}`, rank: i, done: false }));
        }
        for (let i = 0; i < 10; i += 1) {
          trueIds.push(db.insert(todos, { title: `T-${i}`, rank: i, done: true }));
        }

        const expectedInitial = trueIds.toSorted((a, b) => a.localeCompare(b)).slice(0, 5);
        await waitForCondition(
          () =>
            latest.length === 5 &&
            latest.every((row) => row.done) &&
            latest.map((row) => row.id).every((id, i) => id === expectedInitial[i]),
          10_000,
          "expected top-5 matching rows after where filter (not window over all rows)",
        );

        expect(latest.map((row) => row.id)).toEqual(expectedInitial);

        const promoteId = falseIds[0]!;
        db.update(todos, promoteId, { done: true, title: "promoted-to-true" });

        const expectedAfterUpdate = [...trueIds, promoteId]
          .toSorted((a, b) => a.localeCompare(b))
          .slice(0, 5);
        await waitForCondition(
          () =>
            latest.length === 5 &&
            latest.every((row) => row.done) &&
            latest.map((row) => row.id).every((id, i) => id === expectedAfterUpdate[i]),
          10_000,
          "expected where-filtered top-5 window to refresh after update",
        );

        expect(latest.map((row) => row.id)).toEqual(expectedAfterUpdate);
      } finally {
        unsubscribe();
      }
    });
  });

  it("applies limit to initial sorted snapshot", async () => {
    await withDb("initial-limit", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({ orderBy: [["rank", "asc"]], limit: 2 }),
        (delta: SubscriptionDelta<Todo>) => {
          snapshots.push(delta.all);
        },
      );

      try {
        const idA = db.insert(todos, { title: "A", rank: 10, done: false });
        const idB = db.insert(todos, { title: "B", rank: 20, done: false });
        db.insert(todos, { title: "C", rank: 30, done: false });

        await waitForCondition(
          () => latestRows(snapshots).length === 2,
          10_000,
          "expected first limited window",
        );

        expect(latestIds(snapshots)).toEqual([idA, idB]);
        expectUniqueIds(latestRows(snapshots));
      } finally {
        unsubscribe();
      }
    });
  });

  it("shifts window when inserting before limit boundary", async () => {
    await withDb("insert-before-boundary", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({ orderBy: [["rank", "asc"]], limit: 2 }),
        (delta: SubscriptionDelta<Todo>) => {
          snapshots.push(delta.all);
        },
      );

      try {
        const idA = db.insert(todos, { title: "A", rank: 10, done: false });
        const idB = db.insert(todos, { title: "B", rank: 20, done: false });
        db.insert(todos, { title: "C", rank: 30, done: false });

        await waitForCondition(
          () => latestIds(snapshots).length === 2,
          10_000,
          "expected initial limited rows",
        );
        expect(latestIds(snapshots)).toEqual([idA, idB]);

        const idZ = db.insert(todos, { title: "Z", rank: 5, done: false });
        await waitForCondition(
          () => {
            const ids = latestIds(snapshots);
            return ids.length === 2 && ids[0] === idZ && ids[1] === idA;
          },
          10_000,
          "expected window shift after low-rank insert",
        );
      } finally {
        unsubscribe();
      }
    });
  });

  it("does not change window when inserting after limit boundary", async () => {
    await withDb("insert-after-boundary", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({ orderBy: [["rank", "asc"]], limit: 2 }),
        (delta: SubscriptionDelta<Todo>) => {
          snapshots.push(delta.all);
        },
      );

      try {
        const idA = db.insert(todos, { title: "A", rank: 10, done: false });
        const idB = db.insert(todos, { title: "B", rank: 20, done: false });
        db.insert(todos, { title: "C", rank: 30, done: false });

        await waitForCondition(
          () => latestIds(snapshots).length === 2,
          10_000,
          "expected initial limited rows",
        );
        expect(latestIds(snapshots)).toEqual([idA, idB]);

        db.insert(todos, { title: "D", rank: 40, done: false });
        await new Promise((resolve) => setTimeout(resolve, 200));
        expect(latestIds(snapshots)).toEqual([idA, idB]);
      } finally {
        unsubscribe();
      }
    });
  });

  it("backfills from next row when deleting inside the limited window", async () => {
    await withDb("delete-inside-window", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({ orderBy: [["rank", "asc"]], limit: 2 }),
        (delta: SubscriptionDelta<Todo>) => {
          snapshots.push(delta.all);
        },
      );

      try {
        const idA = db.insert(todos, { title: "A", rank: 10, done: false });
        const idB = db.insert(todos, { title: "B", rank: 20, done: false });
        const idC = db.insert(todos, { title: "C", rank: 30, done: false });

        await waitForCondition(
          () => latestIds(snapshots).length === 2,
          10_000,
          "expected initial limited rows",
        );
        expect(latestIds(snapshots)).toEqual([idA, idB]);

        db.deleteFrom(todos, idB);
        await waitForCondition(
          () => {
            const ids = latestIds(snapshots);
            return ids.length === 2 && ids[0] === idA && ids[1] === idC;
          },
          10_000,
          "expected backfill after deleting visible row",
        );
      } finally {
        unsubscribe();
      }
    });
  });

  it("shifts offset window when deleting before the window", async () => {
    await withDb("delete-before-offset-window", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({ orderBy: [["rank", "asc"]], offset: 1, limit: 2 }),
        (delta: SubscriptionDelta<Todo>) => {
          snapshots.push(delta.all);
        },
      );

      try {
        const idA = db.insert(todos, { title: "A", rank: 10, done: false });
        const idB = db.insert(todos, { title: "B", rank: 20, done: false });
        const idC = db.insert(todos, { title: "C", rank: 30, done: false });
        const idD = db.insert(todos, { title: "D", rank: 40, done: false });

        await waitForCondition(
          () => latestIds(snapshots).length === 2,
          10_000,
          "expected initial offset window",
        );
        expect(latestIds(snapshots)).toEqual([idB, idC]);

        db.deleteFrom(todos, idA);
        await waitForCondition(
          () => {
            const ids = latestIds(snapshots);
            return ids.length === 2 && ids[0] === idC && ids[1] === idD;
          },
          10_000,
          "expected offset window shift after deleting leading row",
        );
      } finally {
        unsubscribe();
      }
    });
  });

  it("moves rows into and out of a limited window on rank updates", async () => {
    await withDb("update-crosses-window-boundary", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({ orderBy: [["rank", "asc"]], limit: 2 }),
        (delta: SubscriptionDelta<Todo>) => {
          snapshots.push(delta.all);
        },
      );

      try {
        const idA = db.insert(todos, { title: "A", rank: 10, done: false });
        const idB = db.insert(todos, { title: "B", rank: 20, done: false });
        const idC = db.insert(todos, { title: "C", rank: 30, done: false });

        await waitForCondition(
          () => latestIds(snapshots).length === 2,
          10_000,
          "expected initial limited rows",
        );
        expect(latestIds(snapshots)).toEqual([idA, idB]);

        db.update(todos, idC, { rank: 15 });
        await waitForCondition(
          () => {
            const ids = latestIds(snapshots);
            return ids.length === 2 && ids[0] === idA && ids[1] === idC;
          },
          10_000,
          "expected row C to enter limited window",
        );

        db.update(todos, idA, { rank: 40 });
        await waitForCondition(
          () => {
            const ids = latestIds(snapshots);
            return ids.length === 2 && ids[0] === idC && ids[1] === idB;
          },
          10_000,
          "expected row A to leave limited window",
        );
      } finally {
        unsubscribe();
      }
    });
  });

  it("returns and keeps an empty result for limit 0", async () => {
    await withDb("zero-limit", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({ orderBy: [["rank", "asc"]], limit: 0 }),
        (delta: SubscriptionDelta<Todo>) => {
          snapshots.push(delta.all);
        },
      );

      try {
        await waitForCondition(
          () => snapshots.length > 0,
          10_000,
          "expected initial callback for zero-limit query",
        );
        expect(latestRows(snapshots)).toEqual([]);

        db.insert(todos, { title: "A", rank: 10, done: false });
        db.insert(todos, { title: "B", rank: 20, done: false });
        await new Promise((resolve) => setTimeout(resolve, 200));

        expect(latestRows(snapshots)).toEqual([]);
      } finally {
        unsubscribe();
      }
    });
  });

  it("handles offset greater than result size and fills once enough rows exist", async () => {
    await withDb("offset-greater-than-size", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({ orderBy: [["rank", "asc"]], offset: 3, limit: 2 }),
        (delta: SubscriptionDelta<Todo>) => {
          snapshots.push(delta.all);
        },
      );

      try {
        db.insert(todos, { title: "A", rank: 10, done: false });
        db.insert(todos, { title: "B", rank: 20, done: false });
        db.insert(todos, { title: "C", rank: 30, done: false });

        await waitForCondition(
          () => snapshots.length > 0,
          10_000,
          "expected callback for empty offset window",
        );
        expect(latestRows(snapshots)).toEqual([]);

        const idD = db.insert(todos, { title: "D", rank: 40, done: false });
        await waitForCondition(
          () => latestIds(snapshots).length === 1 && latestIds(snapshots)[0] === idD,
          10_000,
          "expected first row after crossing offset",
        );

        const idE = db.insert(todos, { title: "E", rank: 50, done: false });
        await waitForCondition(
          () => {
            const ids = latestIds(snapshots);
            return ids.length === 2 && ids[0] === idD && ids[1] === idE;
          },
          10_000,
          "expected full window after enough rows exist",
        );
      } finally {
        unsubscribe();
      }
    });
  });

  it("returns correct top-5 for orderBy id asc with 1k rows and stays correct on update", async () => {
    await withDb("id-asc-limit-5-1k", async (db) => {
      let latest: Todo[] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({ orderBy: [["id", "asc"]], limit: 5 }),
        (delta: SubscriptionDelta<Todo>) => {
          latest = delta.all;
        },
      );

      try {
        const insertedIds: string[] = [];
        for (let i = 0; i < 1_000; i += 1) {
          const id = db.insert(todos, { title: `T-${i}`, rank: i, done: false });
          insertedIds.push(id);
        }

        const expected = insertedIds.toSorted((a, b) => a.localeCompare(b)).slice(0, 5);
        await waitForCondition(
          () =>
            latest.length === 5 && latest.map((row) => row.id).every((id, i) => id === expected[i]),
          20_000,
          "expected initial top-5 rows by id asc",
        );

        expect(latest.map((row) => row.id)).toEqual(expected);
        expectUniqueIds(latest);

        const updatedId = expected[2]!;
        db.update(todos, updatedId, { title: "UPDATED-ASC" });

        await waitForCondition(
          () => latest.some((row) => row.id === updatedId && row.title === "UPDATED-ASC"),
          10_000,
          "expected updated row in asc top-5 window",
        );

        expect(latest.map((row) => row.id)).toEqual(expected);
      } finally {
        unsubscribe();
      }
    });
  });

  it("returns correct top-5 for orderBy id desc with 1k rows and stays correct on update", async () => {
    await withDb("id-desc-limit-5-1k", async (db) => {
      let latest: Todo[] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({ orderBy: [["id", "desc"]], limit: 5 }),
        (delta: SubscriptionDelta<Todo>) => {
          latest = delta.all;
        },
      );

      try {
        const insertedIds: string[] = [];
        for (let i = 0; i < 1_000; i += 1) {
          const id = db.insert(todos, { title: `T-${i}`, rank: i, done: false });
          insertedIds.push(id);
        }

        const expected = insertedIds.toSorted((a, b) => b.localeCompare(a)).slice(0, 5);
        await waitForCondition(
          () =>
            latest.length === 5 && latest.map((row) => row.id).every((id, i) => id === expected[i]),
          20_000,
          "expected initial top-5 rows by id desc",
        );

        expect(latest.map((row) => row.id)).toEqual(expected);
        expectUniqueIds(latest);

        const newId = db.insert(todos, { title: "Niu", rank: 1, done: false });
        insertedIds.push(newId);

        await waitForCondition(
          () => latest.some((row) => row.id === newId),
          10_000,
          "expected updated row in desc top-5 window",
        );

        const expectedAfterInsert = insertedIds.toSorted((a, b) => b.localeCompare(a)).slice(0, 5);
        expect(latest.map((row) => row.id)).toEqual(expectedAfterInsert);
      } finally {
        unsubscribe();
      }
    });
  });

  it("applies id-desc offset window (offset 5, limit 5) and shifts correctly on newest insert", async () => {
    await withDb("id-desc-offset-5-limit-5-1k", async (db) => {
      let latest: Todo[] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({ orderBy: [["id", "desc"]], offset: 5, limit: 5 }),
        (delta: SubscriptionDelta<Todo>) => {
          latest = delta.all;
        },
      );

      try {
        const insertedIds: string[] = [];
        for (let i = 0; i < 1_000; i += 1) {
          const id = db.insert(todos, { title: `T-${i}`, rank: i, done: false });
          insertedIds.push(id);
        }

        const expectedInitial = insertedIds.toSorted((a, b) => b.localeCompare(a)).slice(5, 10);
        await waitForCondition(
          () =>
            latest.length === 5 &&
            latest.map((row) => row.id).every((id, i) => id === expectedInitial[i]),
          20_000,
          "expected initial id-desc window with offset 5 and limit 5",
        );

        expect(latest.map((row) => row.id)).toEqual(expectedInitial);
        expectUniqueIds(latest);

        const newestId = db.insert(todos, { title: "Newest", rank: 1, done: false });
        insertedIds.push(newestId);

        const expectedAfterInsert = insertedIds.toSorted((a, b) => b.localeCompare(a)).slice(5, 10);
        await waitForCondition(
          () =>
            latest.length === 5 &&
            latest.map((row) => row.id).every((id, i) => id === expectedAfterInsert[i]),
          20_000,
          "expected id-desc offset window to shift after inserting newest id",
        );

        expect(latest.map((row) => row.id)).toEqual(expectedAfterInsert);
      } finally {
        unsubscribe();
      }
    });
  });

  it("loads non-empty id-desc offset window after restart with 1k persisted rows", async () => {
    const appId = uniqueDbName("id-desc-offset-restart-1k-app");
    const dbName = uniqueDbName("id-desc-offset-restart-1k-db");
    const insertedIds: string[] = [];
    const db = await createDb({ appId, dbName });

    try {
      for (let i = 0; i < 1_000; i += 1) {
        insertedIds.push(db.insert(todos, { title: `T-${i}`, rank: i, done: false }));
      }

      let latest: Todo[] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({ orderBy: [["id", "desc"]], offset: 10, limit: 10 }),
        (delta: SubscriptionDelta<Todo>) => {
          latest = delta.all;
        },
      );

      try {
        const expected = insertedIds.toSorted((a, b) => b.localeCompare(a)).slice(10, 20);
        await waitForCondition(
          () => latest.length > 0,
          5_000,
          "expected non-empty result for persisted id-desc offset window after restart",
        );

        expect(latest.length).toBe(10);
        await waitForCondition(
          () => latest.map((row) => row.id).every((id, i) => id === expected[i]),
          5_000,
          "expected persisted id-desc offset window (offset 10, limit 10) to load after restart",
        );

        expect(latest.map((row) => row.id)).toEqual(expected);
        expectUniqueIds(latest);
      } finally {
        unsubscribe();
      }
    } finally {
      await db.shutdown();
    }
  }, 5_000);

  it("uses deterministic id tie-break at the limit boundary", async () => {
    await withDb("tie-break-boundary", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({ orderBy: [["rank", "asc"]], limit: 2 }),
        (delta: SubscriptionDelta<Todo>) => {
          snapshots.push(delta.all);
        },
      );

      try {
        const idX = db.insert(todos, { title: "X", rank: 1, done: false });
        const idY = db.insert(todos, { title: "Y", rank: 1, done: false });
        const idZ = db.insert(todos, { title: "Z", rank: 2, done: false });

        await waitForCondition(
          () => latestIds(snapshots).length === 2,
          10_000,
          "expected initial two rows at tie boundary",
        );

        const expectedFirstTwo = [idX, idY].toSorted((a, b) => a.localeCompare(b));
        expect(latestIds(snapshots)).toEqual(expectedFirstTwo);

        db.update(todos, idZ, { rank: 1 });
        const newId = db.insert(todos, { title: "Niu", rank: 1, done: false });

        await waitForCondition(
          () => latestIds(snapshots).length === 2,
          10_000,
          "expected stable limited window after tie expansion",
        );

        const expectedAfterTie = [idX, idY, idZ, newId]
          .toSorted((a, b) => a.localeCompare(b))
          .slice(0, 2);
        expect(latestIds(snapshots)).toEqual(expectedAfterTie);
      } finally {
        unsubscribe();
      }
    });
  });
});

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

const sortedByRankAscQuery = makeTodosQuery({ orderBy: [["rank", "asc"]] });

function uniqueDbName(label: string): string {
  return `db-subscribe-all-sort-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
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

function latestIds(snapshots: Todo[][]): string[] {
  return (snapshots[snapshots.length - 1] ?? []).map((row) => row.id);
}

function latestRows(snapshots: Todo[][]): Todo[] {
  return snapshots[snapshots.length - 1] ?? [];
}

describe("db.subscribeAll sorting browser integration", () => {
  it("keeps order stable when updating a non-sort field", async () => {
    await withDb("stable-non-sort-update", async (db) => {
      const deltas: Array<SubscriptionDelta<Todo>> = [];
      const unsubscribe = db.subscribeAll(sortedByRankAscQuery, (delta) => {
        deltas.push(delta);
      });

      try {
        const idA = db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = db.insert(todos, { title: "C", rank: 3, done: false });

        await waitForCondition(
          () => deltas.some((delta) => delta.all.length === 3),
          10_000,
          "expected initial sorted snapshot",
        );

        const before = deltas[deltas.length - 1].all.map((row) => row.id);
        expect(before).toEqual([idA, idB, idC]);

        db.update(todos, idB, { title: "B-updated" });

        await waitForCondition(
          () => deltas.some((delta) => delta.updated.some((row) => row.id === idB)),
          10_000,
          "expected update delta for row B",
        );

        const after = deltas[deltas.length - 1].all.map((row) => row.id);
        expect(after).toEqual([idA, idB, idC]);
      } finally {
        unsubscribe();
      }
    });
  });

  it("reorders when updating the sort field", async () => {
    await withDb("move-on-sort-update", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(sortedByRankAscQuery, (delta) => {
        snapshots.push(delta.all);
      });

      try {
        const idA = db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = db.insert(todos, { title: "C", rank: 3, done: false });

        await waitForCondition(
          () => snapshots.some((rows) => rows.length === 3),
          10_000,
          "expected initial sorted rows",
        );

        db.update(todos, idA, { rank: 10 });

        await waitForCondition(
          () => {
            const latest = latestRows(snapshots);
            return latest.length === 3 && latest[2]?.id === idA;
          },
          10_000,
          "expected row A to move to end after rank update",
        );

        expect(latestIds(snapshots)).toEqual([idB, idC, idA]);
      } finally {
        unsubscribe();
      }
    });
  });

  it("preserves sorted order after removing a middle row", async () => {
    await withDb("remove-middle", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(sortedByRankAscQuery, (delta) => {
        snapshots.push(delta.all);
      });

      try {
        const idA = db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = db.insert(todos, { title: "C", rank: 3, done: false });

        await waitForCondition(
          () => snapshots.some((rows) => rows.length === 3),
          10_000,
          "expected initial sorted rows",
        );

        db.deleteFrom(todos, idB);

        await waitForCondition(
          () => {
            const latest = latestRows(snapshots);
            return latest.length === 2;
          },
          10_000,
          "expected one row removed",
        );

        expect(latestIds(snapshots)).toEqual([idA, idC]);
      } finally {
        unsubscribe();
      }
    });
  });

  it("uses stable id ordering when orderBy is omitted", async () => {
    await withDb("default-id-order", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(makeTodosQuery({}), (delta) => {
        snapshots.push(delta.all);
      });

      try {
        const idA = db.insert(todos, { title: "A", rank: 10, done: false });
        const idB = db.insert(todos, { title: "B", rank: 5, done: false });
        const idC = db.insert(todos, { title: "C", rank: 1, done: false });

        await waitForCondition(
          () => latestRows(snapshots).length === 3,
          10_000,
          "expected rows for default-order query",
        );

        const expectedById = [idA, idB, idC].toSorted((a, b) => a.localeCompare(b));
        expect(latestIds(snapshots)).toEqual(expectedById);

        db.update(todos, idB, { title: "B-still" });

        await waitForCondition(
          () => latestRows(snapshots).some((row) => row.id === idB && row.title === "B-still"),
          10_000,
          "expected updated row in snapshots",
        );

        expect(latestIds(snapshots)).toEqual(expectedById);
      } finally {
        unsubscribe();
      }
    });
  });

  it("supports descending sort and reorders correctly", async () => {
    await withDb("desc-order", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({ orderBy: [["rank", "desc"]] }),
        (delta) => {
          snapshots.push(delta.all);
        },
      );

      try {
        const idA = db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = db.insert(todos, { title: "C", rank: 3, done: false });

        await waitForCondition(
          () => latestRows(snapshots).length === 3,
          10_000,
          "expected initial rows",
        );
        expect(latestIds(snapshots)).toEqual([idC, idB, idA]);

        db.update(todos, idA, { rank: 10 });

        await waitForCondition(
          () => latestRows(snapshots)[0]?.id === idA,
          10_000,
          "expected row A to move to top for desc sort",
        );

        expect(latestIds(snapshots)).toEqual([idA, idC, idB]);
      } finally {
        unsubscribe();
      }
    });
  });

  it("uses id as deterministic tie-break for equal sort values", async () => {
    await withDb("tie-break-id", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(sortedByRankAscQuery, (delta) => {
        snapshots.push(delta.all);
      });

      try {
        const idA = db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = db.insert(todos, { title: "B", rank: 1, done: false });
        const idC = db.insert(todos, { title: "C", rank: 1, done: false });

        await waitForCondition(
          () => latestRows(snapshots).length === 3,
          10_000,
          "expected rows with equal rank",
        );

        const expectedById = [idA, idB, idC].toSorted((a, b) => a.localeCompare(b));
        expect(latestIds(snapshots)).toEqual(expectedById);
      } finally {
        unsubscribe();
      }
    });
  });

  it("applies multi-column sorting deterministically", async () => {
    await withDb("multi-column-sort", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({
          orderBy: [
            ["rank", "asc"],
            ["title", "desc"],
          ],
        }),
        (delta) => {
          snapshots.push(delta.all);
        },
      );

      try {
        const idA = db.insert(todos, { title: "A", rank: 1, done: false });
        const idZ = db.insert(todos, { title: "Z", rank: 1, done: false });
        const idM = db.insert(todos, { title: "M", rank: 2, done: false });

        await waitForCondition(
          () => latestRows(snapshots).length === 3,
          10_000,
          "expected initial sorted rows",
        );
        expect(latestIds(snapshots)).toEqual([idZ, idA, idM]);

        db.update(todos, idM, { rank: 1 });

        await waitForCondition(
          () => latestRows(snapshots).length === 3 && latestRows(snapshots)[1]?.id === idM,
          10_000,
          "expected row M to move according to secondary sort key",
        );

        expect(latestIds(snapshots)).toEqual([idZ, idM, idA]);
      } finally {
        unsubscribe();
      }
    });
  });

  it("does not reposition on no-op sort-field update", async () => {
    await withDb("noop-sort-update", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(sortedByRankAscQuery, (delta) => {
        snapshots.push(delta.all);
      });

      try {
        const idA = db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = db.insert(todos, { title: "C", rank: 3, done: false });

        await waitForCondition(
          () => latestRows(snapshots).length === 3,
          10_000,
          "expected initial rows",
        );
        const before = latestIds(snapshots);
        expect(before).toEqual([idA, idB, idC]);

        db.update(todos, idB, { rank: 2 });

        await waitForCondition(
          () => latestRows(snapshots).some((row) => row.id === idB && row.rank === 2),
          10_000,
          "expected updated row",
        );

        expect(latestIds(snapshots)).toEqual(before);
      } finally {
        unsubscribe();
      }
    });
  });

  it("keeps window order correct around limit/offset boundaries", async () => {
    await withDb("limit-offset-boundary", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(
        makeTodosQuery({ orderBy: [["rank", "asc"]], offset: 1, limit: 2 }),
        (delta) => {
          snapshots.push(delta.all);
        },
      );

      try {
        const idA = db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = db.insert(todos, { title: "C", rank: 3, done: false });
        const idD = db.insert(todos, { title: "D", rank: 4, done: false });

        await waitForCondition(
          () => latestRows(snapshots).length === 2,
          10_000,
          "expected initial window",
        );
        expect(latestIds(snapshots)).toEqual([idB, idC]);

        db.update(todos, idD, { rank: 0 });

        await waitForCondition(
          () => {
            const ids = latestIds(snapshots);
            return ids.length === 2 && ids[0] === idA && ids[1] === idB;
          },
          10_000,
          "expected offset window to shift after boundary move",
        );

        expect(latestIds(snapshots)).toEqual([idA, idB]);
      } finally {
        unsubscribe();
      }
    });
  });

  it("handles mixed add/remove/update changes with deterministic final order", async () => {
    await withDb("mixed-delta", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(sortedByRankAscQuery, (delta) => {
        snapshots.push(delta.all);
      });

      try {
        const idA = db.insert(todos, { title: "A", rank: 1, done: false });
        const idB = db.insert(todos, { title: "B", rank: 2, done: false });
        const idC = db.insert(todos, { title: "C", rank: 3, done: false });

        await waitForCondition(
          () => latestRows(snapshots).length === 3,
          10_000,
          "expected initial rows",
        );

        db.update(todos, idC, { rank: 0 });
        db.deleteFrom(todos, idA);
        const idD = db.insert(todos, { title: "D", rank: 2, done: false });

        await waitForCondition(
          () => {
            const rows = latestRows(snapshots);
            return rows.length === 3 && rows.some((row) => row.id === idD);
          },
          10_000,
          "expected mixed changes to settle",
        );

        const expectedTail = [idB, idD].toSorted((a, b) => a.localeCompare(b));
        expect(latestIds(snapshots)).toEqual([idC, ...expectedTail]);
      } finally {
        unsubscribe();
      }
    });
  });

  it("inserts new rows at top, middle, and bottom positions", async () => {
    await withDb("insert-positioning", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(sortedByRankAscQuery, (delta) => {
        snapshots.push(delta.all);
      });

      try {
        const idB = db.insert(todos, { title: "B", rank: 20, done: false });
        const idD = db.insert(todos, { title: "D", rank: 40, done: false });

        await waitForCondition(
          () => latestRows(snapshots).length === 2,
          10_000,
          "expected initial rows",
        );
        expect(latestIds(snapshots)).toEqual([idB, idD]);

        const idA = db.insert(todos, { title: "A", rank: 10, done: false });
        await waitForCondition(
          () => latestRows(snapshots)[0]?.id === idA,
          10_000,
          "expected top insert to appear first",
        );

        const idC = db.insert(todos, { title: "C", rank: 30, done: false });
        await waitForCondition(
          () => {
            const ids = latestIds(snapshots);
            return ids.length === 4 && ids[2] === idC;
          },
          10_000,
          "expected middle insert to appear in middle",
        );

        const idE = db.insert(todos, { title: "E", rank: 50, done: false });
        await waitForCondition(
          () => latestRows(snapshots)[4]?.id === idE,
          10_000,
          "expected bottom insert to appear last",
        );

        expect(latestIds(snapshots)).toEqual([idA, idB, idC, idD, idE]);
      } finally {
        unsubscribe();
      }
    });
  });

  it("keeps snapshot order deterministic across restart", async () => {
    const appId = uniqueDbName("restart");
    const dbName = uniqueDbName("restart");

    const db1 = await createDb({ appId, dbName });
    const idA = db1.insert(todos, { title: "A", rank: 3, done: false });
    const idB = db1.insert(todos, { title: "B", rank: 1, done: false });
    const idC = db1.insert(todos, { title: "C", rank: 2, done: false });
    await db1.shutdown();

    const db2 = await createDb({ appId, dbName });
    const snapshots: Todo[][] = [];
    const unsubscribe = db2.subscribeAll(sortedByRankAscQuery, (delta) => {
      snapshots.push(delta.all);
    });

    try {
      await waitForCondition(
        () => latestRows(snapshots).length === 3,
        10_000,
        "expected sorted snapshot after restart",
      );

      expect(latestIds(snapshots)).toEqual([idB, idC, idA]);
    } finally {
      unsubscribe();
      await db2.shutdown();
    }
  });

  it("keeps null/undefined sort-value ordering stable", async () => {
    await withDb("null-sort-values", async (db) => {
      const snapshots: Todo[][] = [];
      const unsubscribe = db.subscribeAll(sortedByRankAscQuery, (delta) => {
        snapshots.push(delta.all);
      });

      try {
        const idNull = db.insert(todos, { title: "N", rank: undefined, done: false });
        const idOne = db.insert(todos, { title: "A", rank: 1, done: false });
        const idTwo = db.insert(todos, { title: "B", rank: 2, done: false });

        await waitForCondition(
          () => latestRows(snapshots).length === 3,
          10_000,
          "expected rows including null rank",
        );

        const initial = latestIds(snapshots);
        expect(initial).toEqual(expect.arrayContaining([idNull, idOne, idTwo]));

        db.update(todos, idNull, { title: "N-updated" });

        await waitForCondition(
          () => latestRows(snapshots).some((row) => row.id === idNull && row.title === "N-updated"),
          10_000,
          "expected null-rank row update",
        );

        expect(latestIds(snapshots)).toEqual(initial);
      } finally {
        unsubscribe();
      }
    });
  });
});

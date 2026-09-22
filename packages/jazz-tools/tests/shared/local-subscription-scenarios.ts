import { expect } from "vitest";
import type { Db, QueryBuilder, TableProxy } from "../../src/runtime/db.js";
import type { WasmSchema } from "../../src/drivers/types.js";
export interface Todo {
  id: string;
  title: string;
  rank: number | null;
  done: boolean;
}

export const schema: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "rank", column_type: { type: "Integer" }, nullable: true },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

export const todos: TableProxy<Todo, Omit<Todo, "id">> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as Omit<Todo, "id">,
};

export function makeTodosQuery(body: {
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

export const sortedByRankAscQuery = makeTodosQuery({ orderBy: [["rank", "asc"]] });

// Browser subscription delivery is asynchronous. Keep this aligned with the
// rest of this suite's convergence waits: a sub-second deadline flakes when
// the full browser suite is sharing a worker, without testing a latency SLO.
export const SUBSCRIPTION_CONVERGENCE_TIMEOUT_MS = 10_000;

export async function waitForCondition(
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

export function latestIds(snapshots: Todo[][]): string[] {
  return (snapshots[snapshots.length - 1] ?? []).map((row) => row.id);
}

export function latestRows(snapshots: Todo[][]): Todo[] {
  return snapshots[snapshots.length - 1] ?? [];
}

export type SubscribeSnapshots = (
  query: QueryBuilder<Todo>,
  onRows: (rows: Todo[]) => void,
) => () => void;

export async function assertSubscriptionTies(db: Db, subscribe: SubscribeSnapshots): Promise<void> {
  const snapshots: Todo[][] = [];
  const unsubscribe = subscribe(sortedByRankAscQuery, (rows) => {
    snapshots.push(rows);
  });

  try {
    const {
      value: { id: idA },
    } = await db.insert(todos, { title: "A", rank: 1, done: false });
    const {
      value: { id: idB },
    } = await db.insert(todos, { title: "B", rank: 1, done: false });
    const {
      value: { id: idC },
    } = await db.insert(todos, { title: "C", rank: 1, done: false });

    await waitForCondition(
      () => latestRows(snapshots).length === 3,
      10_000,
      "expected rows with equal rank",
    );

    const expectedById = [idA, idB, idC].sort((a, b) => a.localeCompare(b));
    expect(latestIds(snapshots)).toEqual(expectedById);
  } finally {
    unsubscribe();
  }
}

export async function assertSubscriptionNoop(db: Db, subscribe: SubscribeSnapshots): Promise<void> {
  const snapshots: Todo[][] = [];
  const unsubscribe = subscribe(sortedByRankAscQuery, (rows) => {
    snapshots.push(rows);
  });

  try {
    const {
      value: { id: idA },
    } = await db.insert(todos, { title: "A", rank: 1, done: false });
    const {
      value: { id: idB },
    } = await db.insert(todos, { title: "B", rank: 2, done: false });
    const {
      value: { id: idC },
    } = await db.insert(todos, { title: "C", rank: 3, done: false });

    await waitForCondition(
      () => latestRows(snapshots).length === 3,
      10_000,
      "expected initial rows",
    );
    const before = latestIds(snapshots);
    expect(before).toEqual([idA, idB, idC]);

    // The changed non-sort field proves delivery of the no-op rank update.
    await db.update(todos, idB, { rank: 2, done: true });

    await waitForCondition(
      () => latestRows(snapshots).some((row) => row.id === idB && row.rank === 2 && row.done),
      10_000,
      "expected updated row",
    );

    expect(latestIds(snapshots)).toEqual(before);
  } finally {
    unsubscribe();
  }
}

export async function assertSubscriptionWindow(
  db: Db,
  subscribe: SubscribeSnapshots,
): Promise<void> {
  const snapshots: Todo[][] = [];
  const unsubscribe = subscribe(
    makeTodosQuery({ orderBy: [["rank", "asc"]], offset: 1, limit: 2 }),
    (rows) => {
      snapshots.push(rows);
    },
  );

  try {
    const {
      value: { id: idA },
    } = await db.insert(todos, { title: "A", rank: 1, done: false });
    const {
      value: { id: idB },
    } = await db.insert(todos, { title: "B", rank: 2, done: false });
    const {
      value: { id: idC },
    } = await db.insert(todos, { title: "C", rank: 3, done: false });
    const {
      value: { id: idD },
    } = await db.insert(todos, { title: "D", rank: 4, done: false });

    await waitForCondition(
      () => latestRows(snapshots).length === 2,
      10_000,
      "expected initial window",
    );
    expect(latestIds(snapshots)).toEqual([idB, idC]);

    await db.update(todos, idD, { rank: 0 });

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
}

export async function assertSubscriptionNull(db: Db, subscribe: SubscribeSnapshots): Promise<void> {
  const snapshots: Todo[][] = [];
  const unsubscribe = subscribe(sortedByRankAscQuery, (rows) => snapshots.push(rows));
  try {
    await waitForCondition(
      () => snapshots.length > 0,
      SUBSCRIPTION_CONVERGENCE_TIMEOUT_MS,
      "expected initial snapshot",
    );
    const {
      value: { id: idNull },
    } = await db.insert(todos, { title: "N", rank: null, done: false });
    const {
      value: { id: idOne },
    } = await db.insert(todos, { title: "A", rank: 1, done: false });
    const {
      value: { id: idTwo },
    } = await db.insert(todos, { title: "B", rank: 2, done: false });
    await waitForCondition(
      () => latestRows(snapshots).length === 3,
      SUBSCRIPTION_CONVERGENCE_TIMEOUT_MS,
      "expected rows including null rank",
    );
    const initial = latestIds(snapshots);
    expect(initial).toEqual(expect.arrayContaining([idNull, idOne, idTwo]));
    await db.update(todos, idNull, { title: "N-updated" });
    await waitForCondition(
      () => latestRows(snapshots).some((row) => row.id === idNull && row.title === "N-updated"),
      SUBSCRIPTION_CONVERGENCE_TIMEOUT_MS,
      "expected null-rank row update",
    );
    expect(latestIds(snapshots)).toEqual(initial);
  } finally {
    unsubscribe();
  }
}

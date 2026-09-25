/**
 * Position lookups, the undo log and the lazy ordered-id index across
 * frames that shift rows, including frames that fail and roll back.
 */
import { describe, expect, it } from "vitest";
import type { RuntimeSubscriptionDelta, Value, WasmRow } from "../drivers/types.js";
import { SubscriptionManager } from "./subscription-manager.js";

const id = (n: number) => `00000000-0000-4000-8000-${n.toString(16).padStart(12, "0")}`;
const uuidBytes = (value: string) =>
  Uint8Array.from(
    value
      .replaceAll("-", "")
      .match(/../g)!
      .map((h) => Number.parseInt(h, 16)),
  );
const occurrenceKey = (value: string) =>
  Uint8Array.from([1, ...uuidBytes(value), 0, 0, 0, 0, 0, 0, 0, 0]);

type Root = { id: string; title: string };
function row(rowId: string, title: string): WasmRow {
  const values: Value[] = [
    { type: "Text", value: title },
    { type: "Array", value: [] },
  ];
  const result = { id: rowId, values };
  Object.defineProperty(result, "valuesByColumn", {
    value: new Map([
      ["title", values[0]!],
      ["children", values[1]!],
    ]),
  });
  return result;
}
const transform = (r: WasmRow): Root => ({
  id: r.id,
  title: (r.values[0] as { type: "Text"; value: string }).value,
});
const frame = (overrides: Partial<RuntimeSubscriptionDelta>): RuntimeSubscriptionDelta => ({
  added: [],
  removed: [],
  updated: [],
  ...overrides,
});
const added = (n: number, index: number, title = `r${n}`) => ({
  sourceId: id(n),
  occurrenceKey: occurrenceKey(id(n)),
  index,
  row: row(id(n), title),
});
const updated = (n: number, index: number, title = `u${n}`) => ({
  sourceId: id(n),
  occurrenceKey: occurrenceKey(id(n)),
  index,
  row: row(id(n), title),
});
const removed = (n: number, index: number) => ({
  sourceId: id(n),
  occurrenceKey: occurrenceKey(id(n)),
  index,
});
const missingChildRemoval = (root: number) => ({
  root_key: [10, ...uuidBytes(id(root))],
  path: [{ Collection: 1 }],
  edit: { Remove: { key: [10, ...uuidBytes("00000000-0000-4000-9000-0000000000ff")] } },
});

function seeded(count: number) {
  const manager = new SubscriptionManager<Root>();
  manager.handleDelta(
    frame({ added: Array.from({ length: count }, (_, n) => added(n, n)) }),
    transform,
  );
  return manager;
}
const order = (manager: SubscriptionManager<Root>) => manager.all().map((root) => root.id);

/**
 * The ordered-id index must be exact once a frame ends, whether it committed
 * or rolled back. Lookups tolerate a stale index, so without this check a
 * missing reindex would only show up as slower frames.
 */
function expectExactIndex(manager: SubscriptionManager<Root>): void {
  const state = manager as unknown as {
    orderedIds: string[];
    orderedIdIndex: Map<string, number>;
    staleFrom: number | null;
    staleSplices: number;
  };
  expect(state.staleFrom).toBeNull();
  expect(state.staleSplices).toBe(0);
  expect(state.orderedIdIndex.size).toBe(state.orderedIds.length);
  state.orderedIds.forEach((rowId, position) => {
    expect(state.orderedIdIndex.get(rowId)).toBe(position);
  });
}

describe("ordered positions across shifting frames", () => {
  // Fails if positionOf searches only one position either side (M2).
  it("finds a row shifted by two inserts earlier in the same frame", () => {
    const manager = seeded(10);
    const expected = [id(100), id(101), ...Array.from({ length: 10 }, (_, n) => id(n))];
    manager.handleDelta(
      frame({ added: [added(100, 0), added(101, 1)], updated: [updated(5, 7)] }),
      transform,
    );
    expect(order(manager)).toEqual(expected);
    expect(manager.all()[7]).toEqual({ id: id(5), title: "u5" });
    expectExactIndex(manager);
  });

  // Fails if a run of removals counts as one splice (M1).
  it("finds a row shifted by a batched run of removals in the same frame", () => {
    const manager = seeded(10);
    manager.handleDelta(
      frame({ removed: [removed(0, 0), removed(1, 1)], updated: [updated(5, 3)] }),
      transform,
    );
    expect(order(manager)).toEqual([2, 3, 4, 5, 6, 7, 8, 9].map(id));
    expect(manager.all()[3]).toEqual({ id: id(5), title: "u5" });
    expectExactIndex(manager);
  });

  // Fails if the journal keeps the latest instead of the first prior value (M10).
  it("restores a moved row's index when a later terminal edit fails", () => {
    const manager = seeded(10);
    expect(() =>
      manager.handleDelta(
        frame({ updated: [updated(5, 0)], terminalOperations: [missingChildRemoval(8)] }),
        transform,
      ),
    ).toThrow(/terminal child removal addressed missing key/);
    const before = Array.from({ length: 10 }, (_, n) => id(n));
    expect(order(manager)).toEqual(before);
    expectExactIndex(manager);
    // The rolled-back index must still locate row 5 exactly.
    manager.handleDelta(frame({ updated: [updated(5, 5, "again")] }), transform);
    expect(order(manager)).toEqual(before);
    manager.handleDelta(frame({ removed: [removed(5, 5)] }), transform);
    expect(order(manager)).toEqual(before.filter((value) => value !== id(5)));
  });
  it("keeps the index exact after every committed or rolled-back frame", () => {
    let state = 11;
    const rnd = (n: number) => {
      state = (state * 1103515245 + 12345) & 0x7fffffff;
      return (state >>> 8) % n;
    };
    const manager = seeded(40);
    let nextId = 1000;
    for (let step = 0; step < 300; step++) {
      const ids = order(manager);
      const used = new Set<string>();
      const removedRows: ReturnType<typeof removed>[] = [];
      const updatedRows: ReturnType<typeof updated>[] = [];
      for (let n = rnd(4); n > 0 && ids.length > 0; n--) {
        const position = rnd(ids.length);
        const rowId = ids[position]!;
        if (used.has(rowId)) continue;
        used.add(rowId);
        removedRows.push({ sourceId: rowId, occurrenceKey: occurrenceKey(rowId), index: position });
      }
      const remaining = ids.length - removedRows.length;
      for (let n = rnd(4); n > 0 && ids.length > 0; n--) {
        const rowId = ids[rnd(ids.length)]!;
        if (used.has(rowId)) continue;
        used.add(rowId);
        updatedRows.push({
          sourceId: rowId,
          occurrenceKey: occurrenceKey(rowId),
          index: rnd(Math.max(1, remaining)),
          row: row(rowId, `s${step}`),
        });
      }
      const addedRows = Array.from({ length: rnd(4) }, () => {
        const n = nextId++;
        return added(n, rnd(remaining + 1));
      });
      // A child removal under a root that stays fails the whole frame.
      const survivor = ids.find((rowId) => !used.has(rowId));
      const fails = rnd(5) === 0 && survivor !== undefined;
      const delta = frame({
        added: addedRows,
        removed: removedRows,
        updated: updatedRows,
        ...(fails
          ? { terminalOperations: [missingChildRemoval(Number.parseInt(survivor!.slice(-12), 16))] }
          : {}),
      });
      const before = order(manager);
      if (fails) {
        expect(() => manager.handleDelta(delta, transform)).toThrow();
        expect(order(manager)).toEqual(before);
      } else {
        manager.handleDelta(delta, transform);
      }
      expectExactIndex(manager);
    }
  });
});

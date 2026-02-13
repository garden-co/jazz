/**
 * Integration test: SubscriptionManager with real WASM runtime.
 *
 * This test verifies that SubscriptionManager correctly consumes row-level
 * deltas from the actual groove-wasm runtime and produces typed SubscriptionDelta
 * objects with accurate index positions for add/update/remove.
 *
 * Flow:
 *   1. Create WASM runtime + subscribe to "todos ORDER BY rank ASC"
 *   2. Insert "Buy milk"(10), "Call dentist"(20), "Fix bug"(30) → add deltas at indices 0,1,2
 *   3. Update "Fix bug" rank to 0 → expect update delta (moves from index 2 → 0)
 *   4. Delete "Buy milk" → remove delta at correct index; final state [Fix bug, Call dentist]
 */
import { describe, expect, it } from "vitest";
import { SubscriptionManager } from "./subscription-manager.js";
import { translateQuery } from "./query-adapter.js";
import type { RowDelta, WasmRow, WasmSchema } from "../drivers/types.js";
import { createWasmRuntime } from "./testing/wasm-runtime-test-utils.js";

interface Todo {
  id: string;
  title: string;
  rank: number;
}

/** Schema for the test: single table "todos" with title (Text) and rank (Integer). */
const schema: WasmSchema = {
  tables: {
    todos: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "rank", column_type: { type: "Integer" }, nullable: false },
      ],
    },
  },
};

/** Build WASM query string: todos ordered by rank ascending. */
function makeOrderedTodosQuery(): string {
  return translateQuery(
    JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [["rank", "asc"]],
    }),
    schema,
  );
}

/** Convert raw WasmRow (id + typed values) to typed Todo. */
function transformRow(row: WasmRow): Todo {
  return {
    id: row.id,
    title: (row.values[0] as { type: "Text"; value: string }).value,
    rank: (row.values[1] as { type: "Integer"; value: number }).value,
  };
}

/** Poll an array until a matching item appears, or timeout. */
async function pollUntil<T>(arr: T[], pred: (item: T) => boolean, timeoutMs = 2000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const match = arr.find(pred);
    if (match) return match;
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error(`Timed out waiting for matching delta: ${JSON.stringify(arr)}`);
}

describe("SubscriptionManager WASM integration", () => {
  it("consumes real wasm runtime deltas with indexed add/update/remove", async () => {
    const runtime = await createWasmRuntime(schema, {
      appId: "test-sub-manager-wasm-integration",
    });

    const manager = new SubscriptionManager<Todo>();
    const typedDeltas: Array<ReturnType<typeof manager.handleDelta>> = [];
    const rawDeltas: RowDelta[] = [];

    // Subscribe: every delta from WASM is pushed to rawDeltas and passed through
    // SubscriptionManager.handleDelta; typed result goes to typedDeltas.
    const subId = runtime.subscribe(
      makeOrderedTodosQuery(),
      (deltaJsonOrObject: RowDelta | string) => {
        const raw: RowDelta =
          typeof deltaJsonOrObject === "string" ? JSON.parse(deltaJsonOrObject) : deltaJsonOrObject;
        rawDeltas.push(raw);
        typedDeltas.push(manager.handleDelta(raw, transformRow));
      },
    );

    // Give subscription time to register before mutations.
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Insert realistic todos. Order by rank: Buy milk, Call dentist, Fix bug → indices 0, 1, 2.
    const idBuyMilk = runtime.insert("todos", [
      { type: "Text", value: "Buy milk" },
      { type: "Integer", value: 10 },
    ]);
    const idCallDentist = runtime.insert("todos", [
      { type: "Text", value: "Call dentist" },
      { type: "Integer", value: 20 },
    ]);
    const idFixBug = runtime.insert("todos", [
      { type: "Text", value: "Fix bug in subscription manager" },
      { type: "Integer", value: 30 },
    ]);

    // Assert: add delta for "Fix bug" arrives with index 2 (last in sorted order).
    const addFixBug = await pollUntil(typedDeltas, (delta) =>
      delta.added.some((entry) => entry.item.id === idFixBug && entry.index === 2),
    );
    expect(addFixBug.added.find((entry) => entry.item.id === idFixBug)?.index).toBe(2);
    expect(rawDeltas.some((delta) => delta.added.some((entry) => entry.row.id === idFixBug))).toBe(
      true,
    );

    // Promote "Fix bug" to top: rank 30 → 0. New order: Fix bug, Buy milk, Call dentist.
    runtime.update(idFixBug, { rank: { type: "Integer", value: 0 } });
    const updated = await pollUntil(typedDeltas, (delta) =>
      delta.updated.some((entry) => entry.newItem.id === idFixBug),
    );
    const updatedEntry = updated.updated.find((entry) => entry.newItem.id === idFixBug);
    expect(updatedEntry).toBeDefined();
    expect(typeof updatedEntry?.oldIndex).toBe("number");
    expect(typeof updatedEntry?.newIndex).toBe("number");
    expect(
      rawDeltas.some((delta) =>
        delta.updated.some(
          (entry) => entry.old_row.id === idFixBug && entry.new_row.id === idFixBug,
        ),
      ),
    ).toBe(true);

    // Delete "Buy milk". Final order: Fix bug, Call dentist.
    runtime.delete(idBuyMilk);
    const removed = await pollUntil(typedDeltas, (delta) =>
      delta.removed.some((entry) => entry.item.id === idBuyMilk),
    );
    const expectedRemovedIndex = updated.all.findIndex((item) => item.id === idBuyMilk);
    expect(removed.removed.find((entry) => entry.item.id === idBuyMilk)?.index).toBe(
      expectedRemovedIndex,
    );
    expect(removed.all.map((item) => item.id).sort()).toEqual([idCallDentist, idFixBug].sort());

    runtime.unsubscribe(subId);
  }, 15000);

  it("adds first item and shifts prior second item to third position", async () => {
    const runtime = await createWasmRuntime(schema, {
      appId: "test-sub-manager-add-first-shift-second",
    });
    const manager = new SubscriptionManager<Todo>();
    const typedDeltas: Array<ReturnType<typeof manager.handleDelta>> = [];

    const subId = runtime.subscribe(
      makeOrderedTodosQuery(),
      (deltaJsonOrObject: RowDelta | string) => {
        const raw: RowDelta =
          typeof deltaJsonOrObject === "string" ? JSON.parse(deltaJsonOrObject) : deltaJsonOrObject;
        typedDeltas.push(manager.handleDelta(raw, transformRow));
      },
    );

    await new Promise((resolve) => setTimeout(resolve, 50));

    // Initial sorted state: [A(10), B(20)].
    const idA = runtime.insert("todos", [
      { type: "Text", value: "A" },
      { type: "Integer", value: 10 },
    ]);
    const idB = runtime.insert("todos", [
      { type: "Text", value: "B" },
      { type: "Integer", value: 20 },
    ]);
    await pollUntil(
      typedDeltas,
      (d) => d.all.length === 2 && d.all[0].id === idA && d.all[1].id === idB,
    );

    // Add a new first item C(0): resulting order should be [C, A, B].
    const idC = runtime.insert("todos", [
      { type: "Text", value: "C" },
      { type: "Integer", value: 0 },
    ]);

    const addFirstDelta = await pollUntil(typedDeltas, (d) =>
      d.added.some((entry) => entry.item.id === idC && entry.index === 0),
    );
    expect(addFirstDelta.all.map((item) => item.id)).toEqual([idC, idA, idB]);
    expect(addFirstDelta.all[2]?.id).toBe(idB);

    runtime.unsubscribe(subId);
  });

  it("handles same-delta remove-first and move-second", () => {
    const manager = new SubscriptionManager<Todo>();
    const mkRow = (id: string, title: string, rank: number): WasmRow => ({
      id,
      values: [
        { type: "Text", value: title },
        { type: "Integer", value: rank },
      ],
    });

    // Seed state: [A, B, C]
    const seeded = manager.handleDelta(
      {
        added: [
          { row: mkRow("a", "A", 10), index: 0 },
          { row: mkRow("b", "B", 20), index: 1 },
          { row: mkRow("c", "C", 30), index: 2 },
        ],
        updated: [],
        removed: [],
        pending: false,
      },
      transformRow,
    );
    expect(seeded.all.map((item) => item.id)).toEqual(["a", "b", "c"]);

    // Same delta:
    // - remove first item A (index 0)
    // - move second item B from index 1 -> 0
    // post should be [B, C]
    const next = manager.handleDelta(
      {
        added: [],
        updated: [
          {
            old_row: mkRow("b", "B", 20),
            new_row: mkRow("b", "B", 20),
            old_index: 1,
            new_index: 0,
          },
        ],
        removed: [{ row: mkRow("a", "A", 10), index: 0 }],
        pending: false,
      },
      transformRow,
    );

    expect(next.removed.find((entry) => entry.item.id === "a")?.index).toBe(0);
    const moved = next.updated.find((entry) => entry.newItem.id === "b");
    expect(moved?.oldIndex).toBe(1);
    expect(moved?.newIndex).toBe(0);
    expect(next.all.map((item) => item.id)).toEqual(["b", "c"]);
  });
});

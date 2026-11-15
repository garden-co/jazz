/**
 * A coalesced backlog: when a subscriber fell too far behind, the runtime
 * replaces its undelivered deltas with one reset of the current result,
 * marked `reason: "backlog"`. The manager replaces its rows as for any reset
 * and passes the reason on; other resets and deltas carry none.
 */
import { describe, expect, it } from "vitest";
import type { RuntimeSubscriptionDelta, Value, WasmRow } from "../drivers/types.js";
import { SubscriptionManager } from "./subscription-manager.js";

const id = (n: number) => `00000000-0000-4000-8000-${n.toString(16).padStart(12, "0")}`;
const occurrenceKey = (value: string) =>
  Uint8Array.from([
    2,
    ...value
      .replaceAll("-", "")
      .match(/../g)!
      .map((h) => Number.parseInt(h, 16)),
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
  ]);

type Note = { id: string; title: string };
function row(rowId: string, title: string): WasmRow {
  const values: Value[] = [{ type: "Text", value: title }];
  const result = { id: rowId, values };
  Object.defineProperty(result, "valuesByColumn", {
    value: new Map([["title", values[0]!]]),
  });
  return result;
}
const transform = (r: WasmRow): Note => ({
  id: r.id,
  title: (r.values[0] as { type: "Text"; value: string }).value,
});
const added = (n: number, index: number, title = `n${n}`) => ({
  sourceId: id(n),
  occurrenceKey: occurrenceKey(id(n)),
  index,
  row: row(id(n), title),
});
const frame = (overrides: Partial<RuntimeSubscriptionDelta>): RuntimeSubscriptionDelta => ({
  added: [],
  removed: [],
  updated: [],
  ...overrides,
});

describe("coalesced subscription backlogs", () => {
  it("replaces the result and reports reason backlog", () => {
    const manager = new SubscriptionManager<Note>();
    const opening = manager.handleDelta(
      frame({ reset: true, added: [added(1, 0), added(2, 1)] }),
      transform,
    );
    expect(opening.reset).toBe(true);
    expect("reason" in opening).toBe(false);

    const coalesced = manager.handleDelta(
      frame({ reset: true, reason: "backlog", added: [added(2, 0, "latest"), added(3, 1)] }),
      transform,
    );
    expect(coalesced).toMatchObject({ reset: true, reason: "backlog" });
    expect(coalesced.all?.map((note) => [note.id, note.title])).toEqual([
      [id(2), "latest"],
      [id(3), "n3"],
    ]);
  });

  it("ignores a reason on a delta that is not a reset", () => {
    const manager = new SubscriptionManager<Note>();
    manager.handleDelta(frame({ reset: true, added: [added(1, 0)] }), transform);
    const delta = manager.handleDelta(
      frame({ reason: "backlog", added: [added(2, 1)] }),
      transform,
    );
    expect("reason" in delta).toBe(false);
    expect(delta.all?.length).toBe(2);
  });
});

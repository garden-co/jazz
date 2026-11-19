import { describe, expect, it } from "vitest";
import type { RuntimeSubscriptionDelta } from "../drivers/types.js";
import { SpaceUse } from "./space-use.js";

// A subscription on an encrypted table uses the spaces of the rows it
// delivers; the runtime hears of each use and each release, and counts them.

function target() {
  const events: string[] = [];
  return {
    events,
    encryptionSpaces: () => JSON.stringify({ tasks: { scope: "projects", column: "projectId" } }),
    encryptionUseSpace: (table: string, id: string, open: boolean) =>
      void events.push(`${open ? "use" : "release"} ${table}/${id}`),
  };
}

const delta = (...projects: string[]): RuntimeSubscriptionDelta =>
  ({
    added: projects.map((project, index) => ({
      sourceId: "tasks",
      occurrenceKey: new Uint8Array(),
      index,
      row: { id: `row-${project}-${index}`, values: [{ type: "Uuid", value: project }] },
    })),
    removed: [],
    updated: [],
  }) as unknown as RuntimeSubscriptionDelta;

const columns = [{ name: "projectId" }];

describe("SpaceUse", () => {
  it("uses each space of the delivered rows once per subscription and releases them when the subscription ends", () => {
    const native = target();
    const uses = new SpaceUse(native);
    const subscription = uses.forSubscription("tasks", columns)!;
    subscription.observe(delta("p1", "p2", "p1"));
    expect(native.events).toEqual(["use projects/p1", "use projects/p2"]);
    subscription.release();
    expect(native.events.slice(2).sort()).toEqual(["release projects/p1", "release projects/p2"]);
  });

  it("tells the runtime of every subscription's use of a space; it counts them", () => {
    const native = target();
    const uses = new SpaceUse(native);
    const first = uses.forSubscription("tasks", columns)!;
    const second = uses.forSubscription("tasks", columns)!;
    first.observe(delta("p1"));
    second.observe(delta("p1"));
    expect(native.events).toEqual(["use projects/p1", "use projects/p1"]);
    first.release();
    second.release();
    expect(native.events.slice(2)).toEqual(["release projects/p1", "release projects/p1"]);
  });

  it("takes nothing from a delta that arrives after the subscription ended", () => {
    const native = target();
    const uses = new SpaceUse(native);
    const subscription = uses.forSubscription("tasks", columns)!;
    subscription.observe(delta("p1"));
    subscription.release();
    subscription.observe(delta("p2"));
    expect(native.events).toEqual(["use projects/p1", "release projects/p1"]);
  });

  it("has nothing to do for a table that is not encrypted, or a query without the space column", () => {
    const uses = new SpaceUse(target());
    expect(uses.forSubscription("notes", columns)).toBeNull();
    expect(uses.forSubscription("tasks", [{ name: "title" }])).toBeNull();
  });
});

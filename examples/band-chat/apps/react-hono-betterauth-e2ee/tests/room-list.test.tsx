// @vitest-environment jsdom
// The room list: every room the reader is sent, ordered and marked unread from
// each room's newest message through plain columns only, without waiting for
// read markers.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { resetWorld, sameFilter, world, type Q } from "./support/fake-jazz";

vi.mock("../jazz.generated", async () => ({ app: (await import("./support/fake-jazz")).fakeApp }));
vi.mock("jazz-tools/react", async () => (await import("./support/fake-jazz")).reactMock);

import { BandChat } from "../src/BandChat";
import { summariseRooms } from "../src/lib/room-summaries";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const me = { id: "p-alice", author: "alice-account", displayName: "Alice" };
const room = (id: string, name: string, at: number, creator = "alice-account") => ({
  id,
  name,
  $createdAt: new Date(at),
  $createdBy: { account: creator },
});

beforeEach(resetWorld);
afterEach(cleanup);

function answerWith(rooms: unknown[], newest: Record<string, unknown>) {
  world.answer = (q: Q) => {
    if (q.table === "profiles") return { data: [me] };
    if (q.table === "rooms") return { data: rooms };
    if (q.table === "messages" && q.max === 1 && q.filter && "roomId" in q.filter)
      return newest[q.filter.roomId as string] === undefined
        ? undefined
        : { data: newest[q.filter.roomId as string] as unknown[] };
    // Markers, memberships, counts, room histories: nothing has arrived.
    return undefined;
  };
}

describe("the room list", () => {
  it("lists every room the reader is sent, newest message first, while the markers have no result", async () => {
    // "old" was made last but has the oldest message; none has a roomMembers row.
    answerWith([room("old", "Old", 3000), room("new", "New", 1000), room("quiet", "Quiet", 2000)], {
      old: [{ id: "m1", senderId: "p-bob", $createdAt: new Date(1500) }],
      new: [{ id: "m2", senderId: "p-bob", $createdAt: new Date(9000) }],
      quiet: [],
    });
    render(<BandChat />);
    const names = (await screen.findAllByRole("button")).map((b) => b.textContent ?? "");
    const order = ["New", "Quiet", "Old"].map((n) => names.findIndex((t) => t.includes(n)));
    expect(order.every((i) => i >= 0)).toBe(true);
    // New (9000), Quiet (its own creation, 2000), Old (1500)
    expect(order).toEqual([...order].sort((a, b) => a - b));
    // The list never asked for membership rows or waited on the markers.
    expect(world.asked.some((q) => q.table === "roomMembers")).toBe(false);
  });

  it("asks for each room's newest message with plain columns only and a limit of one", async () => {
    answerWith([room("a", "A", 1), room("b", "B", 2)], { a: [], b: [] });
    render(<BandChat />);
    await screen.findAllByRole("button");
    const newest = world.asked.filter(
      (q) => q.table === "messages" && q.max === 1 && q.filter && "roomId" in q.filter,
    );
    expect(new Set(newest.map((q) => q.filter!.roomId))).toEqual(new Set(["a", "b"]));
    for (const q of newest) {
      expect(q.columns).toEqual(["id", "senderId", "$createdAt"]);
      expect(q.order).toEqual(["$createdAt", "desc"]);
      expect(sameFilter(q, { roomId: q.filter!.roomId })).toBe(true);
    }
  });
});

describe("unread state", () => {
  const rooms = [room("r", "R", 100) as never];
  const base = { rooms, author: "alice-account", profileId: "p-alice" };
  const theirs = new Map([["r", { id: "m", senderId: "p-bob", $createdAt: new Date(500) }]]);

  it("marks a room unread when its newest message is someone else's and newer than the marker", () => {
    const read = (at: number) => new Map([["r", new Date(at)]]);
    expect(summariseRooms({ ...base, newest: theirs, lastRead: read(400) })[0]!.hasUnread).toBe(
      true,
    );
    expect(summariseRooms({ ...base, newest: theirs, lastRead: read(500) })[0]!.hasUnread).toBe(
      false,
    );
  });

  it("marks nothing unread before the markers arrive, and never my own message", () => {
    expect(summariseRooms({ ...base, newest: theirs, lastRead: undefined })[0]!.hasUnread).toBe(
      false,
    );
    const mine = new Map([["r", { id: "m", senderId: "p-alice", $createdAt: new Date(500) }]]);
    expect(summariseRooms({ ...base, newest: mine, lastRead: new Map() })[0]!.hasUnread).toBe(
      false,
    );
  });
});

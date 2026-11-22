// @vitest-environment jsdom
// The open room: locked messages are shown as locked, never as an empty room,
// and a read marker is written only once the newest message is decrypted here.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { db, resetWorld, world, type Q } from "./support/fake-jazz";

vi.mock("../jazz.generated", async () => ({ app: (await import("./support/fake-jazz")).fakeApp }));
vi.mock("jazz-tools/react", async () => (await import("./support/fake-jazz")).reactMock);

import { RoomView } from "../src/components/RoomView";
import { UnsentMessagesProvider } from "../src/components/UnsentNotices";
import { ProfileDirectoryProvider } from "../src/lib/profiles";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const alice = { id: "p-alice", author: "alice-account", displayName: "Alice" };
const bob = { id: "p-bob", author: "bob-account", displayName: "Bob" };
const newest = { id: "m3", senderId: "p-bob", $createdAt: new Date("2026-01-03T00:00:00Z") };
const message = (id: string, at: string) => ({
  id,
  roomId: "room",
  senderId: "p-bob",
  text: `text ${id}`,
  $createdAt: new Date(at),
});
const summary = {
  room: {
    id: "room",
    name: "Rehearsal",
    $createdAt: new Date("2026-01-01T00:00:00Z"),
    $createdBy: { account: "alice-account" },
  },
  activityAt: newest.$createdAt,
  readAt: undefined,
  isCreator: true,
  hasUnread: true,
};

function show(answers: {
  messages: { data: unknown[]; unreadable?: unknown[] } | undefined;
  newest?: unknown[];
  markers?: unknown[];
}) {
  world.answer = (q: Q) => {
    if (q.table === "profiles") return { data: [alice, bob] };
    if (q.table === "messages" && q.max === 1) return { data: answers.newest ?? [newest] };
    if (q.table === "messages") return answers.messages;
    if (q.table === "readMarkers") return { data: answers.markers ?? [] };
    if (q.table === "reactions" || q.table === "joinRequests" || q.table === "__e2ee_members")
      return { data: [] };
    return undefined;
  };
  return render(
    <ProfileDirectoryProvider me={alice as never}>
      <UnsentMessagesProvider>
        <RoomView summary={summary as never} author="alice-account" />
      </UnsentMessagesProvider>
    </ProfileDirectoryProvider>,
  );
}
const markerWrites = () => db.insert.mock.calls.length + db.update.mock.calls.length;

beforeEach(resetWorld);
afterEach(cleanup);

describe("locked messages", () => {
  it("says messages are locked on this browser, and does not show an empty room", async () => {
    show({
      messages: { data: [], unreadable: [{ table: "messages", id: "m3", code: "key-pending" }] },
    });
    expect(await screen.findByText(/1 message is locked on this browser/)).toBeTruthy();
    expect(screen.queryByText("No messages yet")).toBeNull();
  });

  it("says no messages yet only for a room that is readable and empty", async () => {
    show({ messages: { data: [] }, newest: [] });
    expect(await screen.findByText("No messages yet")).toBeTruthy();
    expect(screen.queryByText(/locked on this browser/)).toBeNull();
  });
});

describe("the room view's read marker", () => {
  it("writes nothing while the newest message is locked, though an older one is readable", async () => {
    show({
      messages: {
        data: [message("m1", "2026-01-01T00:00:00Z")],
        unreadable: [{ table: "messages", id: "m3", code: "key-pending" }],
      },
    });
    await screen.findByText(/locked on this browser/);
    expect(markerWrites()).toBe(0);
  });

  it("writes nothing for a room with no message", async () => {
    show({ messages: { data: [] }, newest: [] });
    await screen.findByText("No messages yet");
    expect(markerWrites()).toBe(0);
  });

  it("writes one marker once the newest message is decrypted", async () => {
    show({
      messages: {
        data: [message("m3", "2026-01-03T00:00:00Z"), message("m1", "2026-01-01T00:00:00Z")],
      },
    });
    await screen.findByText("text m3");
    expect(db.insert).toHaveBeenCalledTimes(1);
  });
});

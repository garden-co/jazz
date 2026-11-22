// @vitest-environment jsdom
// The open room: locked messages are shown as locked, never as an empty room,
// and a read marker is written only once the newest message is decrypted here.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { db, reactMock, resetWorld, world, type Q } from "./support/fake-jazz";

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
  attachment?: ReturnType<typeof world.answer>;
}) {
  world.answer = (q: Q) => {
    if (q.table === "profiles") return { data: [alice, bob] };
    if (q.table === "messages" && q.columns?.includes("attachment")) return answers.attachment;
    if (q.table === "messages" && q.max === 1) return { data: answers.newest ?? [newest] };
    if (q.table === "messages") return answers.messages;
    if (q.table === "readMarkers") return { data: answers.markers ?? [] };
    if (q.table === "__e2ee_members") return { data: [{ id: "member", accountId: alice.author }] };
    if (q.table === "reactions" || q.table === "joinRequests") return { data: [] };
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

describe("an unreadable attachment", () => {
  it.each([
    ["image/png", { data: [], unreadable: [{ code: "invalid-ciphertext" }] }, "invalid-ciphertext"],
    [
      "audio/ogg",
      { error: new Error("unsupported attachment read") },
      "unsupported attachment read",
    ],
  ] satisfies [string, NonNullable<ReturnType<typeof world.answer>>, string][])(
    "shows the reason for a preview of %s",
    async (attachmentType, answer, reason) => {
      show({
        messages: {
          data: [{ ...message("m3", "2026-01-03"), attachmentName: "take", attachmentType }],
        },
        attachment: answer,
      });
      expect(await screen.findByText("Attachment is unreadable on this browser")).toBeTruthy();
      expect(screen.getByText(reason)).toBeTruthy();
      expect(screen.getByText("text m3")).toBeTruthy();
    },
  );

  it("shows why a requested download was omitted and keeps the message readable", async () => {
    const rows: unknown[] = [];
    db.all.mockImplementation(async (query) =>
      (query as Q).columns?.includes("attachment") ? rows : [{ id: "m" }],
    );
    const unreadableRows = vi
      .spyOn(reactMock, "unreadableRows")
      .mockImplementation((result) => (result === rows ? [{ code: "unsupported-format" }] : []));
    try {
      show({
        messages: { data: [{ ...message("m3", "2026-01-03"), attachmentName: "take.txt" }] },
      });
      fireEvent.click(await screen.findByLabelText("Download take.txt"));
      expect(await screen.findByText("Attachment is unreadable on this browser")).toBeTruthy();
      expect(screen.getByText("unsupported-format")).toBeTruthy();
      expect(screen.getByText("text m3")).toBeTruthy();
    } finally {
      unreadableRows.mockRestore();
    }
  });
});

it("shows why members could not load and retries in the same room", async () => {
  db.encryption.members.where.mockRejectedValueOnce(
    new Error("The table identity has not been observed"),
  );
  show({ messages: { data: [] }, newest: [] });
  fireEvent.click(await screen.findByRole("button", { name: "Invite" }));
  const dialog = within(screen.getByRole("dialog"));
  expect(await dialog.findByText("Could not load members")).toBeTruthy();
  expect(dialog.getByText("The table identity has not been observed")).toBeTruthy();
  expect(dialog.queryByText("Loading members…")).toBeNull();
  fireEvent.click(dialog.getByRole("button", { name: "Retry" }));
  expect(await dialog.findByText("1 members")).toBeTruthy();
  expect(db.encryption.members.where).toHaveBeenCalledTimes(2);
});

it("shows when a send used the current key after rotation failed and retries for this room", async () => {
  show({ messages: { data: [message("m3", "2026-01-03")] } });
  const report = db.encryption.onReport.mock.calls[0]![0];
  act(() =>
    report({
      type: "rotation-failed",
      space: { table: "rooms", id: "elsewhere" },
      reason: "offline",
    }),
  );
  expect(screen.queryByText("The room key could not be renewed")).toBeNull();
  act(() =>
    report({ type: "rotation-failed", space: { table: "rooms", id: "room" }, reason: "offline" }),
  );
  expect(await screen.findByText("The room key could not be renewed")).toBeTruthy();
  expect(screen.getByText("offline")).toBeTruthy();
  expect(screen.getByText("text m3")).toBeTruthy();

  db.encryption.keys.rotate.mockRejectedValueOnce(new Error("refused"));
  fireEvent.click(screen.getByRole("button", { name: "Retry" }));
  expect(await screen.findByText("refused")).toBeTruthy();
  await act(async () => fireEvent.click(screen.getByRole("button", { name: "Retry" })));
  expect(screen.queryByText("The room key could not be renewed")).toBeNull();
  expect(db.encryption.keys.rotate.mock.calls).toEqual([
    [{ table: "rooms", id: "room" }],
    [{ table: "rooms", id: "room" }],
  ]);
});

// @vitest-environment jsdom
// The open room: locked messages are shown as locked, never as an empty room,
// and a read marker is written only once the newest message is decrypted here.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { db, reactMock, resetWorld, world, type Q } from "./support/fake-jazz";

vi.mock("../jazz.generated", async () => ({ app: (await import("./support/fake-jazz")).fakeApp }));
vi.mock("jazz-tools/react", async () => (await import("./support/fake-jazz")).reactMock);
vi.mock("../src/lib/attachments", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../src/lib/attachments")>()),
  downloadBytes: vi.fn(),
}));
const deviceStatus = vi.hoisted(() => ({ current: null as string | null }));
vi.mock("../src/lib/device-state", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../src/lib/device-state")>()),
  useDeviceStateIfAny: () =>
    deviceStatus.current === null ? null : { status: deviceStatus.current },
}));
vi.mock("../src/lib/use-object-url", () => ({
  useObjectUrl: (bytes: Uint8Array | undefined) => (bytes ? "blob:preview" : undefined),
}));

import { RoomView } from "../src/components/RoomView";
import { UnsentMessagesProvider } from "../src/components/UnsentNotices";
import { downloadBytes } from "../src/lib/attachments";
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

beforeEach(() => {
  resetWorld();
  deviceStatus.current = null;
});
afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe("locked messages", () => {
  const pending = { table: "messages", id: "m3", code: "key-pending" };

  it("says a message is decrypting while the room's key is on its way, not that the room is empty", async () => {
    deviceStatus.current = "active";
    show({ messages: { data: [], unreadable: [pending] } });
    expect(await screen.findByText(/Decrypting 1 message/)).toBeTruthy();
    expect(screen.queryByText(/locked on this browser/)).toBeNull();
    expect(screen.queryByText("No messages yet")).toBeNull();
  });

  it("says messages are waiting for the room's key once it has been on its way for a while", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    deviceStatus.current = "active";
    show({ messages: { data: [], unreadable: [pending] } });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect(screen.getByText(/1 message is waiting for the room's key/)).toBeTruthy();
    expect(screen.queryByText(/Decrypting/)).toBeNull();
  });

  it("says messages are locked once a room has had no key record for a while", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    show({
      messages: {
        data: [],
        unreadable: [{ table: "messages", id: "m3", code: "space-pending" }],
      },
    });
    expect(screen.getByText(/Decrypting 1 message/)).toBeTruthy();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect(screen.getByText(/1 message is locked on this browser/)).toBeTruthy();
    expect(screen.queryByText(/waiting for the room's key/)).toBeNull();
  });

  it("says messages are locked after a while when this browser's status is still unknown", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    show({ messages: { data: [], unreadable: [pending] } });
    expect(screen.getByText(/Decrypting 1 message/)).toBeTruthy();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect(screen.getByText(/1 message is locked on this browser/)).toBeTruthy();
  });

  it("says messages are locked on this browser while it waits for approval", async () => {
    deviceStatus.current = "awaiting-approval";
    show({ messages: { data: [], unreadable: [pending] } });
    expect(await screen.findByText(/1 message is locked on this browser/)).toBeTruthy();
    expect(screen.queryByText(/Decrypting/)).toBeNull();
  });

  it("says messages are locked on this browser when it has no access to the key", async () => {
    show({
      messages: {
        data: [],
        unreadable: [{ table: "messages", id: "m3", code: "key-unavailable" }],
      },
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
    await screen.findByText(/Decrypting 1 message/);
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

  it.each(["image/png", "audio/ogg"])(
    "starts a new %s preview after a terminal error",
    async (attachmentType) => {
      const answers: Parameters<typeof show>[0] = {
        messages: {
          data: [{ ...message("m3", "2026-01-03"), attachmentName: "take", attachmentType }],
        },
        attachment: { error: new Error("Connection lost") },
      };
      show(answers);
      expect(await screen.findByText("Connection lost")).toBeTruthy();

      answers.attachment = { data: [{ attachment: new Uint8Array([1, 2, 3]) }] };
      await act(async () => fireEvent.click(screen.getByRole("button", { name: "Retry preview" })));
      expect(screen.queryByText("Connection lost")).toBeNull();
      expect(db.subscribe).toHaveBeenCalledTimes(2);
      if (attachmentType === "image/png") {
        expect(screen.getByRole("img", { name: "take" }).getAttribute("src")).toBe("blob:preview");
      } else {
        expect(screen.getByLabelText("take").getAttribute("src")).toBe("blob:preview");
      }
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

  it("retries a locked download and clears the reason once the file downloads", async () => {
    const missing: unknown[] = [];
    const attachment = new Uint8Array([1, 2, 3]);
    const download = vi.mocked(downloadBytes);
    download.mockClear();
    const reads = [missing, [{ attachment }]];
    db.all.mockImplementation(async (query) =>
      (query as Q).columns?.includes("attachment") ? (reads.shift() ?? []) : [{ id: "m" }],
    );
    const unreadableRows = vi
      .spyOn(reactMock, "unreadableRows")
      .mockImplementation((result) => (result === missing ? [{ code: "key-pending" }] : []));
    try {
      show({
        messages: {
          data: [
            {
              ...message("m3", "2026-01-03"),
              attachmentName: "take.txt",
              attachmentType: "text/plain",
            },
          ],
        },
      });
      fireEvent.click(await screen.findByLabelText("Download take.txt"));
      expect(await screen.findByText("key-pending")).toBeTruthy();
      expect(download).not.toHaveBeenCalled();

      await act(async () =>
        fireEvent.click(screen.getByRole("button", { name: "Retry download" })),
      );
      expect(download).toHaveBeenCalledExactlyOnceWith(attachment, "take.txt", "text/plain");
      expect(screen.queryByText("key-pending")).toBeNull();
      expect(screen.getByLabelText("Download take.txt")).toBeTruthy();
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

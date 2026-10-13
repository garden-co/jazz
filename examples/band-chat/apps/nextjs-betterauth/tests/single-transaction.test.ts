import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { historyQuery, mergeHistoryPages } from "../src/components/RoomView";
import { app } from "../schema";
import permissions from "../permissions";

// The app writes a room with its creator's membership, and a sketch's canvas
// with its message, in one transaction each: a policy `exists` check sees rows
// the same transaction wrote earlier (garden-co/jazz#3755).

let testApp: PolicyTestApp;

beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});
afterEach(async () => {
  await testApp.shutdown();
});

it("creates a room with its creator's membership, then a sketch with its message, in one transaction each", async () => {
  const ownerAuthor = "00000000-0000-4000-8000-000000000021";
  const owner = testApp.as({
    issuer: "https://bandchat.example",
    user_id: "owner",
    account_id: ownerAuthor,
    claims: {},
    authMode: "external",
  });
  const profile = await owner
    .insert(app.profiles, { author: ownerAuthor, displayName: "Owner" })
    .wait({ tier: "global" });

  // As in NewRoomDialog.
  const creating = await owner.transaction((tx) => {
    const room = tx.insert(app.rooms, { name: "Soundcheck" });
    tx.insert(app.roomMembers, {
      roomId: room.id,
      memberAuthor: ownerAuthor,
      memberProfileId: profile.id,
    });
    return room;
  });
  const room = await creating.wait({ tier: "global" });
  expect(
    (await owner.all(app.roomMembers.where({ roomId: room.id }), { tier: "remote" })).map(
      (member) => member.memberAuthor,
    ),
  ).toEqual([ownerAuthor]);

  // As in RoomView's "Start a sketch".
  const sketching = await owner.transaction((tx) => {
    const canvas = tx.insert(app.canvases, { roomId: room.id, title: "Sketch" });
    tx.insert(app.messages, {
      roomId: room.id,
      senderId: profile.id,
      text: "",
      canvasId: canvas.id,
    });
    return canvas;
  });
  const canvas = await sketching.wait({ tier: "global" });
  expect(
    (await owner.all(app.messages.where({ roomId: room.id }), { tier: "remote" })).map(
      (message) => message.canvasId,
    ),
  ).toEqual([canvas.id]);
});

it("does not lose messages when a history page boundary shares a timestamp", async () => {
  const ownerAuthor = "00000000-0000-4000-8000-000000000031";
  const owner = testApp.as({
    issuer: "https://bandchat.example",
    user_id: "pagination-owner",
    account_id: ownerAuthor,
    claims: {},
    authMode: "external",
  });
  const profile = await owner
    .insert(app.profiles, { author: ownerAuthor, displayName: "Pagination owner" })
    .wait({ tier: "global" });
  const roomResult = await owner.transaction((tx) => {
    const room = tx.insert(app.rooms, { name: "Same timestamp" });
    tx.insert(app.roomMembers, {
      roomId: room.id,
      memberAuthor: ownerAuthor,
      memberProfileId: profile.id,
    });
    return room;
  });
  const room = await roomResult.wait({ tier: "global" });
  const realNow = Date.now();
  const messages = await (async () => {
    const clock = vi.spyOn(Date, "now").mockReturnValue(realNow);
    try {
      return await owner.transaction((tx) => {
        const inserted = [];
        for (let index = 0; index < 55; index++) {
          inserted.push(
            tx.insert(app.messages, {
              roomId: room.id,
              senderId: profile.id,
              text: `message ${index}`,
            }),
          );
        }
        return inserted;
      });
    } finally {
      clock.mockRestore();
    }
  })();
  await messages.wait({ tier: "global" });

  const firstPage = await owner.all(historyQuery(room.id, {}), { tier: "global" });
  expect(firstPage).toHaveLength(50);

  const oldest = firstPage.at(-1)!;
  const offset = firstPage.filter(
    (message) => message.$createdAt.getTime() === oldest.$createdAt.getTime(),
  ).length;
  const pinnedLiveWindow = await owner.all(historyQuery(room.id, { from: oldest.$createdAt }), {
    tier: "global",
  });
  const olderPage = await owner.all(
    historyQuery(room.id, { before: { at: oldest.$createdAt, offset } }),
    { tier: "global" },
  );
  expect(olderPage[0].$createdAt.getTime()).toBe(oldest.$createdAt.getTime());
  const visibleMessages = mergeHistoryPages(pinnedLiveWindow, olderPage);
  expect(visibleMessages).toHaveLength(55);
  expect(new Set(visibleMessages.map((message) => message.id)).size).toBe(55);
});

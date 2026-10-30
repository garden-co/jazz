import { afterEach, beforeEach, expect, it } from "vitest";
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
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
    tx.update(app.rooms, room.id, { lastActivityAt: new Date() });
    return canvas;
  });
  const canvas = await sketching.wait({ tier: "global" });
  expect(
    (await owner.all(app.messages.where({ roomId: room.id }), { tier: "remote" })).map(
      (message) => message.canvasId,
    ),
  ).toEqual([canvas.id]);
});

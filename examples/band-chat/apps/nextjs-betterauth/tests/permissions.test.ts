import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { app } from "../schema";
import permissions from "../permissions";

let testApp: PolicyTestApp;

beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});
afterEach(async () => {
  await testApp.shutdown();
});

describe("BandChat room admission and authorship", () => {
  it("allows owner bootstrap/invite/message and denies self-admission, forged authorship, and post-removal writes", async () => {
    const ownerId = "owner";
    const guestId = "guest";
    const owner = testApp.as({
      issuer: "https://bandchat.example",
      user_id: ownerId,
      claims: {},
      authMode: "external",
    });
    const guest = testApp.as({
      issuer: "https://bandchat.example",
      user_id: guestId,
      claims: {},
      authMode: "external",
    });
    const ownerProfile = await testApp.seed((db) =>
      db.insert(app.profiles, { userId: ownerId, displayName: "Owner" }),
    );
    const guestProfile = await testApp.seed((db) =>
      db.insert(app.profiles, { userId: guestId, displayName: "Guest" }),
    );
    const room = await owner
      .insert(app.rooms, { name: "Private rehearsal" })
      .wait({ tier: "edge" });

    await owner
      .insert(app.roomMembers, { roomId: room.id, userId: ownerId })
      .wait({ tier: "edge" });
    await guest.expectDenied((db) =>
      db.insert(app.roomMembers, { roomId: room.id, userId: guestId }),
    );
    const membership = await owner
      .insert(app.roomMembers, { roomId: room.id, userId: guestId })
      .wait({ tier: "edge" });
    await guest
      .insert(app.messages, { roomId: room.id, senderId: guestProfile.id, text: "legitimate" })
      .wait({ tier: "edge" });
    await guest.expectDenied((db) =>
      db.insert(app.messages, { roomId: room.id, senderId: ownerProfile.id, text: "forged" }),
    );
    await owner.delete(app.roomMembers, membership.id).wait({ tier: "edge" });
    await guest.expectDenied((db) =>
      db.insert(app.messages, {
        roomId: room.id,
        senderId: guestProfile.id,
        text: "after removal",
      }),
    );
  });
});

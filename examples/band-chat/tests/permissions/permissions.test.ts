import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { app } from "../../schema.js";
import permissions from "../../permissions.js";

let testApp: PolicyTestApp;
beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});
afterEach(async () => {
  await testApp.shutdown();
});

/** Unlike PolicyTestApp.expectAllowed, this requires a serving-authority receipt. */
async function expectAcceptedAtEdge<T>(write: {
  wait(options: { tier: "edge" }): Promise<T>;
}): Promise<T> {
  return await write.wait({ tier: "edge" });
}

describe("BandChat admission and authorship boundary", () => {
  it("persists owner bootstrap/invite/send and rejects self-admission and forged authorship at edge", async () => {
    const owner = testApp.as({ user_id: "owner", claims: {}, authMode: "local-first" });
    const guest = testApp.as({ user_id: "guest", claims: {}, authMode: "local-first" });
    const ownerProfile = await testApp.seed((db) =>
      db.insert(app.profiles, { userId: "owner", displayName: "Owner" }),
    );
    const guestProfile = await testApp.seed((db) =>
      db.insert(app.profiles, { userId: "guest", displayName: "Guest" }),
    );
    const room = await expectAcceptedAtEdge(
      owner.insert(app.rooms, { name: "Private rehearsal", createdBy: "owner" }),
    );

    await expectAcceptedAtEdge(owner.insert(app.roomMembers, { roomId: room.id, userId: "owner" }));
    await guest.expectDenied((db) =>
      db.insert(app.roomMembers, { roomId: room.id, userId: "guest" }),
    );
    const guestMembership = await expectAcceptedAtEdge(
      owner.insert(app.roomMembers, { roomId: room.id, userId: "guest" }),
    );
    await expectAcceptedAtEdge(
      guest.insert(app.messages, {
        roomId: room.id,
        senderId: guestProfile.id,
        text: "legit",
        createdAt: new Date(),
      }),
    );
    await guest.expectDenied((db) =>
      db.insert(app.messages, {
        roomId: room.id,
        senderId: ownerProfile.id,
        text: "forged",
        createdAt: new Date(),
      }),
    );
    await expectAcceptedAtEdge(owner.delete(app.roomMembers, guestMembership.id));
    await guest.expectDenied((db) =>
      db.insert(app.messages, {
        roomId: room.id,
        senderId: guestProfile.id,
        text: "after removal",
        createdAt: new Date(),
      }),
    );
  });
});

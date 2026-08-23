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
    const ownerId = "019d4349-24b0-72a9-ae86-8ed24a7e3a90";
    const guestId = "019d4349-24b0-72a9-ae86-8ed24a7e3a91";
    const owner = testApp.as({ user_id: ownerId, claims: {}, authMode: "external" });
    const guest = testApp.as({ user_id: guestId, claims: {}, authMode: "external" });
    const ownerProfile = await testApp.seed((db) =>
      db.insert(app.profiles, { userId: ownerId, displayName: "Owner" }),
    );
    const guestProfile = await testApp.seed((db) =>
      db.insert(app.profiles, { userId: guestId, displayName: "Guest" }),
    );
    const room = await expectAcceptedAtEdge(owner.insert(app.rooms, { name: "Private rehearsal" }));

    await expectAcceptedAtEdge(owner.insert(app.roomMembers, { roomId: room.id, userId: ownerId }));
    await guest.expectDenied((db) =>
      db.insert(app.roomMembers, { roomId: room.id, userId: guestId }),
    );
    const guestMembership = await expectAcceptedAtEdge(
      owner.insert(app.roomMembers, { roomId: room.id, userId: guestId }),
    );
    await expectAcceptedAtEdge(
      guest.insert(app.messages, {
        roomId: room.id,
        senderId: guestProfile.id,
        text: "legit",
      }),
    );
    await guest.expectDenied((db) =>
      db.insert(app.messages, {
        roomId: room.id,
        senderId: ownerProfile.id,
        text: "forged",
      }),
    );
    await expectAcceptedAtEdge(owner.delete(app.roomMembers, guestMembership.id));
    await guest.expectDenied((db) =>
      db.insert(app.messages, {
        roomId: room.id,
        senderId: guestProfile.id,
        text: "after removal",
      }),
    );
  });
});

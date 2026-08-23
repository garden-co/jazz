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

describe("BandChat admission and authorship boundary", () => {
  it("allows creator bootstrap/invite/send but rejects self-admission and forged authorship", async () => {
    const owner = testApp.as({ user_id: "owner", claims: {}, authMode: "local-first" });
    const guest = testApp.as({ user_id: "guest", claims: {}, authMode: "local-first" });
    const ownerProfile = await testApp.seed((db) =>
      db.insert(app.profiles, { userId: "owner", displayName: "Owner" }),
    );
    const guestProfile = await testApp.seed((db) =>
      db.insert(app.profiles, { userId: "guest", displayName: "Guest" }),
    );
    owner.expectAllowed((db) =>
      db.insert(app.rooms, { name: "Creator-owned room", createdBy: "owner" }),
    );
    const room = await testApp.seed((db) =>
      db.insert(app.rooms, { name: "Private rehearsal", createdBy: "owner" }),
    );

    owner.expectAllowed((db) => db.insert(app.roomMembers, { roomId: room.id, userId: "owner" }));
    await guest.expectDenied((db) =>
      db.insert(app.roomMembers, { roomId: room.id, userId: "guest" }),
    );
    owner.expectAllowed((db) => db.insert(app.roomMembers, { roomId: room.id, userId: "guest" }));
    guest.expectAllowed((db) =>
      db.insert(app.messages, {
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
  });
});

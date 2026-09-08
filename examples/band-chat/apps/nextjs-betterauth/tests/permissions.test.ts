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
    const ownerAuthor = "00000000-0000-4000-8000-000000000001";
    const guestAuthor = "00000000-0000-4000-8000-000000000002";
    const owner = testApp.as({
      issuer: "https://bandchat.example",
      user_id: ownerId,
      account_id: ownerAuthor,
      claims: {},
      authMode: "external",
    });
    const guest = testApp.as({
      issuer: "https://bandchat.example",
      user_id: guestId,
      account_id: guestAuthor,
      claims: {},
      authMode: "external",
    });
    const ownerProfile = await owner
      .insert(app.profiles, { author: ownerAuthor, displayName: "Owner" })
      .wait({ tier: "edge" });
    const guestProfile = await guest
      .insert(app.profiles, { author: guestAuthor, displayName: "Guest" })
      .wait({ tier: "edge" });
    const room = await owner
      .insert(app.rooms, { name: "Private rehearsal" })
      .wait({ tier: "edge" });

    await owner
      .insert(app.roomMembers, { roomId: room.id, memberAuthor: ownerAuthor })
      .wait({ tier: "edge" });
    const sameSubjectFromAnotherIssuer = testApp.as({
      issuer: "https://other-provider.example",
      user_id: ownerId,
      account_id: "00000000-0000-4000-8000-000000000003",
      claims: {},
      authMode: "external",
    });
    await sameSubjectFromAnotherIssuer.expectDenied((db) =>
      db.insert(app.roomMembers, { roomId: room.id, memberAuthor: guestAuthor }),
    );
    await sameSubjectFromAnotherIssuer.expectDenied((db) =>
      db.insert(app.profiles, { author: ownerAuthor, displayName: "Impostor" }),
    );
    expect(await sameSubjectFromAnotherIssuer.all(app.profiles)).toEqual([]);
    expect(await sameSubjectFromAnotherIssuer.all(app.rooms)).toEqual([]);
    await sameSubjectFromAnotherIssuer.expectDenied((db) =>
      db.insert(app.messages, {
        roomId: room.id,
        senderId: ownerProfile.id,
        text: "cross-issuer impostor",
      }),
    );
    await guest.expectDenied((db) =>
      db.insert(app.roomMembers, { roomId: room.id, memberAuthor: guestAuthor }),
    );
    const membership = await owner
      .insert(app.roomMembers, { roomId: room.id, memberAuthor: guestAuthor })
      .wait({ tier: "edge" });
    const guestMessage = await guest
      .insert(app.messages, { roomId: room.id, senderId: guestProfile.id, text: "legitimate" })
      .wait({ tier: "edge" });
    await guest
      .insert(app.reactions, {
        roomId: room.id,
        messageId: guestMessage.id,
        author: guestAuthor,
        emoji: "🎸",
      })
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

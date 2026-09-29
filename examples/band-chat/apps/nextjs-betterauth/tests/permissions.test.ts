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
      .wait({ tier: "global" });
    const guestProfile = await guest
      .insert(app.profiles, { author: guestAuthor, displayName: "Guest" })
      .wait({ tier: "global" });
    const room = await owner
      .insert(app.rooms, { name: "Private rehearsal" })
      .wait({ tier: "global" });

    await owner
      .insert(app.roomMembers, { roomId: room.id, memberAuthor: ownerAuthor })
      .wait({ tier: "global" });
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
      .wait({ tier: "global" });
    const guestMessage = await guest
      .insert(app.messages, { roomId: room.id, senderId: guestProfile.id, text: "legitimate" })
      .wait({ tier: "global" });
    await guest
      .insert(app.reactions, {
        roomId: room.id,
        messageId: guestMessage.id,
        author: guestAuthor,
        emoji: "🎸",
      })
      .wait({ tier: "global" });
    await guest.expectDenied((db) =>
      db.insert(app.messages, { roomId: room.id, senderId: ownerProfile.id, text: "forged" }),
    );
    await owner.delete(app.roomMembers, membership.id).wait({ tier: "global" });
    await guest.expectDenied((db) =>
      db.insert(app.messages, {
        roomId: room.id,
        senderId: guestProfile.id,
        text: "after removal",
      }),
    );
  });

  it("routes join requests to the creator, keeps admission creator-only, and scopes profiles, markers and sketches", async () => {
    const ownerAuthor = "00000000-0000-4000-8000-000000000011";
    const guestAuthor = "00000000-0000-4000-8000-000000000012";
    const strangerAuthor = "00000000-0000-4000-8000-000000000013";
    const as = (userId: string, accountId: string) =>
      testApp.as({
        issuer: "https://bandchat.example",
        user_id: userId,
        account_id: accountId,
        claims: {},
        authMode: "external",
      });
    const owner = as("owner", ownerAuthor);
    const guest = as("guest", guestAuthor);
    const stranger = as("stranger", strangerAuthor);
    const ownerProfile = await owner
      .insert(app.profiles, { author: ownerAuthor, displayName: "Owner" })
      .wait({ tier: "global" });
    const guestProfile = await guest
      .insert(app.profiles, { author: guestAuthor, displayName: "Guest" })
      .wait({ tier: "global" });
    const strangerProfile = await stranger
      .insert(app.profiles, { author: strangerAuthor, displayName: "Stranger" })
      .wait({ tier: "global" });
    const room = await owner.insert(app.rooms, { name: "Setlist" }).wait({ tier: "global" });
    await owner
      .insert(app.roomMembers, {
        roomId: room.id,
        memberAuthor: ownerAuthor,
        memberProfileId: ownerProfile.id,
      })
      .wait({ tier: "global" });

    // Nobody but yourself sees your profile before you share a room or ask to join.
    expect((await owner.all(app.profiles)).map((profile) => profile.id)).toEqual([ownerProfile.id]);

    // A join request must name the requester's own profile.
    await guest.expectDenied((db) =>
      db.insert(app.joinRequests, {
        roomId: room.id,
        requester: guestAuthor,
        profileId: strangerProfile.id,
      }),
    );
    await guest.expectDenied((db) =>
      db.insert(app.joinRequests, {
        roomId: room.id,
        requester: strangerAuthor,
        profileId: guestProfile.id,
      }),
    );
    const request = await guest
      .insert(app.joinRequests, {
        roomId: room.id,
        requester: guestAuthor,
        profileId: guestProfile.id,
      })
      .wait({ tier: "global" });
    // The creator sees the request and the requester's profile; others do not.
    expect((await owner.all(app.joinRequests)).map((row) => row.id)).toEqual([request.id]);
    expect((await owner.all(app.profiles)).map((profile) => profile.displayName).sort()).toEqual([
      "Guest",
      "Owner",
    ]);
    expect(await stranger.all(app.joinRequests)).toEqual([]);
    // Asking is not admission.
    await guest.expectDenied((db) =>
      db.insert(app.roomMembers, { roomId: room.id, memberAuthor: guestAuthor }),
    );
    // The admitted profile must belong to the admitted account.
    await owner.expectDenied((db) =>
      db.insert(app.roomMembers, {
        roomId: room.id,
        memberAuthor: guestAuthor,
        memberProfileId: strangerProfile.id,
      }),
    );
    const guestMembership = await owner
      .insert(app.roomMembers, {
        roomId: room.id,
        memberAuthor: guestAuthor,
        memberProfileId: guestProfile.id,
      })
      .wait({ tier: "global" });
    await owner.delete(app.joinRequests, request.id).wait({ tier: "global" });

    // Co-members see each other's profiles.
    expect((await guest.all(app.profiles)).map((profile) => profile.displayName).sort()).toEqual([
      "Guest",
      "Owner",
    ]);
    // A member cannot admit someone else or remove another member.
    await guest.expectDenied((db) =>
      db.insert(app.roomMembers, {
        roomId: room.id,
        memberAuthor: strangerAuthor,
        memberProfileId: strangerProfile.id,
      }),
    );
    const ownerMembership = (
      await guest.all(app.roomMembers.where({ memberAuthor: ownerAuthor }))
    )[0]!;
    await guest.expectDenied((db) => db.delete(app.roomMembers, ownerMembership.id));
    // A member records activity but cannot rename the room.
    await guest.update(app.rooms, room.id, { lastActivityAt: new Date() }).wait({ tier: "global" });
    await guest.expectDenied((db) => db.update(app.rooms, room.id, { name: "Renamed" }));
    await owner.update(app.rooms, room.id, { name: "Setlist v2" }).wait({ tier: "global" });

    // Read markers are private to their reader and need membership.
    const marker = await guest
      .insert(app.readMarkers, { roomId: room.id, reader: guestAuthor, lastReadAt: new Date() })
      .wait({ tier: "global" });
    expect(await owner.all(app.readMarkers)).toEqual([]);
    // Someone else's marker is not even visible, so the update fails up front.
    expect(() => owner.update(app.readMarkers, marker.id, { lastReadAt: new Date() })).toThrow(
      /read policy denied/,
    );
    await stranger.expectDenied((db) =>
      db.insert(app.readMarkers, {
        roomId: room.id,
        reader: strangerAuthor,
        lastReadAt: new Date(),
      }),
    );

    // Members draw on a room's sketch; outsiders and forged authors cannot.
    const canvas = await guest
      .insert(app.canvases, { roomId: room.id, title: "Stage plot" })
      .wait({ tier: "global" });
    await guest
      .insert(app.messages, {
        roomId: room.id,
        senderId: guestProfile.id,
        text: "",
        canvasId: canvas.id,
      })
      .wait({ tier: "global" });
    await owner
      .insert(app.strokes, {
        canvasId: canvas.id,
        roomId: room.id,
        author: ownerAuthor,
        color: "blue",
        width: 6,
        points: [10, 10, 200, 200],
      })
      .wait({ tier: "global" });
    await guest.expectDenied((db) =>
      db.insert(app.strokes, {
        canvasId: canvas.id,
        roomId: room.id,
        author: ownerAuthor,
        color: "red",
        width: 6,
        points: [1, 1],
      }),
    );
    await stranger.expectDenied((db) =>
      db.insert(app.strokes, {
        canvasId: canvas.id,
        roomId: room.id,
        author: strangerAuthor,
        color: "red",
        width: 6,
        points: [1, 1],
      }),
    );
    expect(await stranger.all(app.strokes)).toEqual([]);

    // A member may leave on their own; afterwards they can no longer draw.
    await guest.delete(app.roomMembers, guestMembership.id).wait({ tier: "global" });
    await guest.expectDenied((db) =>
      db.insert(app.strokes, {
        canvasId: canvas.id,
        roomId: room.id,
        author: guestAuthor,
        color: "red",
        width: 6,
        points: [1, 1],
      }),
    );
  });
});

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
      .insert(app.roomMembers, {
        roomId: room.id,
        memberAuthor: ownerAuthor,
        memberProfileId: ownerProfile.id,
      })
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
    // Knowing an account id is not enough: the creator admits the guest's own
    // profile after the guest asks to join.
    await owner.expectDenied((db) =>
      db.insert(app.roomMembers, { roomId: room.id, memberAuthor: guestAuthor }),
    );
    await owner.expectDenied((db) =>
      db.insert(app.roomMembers, {
        roomId: room.id,
        memberAuthor: guestAuthor,
        memberProfileId: guestProfile.id,
      }),
    );
    await guest
      .insert(app.joinRequests, {
        roomId: room.id,
        requester: guestAuthor,
        profileId: guestProfile.id,
      })
      .wait({ tier: "global" });
    const membership = await owner
      .insert(app.roomMembers, {
        roomId: room.id,
        memberAuthor: guestAuthor,
        memberProfileId: guestProfile.id,
      })
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
    // Without a join request, the creator cannot add them, even with the
    // matching account id and profile.
    await owner.expectDenied((db) =>
      db.insert(app.roomMembers, {
        roomId: room.id,
        memberAuthor: strangerAuthor,
        memberProfileId: strangerProfile.id,
      }),
    );
    // A membership must name a profile.
    await owner.expectDenied((db) =>
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
    // The app admits and clears the request in one transaction.
    const admission = await owner.transaction((tx) => {
      const member = tx.insert(app.roomMembers, {
        roomId: room.id,
        memberAuthor: guestAuthor,
        memberProfileId: guestProfile.id,
      });
      tx.delete(app.joinRequests, request.id);
      return member;
    });
    const guestMembership = await admission.wait({ tier: "global" });
    expect(await owner.all(app.joinRequests)).toEqual([]);

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
    // Members never write to the room; only its creator renames it.
    await guest.expectDenied((db) => db.update(app.rooms, room.id, { name: "Renamed" }));
    await owner.update(app.rooms, room.id, { name: "Setlist v2" }).wait({ tier: "global" });

    // A read marker is one "read up to" row per member: co-members see it
    // (check marks and "Read by"), outsiders do not.
    const marker = await guest
      .insert(app.readMarkers, { roomId: room.id, reader: guestAuthor, lastReadAt: new Date() })
      .wait({ tier: "global" });
    expect((await owner.all(app.readMarkers)).map((row) => row.reader)).toEqual([guestAuthor]);
    expect(await stranger.all(app.readMarkers)).toEqual([]);
    // Only the reader moves their marker.
    await owner.expectDenied((db) =>
      db.update(app.readMarkers, marker.id, { lastReadAt: new Date() }),
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
    // The app writes the canvas and its message in one transaction (see
    // tests/single-transaction.test.ts); here the canvas is already
    // committed, which the message policy accepts too.
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

    // A member may leave on their own, dropping their read markers in the
    // same transaction; afterwards they can no longer draw.
    const guestMarkers = await guest.all(app.readMarkers.where({ reader: guestAuthor }));
    const leaving = await guest.transaction((tx) => {
      for (const marker of guestMarkers) tx.delete(app.readMarkers, marker.id);
      tx.delete(app.roomMembers, guestMembership.id);
    });
    await leaving.wait({ tier: "global" });
    expect(await guest.all(app.readMarkers)).toEqual([]);
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

describe("BandChat read markers", () => {
  const ownerAuthor = "00000000-0000-4000-8000-000000000021";
  const guestAuthor = "00000000-0000-4000-8000-000000000022";
  const drummerAuthor = "00000000-0000-4000-8000-000000000023";
  const strangerAuthor = "00000000-0000-4000-8000-000000000024";
  const as = (userId: string, accountId: string) =>
    testApp.as({
      issuer: "https://bandchat.example",
      user_id: userId,
      account_id: accountId,
      claims: {},
      authMode: "external",
    });

  /** A room with its creator and two admitted members. */
  async function roomWithMembers() {
    const owner = as("owner", ownerAuthor);
    const guest = as("guest", guestAuthor);
    const drummer = as("drummer", drummerAuthor);
    const stranger = as("stranger", strangerAuthor);
    const profileOf = (db: typeof owner, author: string, displayName: string) =>
      db.insert(app.profiles, { author, displayName }).wait({ tier: "global" });
    const ownerProfile = await profileOf(owner, ownerAuthor, "Owner");
    const room = await owner.insert(app.rooms, { name: "Setlist" }).wait({ tier: "global" });
    await owner
      .insert(app.roomMembers, {
        roomId: room.id,
        memberAuthor: ownerAuthor,
        memberProfileId: ownerProfile.id,
      })
      .wait({ tier: "global" });
    const memberships: Record<string, string> = {};
    for (const [db, author, name] of [
      [guest, guestAuthor, "Guest"],
      [drummer, drummerAuthor, "Drummer"],
    ] as const) {
      const profile = await profileOf(db, author, name);
      const request = await db
        .insert(app.joinRequests, { roomId: room.id, requester: author, profileId: profile.id })
        .wait({ tier: "global" });
      const admission = await owner.transaction((tx) => {
        const member = tx.insert(app.roomMembers, {
          roomId: room.id,
          memberAuthor: author,
          memberProfileId: profile.id,
        });
        tx.delete(app.joinRequests, request.id);
        return member;
      });
      memberships[author] = (await admission.wait({ tier: "global" })).id;
    }
    const markerOf = (db: typeof owner, reader: string, lastReadAt: Date) =>
      db.insert(app.readMarkers, { roomId: room.id, reader, lastReadAt }).wait({ tier: "global" });
    return { owner, guest, drummer, stranger, room, memberships, markerOf };
  }

  it("lets only the reader move their marker, and only while a member", async () => {
    const { owner, guest, drummer, markerOf } = await roomWithMembers();
    const marker = await markerOf(guest, guestAuthor, new Date("2026-11-12T10:00:00Z"));
    await guest
      .update(app.readMarkers, marker.id, { lastReadAt: new Date("2026-11-12T10:05:00Z") })
      .wait({ tier: "global" });
    await owner.expectDenied((db) =>
      db.update(app.readMarkers, marker.id, { lastReadAt: new Date("2026-11-12T10:06:00Z") }),
    );
    await drummer.expectDenied((db) =>
      db.update(app.readMarkers, marker.id, { lastReadAt: new Date("2026-11-12T10:06:00Z") }),
    );
    // Nobody can hand their marker to someone else.
    await guest.expectDenied((db) =>
      db.update(app.readMarkers, marker.id, { reader: drummerAuthor }),
    );
  });

  it("lets only the reader or the room creator delete a marker", async () => {
    const { owner, guest, drummer, stranger, markerOf } = await roomWithMembers();
    const guestMarker = await markerOf(guest, guestAuthor, new Date());
    const drummerMarker = await markerOf(drummer, drummerAuthor, new Date());
    // Another member, or an outsider, cannot delete someone's marker.
    await drummer.expectDenied((db) => db.delete(app.readMarkers, guestMarker.id));
    await stranger.expectDenied((db) => db.delete(app.readMarkers, guestMarker.id));
    // The reader can (leaving), and so can the creator (removing a member).
    await guest.delete(app.readMarkers, guestMarker.id).wait({ tier: "global" });
    await owner.delete(app.readMarkers, drummerMarker.id).wait({ tier: "global" });
    expect(await owner.all(app.readMarkers, { tier: "remote" })).toEqual([]);
  });

  it("hides markers from outsiders and ex-members, and drops them with their member", async () => {
    const { owner, guest, drummer, stranger, memberships, markerOf } = await roomWithMembers();
    await markerOf(guest, guestAuthor, new Date());
    const drummerMarker = await markerOf(drummer, drummerAuthor, new Date());
    expect(await stranger.all(app.readMarkers, { tier: "remote" })).toEqual([]);
    expect(
      (await drummer.all(app.readMarkers, { tier: "remote" })).map((row) => row.reader).sort(),
    ).toEqual([guestAuthor, drummerAuthor].sort());

    // The creator removes the drummer and their marker together, as the app does.
    const removal = await owner.transaction((tx) => {
      tx.delete(app.readMarkers, drummerMarker.id);
      tx.delete(app.roomMembers, memberships[drummerAuthor]!);
    });
    await removal.wait({ tier: "global" });
    // The ex-member sees no markers; the others no longer see theirs.
    expect(await drummer.all(app.readMarkers, { tier: "remote" })).toEqual([]);
    expect((await owner.all(app.readMarkers, { tier: "remote" })).map((row) => row.reader)).toEqual(
      [guestAuthor],
    );
    // And an ex-member cannot write a marker back.
    await drummer.expectDenied((db) =>
      db.insert(app.readMarkers, {
        roomId: drummerMarker.roomId,
        reader: drummerAuthor,
        lastReadAt: new Date(),
      }),
    );
  });
});

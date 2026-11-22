// Who may read and manage a room. Jazz's member rows are the one membership
// truth: they decide who reads a room and its messages. Only a room's
// creator admits, removes, renames or otherwise updates it; any member may
// leave; no member adds anyone.
import { afterEach, describe, expect, it } from "vitest";
import { unreadableRows } from "jazz-tools/backend";
import { app, cleanUp, createRoom, device, memberRow, serve, until, type Device } from "./harness";

afterEach(cleanUp);

/** The messages of `roomId` that `reader` is sent, readable or not. */
async function messagesFor(reader: Device, roomId: string) {
  const rows = await reader.db.all(app.messages.where({ roomId }), { tier: "remote" });
  return { rows, unreadable: unreadableRows(rows) };
}

async function rejected(write: { wait(options: { tier: "global" }): Promise<unknown> }) {
  await expect(write.wait({ tier: "global" })).rejects.toThrow();
}

describe("one membership truth: the member row", () => {
  it("a profile membership without a member row reads neither the room nor its messages", async () => {
    const host = await serve();
    const alice = await device(host);
    const bob = await device(host);
    const room = await createRoom(alice, "Rehearsal");
    // Bob asks to join and alice gives him a profile membership, but no member row.
    await bob.db
      .insert(app.joinRequests, { roomId: room, requester: bob.account, profileId: bob.profile.id })
      .wait({ tier: "global" });
    await alice.db
      .insert(app.roomMembers, {
        roomId: room,
        memberAuthor: bob.account,
        memberProfileId: bob.profile.id,
      })
      .wait({ tier: "global" });
    await alice.db
      .insert(app.messages, { roomId: room, senderId: alice.profile.id, text: "set list" })
      .wait({ tier: "global" });

    expect(await bob.db.all(app.rooms, { tier: "remote" })).toEqual([]);
    expect(await messagesFor(bob, room)).toEqual({ rows: [], unreadable: [] });

    // The member row is what admits him: with it he reads the room and the message.
    await alice.db.insert(app.__e2ee_members, await memberRow(alice, room, bob.account)).wait({
      tier: "global",
    });
    await alice.db.encryption.keys.deliver();
    const seen = await until(async () => {
      const { rows } = await messagesFor(bob, room);
      return rows.length > 0 ? rows : undefined;
    }, "bob to read the message once he has a member row");
    expect(seen.map((message) => message.text)).toEqual(["set list"]);
  }, 90_000);

  it("a member row without a profile membership reads the room and its messages", async () => {
    const host = await serve();
    const alice = await device(host);
    const carol = await device(host);
    const room = await createRoom(alice, "Rehearsal");
    await alice.db.insert(app.__e2ee_members, await memberRow(alice, room, carol.account)).wait({
      tier: "global",
    });
    await alice.db
      .insert(app.messages, { roomId: room, senderId: alice.profile.id, text: "set list" })
      .wait({ tier: "global" });
    await alice.db.encryption.keys.deliver();

    expect(
      await carol.db.all(app.roomMembers.where({ roomId: room, memberAuthor: carol.account })),
    ).toEqual([]);
    const rooms = await carol.db.all(app.rooms, { tier: "remote" });
    expect(rooms.map((row) => row.name)).toEqual(["Rehearsal"]);
    const seen = await until(async () => {
      const { rows } = await messagesFor(carol, room);
      return rows.length > 0 ? rows : undefined;
    }, "carol to read the message");
    expect(seen.map((message) => message.text)).toEqual(["set list"]);
    // And, as a member, she may write.
    await carol.db
      .insert(app.messages, { roomId: room, senderId: carol.profile.id, text: "on my way" })
      .wait({ tier: "global" });
  }, 90_000);
});

describe("only the creator manages a room", () => {
  it("admits, removes and renames; a member does none of these, and may leave", async () => {
    const host = await serve();
    const alice = await device(host);
    const bob = await device(host);
    const carol = await device(host);
    const room = await createRoom(alice, "Rehearsal");

    // The creator admits bob as BandChat does: his member row and profile
    // membership with his join request cleared, in one transaction.
    const request = await bob.db
      .insert(app.joinRequests, { roomId: room, requester: bob.account, profileId: bob.profile.id })
      .wait({ tier: "global" });
    const bobRow = await memberRow(alice, room, bob.account);
    let bobMember!: { id: string };
    let bobMembership!: { id: string };
    const admitted = await alice.db.transaction((tx) => {
      bobMember = tx.insert(app.__e2ee_members, bobRow);
      bobMembership = tx.insert(app.roomMembers, {
        roomId: room,
        memberAuthor: bob.account,
        memberProfileId: bob.profile.id,
      });
      tx.delete(app.joinRequests, request.id);
    });
    await admitted.wait({ tier: "global" });
    await until(async () => {
      const rooms = await bob.db.all(app.rooms, { tier: "remote" });
      return rooms.length > 0 ? rooms : undefined;
    }, "bob to see the room he was admitted to");

    // A member adds nobody: not carol by a member row, and not through a
    // profile membership either, even when carol asked to join.
    await rejected(bob.db.insert(app.__e2ee_members, await memberRow(bob, room, carol.account)));
    await carol.db
      .insert(app.joinRequests, {
        roomId: room,
        requester: carol.account,
        profileId: carol.profile.id,
      })
      .wait({ tier: "global" });
    await rejected(
      bob.db.insert(app.roomMembers, {
        roomId: room,
        memberAuthor: carol.account,
        memberProfileId: carol.profile.id,
      }),
    );
    expect(await carol.db.all(app.rooms, { tier: "remote" })).toEqual([]);

    // A member doesn't update the room at all: no rename, and not even a
    // save that keeps the name (whoever may update a room manages its members).
    await rejected(bob.db.update(app.rooms, room, { name: "Bob's room" }));
    await rejected(bob.db.update(app.rooms, room, { name: "Rehearsal" }));
    // The creator renames it.
    await alice.db.update(app.rooms, room, { name: "Gig" }).wait({ tier: "global" });

    // A member removes nobody else.
    const aliceRows = await alice.db.all(
      app.__e2ee_members.where({ ...bobRow, accountId: alice.account }),
      { tier: "remote" },
    );
    expect(aliceRows).toHaveLength(1);
    await rejected(bob.db.delete(app.__e2ee_members, aliceRows[0]!.id));

    // A member may leave: his own rows go.
    await bob.db.delete(app.__e2ee_members, bobMember.id).wait({ tier: "global" });
    await bob.db.delete(app.roomMembers, bobMembership.id).wait({ tier: "global" });
    expect(await bob.db.all(app.rooms, { tier: "remote" })).toEqual([]);

    // The creator admits carol, who asked, and removes her again.
    const carolMember = await alice.db
      .insert(app.__e2ee_members, await memberRow(alice, room, carol.account))
      .wait({ tier: "global" });
    await until(async () => {
      const rooms = await carol.db.all(app.rooms, { tier: "remote" });
      return rooms.length > 0 ? rooms : undefined;
    }, "carol to see the room");
    await alice.db.delete(app.__e2ee_members, carolMember.id).wait({ tier: "global" });
    expect(await carol.db.all(app.rooms, { tier: "remote" })).toEqual([]);
  }, 120_000);
});

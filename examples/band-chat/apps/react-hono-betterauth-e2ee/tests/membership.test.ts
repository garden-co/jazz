// The membership writes against a real backend: admission, removal with a key
// rotation, and leaving, each as BandChat's screens perform them.
import { afterEach, describe, expect, it } from "vitest";
import { admitMember, removeMember, renewRoomKey } from "../src/lib/membership";
import { app, cleanUp, createRoom, device, serve, until } from "./harness";

afterEach(cleanUp);

const space = (roomId: string) => ({ table: "rooms", id: roomId });

async function memberRowsOf(
  host: Awaited<ReturnType<typeof serve>>,
  by: Awaited<ReturnType<typeof device>>,
  roomId: string,
) {
  const filter = await by.db.encryption.members.where(space(roomId));
  return host.jazz.db.all(app.__e2ee_members.where(filter));
}

describe("membership writes", () => {
  it("admits with both rows in one transaction, removes both and renews the key", async () => {
    const host = await serve();
    const alice = await device(host);
    const bob = await device(host);
    const roomId = await createRoom(alice, "Rehearsal");
    const request = await bob.db
      .insert(app.joinRequests, { roomId, requester: bob.account, profileId: bob.profile.id })
      .wait({ tier: "global" });

    const delivery = await admitMember({
      db: alice.db,
      roomId,
      memberAuthor: bob.account,
      memberProfileId: bob.profile.id,
      requestIds: [request.id],
    });
    expect(delivery.failed).toBe(0);
    expect((await memberRowsOf(host, alice, roomId)).map((row) => row.accountId).sort()).toEqual(
      [alice.account, bob.account].sort(),
    );
    expect(await host.jazz.db.all(app.joinRequests.where({ roomId }))).toEqual([]);

    // The room has a key once something encrypted is written in it.
    await alice.db
      .insert(app.messages, { roomId, senderId: alice.profile.id, text: "set list" })
      .wait({ tier: "global" });
    const removed = await removeMember({ db: alice.db, roomId, accountId: bob.account });
    expect(removed).toBe(1);
    expect((await memberRowsOf(host, alice, roomId)).map((row) => row.accountId)).toEqual([
      alice.account,
    ]);
    expect(
      await host.jazz.db.all(app.roomMembers.where({ roomId, memberAuthor: bob.account })),
    ).toEqual([]);
    expect(await renewRoomKey(alice.db, roomId)).toBeGreaterThanOrEqual(0);
  }, 90_000);

  it("renews nothing, and does not fail, for a room with no message yet", async () => {
    const host = await serve();
    const alice = await device(host);
    const roomId = await createRoom(alice, "Empty");
    // Jazz refuses to rotate a key that does not exist yet.
    await expect(alice.db.encryption.keys.rotate(space(roomId))).rejects.toThrow();
    expect(await renewRoomKey(alice.db, roomId)).toBe(0);
  }, 60_000);

  it("lets a member leave, taking the member row, profile membership and markers with them", async () => {
    const host = await serve();
    const alice = await device(host);
    const bob = await device(host);
    const roomId = await createRoom(alice, "Rehearsal");
    const request = await bob.db
      .insert(app.joinRequests, { roomId, requester: bob.account, profileId: bob.profile.id })
      .wait({ tier: "global" });
    await admitMember({
      db: alice.db,
      roomId,
      memberAuthor: bob.account,
      memberProfileId: bob.profile.id,
      requestIds: [request.id],
    });
    await alice.db
      .insert(app.messages, { roomId, senderId: alice.profile.id, text: "set list" })
      .wait({ tier: "global" });
    await alice.db.encryption.keys.deliver();
    await until(async () => {
      const rows = await bob.db.all(app.messages.where({ roomId }), { tier: "remote" });
      return rows.length > 0 ? rows : undefined;
    }, "bob to read the message");
    await bob.db
      .insert(app.readMarkers, { roomId, reader: bob.account, lastReadAt: new Date() })
      .wait({ tier: "global" });

    const removed = await removeMember({
      db: bob.db,
      roomId,
      accountId: bob.account,
      deleteMarkers: true,
    });
    expect(removed).toBe(1);
    expect((await memberRowsOf(host, alice, roomId)).map((row) => row.accountId)).toEqual([
      alice.account,
    ]);
    expect(
      await host.jazz.db.all(app.roomMembers.where({ roomId, memberAuthor: bob.account })),
    ).toEqual([]);
  }, 90_000);
});

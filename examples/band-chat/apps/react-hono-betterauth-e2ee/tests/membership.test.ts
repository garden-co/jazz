// The membership writes against a real backend: admission, removal with a key
// rotation, and leaving, each as BandChat's screens perform them.
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Db } from "jazz-tools";
import { admitMember, removeMember, renewRoomKey } from "../src/lib/membership";
import { app, cleanUp, createRoom, device, memberRow, serve, until } from "./harness";

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

async function admit(
  alice: Awaited<ReturnType<typeof device>>,
  bob: Awaited<ReturnType<typeof device>>,
  roomId: string,
) {
  const request = await bob.db
    .insert(app.joinRequests, { roomId, requester: bob.account, profileId: bob.profile.id })
    .wait({ tier: "global" });
  return admitMember({
    db: alice.db,
    roomId,
    memberAuthor: bob.account,
    memberProfileId: bob.profile.id,
    requestIds: [request.id],
  });
}

async function interleave(
  db: Db,
  afterReadOf: string,
  effect: () => Promise<void>,
  run: (transaction: ReturnType<typeof vi.spyOn>) => Promise<void>,
) {
  const transaction = db.serializableTransaction.bind(db);
  const transactionSpy = vi.spyOn(db, "serializableTransaction");
  let completed = false;
  transactionSpy.mockImplementation((callback) =>
    transaction(async (tx) => {
      const all = tx.all.bind(tx);
      tx.all = (async (...args: Parameters<Db["all"]>) => {
        const rows = await all(...args);
        if (!completed && args[0]._table === afterReadOf) {
          completed = true;
          await effect();
        }
        return rows;
      }) as typeof tx.all;
      const result = await callback(tx);
      if (!completed) {
        completed = true;
        await effect();
      }
      return result;
    }),
  );
  try {
    await run(transactionSpy);
  } finally {
    transactionSpy.mockRestore();
  }
}

describe("membership writes", () => {
  it("admits with both rows in one transaction, removes both and renews the key", async () => {
    const host = await serve();
    const alice = await device(host);
    const bob = await device(host);
    const roomId = await createRoom(alice, "Rehearsal");
    await admit(alice, bob, roomId);
    expect((await alice.db.encryption.keys.deliver()).failed).toBe(0);
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

  it("retries the paired removal if another admission commits after its reads", async () => {
    const host = await serve();
    const alice = await device(host);
    const bob = await device(host);
    const roomId = await createRoom(alice, "Rehearsal");
    await admit(alice, bob, roomId);

    // Run a second real transaction after the removal has taken its snapshot,
    // before the server sees its commit. The authority decides the conflict.
    await interleave(
      alice.db,
      "roomMembers",
      () => admit(alice, bob, roomId),
      async (removal) => {
        expect(await removeMember({ db: alice.db, roomId, accountId: bob.account })).toBe(2);
        expect(removal).toHaveBeenCalledTimes(2);
        expect((await memberRowsOf(host, alice, roomId)).map((row) => row.accountId)).toEqual([
          alice.account,
        ]);
        expect(
          await host.jazz.db.all(app.roomMembers.where({ roomId, memberAuthor: bob.account })),
        ).toEqual([]);
      },
    );
  }, 90_000);

  it("retries when a concurrent admission changes only Jazz's member rows", async () => {
    const host = await serve();
    const alice = await device(host);
    const bob = await device(host);
    const roomId = await createRoom(alice, "Rehearsal");
    const row = await memberRow(alice, roomId, bob.account);
    const addMember = () => alice.db.insert(app.__e2ee_members, row).wait({ tier: "global" });
    await addMember();

    // Both reads have returned. Only the member read should conflict.
    await interleave(alice.db, "roomMembers", addMember, async (removal) => {
      expect(await removeMember({ db: alice.db, roomId, accountId: bob.account })).toBe(2);
      expect(removal).toHaveBeenCalledTimes(2);
      expect((await memberRowsOf(host, alice, roomId)).map((member) => member.accountId)).toEqual([
        alice.account,
      ]);
    });
  }, 90_000);

  it("renews the key of a room with no message yet without failing: there is nothing to renew", async () => {
    const host = await serve();
    const alice = await device(host);
    const roomId = await createRoom(alice, "Empty");
    expect(await renewRoomKey(alice.db, roomId)).toBe(0);
  }, 60_000);

  it("lets a member leave, taking the member row, profile membership and markers with them", async () => {
    const host = await serve();
    const alice = await device(host);
    const bob = await device(host);
    const roomId = await createRoom(alice, "Rehearsal");
    await admit(alice, bob, roomId);
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

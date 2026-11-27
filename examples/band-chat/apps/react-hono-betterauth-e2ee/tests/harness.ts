// BandChat's generated app on a real Jazz backend that holds no keys, with
// device clients that each keep their own keys: what the browser does,
// on Node. Device accounts stand in for Better Auth users here, so the
// app's permissions are tested apart from its sign-in.
import { cleanUpTestApp, openEncryptedTestDevice, serveEncryptedTestApp } from "jazz-tools/testing";
import { app } from "../jazz.generated";
import permissions from "../jazz.permissions.json" with { type: "json" };

export { app };
export { storedBytes, pollTestValue as until } from "jazz-tools/testing";

const cleanups: Array<() => Promise<unknown>> = [];

export function cleanUp(): Promise<void> {
  return cleanUpTestApp(cleanups);
}

export function serve() {
  return serveEncryptedTestApp({ app, permissions }, cleanups);
}

type Host = Awaited<ReturnType<typeof serve>>;

/** A device client with its own keys and a BandChat profile. */
export async function device(host: Host) {
  const { db, account } = await openEncryptedTestDevice(host, { app, permissions }, cleanups);
  const profile = await db
    .insert(app.profiles, { author: account, displayName: account.slice(0, 8) })
    .wait({ tier: "global" });
  return { db, account, profile };
}

export type Device = Awaited<ReturnType<typeof device>>;

/** A room's member row for `account`, as `by` fills in its address. */
export function memberRow(by: Device, roomId: string, account: string) {
  return by.db.encryption.members.row({ table: "rooms", id: roomId }, account);
}

/** A new room, made as BandChat makes it: the room, its creator's member row and profile membership in one transaction. */
export async function createRoom(creator: Device, name: string) {
  const id = crypto.randomUUID();
  const self = await memberRow(creator, id, creator.account);
  const written = await creator.db.transaction((tx) => {
    tx.insert(app.rooms, { name }, { id });
    tx.insert(app.__e2ee_members, self);
    tx.insert(app.roomMembers, {
      roomId: id,
      memberAuthor: creator.account,
      memberProfileId: creator.profile.id,
    });
  });
  await written.wait({ tier: "global" });
  return id;
}

// BandChat's generated app on a real Jazz backend that holds no keys, with
// device clients that each keep their own keys: what the browser does,
// on Node. Device accounts stand in for Better Auth users here, so the
// app's permissions are tested apart from its sign-in.
import { mkdtemp, readdir, readFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect } from "vitest";
import {
  acceptDeviceAccounts,
  createJazz,
  createJazzClient,
  memoryPrivateStore,
  sqlite,
} from "jazz-tools/backend";
import { app } from "../jazz.generated";
import permissions from "../jazz.permissions.json" with { type: "json" };

export { app };

const cleanups: Array<() => Promise<unknown>> = [];

/** Runs every cleanup registered since the last call, newest first. */
export async function cleanUp(): Promise<void> {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
}

/** A BandChat backend on a free port, storing its data under `dir`. */
export async function serve() {
  const dir = await mkdtemp(join(tmpdir(), "band-chat-e2ee-"));
  const jazz = await createJazz({
    app,
    permissions,
    store: sqlite({ path: `${dir}/core.sqlite` }),
    authenticate: acceptDeviceAccounts,
  }).ready();
  cleanups.push(() => jazz.close());
  const server: Server = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end();
    }),
  );
  await jazz.attach(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const stop = async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    await jazz.close();
  };
  cleanups.push(stop);
  const port = (server.address() as AddressInfo).port;
  return { jazz, dir, serverUrl: `http://127.0.0.1:${port}/jazz`, stop };
}

export type Host = Awaited<ReturnType<typeof serve>>;

/** A device client with its own keys, signed in as a new account, its encryption active. */
export async function device(host: Host) {
  let stored: string | null = null;
  const session = await createJazzClient({
    serverUrl: host.serverUrl,
    appId: host.jazz.appId,
    app,
    permissions,
    store: {
      read: async () => stored,
      update: async (transform) => void (stored = transform(stored)),
    },
    initial: "device",
    driver: { type: "memory" },
    encryption: { store: memoryPrivateStore() },
  });
  cleanups.push(() => session.close());
  const db = session.db;
  expect(await db.encryption.ready({ timeoutMs: 20_000 })).toBe("active");
  const account = session.getSnapshot().user!.accountId;
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

/** Waits until `read` returns something other than `undefined`. */
export async function until<T>(
  read: () => Promise<T | undefined>,
  what: string,
  ms = 20_000,
): Promise<T> {
  const end = Date.now() + ms;
  for (;;) {
    let failure: unknown;
    const value = await read().catch((error) => {
      failure = error;
      return undefined;
    });
    if (value !== undefined) return value;
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}: ${String(failure)}`);
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

/** Every byte under `path` (a file, or a directory of them). */
export async function storedBytes(path: string): Promise<Buffer> {
  const entries = await readdir(path, { withFileTypes: true }).catch(() => undefined);
  if (!entries) return readFile(path);
  const parts = await Promise.all(entries.map((entry) => storedBytes(`${path}/${entry.name}`)));
  return Buffer.concat(parts);
}

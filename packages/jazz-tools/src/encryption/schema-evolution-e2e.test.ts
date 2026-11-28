import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { AccountStore } from "../accounts/persistence.js";
import { acceptDeviceAccounts, createJazz, createJazzClient, sqlite } from "../backend/index.js";
import { createAppMigration, generateApp } from "../dev/project.js";
import { testApp } from "../testing/index.js";
import { memoryPrivateStore, unreadableRows } from "./index.js";

// End to end on Node: an encrypted app written in schema.ts gains a column
// in its second version, through `migrations create` and `generate`, and a
// row written under the first version still reads under the second.

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

function schema(messages: string): string {
  return `
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  rooms: s.table({ name: s.string() }, {}, { space: "id" }),
  messages: s.table(
    { roomId: s.uuid(), text: s.string().encrypted()${messages} },
    { room: s.rel("rooms", "roomId") },
    { space: "room" },
  ),
});

const { definePermissions, isMember, session } = schema;
export const permissions = definePermissions({
  rooms: {
    read: (room) => isMember("rooms", room.id),
    insert: { "$createdBy.account": session.user },
    update: { "$createdBy.account": session.user },
  },
  messages: {
    read: (message) => isMember("rooms", message.roomId),
    insert: (message) => isMember("rooms", message.roomId),
  },
});
`;
}

/** The app's two files as `generate` writes them. */
async function files(dir: string) {
  await generateApp({ cwd: dir });
  return {
    app: testApp(await readFile(join(dir, "jazz.app.json"), "utf8")),
    permissions: JSON.parse(await readFile(join(dir, "jazz.permissions.json"), "utf8")),
  };
}

type Files = Awaited<ReturnType<typeof files>>;

async function serve(version: Files, dir: string, port = 0) {
  const jazz = await createJazz({
    app: version.app,
    permissions: version.permissions,
    store: sqlite({ path: `${dir}/core.sqlite` }),
    authenticate: acceptDeviceAccounts,
  }).ready();
  const server: Server = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end();
    }),
  );
  await jazz.attach(server);
  await new Promise<void>((resolve) => server.listen(port, "127.0.0.1", resolve));
  const stop = async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    await jazz.close();
  };
  cleanups.push(stop);
  const listening = (server.address() as AddressInfo).port;
  return { jazz, port: listening, serverUrl: `http://127.0.0.1:${listening}/jazz`, stop };
}

function memoryStore(): AccountStore {
  let stored: string | null = null;
  return {
    read: async () => stored,
    update: async (transform) => void (stored = transform(stored)),
  };
}

describe("an encrypted app's second version", () => {
  it("adds an optional encrypted column that rows written before it read as null", async () => {
    const project = await mkdtemp(`${tmpdir()}/e2ee-evolve-app-`);
    await writeFile(join(project, "package.json"), '{ "type": "module" }\n');
    await writeFile(join(project, "schema.ts"), schema(""));
    await createAppMigration({ cwd: project, name: "init" });
    const first = await files(project);
    await writeFile(
      join(project, "schema.ts"),
      schema(", subject: s.string().optional().encrypted()"),
    );
    const created = await createAppMigration({ cwd: project, name: "subject" });
    expect(created.todos).toEqual([]);
    const second = await files(project);

    const serverDir = await mkdtemp(`${tmpdir()}/e2ee-evolve-server-`);
    const accounts = memoryStore();
    const privateStore = memoryPrivateStore();
    const dataPath = `${await mkdtemp(`${tmpdir()}/e2ee-evolve-client-`)}/device`;
    const open = async (version: Files, host: Awaited<ReturnType<typeof serve>>) => {
      const session = await createJazzClient({
        serverUrl: host.serverUrl,
        appId: host.jazz.appId,
        app: version.app,
        permissions: version.permissions,
        store: accounts,
        initial: "device",
        driver: { type: "persistent", dataPath },
        encryption: { store: privateStore },
      });
      cleanups.push(() => session.close());
      expect(await session.db.encryption.ready({ timeoutMs: 20_000 })).toBe("active");
      return session;
    };

    // The first version: a room and a message in it.
    const before = await serve(first, serverDir);
    const alice = await open(first, before);
    const account = alice.getSnapshot().user!.accountId;
    const v1 = first.app as never as Record<string, never>;
    const room = crypto.randomUUID();
    const member = await alice.db.encryption.members.row({ table: "rooms", id: room }, account);
    const written = await alice.db.transaction((tx) => {
      tx.insert(v1.rooms!, { name: "band" } as never, { id: room });
      tx.insert(v1.__e2ee_members!, member as never);
    });
    await written.wait({ tier: "global" });
    await alice.db
      .insert(v1.messages!, { roomId: room, text: "written before" } as never)
      .wait({ tier: "global" });
    await alice.close();
    const { port } = before;
    await before.stop();

    // The second version, on the same server store and the same device.
    const after = await serve(second, serverDir, port);
    const again = await open(second, after);
    const v2 = second.app as never as Record<string, never>;
    await again.db
      .insert(v2.messages!, { roomId: room, text: "written after", subject: "hi" } as never)
      .wait({ tier: "global" });
    const rows = (await again.db.all(
      (v2.messages as unknown as { where(c: unknown): never }).where({ roomId: room }),
      { tier: "remote" },
    )) as Array<{ text: string; subject: string | null }>;
    expect(unreadableRows(rows as never)).toEqual([]);
    expect(
      rows
        .map(({ text, subject }) => ({ text, subject }))
        .sort((a, b) => (a.text < b.text ? 1 : -1)),
    ).toEqual([
      { text: "written before", subject: null },
      { text: "written after", subject: "hi" },
    ]);
  }, 120_000);
});

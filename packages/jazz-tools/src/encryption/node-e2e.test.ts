import { mkdtemp, readdir, readFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { afterEach, describe, expect, it } from "vitest";
import { app } from "../../tests/fixtures/encrypted/jazz.generated.js";
import permissions from "../../tests/fixtures/encrypted/jazz.permissions.json";
import { testApp } from "../testing/index.js";
import {
  acceptDeviceAccounts,
  createJazz,
  createJazzClient,
  memory,
  sqlite,
} from "../backend/index.js";
import { generateTs } from "../dev/codegen-napi.js";
import type { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import type { AccountStore } from "../accounts/persistence.js";
import {
  EncryptionNotDeclaredError,
  EncryptionStoreRequiredError,
  memoryPrivateStore,
  unreadableRows,
  type PrivateStore,
} from "./index.js";

// End to end on Node: device clients with their own private stores talk to a
// real server through createJazz, which relays them and holds no keys.

it.each(["../../tests/fixtures/encrypted/", "../../tests/browser/apps/encrypted-tab/"])(
  "keeps the generated bindings for %s current",
  async (path) => {
    const directory = new URL(path, import.meta.url);
    const [source, bindings] = await Promise.all([
      readFile(new URL("jazz.app.json", directory), "utf8"),
      readFile(new URL("jazz.generated.ts", directory)),
    ]);
    expect(Buffer.from(await generateTs(source)).equals(bindings)).toBe(true);
  },
);

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

async function serve(
  again?: { dir: string; port: number },
  served = permissions,
  storage: "sqlite" | "memory" = "sqlite",
) {
  const dir = again?.dir ?? (await mkdtemp(`${tmpdir()}/e2ee-server-`));
  const jazz = await createJazz({
    app,
    permissions: served,
    store: storage === "memory" ? memory() : sqlite({ path: `${dir}/core.sqlite` }),
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
  await new Promise<void>((resolve) => server.listen(again?.port ?? 0, "127.0.0.1", resolve));
  cleanups.push(
    () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(resolve);
      }),
  );
  const port = (server.address() as AddressInfo).port;
  return {
    jazz,
    permissions: served,
    dir,
    port,
    storePath: `${dir}/core.sqlite`,
    serverUrl: `http://127.0.0.1:${port}/jazz`,
    /** Takes the server down, keeping its data. */
    async stop() {
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
      await jazz.close();
    },
  };
}

function memoryStore(): AccountStore {
  let stored: string | null = null;
  return {
    read: async () => stored,
    update: async (transform) => void (stored = transform(stored)),
  };
}

/** Every byte under `path` (a file, or a directory of them). */
async function storedBytes(path: string): Promise<Buffer> {
  const entries = await readdir(path, { withFileTypes: true }).catch(() => undefined);
  if (!entries) return readFile(path);
  const parts = await Promise.all(entries.map((entry) => storedBytes(`${path}/${entry.name}`)));
  return Buffer.concat(parts);
}

type Server_ = Awaited<ReturnType<typeof serve>>;

/** A device client with its own private store, signed in as a new device account or as `key`'s. */
async function device(
  host: Pick<Server_, "serverUrl" | "jazz"> & { permissions?: typeof permissions },
  options: {
    key?: string;
    privateStore?: PrivateStore;
    accounts?: AccountStore;
    dataPath?: string;
    wait?: boolean;
  } = {},
) {
  const session = await createJazzClient({
    serverUrl: host.serverUrl,
    appId: host.jazz.appId,
    app,
    permissions: host.permissions ?? permissions,
    store: options.accounts ?? memoryStore(),
    initial: "device",
    driver: options.dataPath
      ? { type: "persistent", dataPath: options.dataPath }
      : { type: "memory" },
    encryption: { store: options.privateStore ?? memoryPrivateStore() },
  });
  cleanups.push(() => session.close());
  if (options.key) await session.restoreDeviceAccount(options.key);
  const db = session.db;
  if (options.wait === false)
    return { session, db, account: session.getSnapshot().user!.accountId };
  const status = await db.encryption.ready({ timeoutMs: 20_000 });
  expect(status).toBe("active");
  return { session, db, account: session.getSnapshot().user!.accountId };
}

type Device = Awaited<ReturnType<typeof device>>;

const spacesTable = (app as unknown as Record<string, never>)["__e2ee_spaces"];

/** A new project, with its creator as its first member in the same transaction. */
async function makeProject(owner: Device, name: string) {
  const id = crypto.randomUUID();
  const self = await owner.db.encryption.members.row({ table: "projects", id }, owner.account);
  const written = await owner.db.transaction((tx) => {
    tx.insert(app.projects, { name, ownerId: owner.account }, { id });
    tx.insert(app.__e2ee_members, self);
  });
  await written.wait({ tier: "global" });
  return id;
}

async function addMember(owner: Device, project: string, account: string) {
  const row = await owner.db.encryption.members.row({ table: "projects", id: project }, account);
  await owner.db.insert(app.__e2ee_members, row).wait({ tier: "global" });
}

/** Waits until `read` returns something. */
async function until<T>(read: () => Promise<T | undefined>, what: string, ms = 20_000): Promise<T> {
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

describe("encryption on Node, end to end", () => {
  it.each(["new", "existing"] as const)(
    "streams encrypted bytes when the project key is %s",
    async (key) => {
      const host = await serve();
      const alice = await device(host);
      const projectId = await makeProject(alice, "plans");
      const content = new TextEncoder().encode("private attachment bytes across two chunks");
      const ordinary = () =>
        alice.db.insert(app.files, { projectId, content }).wait({ tier: "global" });
      if (key === "existing") await ordinary();
      async function* chunks() {
        yield content.subarray(0, 9);
        yield content.subarray(9);
      }
      const streamed = await alice.db.insertStreaming(app.files, { projectId, content: chunks() });
      await streamed.wait({ tier: "global" });
      expect(await host.jazz.db.all(spacesTable)).toMatchObject([{ identifier: projectId }]);
      if (key === "new") await ordinary();
      const rows = await alice.db.all(app.files.where({ projectId }), { tier: "remote" });
      expect(rows.map((row) => row.content)).toEqual([content, content]);
      const stored = await storedBytes(host.dir);
      expect(stored.includes(Buffer.from(content.subarray(0, 9)))).toBe(false);
      expect(stored.includes(Buffer.from(content.subarray(9)))).toBe(false);
    },
    60_000,
  );

  it("aborting the first encrypted stream discards its upload and publishes no root or row", async () => {
    const host = await serve();
    const alice = await device(host);
    const projectId = await makeProject(alice, "plans");
    async function* interrupted() {
      yield new Uint8Array(256 * 1024).fill(42);
      throw new Error("attachment interrupted");
    }
    await expect(
      alice.db.insertStreaming(app.files, { projectId, content: interrupted() }),
    ).rejects.toThrow("attachment interrupted");
    const runtime = alice.db.currentRuntime() as NativeRuntimeAdapter;
    runtime.setLargeValueStagingPolicy(Number.MAX_SAFE_INTEGER, 60_000, 0);
    expect(await runtime.evictExpiredStagedLargeValues()).toBe(0);
    expect(await host.jazz.db.all(spacesTable)).toEqual([]);
    expect(await alice.db.all(app.files.where({ projectId }), { tier: "remote" })).toEqual([]);
    async function* replacement() {
      yield Uint8Array.of(7);
    }
    const written = await alice.db.insertStreaming(app.files, {
      projectId,
      content: replacement(),
    });
    await written.wait({ tier: "global" });
    expect(await host.jazz.db.all(spacesTable)).toMatchObject([{ identifier: projectId }]);
    expect(await alice.db.all(app.files.where({ projectId }), { tier: "remote" })).toMatchObject([
      { content: Uint8Array.of(7) },
    ]);
  }, 60_000);

  it.each(["files", "captionedFiles"] as const)(
    "rotates before pulling the first byte from %s after a member is removed",
    async (table) => {
      const open = structuredClone(permissions) as { tables: Record<string, unknown> };
      // Bob can fetch the new row after removal, so the read tests decryption.
      open.tables[table] = { read: "true", insert: "allowedTo.read(project)" };
      const host = await serve(undefined, open as never);
      const alice = await device(host);
      const bob = await device(host);
      const projectId = await makeProject(alice, "plans");
      await addMember(alice, projectId, bob.account);
      await alice.db
        .insert(app.tasks, { projectId, title: "before removal" })
        .wait({ tier: "global" });
      await alice.db.encryption.keys.deliver();
      await until(async () => {
        const rows = await bob.db.all(app.tasks.where({ projectId }), { tier: "remote" });
        return rows[0]?.title === "before removal" ? true : undefined;
      }, "Bob to receive the original key");

      const members = await alice.db.all(app.__e2ee_members, { tier: "remote" });
      const member = members.find((row) => row.accountId === bob.account)!;
      await alice.db.delete(app.__e2ee_members, member.id).wait({ tier: "global" });
      async function* chunks() {
        const successors = (app as unknown as Record<string, never>)["__e2ee_space_successors"];
        expect(await host.jazz.db.all(successors)).toMatchObject([{ identifier: projectId }]);
        yield Uint8Array.of(3, 4);
      }
      const streamed =
        table === "files"
          ? await alice.db.insertStreaming(app.files, { projectId, content: chunks() })
          : await alice.db.insertStreaming(app.captionedFiles, {
              projectId,
              content: chunks(),
              caption: "after removal",
            });
      await streamed.wait({ tier: "global" });

      const query = app[table].where({ projectId });
      expect(await alice.db.all(query, { tier: "remote" })).toMatchObject([
        {
          content: Uint8Array.of(3, 4),
          ...(table === "captionedFiles" ? { caption: "after removal" } : {}),
        },
      ]);
      // Read each encrypted column on its own: one unreadable column hides the whole row.
      const contentQuery =
        table === "files"
          ? app.files.where({ projectId }).select("projectId", "content")
          : app.captionedFiles.where({ projectId }).select("projectId", "content");
      const locked = await bob.db.all(contentQuery, { tier: "remote" });
      expect(locked).toEqual([]);
      expect(unreadableRows(locked)).toMatchObject([{ column: "content" }]);
      if (table === "captionedFiles") {
        const caption = await bob.db.all(
          app.captionedFiles.where({ projectId }).select("projectId", "caption"),
          { tier: "remote" },
        );
        expect(caption).toEqual([]);
        expect(unreadableRows(caption)).toMatchObject([{ column: "caption" }]);
      }
    },
    90_000,
  );

  it("two members share a space and read each other's encrypted rows", async () => {
    await twoMembersShareASpace(await serve());
  }, 90_000);

  it("two members share a space on a server with in-memory storage", async () => {
    await twoMembersShareASpace(await serve(undefined, permissions, "memory"));
  }, 90_000);

  async function twoMembersShareASpace(host: Server_) {
    const alice = await device(host);
    const bob = await device(host);
    const project = await makeProject(alice, "plans");
    await addMember(alice, project, bob.account);
    await alice.db
      .insert(app.tasks, { projectId: project, title: "alice's secret" })
      .wait({ tier: "global" });
    // Alice's first write made the space's key: she serves her member now
    // (otherwise the pass runs at the next natural moment, or while she has
    // a subscription on the space).
    await alice.db.encryption.keys.deliver();
    const seen = await until(async () => {
      const rows = await bob.db.all(app.tasks.where({ projectId: project }), { tier: "remote" });
      return rows.length > 0 ? rows : undefined;
    }, "bob to read alice's task");
    expect(seen[0]!.title).toBe("alice's secret");
    await bob.db
      .insert(app.tasks, { projectId: project, title: "bob's reply" })
      .wait({ tier: "global" });
    const back = await until(async () => {
      const rows = await alice.db.all(app.tasks.where({ projectId: project }), { tier: "remote" });
      return rows.length > 1 ? rows : undefined;
    }, "alice to read bob's task");
    expect(back.map((row) => row.title).sort()).toEqual(["alice's secret", "bob's reply"]);
  }

  it("rotates an empty space and explains when a member is waiting for delivery", async () => {
    const host = await serve();
    const alice = await device(host);
    const bob = await device(host);
    const project = await makeProject(alice, "plans");
    const space = { table: "projects", id: project };
    await expect.soft(alice.db.encryption.keys.rotate(space)).resolves.toBe(0);

    await alice.db
      .insert(app.tasks, { projectId: project, title: "first" })
      .wait({ tier: "global" });
    // The only holder is offline when Bob joins, so no delivery can race his rotation.
    await alice.session.close();
    const row = await bob.db.encryption.members.row(space, bob.account);
    await host.jazz.db.insert(app.__e2ee_members, row).wait({ tier: "global" });
    await expect(bob.db.encryption.keys.rotate(space)).rejects.toMatchObject({
      code: "rotation-needs-key",
      message: "This device does not hold the space's current key yet",
    });
  }, 90_000);

  it("a removed member cannot open what is written after the rotation", async () => {
    // Tasks are readable to everyone here, so the removed member is handed the
    // ciphertext of the rows written after the rotation: it holds no key for them.
    const open = structuredClone(permissions) as { tables: Record<string, unknown> };
    open.tables.tasks = { read: "true", insert: "allowedTo.read(project)" };
    const host = await serve(undefined, open as never);
    const alice = await device(host);
    const bob = await device(host);
    const project = await makeProject(alice, "plans");
    await addMember(alice, project, bob.account);
    await alice.db
      .insert(app.tasks, { projectId: project, title: "before", label: "1" })
      .wait({ tier: "global" });
    await alice.db.encryption.keys.deliver();
    await until(async () => {
      const rows = await bob.db.all(app.tasks.where({ projectId: project }), { tier: "remote" });
      return rows.length > 0 ? rows : undefined;
    }, "bob to read the first task");
    // Alice removes bob and rotates the space's key at once.
    const rows = await alice.db.all(app.__e2ee_members, { tier: "remote" });
    const bobs = rows.filter((row) => row.accountId === bob.account);
    for (const row of bobs)
      await alice.db.delete(app.__e2ee_members, row.id).wait({ tier: "global" });
    const delivered = await alice.db.encryption.keys.rotate({ table: "projects", id: project });
    expect(delivered).toBe(0);
    await alice.db
      .insert(app.tasks, { projectId: project, title: "after", label: "2" })
      .wait({ tier: "global" });
    const later = await alice.db.all(app.tasks.where({ projectId: project }), { tier: "remote" });
    expect(later.map((row) => row.title).sort()).toEqual(["after", "before"]);
    const after = later.find((row) => row.title === "after")!.id;
    // Bob is sent both rows; the one written before he was removed still
    // opens under the key he holds, the one after does not. His read returns
    // the first and names the second as left out, with why; the runtime's
    // reports say so too.
    const reports: { code?: string; table?: string }[] = [];
    bob.db.encryption.onUnreadable((report) => reports.push(report as never));
    const tasks = app.tasks.where({ projectId: project });
    const bobSees = await until(async () => {
      const found = await bob.db.all(tasks, { tier: "remote" });
      return unreadableRows(found).length > 0 ? found : undefined;
    }, "bob's read to name the rotated row as unreadable");
    expect(bobSees.map((row) => row.title)).toEqual(["before"]);
    expect(unreadableRows(bobSees)).toEqual([
      { table: "tasks", id: after, column: "title", code: expect.stringMatching(/^key-/) },
    ]);
    await until(async () => (reports.length > 0 ? reports : undefined), "the report");
    expect(
      reports.some((report) => report.table === "tasks" && /^key-/.test(report.code ?? "")),
    ).toBe(true);

    // Pages count the row he cannot open: the second page is empty but
    // names it, so it does not look like the end; the third is the end.
    const page = (offset: number) =>
      bob.db.all(tasks.orderBy("label").limit(1).offset(offset), { tier: "remote" });
    const first = await page(0);
    expect(first.map((row) => row.title)).toEqual(["before"]);
    expect(unreadableRows(first)).toEqual([]);
    const second = await page(1);
    expect(second).toEqual([]);
    expect(unreadableRows(second).map((row) => row.id)).toEqual([after]);
    const third = await page(2);
    expect(third).toEqual([]);
    expect(unreadableRows(third)).toEqual([]);
    // `limit` alone: one row asked for, none returned, and the result says why.
    const limited = await bob.db.all(tasks.orderBy("label", "desc").limit(1), { tier: "remote" });
    expect(limited).toEqual([]);
    expect(unreadableRows(limited).map((row) => row.id)).toEqual([after]);
    // Alice holds every key: her results are complete.
    expect(unreadableRows(later)).toEqual([]);

    // His subscriptions say the same, and keep saying it as the result
    // changes: each delivered result names the rows it leaves out, a page
    // counts them, and the list follows the rows the result would hold.
    let whole: (typeof later)[number][] | undefined;
    let windowed: (typeof later)[number][] | undefined;
    const stopWhole = bob.db.subscribe(tasks, (rows) => (whole = rows), { tier: "remote" });
    const stopWindowed = bob.db.subscribe(
      tasks.orderBy("label", "desc").limit(1),
      (rows) => (windowed = rows),
      { tier: "remote" },
    );
    try {
      const named = await until(
        async () => (whole && unreadableRows(whole).length > 0 ? whole : undefined),
        "bob's subscription to name the rotated row",
      );
      expect(named.map((row) => row.title)).toEqual(["before"]);
      expect(unreadableRows(named)).toEqual([
        { table: "tasks", id: after, column: "title", code: expect.stringMatching(/^key-/) },
      ]);
      const page = await until(
        async () => (windowed && unreadableRows(windowed).length > 0 ? windowed : undefined),
        "bob's windowed subscription to name the rotated row",
      );
      expect(page).toEqual([]);
      expect(unreadableRows(page).map((row) => row.id)).toEqual([after]);

      // Another row he cannot open: his visible rows stay as they were, but
      // the next result names both, and the page moves on to the new row.
      await alice.db
        .insert(app.tasks, { projectId: project, title: "later", label: "3" })
        .wait({ tier: "global" });
      const all = await alice.db.all(tasks, { tier: "remote" });
      const laterId = all.find((row) => row.title === "later")!.id;
      const grown = await until(
        async () => (whole && unreadableRows(whole).length === 2 ? whole : undefined),
        "bob's subscription to name the second rotated row",
      );
      expect(grown.map((row) => row.title)).toEqual(["before"]);
      expect(
        unreadableRows(grown)
          .map((row) => row.id)
          .sort(),
      ).toEqual([after, laterId].sort());
      const moved = await until(
        async () =>
          windowed && unreadableRows(windowed)[0]?.id === laterId ? windowed : undefined,
        "bob's page to name the newest row instead",
      );
      expect(moved).toEqual([]);
      expect(unreadableRows(moved)).toHaveLength(1);
    } finally {
      stopWhole();
      stopWindowed();
    }
  }, 90_000);

  it("a device account restored from its key on a new device reads again, with no other device", async () => {
    const host = await serve();
    const first = await device(host);
    const project = await makeProject(first, "plans");
    await first.db
      .insert(app.tasks, { projectId: project, title: "kept" })
      .wait({ tier: "global" });
    const key = first.session.exportDeviceKey();
    // The first device goes away; a new device (an empty private store) restores the account.
    await first.session.close();
    const second = await device(host, { key });
    expect(second.account).toBe(first.account);
    const rows = await until(async () => {
      const found = await second.db.all(app.tasks.where({ projectId: project }), {
        tier: "remote",
      });
      return found.length > 0 ? found : undefined;
    }, "the restored device to read");
    expect(rows[0]!.title).toBe("kept");
    expect((await second.db.encryption.recovery.list()).length).toBe(1);
  }, 90_000);

  it("an equality query on an encrypted column finds the row by plaintext", async () => {
    const host = await serve();
    const alice = await device(host);
    const project = await makeProject(alice, "plans");
    for (const title of ["alpha", "beta", "alpha"]) {
      await alice.db.insert(app.tasks, { projectId: project, title }).wait({ tier: "global" });
    }
    const found = await alice.db.all(app.tasks.where({ projectId: project, title: "alpha" }), {
      tier: "remote",
    });
    expect(found.map((row) => row.title)).toEqual(["alpha", "alpha"]);
    expect(
      await alice.db.all(app.tasks.where({ projectId: project, title: "gamma" }), {
        tier: "remote",
      }),
    ).toEqual([]);
  }, 90_000);

  it("a backend relays, reads plain columns, and refuses encrypted ones, storing no plaintext", async () => {
    const host = await serve();
    const alice = await device(host);
    const project = await makeProject(alice, "plans");
    await alice.db
      .insert(app.tasks, { projectId: project, title: "ALICE-ONLY-PLAINTEXT", label: "plain" })
      .wait({ tier: "global" });
    // Plain tables and the plain columns of an encrypted table work for the backend.
    const projects = await host.jazz.db.all(app.projects);
    expect(projects.map((row) => row.name)).toEqual(["plans"]);
    const tasks = await until(async () => {
      const rows = await host.jazz.db.all(app.tasks);
      return rows.length > 0 ? rows : undefined;
    }, "the backend to see the task");
    expect(tasks[0]!.label).toBe("plain");
    expect((tasks[0] as { title?: unknown }).title).toBeUndefined();
    // It cannot write an encrypted column, and nothing plaintext is stored.
    await expect(
      Promise.resolve().then(() =>
        host.jazz.db.insert(app.tasks, { projectId: project, title: "BACKEND-PLAINTEXT" }),
      ),
    ).rejects.toThrow(/holds no encryption keys/);
    await expect(
      Promise.resolve().then(() =>
        host.jazz.db.all(app.tasks.where({ title: "ALICE-ONLY-PLAINTEXT" })),
      ),
    ).rejects.toThrow(/not supported/);
    await host.jazz.close();
    const stored = await storedBytes(host.storePath);
    expect(stored.includes(Buffer.from("BACKEND-PLAINTEXT"))).toBe(false);
    expect(stored.includes(Buffer.from("ALICE-ONLY-PLAINTEXT"))).toBe(false);
  }, 90_000);

  it("an encrypted app without a private store fails, naming the option", async () => {
    const host = await serve();
    const opened = createJazzClient({
      serverUrl: host.serverUrl,
      appId: host.jazz.appId,
      app,
      permissions,
      store: memoryStore(),
      initial: "device",
      driver: { type: "memory" },
    });
    await expect(opened).rejects.toBeInstanceOf(EncryptionStoreRequiredError);
    await expect(opened).rejects.toThrow(/encryption: \{ store \}/);
  }, 60_000);

  it("db.encryption throws for an app that declares no encryption", async () => {
    const plain = testApp({ format: 1, tables: { notes: { columns: { text: "string" } } } });
    const plainPermissions = { format: 1, tables: { notes: { read: "true", insert: "true" } } };
    const jazz = await createJazz({
      app: plain,
      permissions: plainPermissions as never,
      store: sqlite({ path: `${await mkdtemp(`${tmpdir()}/e2ee-plain-`)}/core.sqlite` }),
      authenticate: acceptDeviceAccounts,
    }).ready();
    cleanups.push(() => jazz.close());
    await jazz.db.insert(plain.notes, { text: "hello" }).wait({ tier: "global" });
    expect((await jazz.db.all(plain.notes)).map((row) => row.text)).toEqual(["hello"]);
    expect(() => jazz.db.encryption.devices).toThrow(EncryptionNotDeclaredError);
  }, 60_000);

  it("a device opened while the server is out of reach verifies once it is back", async () => {
    const first = await serve();
    const accounts = memoryStore();
    const privateStore = memoryPrivateStore();
    const dataPath = `${await mkdtemp(`${tmpdir()}/e2ee-client-`)}/device`;
    const online = await device(first, { accounts, privateStore, dataPath });
    const project = await makeProject(online, "plans");
    await online.db
      .insert(app.tasks, { projectId: project, title: "kept" })
      .wait({ tier: "global" });
    await online.session.close();
    const { dir, port, serverUrl, jazz } = first;
    const appId = jazz.appId;
    await first.stop();
    // The same device opens with the server down: its verification fails and is tried again.
    const offline = await device(
      { serverUrl, jazz: { appId } as never },
      { accounts, privateStore, dataPath, wait: false },
    );
    await new Promise((resolve) => setTimeout(resolve, 1500));
    expect(offline.db.encryption).toBeDefined();
    // The server comes back on the same address: the device verifies without being asked again.
    const back = await serve({ dir, port });
    expect(back.jazz.appId).toBe(appId);
    expect(await offline.db.encryption.ready({ timeoutMs: 30_000 })).toBe("active");
  }, 120_000);

  it("a returning device reads its encrypted rows offline, and its live query goes on once the server is back", async () => {
    const first = await serve();
    const accounts = memoryStore();
    const privateStore = memoryPrivateStore();
    const dataPath = `${await mkdtemp(`${tmpdir()}/e2ee-client-`)}/device`;
    const online = await device(first, { accounts, privateStore, dataPath });
    const project = await makeProject(online, "plans");
    await online.db
      .insert(app.tasks, { projectId: project, title: "kept" })
      .wait({ tier: "global" });
    await online.session.close();
    const { dir, port, serverUrl, jazz } = first;
    const appId = jazz.appId;
    await first.stop();
    // The same device opens with the server down. It has synced before: it
    // reads what its store holds with the keys it holds, at once.
    const offline = await device(
      { serverUrl, jazz: { appId } as never },
      { accounts, privateStore, dataPath, wait: false },
    );
    const seen: string[][] = [];
    const errors: unknown[] = [];
    const stop = offline.db.subscribe(
      app.tasks.where({ projectId: project }),
      {
        onUpdate: (rows) => seen.push(rows.map((row) => row.title).sort()),
        onError: (error) => errors.push(error),
      },
      { tier: "local-first" },
    );
    cleanups.push(async () => stop());
    await expect.poll(() => seen.at(-1), { timeout: 5_000 }).toEqual(["kept"]);
    expect(errors).toEqual([]);
    // The server comes back. Another client of the device writes a task: the
    // same live query, never ended, picks it up.
    const back = await serve({ dir, port });
    const other = await device(back, { accounts, privateStore });
    await other.db
      .insert(app.tasks, { projectId: project, title: "later" })
      .wait({ tier: "global" });
    await expect.poll(() => seen.at(-1), { timeout: 30_000 }).toEqual(["kept", "later"]);
    expect(errors).toEqual([]);
    // Its open sockets would hold the restarted server's close.
    stop();
    await other.session.close();
    await offline.session.close();
  }, 120_000);

  it("a device that never synced answers an encrypted live query only once it has synced", async () => {
    const first = await serve();
    const accounts = memoryStore();
    const privateStore = memoryPrivateStore();
    const online = await device(first, { accounts, privateStore });
    const project = await makeProject(online, "plans");
    await online.db
      .insert(app.tasks, { projectId: project, title: "kept" })
      .wait({ tier: "global" });
    await online.session.close();
    const { dir, port, serverUrl, jazz } = first;
    const appId = jazz.appId;
    await first.stop();
    // A new store, with the server down: it has never seen the app's
    // catalogue, so it cannot say what its encrypted rows are. The query
    // waits and says nothing, rather than answer an empty result.
    const fresh = await device(
      { serverUrl, jazz: { appId } as never },
      { accounts, privateStore, wait: false },
    );
    const seen: string[][] = [];
    const errors: unknown[] = [];
    const stop = fresh.db.subscribe(
      app.tasks.where({ projectId: project }),
      {
        onUpdate: (rows) => seen.push(rows.map((row) => row.title).sort()),
        onError: (error) => errors.push(error),
      },
      { tier: "local-first" },
    );
    cleanups.push(async () => stop());
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    expect(seen).toEqual([]);
    expect(errors).toEqual([]);
    // The server comes back: the first sync brings the catalogue and the rows.
    await serve({ dir, port });
    await expect.poll(() => seen.at(-1), { timeout: 30_000 }).toEqual(["kept"]);
    expect(errors).toEqual([]);
    // Its open socket would hold the restarted server's close.
    stop();
    await fresh.session.close();
  }, 120_000);

  it("the runtime's loop tries a failed verification again (a store that fails twice)", async () => {
    const host = await serve();
    const inner = memoryPrivateStore();
    let failures = 0;
    // The store refuses the first two saves of the account's floor: the
    // verification fails for a reason that passes, and nothing but the loop
    // asks again.
    const flaky: PrivateStore = {
      ...inner,
      async save(scope, incarnation, doc, bytes, expected) {
        if (doc.startsWith("floor-") && failures < 2) {
          failures += 1;
          throw new Error("the disk is busy");
        }
        return inner.save(scope, incarnation, doc, bytes, expected);
      },
    };
    const opened = await device(host, { privateStore: flaky, wait: false });
    expect(await opened.db.encryption.ready({ timeoutMs: 30_000 })).toBe("active");
    expect(failures).toBe(2);
  }, 90_000);

  it("an equality query on an encrypted JSON column finds the row by its value", async () => {
    const host = await serve();
    const alice = await device(host);
    const project = await makeProject(alice, "plans");
    await alice.db
      .insert(app.records, { projectId: project, data: { tags: ["x"], n: 1 } } as never)
      .wait({ tier: "global" });
    await alice.db
      .insert(app.records, { projectId: project, data: { tags: ["y"], n: 2 } } as never)
      .wait({ tier: "global" });
    const found = await alice.db.all(
      app.records.where({ projectId: project, data: { eq: { tags: ["y"], n: 2 } } } as never),
      { tier: "remote" },
    );
    expect(found.map((row) => row.data)).toEqual([{ tags: ["y"], n: 2 }]);
    expect(
      await alice.db.all(
        app.records.where({ projectId: project, data: { eq: { n: 3 } } } as never),
        {
          tier: "remote",
        },
      ),
    ).toEqual([]);
  }, 90_000);

  it("devices.list shows both devices of a device account and marks this one", async () => {
    const host = await serve();
    const first = await device(host);
    const key = first.session.exportDeviceKey();
    const second = await device(host, { key });
    expect(second.account).toBe(first.account);
    const mine = await second.db.encryption.deviceId();
    const listed = await until(async () => {
      const devices = await second.db.encryption.devices.list();
      return devices.length >= 2 ? devices : undefined;
    }, "both devices to be listed");
    expect(listed.map((device) => device.status)).toEqual(["active", "active"]);
    expect(listed.find((device) => device.isThisDevice)?.id).toBe(mine);
    expect(listed.filter((device) => device.isThisDevice)).toHaveLength(1);
  }, 90_000);
});

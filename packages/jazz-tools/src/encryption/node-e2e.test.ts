import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { afterEach, describe, expect, it, vi } from "vitest";
import { NapiDb } from "jazz-napi";
import { app } from "../../tests/fixtures/encrypted/jazz.generated.js";
import appFile from "../../tests/fixtures/encrypted/jazz.app.json";
import permissions from "../../tests/fixtures/encrypted/jazz.permissions.json";
import {
  cleanUpTestApp,
  memoryTestAccountStore as memoryStore,
  openEncryptedTestDevice,
  pollTestValue as until,
  serveEncryptedTestApp,
  startTestJazz,
  storedBytes,
  testApp,
} from "../testing/index.js";
import { acceptDeviceAccounts, createJazz, createJazzClient, sqlite } from "../backend/index.js";
import { generateTs } from "../dev/codegen-napi.js";
import type { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import type { AccountStore } from "../accounts/persistence.js";
import {
  EncryptionError,
  EncryptionNotDeclaredError,
  EncryptionStoreRequiredError,
  memoryPrivateStore,
  unreadableRows,
  type PrivateStore,
} from "./index.js";

// End to end on Node: device clients with their own private stores talk to a
// real server through createJazz, which relays them and holds no keys.

it("keeps the generated bindings for the shared encrypted fixture current", async () => {
  const directory = new URL("../../tests/fixtures/encrypted/", import.meta.url);
  const [source, bindings] = await Promise.all([
    readFile(new URL("jazz.app.json", directory), "utf8"),
    readFile(new URL("jazz.generated.ts", directory)),
  ]);
  expect(Buffer.from(await generateTs(source)).equals(bindings)).toBe(true);
});

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(() => cleanUpTestApp(cleanups));

async function serve(
  again?: { dir: string; port: number },
  served = permissions,
  storage: "sqlite" | "memory" = "sqlite",
) {
  const host = await serveEncryptedTestApp(
    { app, permissions: served, storage, ...again },
    cleanups,
  );
  return { ...host, permissions: served };
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
  return openEncryptedTestDevice(
    host,
    { app, permissions: host.permissions ?? permissions, ...options },
    cleanups,
  );
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

function observeStoreClock() {
  const armed: number[] = [];
  const fired: number[] = [];
  const timeout = globalThis.setTimeout;
  const spy = vi.spyOn(globalThis, "setTimeout").mockImplementation((callback, delay, ...args) => {
    if (callback.name !== "wakeAtDeadline" || delay === undefined) {
      return timeout(callback, delay, ...args);
    }
    armed.push(delay);
    return timeout(
      (...values) => {
        fired.push(delay);
        callback(...values);
      },
      delay,
      ...args,
    );
  });
  cleanups.push(async () => {
    spy.mockRestore();
  });
  return { armed, fired };
}

it("waits for the first catalogue without waking the private-store clock", async () => {
  const first = await serve();
  await first.stop();
  const { fired } = observeStoreClock();
  const alice = await device(first, { wait: false });
  await alice.db.disconnect();
  const state = () => JSON.parse(alice.db.encryption["native"].encryptionState());
  expect(state().readable).toBe(false);

  await new Promise((resolve) => setTimeout(resolve, 60_000));
  expect(fired).toHaveLength(0);

  await serve({ dir: first.dir, port: first.port });
  try {
    await alice.db.reconnect();
    await expect.poll(() => state().status, { timeout: 5_000 }).toBe("active");
  } finally {
    await alice.session.close();
  }
}, 90_000);

it("arms verification backoff after a transient private-store error", async () => {
  const { armed, fired } = observeStoreClock();
  const host = await serve(undefined, permissions, "memory");
  const store = memoryPrivateStore();
  const save = store.save.bind(store);
  let failures = 0;
  store.save = async (...args) => {
    if (args[2].startsWith("floor-") && failures < 2) {
      failures++;
      throw new Error("the disk is busy");
    }
    return save(...args);
  };
  const alice = await device(host, { privateStore: store, wait: false });
  expect(await alice.db.encryption.ready({ timeoutMs: 30_000 })).toBe("active");
  expect(failures).toBe(2);
  expect(armed.some((delay) => delay >= 240 && delay <= 251)).toBe(true);
  expect(armed.some((delay) => delay >= 490 && delay <= 501)).toBe(true);
  expect(fired.some((delay) => delay >= 240 && delay <= 501)).toBe(true);
}, 60_000);

it("detects a challenge through the approval watch, without a polling timer", async () => {
  const { armed, fired } = observeStoreClock();
  const schema = testApp(appFile);
  const host = await startTestJazz({
    app: schema,
    permissions,
    coreOptions: { appId: crypto.randomUUID() },
  });
  cleanups.push(() => host.stop());
  const login = await host.as("alice");
  const open = async () => {
    const session = await createJazzClient({
      app: schema,
      permissions,
      appId: host.appId,
      serverUrl: login.serverUrl,
      driver: { type: "memory" },
      store: memoryStore(),
      encryption: { store: memoryPrivateStore() },
    });
    cleanups.push(() => session.close());
    await session.refresh();
    return session.db.encryption;
  };
  const first = await open();
  expect(await first.ready()).toBe("active");
  const waiting = await open();
  expect(await waiting.ready()).toBe("awaiting-approval");
  const challenge = vi.fn();
  const stop = waiting.onChallenge(challenge);
  try {
    await first.devices.challenge(await waiting.deviceId());
    await expect.poll(() => challenge.mock.calls.length, { timeout: 6_000 }).toBe(1);
    // The watch woke the loop: no two-second approval probe was armed.
    expect(
      armed.some((delay) => delay > 500 && delay <= 2001),
      JSON.stringify({ armed, fired }),
    ).toBe(false);
  } finally {
    stop();
  }
}, 60_000);

it("wakes for a write deadline while the active device is offline", async () => {
  const { fired } = observeStoreClock();
  const host = await serve(undefined, permissions, "memory");
  const alice = await device(host);
  const id = await makeProject(alice, "plans");
  const scopeId = (alice.db.currentRuntime() as NativeRuntimeAdapter).tableIdentity("projects");
  await alice.db.disconnect();
  fired.length = 0;
  const started = Date.now();
  await alice.db.encryption["native"].encryptionCall("retryWrite", {
    scopeId,
    identifier: id,
    startedAt: started - 9_800,
    repeated: true,
  });
  expect(Date.now() - started).toBeGreaterThanOrEqual(200);
  expect(fired.filter((delay) => delay > 0 && delay <= 201)).toHaveLength(1);
}, 60_000);

describe("encryption on Node, end to end", () => {
  it.each([false, true])(
    "enforces the account reset opt-in (%s)",
    async (allowReset) => {
      const schema = testApp({
        ...appFile,
        ...(allowReset && { encryption: { allowAccountReset: true } }),
      });
      const host = await startTestJazz({
        app: schema,
        permissions,
        coreOptions: { appId: crypto.randomUUID() },
      });
      cleanups.push(() => host.stop());
      const alice = await host.as("alice");
      const open = async (deviceAccount = false) => {
        const session = await createJazzClient({
          app: schema,
          permissions,
          appId: host.appId,
          serverUrl: deviceAccount ? host.url : alice.serverUrl,
          ...(deviceAccount && { initial: "device" as const }),
          driver: { type: "memory" },
          store: memoryStore(),
          encryption: { store: memoryPrivateStore() },
        });
        cleanups.push(() => session.close());
        if (!deviceAccount) await session.refresh();
        return session.db.encryption;
      };
      const first = await open();
      expect(await first.ready()).toBe("active");
      const waiting = await open();
      expect(await waiting.ready()).toBe("awaiting-approval");
      if (!allowReset) {
        await expect(waiting.resetAccount()).rejects.toMatchObject({
          code: "account-reset-not-allowed",
        });
        expect(await waiting.ready()).toBe("awaiting-approval");
        return;
      }
      await expect(first.resetAccount()).rejects.toMatchObject({ code: "reset-device-active" });
      const device = await open(true);
      expect(await device.ready()).toBe("active");
      await expect(device.resetAccount()).rejects.toMatchObject({
        code: "account-reset-not-allowed",
      });
      await expect(host.jazz.db.encryption.resetAccount()).rejects.toMatchObject({
        code: "keyless",
      });
      expect(await waiting.resetAccount()).toBe("active");
      expect(await waiting.ready()).toBe("active");
    },
    90_000,
  );

  it.each(["device", "keyless backend"] as const)(
    "keeps an encrypted row in its original space when the %s updates or upserts it",
    async (writer) => {
      const reference = {
        columns: { projectId: "uuid?", label: "string?" },
        relations: { project: { rel: "projects", column: "projectId" } },
      } as const;
      const tables = testApp({
        tables: {
          projects: { columns: { name: "string", ownerId: "uuid" } },
          links: reference,
          tasks: {
            ...reference,
            columns: { ...reference.columns, title: "bytes" },
            encryption: { space: "projectId", scope: "projects", columns: { title: "string?" } },
          },
        },
      });
      const writable = { read: "true", insert: "true", update: "true" };
      const policy = {
        format: 1,
        tables: { projects: permissions.tables.projects, tasks: writable, links: writable },
      };
      const host = await startTestJazz({
        app: tables,
        permissions: policy,
        coreOptions: { appId: crypto.randomUUID() },
      });
      cleanups.push(() => host.stop());
      const session = await createJazzClient({
        app: tables,
        permissions: policy,
        appId: host.appId,
        serverUrl: host.url,
        initial: "device",
        driver: { type: "memory" },
        store: memoryStore(),
        encryption: { store: memoryPrivateStore() },
      });
      cleanups.push(() => session.close());
      await session.db.encryption.ready();
      const ownerId = session.getSnapshot().user!.accountId;
      const first = await host.jazz.db
        .insert(tables.projects, { name: "First", ownerId })
        .wait({ tier: "global" });
      const other = await host.jazz.db
        .insert(tables.projects, { name: "Other", ownerId })
        .wait({ tier: "global" });
      const member = await host.jazz.db.encryption.members.row(
        { table: "projects", id: first.id },
        ownerId,
      );
      // testApp adds the system tables to this schema at runtime.
      const members = (tables as typeof tables & Pick<typeof app, "__e2ee_members">).__e2ee_members;
      await host.jazz.db.insert(members, member).wait({ tier: "global" });

      const db = writer === "device" ? session.db : host.jazz.db;
      const link = await db.insert(tables.links, { projectId: first.id }).wait({ tier: "global" });
      await db.update(tables.links, link.id, { projectId: other.id }).wait({ tier: "global" });
      expect(await db.all(tables.links)).toMatchObject([{ projectId: other.id }]);

      for (const operation of ["update", "upsert"] as const) {
        for (const destination of [other.id, null]) {
          const row = await session.db
            .insert(tables.tasks, { projectId: first.id, title: "private title" })
            .wait({ tier: "global" });
          const task = tables.tasks.where({ id: row.id });
          expect(await db.all(task, { tier: "remote" })).toMatchObject([{ projectId: first.id }]);
          const write = (change: { projectId?: string | null; label?: string }) =>
            (operation === "update"
              ? db.update(tables.tasks, row.id, change)
              : db.upsert(tables.tasks, row.id, change)
            ).wait({ tier: "global" });
          await write({ projectId: first.id });
          await write({ label: "edited" });
          await expect(
            async () => write({ projectId: destination }),
            `${operation} to ${destination === null ? "null" : "another project"}`,
          ).rejects.toThrow("an encrypted row cannot change its space through an ordinary write");
          expect(await session.db.all(task, { tier: "remote" })).toMatchObject([
            { projectId: first.id, label: "edited", title: "private title" },
          ]);
        }
      }
    },
    60_000,
  );

  it("refuses unknown tables and tables that do not hold spaces before writing", async () => {
    const host = await serve();
    const { db, account } = await device(host);
    for (const table of ["tasks", "no_such_table"]) {
      const space = { table, id: crypto.randomUUID() };
      for (const operation of [
        () => db.encryption.members.row(space, account),
        () => db.encryption.members.where(space),
        () => db.encryption.members.remove(space, account),
      ])
        await expect(operation()).rejects.toThrow(table);
    }
    expect(await host.jazz.db.all(app.__e2ee_members)).toEqual([]);
  });

  it("refuses a space whose catalogue has never reached this device", async () => {
    const host = await serve();
    const accounts = memoryStore();
    const online = await device(host, { accounts });
    await online.session.close();
    await host.stop();
    // The sign-in survives, but a new in-memory replica has never synced.
    const { db, account } = await device(host, { accounts, wait: false });
    const space = { table: "projects", id: crypto.randomUUID() };
    for (const operation of [
      () => db.encryption.members.row(space, account),
      () => db.encryption.members.where(space),
      () => db.encryption.members.remove(space, account),
    ])
      await expect(operation()).rejects.toMatchObject({
        name: "EncryptionError",
        code: "not-observed",
      });
    expect(await db.all(app.__e2ee_members, { tier: "local-first" })).toEqual([]);
  });

  it("removes all of one account's rows in one space and keeps the other members and spaces", async () => {
    const host = await serve();
    const alice = await device(host);
    const first = { table: "projects", id: await makeProject(alice, "first") };
    const second = { table: "projects", id: await makeProject(alice, "second") };
    const bob = crypto.randomUUID();
    await addMember(alice, first.id, bob);
    await addMember(alice, first.id, bob);
    await addMember(alice, second.id, bob);

    const members = alice.db.encryption.members;
    const filter = await members.where(first);
    expect(filter).toEqual({ scopeId: expect.any(String), identifier: first.id });
    expect(await members.where(first, bob)).toEqual({ ...filter, accountId: bob });
    const list = async (space: typeof first) =>
      (await host.jazz.db.all(app.__e2ee_members.where(await members.where(space))))
        .map((row) => row.accountId)
        .sort();
    expect(await list(first)).toEqual([alice.account, bob, bob].sort());
    const rotate = vi.spyOn(alice.db.encryption.keys, "rotate");
    expect(await members.remove(first, bob)).toBe(2);
    expect(await list(first)).toEqual([alice.account]);
    expect(await list(second)).toEqual([alice.account, bob].sort());
    expect(await members.remove(first, bob)).toBe(0);
    expect(rotate).not.toHaveBeenCalled();
  });

  it("a member can leave without keys or update permission and cannot remove another member", async () => {
    const host = await serve();
    const alice = await device(host);
    const bob = await device(host);
    const space = { table: "projects", id: await makeProject(alice, "plans") };
    await addMember(alice, space.id, bob.account);
    const carol = crypto.randomUUID();
    await addMember(alice, space.id, carol);
    const members = bob.db.encryption.members;
    const matching = async (accountId: string) =>
      app.__e2ee_members.where(await members.where(space, accountId));
    await expect(members.remove(space, carol)).rejects.toMatchObject({ code: "permission_denied" });
    expect(await bob.db.all(await matching(carol), { tier: "remote" })).toHaveLength(1);
    expect(await members.remove(space, bob.account)).toBe(1);
    expect(await host.jazz.db.all(await matching(bob.account))).toEqual([]);
    // A fresh outsider sees no membership to delete. Carol remains entitled.
    const outsider = await device(host);
    expect(await outsider.db.all(await matching(carol), { tier: "remote" })).toEqual([]);
    expect(await outsider.db.encryption.members.remove(space, carol)).toBe(0);
    expect(await host.jazz.db.all(await matching(carol))).toHaveLength(1);
  });

  it.each([
    { existing: true, conflicts: 1 },
    { existing: false, conflicts: 1 },
    { existing: true, conflicts: 3 },
  ])(
    "retries a removal after a racing admission (already a member: $existing, conflicts: $conflicts)",
    async ({ existing, conflicts }) => {
      const host = await serve();
      const alice = await device(host);
      const other = await device(host, { key: alice.session.exportDeviceKey() });
      const space = { table: "projects", id: await makeProject(alice, "plans") };
      const bob = crypto.randomUUID();
      if (existing) await addMember(alice, space.id, bob);
      const run = alice.db.serializableTransaction.bind(alice.db);
      const attempts = vi.spyOn(alice.db, "serializableTransaction");
      for (let i = 0; i < conflicts; i++)
        attempts.mockImplementationOnce((body) =>
          run(async (tx) => {
            const result = await body(tx);
            // The read finished. The authority accepts the new admission before this commit.
            await addMember(other, space.id, bob);
            return result;
          }),
        );
      const removal = alice.db.encryption.members.remove(space, bob);
      if (conflicts === 3) {
        await expect(removal).rejects.toMatchObject({
          code: expect.stringMatching(/^(serialization|transaction)_conflict$/),
        });
        expect(attempts).toHaveBeenCalledTimes(3);
      } else {
        expect(await removal).toBe(existing ? 2 : 1);
        expect(attempts).toHaveBeenCalledTimes(2);
        expect(
          await host.jazz.db.all(
            app.__e2ee_members.where(await alice.db.encryption.members.where(space, bob)),
          ),
        ).toEqual([]);
      }
    },
    60_000,
  );
  it("names the project table when a member's lazy rotation is refused", async () => {
    const host = await serve();
    const alice = await device(host);
    const bob = await device(host);
    const carol = await device(host);
    const projectId = await makeProject(alice, "plans");
    await addMember(alice, projectId, bob.account);
    await addMember(alice, projectId, carol.account);
    await alice.db.insert(app.tasks, { projectId, title: "before" }).wait({ tier: "global" });
    await alice.db.encryption.keys.deliver();
    for (const member of [bob, carol]) {
      await until(
        async () =>
          (await member.db.all(app.tasks.where({ projectId }), { tier: "remote" })).length ||
          undefined,
        "the delivery",
      );
    }
    const member = (await alice.db.all(app.__e2ee_members, { tier: "remote" })).find(
      (row) => row.identifier === projectId && row.accountId === carol.account,
    )!;
    await alice.db.delete(app.__e2ee_members, member.id).wait({ tier: "global" });
    await bob.db.encryption.keys.deliver();
    const reports: unknown[] = [];
    const stop = bob.db.encryption.onReport((report) => reports.push(report));
    try {
      await bob.db.insert(app.tasks, { projectId, title: "after" }).wait({ tier: "global" });
      await expect
        .poll(() => reports, { timeout: 15_000 })
        .toContainEqual(
          expect.objectContaining({
            type: "rotation-failed",
            space: { table: "projects", id: projectId, scopeId: member.scopeId },
          }),
        );
    } finally {
      stop();
    }
  }, 90_000);

  it("lists a device's published removal request", async () => {
    const host = await serve();
    const alice = await device(host);
    // A device leaves only through another active device, so alice signs in on a second one first.
    const other = await device(host, { key: alice.session.exportDeviceKey() });
    await until(async () => {
      const devices = await alice.db.encryption.devices.list();
      return devices.filter((d) => d.status === "active").length >= 2 ? devices : undefined;
    }, "alice's first device to see her second one");
    // The other device is offline, so the request stays published rather than completed at once.
    await other.session.close();
    const mine = async () => (await alice.db.encryption.devices.list()).find((d) => d.isThisDevice);
    expect((await mine())?.removalRequested).toBe(false);
    await alice.db.encryption.devices.requestRemoval();
    expect((await mine())?.removalRequested).toBe(true);
  }, 90_000);

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

  it("writes with the current key when rotation misses the write deadline, then rotates before the next write", async () => {
    const shared = structuredClone(permissions);
    shared.tables.tasks.read = "true";
    const host = await serve(undefined, shared);
    const store = memoryPrivateStore();
    const alice = await device(host, { privateStore: store });
    const bob = await device(host);
    const projectId = await makeProject(alice, "plans");
    await addMember(alice, projectId, bob.account);
    const insert = (title: string) =>
      alice.db.insert(app.tasks, { projectId, title }).wait({ tier: "global" });
    await insert("before");
    await alice.db.encryption.keys.deliver();
    const titles = async () =>
      (await bob.db.all(app.tasks.where({ projectId }), { tier: "remote" })).map(
        (row) => row.title,
      );
    await until(
      async () => (await titles()).includes("before") || undefined,
      "Bob to hold the current key",
    );
    const member = (await alice.db.all(app.__e2ee_members, { tier: "remote" })).find(
      (row) => row.accountId === bob.account,
    )!;
    await alice.db.delete(app.__e2ee_members, member.id).wait({ tier: "global" });
    const reports: unknown[] = [];
    const unsubscribe = alice.db.encryption.onReport((report) => reports.push(report));
    function signal() {
      let resolve!: () => void;
      const promise = new Promise<void>((done) => {
        resolve = done;
      });
      return { promise, resolve };
    }
    const resume = signal();
    const saving = signal();
    const save = store.save.bind(store);
    const delayed = vi.spyOn(store, "save").mockImplementationOnce(async (...args) => {
      saving.resolve();
      await resume.promise;
      return save(...args);
    });
    // Hold the runtime gate before preparation starts by blocking the store
    // write made by account verification. The waiting write has its own deadline.
    const verifying = alice.db.encryption["native"].encryptionCall("ready");
    try {
      await saving.promise;
      await insert("while undecided");
      await until(
        async () =>
          reports.some((report) => (report as { type: string }).type === "rotation-failed") ||
          undefined,
        "the undecided rotation report",
      );
      expect(await titles()).toContain("while undecided");
    } finally {
      resume.resolve();
      delayed.mockRestore();
      unsubscribe();
    }
    await verifying;
    const successors = (app as unknown as Record<string, never>)["__e2ee_space_successors"];
    expect(await host.jazz.db.all(successors)).toHaveLength(0);
    await insert("after rotation");
    expect(await host.jazz.db.all(successors)).toHaveLength(1);
    expect(await titles()).not.toContain("after rotation");
  }, 90_000);

  it("recovers a space from an earlier key when the only current holder is gone", async () => {
    const shared = structuredClone(permissions);
    shared.tables.projects.update = "true";
    const host = await serve(undefined, shared);
    const alice = await device(host);
    const bob = await device(host);
    const carol = await device(host);
    const id = await makeProject(alice, "plans");
    const space = { table: "projects", id };
    await addMember(alice, id, bob.account);
    await alice.db.insert(app.tasks, { projectId: id, title: "original" }).wait({ tier: "global" });
    await alice.db.encryption.keys.deliver();
    await until(async () => {
      const rows = await bob.db.all(app.tasks.where({ projectId: id }), { tier: "remote" });
      return rows[0]?.title === "original" ? true : undefined;
    }, "Bob to acquire the original key");
    const members = await alice.db.all(app.__e2ee_members, { tier: "remote" });
    await alice.db
      .delete(app.__e2ee_members, members.find((row) => row.accountId === bob.account)!.id)
      .wait({ tier: "global" });
    await expect(alice.db.encryption.keys.rotate(space)).resolves.toBe(0);
    const bobRow = await alice.db.encryption.members.row(space, bob.account);
    const carolRow = await alice.db.encryption.members.row(space, carol.account);
    await alice.session.close();
    await host.jazz.db
      .delete(app.__e2ee_members, members.find((row) => row.accountId === alice.account)!.id)
      .wait({ tier: "global" });
    await host.jazz.db.insert(app.__e2ee_members, bobRow).wait({ tier: "global" });
    await host.jazz.db.insert(app.__e2ee_members, carolRow).wait({ tier: "global" });
    await expect(bob.db.encryption.keys.rotate(space)).rejects.toMatchObject({
      code: "rotation-needs-key",
    });
    await expect(bob.db.encryption.keys.recoverRotation(space)).resolves.toBe(1);
    await bob.db
      .insert(app.tasks, { projectId: id, title: "after recovery" })
      .wait({ tier: "global" });
    await until(async () => {
      const rows = await carol.db.all(app.tasks.where({ projectId: id }), { tier: "remote" });
      return rows.some((row) => row.title === "after recovery") ? true : undefined;
    }, "Carol to read Bob's new write with the recovered key");
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
    await first.db.encryption.ready();
    const [root] = await first.db.encryption.recovery.list();
    const retirement = first.db.encryption.recovery.retire(root!.id);
    await expect(retirement).rejects.toBeInstanceOf(EncryptionError);
    await expect(retirement).rejects.toMatchObject({
      code: "device-account-root-required",
      message: expect.stringContaining("prevent recovery"),
    });
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

  it("keeps encrypted rows across close and detaches old views at sign-out", async () => {
    const host = await serve();
    const dataPath = `${await mkdtemp(`${tmpdir()}/e2ee-sign-out-`)}/device`;
    const stores = { dataPath, accounts: memoryStore(), privateStore: memoryPrivateStore() };
    const alice = await device(host, stores);
    const projectId = await makeProject(alice, "plans");
    const tasks = app.tasks.where({ projectId });
    await alice.db.insert(app.tasks, { projectId, title: "kept" }).wait({ tier: "global" });
    const id = await alice.db.encryption.deviceId();
    await alice.session.close();

    const { session, db } = await device(host, stores);
    expect(await db.encryption.deviceId()).toBe(id);
    expect(await db.all(tasks, { tier: "local-first" })).toMatchObject([{ title: "kept" }]);
    const key = session.exportDeviceKey();
    const runtime = db.currentRuntime() as NativeRuntimeAdapter;
    const native = db.encryption["native"];
    if (!(native instanceof NapiDb)) throw new Error("expected the Node binding");
    const view = native.registerSchema(runtime["schemaBytes"]);
    try {
      expect(await view.encryptionCall("deviceId", {})).toBe(JSON.stringify(id));
      await session.signOut();
      expect(() => view.encryptionState()).toThrow(/no encryption runtime/);
      await session.restoreDeviceAccount(key);
      expect(await session.db.encryption.deviceId()).toBe(id);
      expect(await session.db.all(tasks, { tier: "local-first" })).toMatchObject([
        { title: "kept" },
      ]);
    } finally {
      await view.close();
    }
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
    await offline.session.close();
  }, 120_000);

  it.each(["all", "one"] as const)(
    "%s waits for readability after reopening, up to 30 seconds",
    async (method) => {
      const host = await serve();
      const options = {
        accounts: memoryStore(),
        privateStore: memoryPrivateStore(),
        dataPath: `${await mkdtemp(`${tmpdir()}/e2ee-client-`)}/device`,
      };
      const opened = await device(host, options);
      const projectId = await makeProject(opened, "reopen");
      const id = crypto.randomUUID();
      await opened.db
        .insert(app.tasks, { projectId, title: "kept" }, { id })
        .wait({ tier: "global" });
      await opened.session.close();

      const query = app.tasks.where({ id });
      const returning = await device(host, { ...options, wait: false });
      const handle = returning.db.encryption;
      const ready = handle.readable();
      let release!: () => void;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      const readable = vi.spyOn(handle, "readable").mockReturnValue(held);
      const read = returning.db[method](query, { tier: "local-first" });
      const early = vi.fn();
      void read.then(early, early);
      await ready;
      expect(readable).toHaveBeenCalledOnce();
      expect(early).not.toHaveBeenCalled();
      release();
      const result = await read;
      expect(Array.isArray(result) ? result[0] : result).toMatchObject({ id, title: "kept" });
      readable.mockRestore();
      await returning.session.close();

      const blocked = await device(host, { ...options, wait: false });
      const blockedReadable = vi
        .spyOn(blocked.db.encryption, "readable")
        .mockReturnValue(new Promise<void>(() => {}));
      vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
      try {
        const read = blocked.db[method](query, { tier: "local-first" });
        const settled = vi.fn();
        void read.then(settled, settled);
        await vi.advanceTimersByTimeAsync(29_999);
        expect(settled).not.toHaveBeenCalled();
        const unreadable = expect(read).rejects.toMatchObject({
          code: "not_observed",
          message: expect.stringContaining(
            "before this client has synced their catalogue identities",
          ),
        });
        await vi.advanceTimersByTimeAsync(1);
        await unreadable;
      } finally {
        vi.useRealTimers();
        blockedReadable.mockRestore();
      }
    },
  );

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
    // The app can ask why the query is still loading: the device can't read yet.
    let readable = false;
    void fresh.db.encryption.readable().then(() => (readable = true));
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    expect(seen).toEqual([]);
    expect(errors).toEqual([]);
    expect(readable).toBe(false);
    // The server comes back: the first sync brings the catalogue and the rows.
    await serve({ dir, port });
    await expect.poll(() => seen.at(-1), { timeout: 30_000 }).toEqual(["kept"]);
    await expect.poll(() => readable, { timeout: 1_000 }).toBe(true);
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

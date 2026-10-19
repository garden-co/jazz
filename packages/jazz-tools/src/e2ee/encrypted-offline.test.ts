import { expect, it, vi } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer, type Socket } from "node:net";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { groupSchema } from "./groups.js";
import { createBrowserDeviceSigner } from "./browser.js";

const app = s.defineApp({
  ...deviceRequestSchema,
  ...spaceSchema,
  projects: s.table({ title: s.string() }, {}),
  notes: s
    .table({ projectId: s.uuid(), title: s.string() }, { project: s.rel("projects", "projectId") })
    .encrypted({ space: "projectId", columns: ["title"] }),
});
const policies = definePermissions(app, ({ policy, session }) => {
  policy.projects.allowRead.always();
  policy.projects.allowInsert.always();
  policy.notes.allowRead.always();
  policy.notes.allowInsert.always();
  policy.notes.allowUpdate.always();
  policy.__e2ee_spaces.allowRead.always();
  policy.__e2ee_spaces.allowInsert.where({ creatorAccountId: session.user.account });
  policy.__e2ee_space_grants.allowRead.always();
  policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
  policy.__e2ee_space_deliveries.allowRead.always();
  policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
});
const permissions = { ...deviceRequestPermissions, ...policies };

it.each(["explicit disconnect", "unexpected transport loss"] as const)(
  "applies default warning and explicit rejection to known-stale offline writes (%s)",
  async (offlineCause) => {
    const rotationPolicy = definePermissions(app, ({ policy }) => {
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.never();
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
    const store = (initial: string | null = null) => {
      let saved = initial;
      return {
        async read() {
          return saved;
        },
        async update(transform: (current: string | null) => string) {
          saved = transform(saved);
        },
      };
    };
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: { ...permissions, ...rotationPolicy },
      });
      const alice = await localAccountConfig(server.appId, server.url);
      const bob = await localAccountConfig(server.appId, server.url);
      const ownerStore = store();
      const owner = await createDb({ ...alice, e2ee: { app, store: ownerStore } });
      clients.push(owner);
      const departing = await createDb({ ...bob, e2ee: { app, store: store() } });
      clients.push(departing);
      await owner.e2ee.devices.list();
      await departing.e2ee.devices.list();
      // Reopen the enrolled owner on an independent replica with the other policy.
      // Copy opaque store bytes; each replica validates history independently.
      const rejecting = await createDb({
        ...alice,
        e2ee: { app, store: store(await ownerStore.read()), staleWrites: "reject" },
      });
      clients.push(rejecting);
      const owners = [owner, rejecting];
      const tx = owner.beginExclusiveTransaction();
      const project = tx.insert(
        app.projects,
        { title: "Stale offline epoch" },
        {
          initialRecipients: [alice.account.id, bob.account.id],
        },
      );
      const note = tx.insert(app.notes, { projectId: project.id, title: "Before removal" });
      await tx.commit().wait({ tier: "global" });
      for (const client of owners)
        expect(await client.one(app.notes.where({ id: note.id }), { tier: "remote" })).toEqual(
          note,
        );
      // The departing member can record removal but cannot rotate for those left behind.
      await departing.e2ee.spaces.revoke(app.projects, project.id, bob.account.id).wait();
      for (const client of owners) {
        expect(await client.all(app.__e2ee_space_grants, { tier: "remote" })).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ operation: "remove", recipientId: bob.account.id }),
          ]),
        );
        // Capture complete accepted membership on each replica before going offline.
        // Rotation is forbidden; a changed sibling snapshot alone cannot authorise reuse.
        expect(await client.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual({
          state: "maintenance-required",
          reason: "recipient-removed",
        });
      }
      if (offlineCause === "explicit disconnect") {
        for (const client of owners) await client.disconnect();
      } else await server.stop();
      warning.mockClear();
      await expect(
        rejecting.update(app.notes, note.id, { title: "Must be rejected" }).wait({ tier: "local" }),
      ).rejects.toMatchObject({ name: "E2eeDataError", code: "maintenance-required" });
      expect(warning).not.toHaveBeenCalled();
      await owner
        .update(app.notes, note.id, { title: "Written with the retained epoch" })
        .wait({ tier: "local" });
      expect(warning).toHaveBeenCalledWith(
        "E2EE: writing offline with a known-stale encryption epoch; removed recipients may still decrypt this write.",
      );
    } finally {
      warning.mockRestore();
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  60_000,
);

const indexedApp = s.defineApp({
  ...deviceRequestSchema,
  ...spaceSchema,
  projects: s.table({ title: s.string() }, {}),
  notes: s
    .table({ projectId: s.uuid(), title: s.string() }, { project: s.rel("projects", "projectId") })
    .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
});

const groupApp = s.defineApp({
  ...deviceRequestSchema,
  ...spaceSchema,
  ...groupSchema,
  projects: s.table({ title: s.string() }, {}),
  notes: s
    .table({ projectId: s.uuid(), title: s.string() }, { project: s.rel("projects", "projectId") })
    .encrypted({ space: "projectId", columns: ["title"] }),
});
const groupPolicies = definePermissions(groupApp, ({ policy, session }) => {
  policy.__e2ee_groups.allowRead.always();
  policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
  policy.__e2ee_group_membership.allowRead.always();
  policy.__e2ee_group_membership.allowInsert.where({ authorAccountId: session.user.account });
  policy.__e2ee_group_successors.allowRead.always();
  policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
  policy.__e2ee_group_deliveries.allowRead.always();
  policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  policy.__e2ee_group_repairs.allowRead.always();
  policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
});

it.each([
  { reopenAt: "after a warming read", app, permissions },
  { reopenAt: "immediately after creation", app, permissions },
  { reopenAt: "without explicit disconnection", app, permissions },
  {
    reopenAt: "with an equality query without explicit disconnection",
    app: indexedApp,
    permissions,
  },
  { reopenAt: "while online preparation stalls", app, permissions },
  {
    reopenAt: "immediately after creation with group tables",
    app: groupApp,
    permissions: { ...permissions, ...groupPolicies },
  },
  {
    reopenAt: "immediately after creation with a group recipient",
    app: groupApp,
    permissions: { ...permissions, ...groupPolicies },
  },
])(
  "reopens a persisted native database offline $reopenAt and updates its encrypted data",
  async ({ reopenAt, app, permissions }) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const directory = await mkdtemp(join(tmpdir(), "jazz-e2ee-offline-reopen-"));
    const store = () => {
      let value: string | null = null;
      return {
        async read() {
          return value;
        },
        async update(transform: (current: string | null) => string) {
          value = transform(value);
        },
      };
    };
    const config = {
      appId: server.appId,
      serverUrl: server.url,
      app,
      permissions,
      driver: { type: "persistent" as const, dataPath: join(directory, "database") },
      initial: "local-first" as const,
      store: store(),
      e2ee: { app, store: store() },
    };
    let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let stalledServer: ReturnType<typeof createServer> | undefined;
    const stalledSockets = new Set<Socket>();
    let connected!: () => void;
    const connectionStarted = new Promise<void>((resolve) => {
      connected = resolve;
    });
    let listingFinished = false;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      owner = await createJazzSession(config);
      const before = owner.getSnapshot().client!.db;
      const accountId = owner.getSnapshot().account!.id;
      const group =
        reopenAt === "immediately after creation with a group recipient"
          ? await before.e2ee.groups.create().wait()
          : undefined;
      const tx = before.beginExclusiveTransaction();
      const project = tx.insert(
        app.projects,
        { title: "Persisted project" },
        group ? { initialRecipients: [group.id] } : undefined,
      );
      const note = tx.insert(app.notes, { projectId: project.id, title: "Before reopening" });
      await tx.commit().wait({ tier: "global" });
      if (reopenAt === "after a warming read") {
        expect(await before.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual({
          state: "ready",
        });
        expect(await before.one(app.notes.where({ id: note.id }), { tier: "remote" })).toEqual(
          note,
        );
      }
      await owner.close();
      owner = undefined;
      await server.stop();

      if (reopenAt === "while online preparation stalls") {
        // Hold the transport outside Jazz, without replacing its query/runtime paths.
        stalledServer = createServer((socket) => {
          stalledSockets.add(socket);
          socket.on("error", () => {});
          connected();
        });
        await new Promise<void>((resolve, reject) => {
          stalledServer!.once("error", reject);
          stalledServer!.listen(Number(new URL(server.url).port), "127.0.0.1", resolve);
        });
      }
      owner = await createJazzSession(config);
      const reopened = owner.getSnapshot().client!.db;
      expect(owner.getSnapshot().account!.id).toBe(accountId);
      if (
        reopenAt !== "without explicit disconnection" &&
        reopenAt !== "with an equality query without explicit disconnection" &&
        reopenAt !== "while online preparation stalls"
      )
        await reopened.disconnect();
      // The ordinary persisted row is a control for reopening without the server.
      expect(
        await reopened.one(app.projects.where({ id: project.id }), { tier: "local-first" }),
      ).toEqual(project);
      if (stalledServer) {
        reopened.e2ee.devices.list().then(
          () => {
            listingFinished = true;
          },
          () => {
            listingFinished = true;
          },
        );
        await connectionStarted;
        expect(listingFinished).toBe(false);
      }
      if (reopenAt === "with an equality query without explicit disconnection") {
        expect(
          await reopened.all(
            app.notes.where({ projectId: project.id, title: "Before reopening" }),
            { tier: "local-first" },
          ),
        ).toEqual([note]);
      }
      const write = reopened.update(app.notes, note.id, { title: "After reopening offline" });
      expect(
        await Promise.race([
          write.wait({ tier: "local" }).then(() => "stored locally"),
          new Promise<string>((resolve) => {
            timer = setTimeout(() => resolve("still waiting"), 5_000);
          }),
        ]),
      ).toBe("stored locally");
      expect(await reopened.one(app.notes.where({ id: note.id }), { tier: "local-first" })).toEqual(
        {
          ...note,
          title: "After reopening offline",
        },
      );
      if (stalledServer) expect(listingFinished).toBe(false);
    } finally {
      clearTimeout(timer);
      for (const socket of stalledSockets) socket.destroy();
      if (stalledServer)
        await new Promise<void>((resolve) => stalledServer!.close(() => resolve()));
      await owner?.close();
      await server.stop();
      await rm(directory, { recursive: true, force: true });
    }
  },
  60_000,
);

it("updates an existing encrypted space offline and accepts the write after reconnect", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let stored: string | null = null;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    db = await createDb({
      ...(await localAccountConfig(server.appId, server.url)),
      e2ee: {
        app,
        store: {
          async read() {
            return stored;
          },
          async update(transform) {
            stored = transform(stored);
          },
        },
      },
    });
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Offline project" });
    const note = tx.insert(app.notes, { projectId: project.id, title: "Before disconnect" });
    await tx.commit().wait({ tier: "global" });
    const query = app.notes.where({ id: note.id });
    expect(await db.one(query, { tier: "remote" })).toEqual(note);

    await db.disconnect();
    const write = db.update(app.notes, note.id, { title: "Written offline" });
    expect(write).not.toBeInstanceOf(Promise);
    const local = write.wait({ tier: "local" });
    local.catch(() => {});
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      expect(
        await Promise.race([
          local.then(() => "stored locally"),
          new Promise<string>((resolve) => {
            timer = setTimeout(() => resolve("still waiting"), 5_000);
          }),
        ]),
      ).toBe("stored locally");
    } finally {
      clearTimeout(timer);
    }
    expect(await db.one(query, { tier: "local-first" })).toEqual({
      ...note,
      title: "Written offline",
    });

    await db.reconnect();
    await write.wait({ tier: "global" });
    expect(await db.one(query, { tier: "remote" })).toEqual({ ...note, title: "Written offline" });
  } finally {
    if (db) {
      await db.reconnect();
      await db.shutdown();
    }
    await server.stop();
  }
}, 60_000);

it.each([false, true])(
  "writes offline after cold creation and verifies online cache failures (cache failure: %s)",
  async (failHistoryPersistence) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let db: Awaited<ReturnType<typeof createDb>> | undefined;
    let stored: string | null = null;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let cacheFailures = 0;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      const config = {
        ...(await localAccountConfig(server.appId, server.url)),
        e2ee: {
          app,
          store: {
            async read() {
              return stored;
            },
            async update(transform: (current: string | null) => string) {
              const next = transform(stored);
              if (failHistoryPersistence && JSON.parse(next).acceptedHistoryV1) {
                cacheFailures++;
                throw new Error("Offline cache storage unavailable");
              }
              stored = next;
            },
          },
        },
      };
      db = await createDb(config);
      const tx = db.beginExclusiveTransaction();
      const project = tx.insert(app.projects, { title: "Immediate offline project" });
      const note = tx.insert(app.notes, { projectId: project.id, title: "Initial value" });
      await tx.commit().wait({ tier: "global" });
      expect(cacheFailures > 0).toBe(failHistoryPersistence);
      // No encrypted read or explain() between acceptance and disconnect.
      await db.disconnect();
      const write = db.update(app.notes, note.id, { title: "Immediate offline update" });
      expect(
        await Promise.race([
          write.wait({ tier: "local" }).then(
            () => "stored locally",
            () => "rejected",
          ),
          new Promise<string>((resolve) => {
            timer = setTimeout(() => resolve("still waiting"), 5_000);
          }),
        ]),
      ).toBe("stored locally");
      expect(await db.one(app.notes.where({ id: note.id }), { tier: "local-first" })).toEqual({
        ...note,
        title: "Immediate offline update",
      });
      await db.reconnect();
      await write.wait({ tier: "global" });
      if (failHistoryPersistence) {
        await db.shutdown();
        db = undefined;
        // Drop the initial in-memory bundle, forcing a verified online history read.
        cacheFailures = 0;
        db = await createDb(config);
        await expect(db.one(app.notes.where({ id: note.id }), { tier: "remote" })).resolves.toEqual(
          {
            ...note,
            title: "Immediate offline update",
          },
        );
        expect(cacheFailures).toBeGreaterThan(0);
      }
    } finally {
      clearTimeout(timer);
      if (db) {
        await db.reconnect();
        await db.shutdown();
      }
      await server.stop();
    }
  },
  60_000,
);

it("stores an encrypted update locally after unexpected transport loss", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let stored: string | null = null;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    db = await createDb({
      ...(await localAccountConfig(server.appId, server.url)),
      e2ee: {
        app,
        store: {
          async read() {
            return stored;
          },
          async update(transform) {
            stored = transform(stored);
          },
        },
      },
    });
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Transport loss project" });
    const note = tx.insert(app.notes, { projectId: project.id, title: "Before transport loss" });
    await tx.commit().wait({ tier: "global" });
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "remote" })).toEqual(note);
    await server.stop();
    // Deliberately do not call disconnect(): the network, not the caller, went away.
    const write = db.update(app.notes, note.id, { title: "After transport loss" });
    expect(
      await Promise.race([
        write.wait({ tier: "local" }).then(
          () => "stored locally",
          () => "rejected",
        ),
        new Promise<string>((resolve) => {
          timer = setTimeout(() => resolve("still waiting"), 5_000);
        }),
      ]),
    ).toBe("stored locally");
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "local-first" })).toEqual({
      ...note,
      title: "After transport loss",
    });
  } finally {
    clearTimeout(timer);
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);
it.each(["ordinary owner read", "owner reconnect"])(
  "%s delivers a newly created space key to another recipient",
  async (trigger) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    const store = () => {
      let value: string | null = null;
      return {
        async read() {
          return value;
        },
        async update(transform: (current: string | null) => string) {
          value = transform(value);
        },
      };
    };
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      const alice = await localAccountConfig(server.appId, server.url);
      const bob = await localAccountConfig(server.appId, server.url);
      const owner = await createDb({ ...alice, e2ee: { app, store: store() } });
      clients.push(owner);
      const reader = await createDb({ ...bob, e2ee: { app, store: store() } });
      clients.push(reader);
      await owner.e2ee.devices.list();
      await reader.e2ee.devices.list();
      const tx = owner.beginExclusiveTransaction();
      const project = tx.insert(
        app.projects,
        { title: "Shared initial space" },
        {
          initialRecipients: [alice.account.id, bob.account.id],
        },
      );
      const note = tx.insert(app.notes, { projectId: project.id, title: "Shared encrypted note" });
      await tx.commit().wait({ tier: "global" });
      // Neither client calls explain(); normal activity must resume delivery.
      if (trigger === "ordinary owner read") {
        expect(await owner.one(app.notes.where({ id: note.id }), { tier: "remote" })).toEqual(note);
      } else {
        await owner.disconnect();
        await owner.reconnect();
      }
      await expect
        .poll(
          () => reader.one(app.notes.where({ id: note.id }), { tier: "remote" }).catch(() => null),
          { timeout: 10_000 },
        )
        .toEqual(note);
    } finally {
      for (const client of clients) await client.shutdown();
      await server.stop();
    }
  },
  60_000,
);

it("keeps independent offline readers revoked after ordinary-query and device-list observations", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const store = (initial: string | null = null) => {
    let value = initial;
    return {
      async read() {
        return value;
      },
      async update(transform: (current: string | null) => string) {
        value = transform(value);
      },
    };
  };
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    const firstStore = store();
    const first = await createDb({ ...account, e2ee: { app, store: firstStore } });
    clients.push(first);
    const [firstDevice] = await first.e2ee.devices.list();
    const second = await createDb({ ...account, e2ee: { app, store: store() } });
    clients.push(second);
    const request = (await second.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    await first.e2ee.devices.approve(request.id).wait();
    await second.e2ee.devices.list();
    const initial = first.beginExclusiveTransaction();
    const project = initial.insert(app.projects, { title: "Pending deletion scope" });
    initial.insert(app.notes, { projectId: project.id, title: "Encrypted note" });
    await initial.commit().wait({ tier: "global" });
    const target = { scope: app.projects, identifier: project.id };
    expect(await first.e2ee.explain(target)).toEqual({ state: "ready" });

    // An independent store/runtime observes the same revocation via devices.list().
    // Its known-revoked flag cannot satisfy the first reader's ordinary-query check.
    const knownRevoked = await createDb({
      ...account,
      e2ee: { app, store: store(await firstStore.read()) },
    });
    clients.push(knownRevoked);
    expect(await knownRevoked.e2ee.explain(target)).toEqual({ state: "ready" });

    await second.e2ee.devices.revoke(firstDevice!.id).wait();
    // Observe through ordinary queries, not devices.list(), so the E2EE reader
    // must notice accepted history even when another caller fetched it.
    const privateHistory = await first.all(app.__e2ee_account_successors, { tier: "remote" });
    const publicHistory = await first.all(app.__e2ee_public_account_successors, { tier: "remote" });
    expect(privateHistory.length).toBeGreaterThan(0);
    expect(publicHistory.length).toBeGreaterThan(0);
    await first.disconnect();
    const deletion = first.beginTransaction();
    for (const row of privateHistory) deletion.delete(app.__e2ee_account_successors, row.id);
    for (const row of publicHistory) deletion.delete(app.__e2ee_public_account_successors, row.id);
    await deletion.commit().wait({ tier: "local" });
    expect(await first.all(app.__e2ee_account_successors, { tier: "local-first" })).toEqual([]);
    expect(await first.all(app.__e2ee_public_account_successors, { tier: "local-first" })).toEqual(
      [],
    );
    const state = await first.e2ee.explain(target).catch(() => ({ state: "unavailable" }));
    expect(["refused", "unavailable"]).toContain(state.state);

    expect(
      (await knownRevoked.e2ee.devices.list()).find((device) => device.id === firstDevice!.id)
        ?.state,
    ).toBe("revoked");
    await knownRevoked.disconnect();
    expect(await knownRevoked.e2ee.explain(target)).toMatchObject({
      state: "refused",
      reason: "device-not-active",
    });
  } finally {
    for (const client of clients) {
      await client.reconnect();
      await client.shutdown();
    }
    await server.stop();
  }
}, 60_000);

it.each([
  { removal: "group", access: "read" },
  { removal: "group", access: "write" },
  { removal: "space grant", access: "read" },
  { removal: "space grant", access: "write" },
])(
  "does not reuse offline space keys for $access after a sibling query observes $removal removal",
  async ({ removal, access }) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    const store = () => {
      let value: string | null = null;
      return {
        async read() {
          return value;
        },
        async update(transform: (current: string | null) => string) {
          value = transform(value);
        },
      };
    };
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: groupApp,
        permissions: { ...permissions, ...groupPolicies },
      });
      const alice = await localAccountConfig(server.appId, server.url);
      const bob = await localAccountConfig(server.appId, server.url);
      const owner = await createDb({ ...alice, e2ee: { app: groupApp, store: store() } });
      clients.push(owner);
      const reader = await createDb({ ...bob, e2ee: { app: groupApp, store: store() } });
      clients.push(reader);
      await owner.e2ee.devices.list();
      await reader.e2ee.devices.list();
      let groupId: string | undefined;
      if (removal === "group") {
        groupId = (await owner.e2ee.groups.create().wait()).id;
        await owner.e2ee.groups.add(groupId, { kind: "account", id: bob.account.id }).wait();
      }
      const tx = owner.beginExclusiveTransaction();
      const project = tx.insert(
        groupApp.projects,
        { title: "Group removal" },
        {
          initialRecipients: removal === "group" ? [groupId!] : [alice.account.id, bob.account.id],
        },
      );
      const note = tx.insert(groupApp.notes, {
        projectId: project.id,
        title: "Shared before removal",
      });
      await tx.commit().wait({ tier: "global" });
      await owner.e2ee.explain({ scope: groupApp.projects, identifier: project.id });
      const space = await owner.one(groupApp.__e2ee_spaces.where({ identifier: project.id }), {
        tier: "remote",
      });
      expect(space).not.toBeNull();
      const query = groupApp.notes.where({ id: note.id });
      expect(await reader.one(query, { tier: "remote" })).toEqual(note);
      const before =
        removal === "group"
          ? await reader.all(groupApp.__e2ee_group_membership, { tier: "remote" })
          : await reader.all(groupApp.__e2ee_space_grants, { tier: "remote" });

      if (removal === "group")
        await owner.e2ee.groups.remove(groupId!, { kind: "account", id: bob.account.id }).wait();
      else await owner.e2ee.spaces.revoke(groupApp.projects, project.id, bob.account.id).wait();
      // Fetch through an ordinary sibling query, without refreshing the space.
      const after =
        removal === "group"
          ? await reader.all(groupApp.__e2ee_group_membership, { tier: "remote" })
          : await reader.all(groupApp.__e2ee_space_grants, { tier: "remote" });
      expect(after.length).toBeGreaterThan(before.length);
      expect(after).toEqual(
        expect.arrayContaining([
          expect.objectContaining(
            removal === "group"
              ? { operation: "remove", groupId: groupId!, memberId: bob.account.id }
              : { operation: "remove", spaceId: space!.id, recipientId: bob.account.id },
          ),
        ]),
      );
      await reader.disconnect();
      const operation =
        access === "read"
          ? reader.one(query, { tier: "local-first" })
          : reader
              .update(groupApp.notes, note.id, { title: "Must not be encrypted" })
              .wait({ tier: "local" });
      // Partial history makes the key unavailable; completed background
      // reconciliation can instead prove that the key is no longer shared.
      // Neither outcome may reuse the removed recipient's cached key.
      await expect(operation).rejects.toMatchObject({
        name: "E2eeDataError",
        code: expect.stringMatching(/^(key-unavailable|key-not-shared)$/),
      });
    } finally {
      for (const client of clients) await client.shutdown();
      await server.stop();
    }
  },
  60_000,
);

it("keeps accepted space access while a removal is pending and after Jazz rejects it", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let saved: string | null = null;
  let hold = false;
  let release!: () => void;
  const resumed = new Promise<void>((resolve) => {
    release = resolve;
  });
  let reached!: () => void;
  const paused = new Promise<void>((resolve) => {
    reached = resolve;
  });
  try {
    const restricted = definePermissions(app, ({ policy, session }) => {
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({
        authorAccountId: session.user.account,
        operation: "add",
      });
    });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: { ...permissions, ...restricted },
    });
    const account = await localAccountConfig(server.appId, server.url);
    const signer = await createBrowserDeviceSigner();
    db = await createDb({
      ...account,
      e2ee: {
        app,
        store: {
          async read() {
            return saved;
          },
          async update(transform) {
            saved = transform(saved);
          },
        },
        crypto: {
          deviceSigner: {
            ...signer,
            async sign(key, record) {
              const signature = await signer.sign(key, record);
              if (hold && new TextDecoder().decode(record).includes('"remove"')) {
                hold = false;
                reached();
                await resumed;
              }
              return signature;
            },
          },
        },
      },
    });
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Rejected removal" });
    const note = tx.insert(app.notes, { projectId: project.id, title: "Accepted data" });
    await tx.commit().wait({ tier: "global" });
    // Finish optional delivery before testing the pending permission change.
    expect(await db.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual({
      state: "ready",
    });
    const query = app.notes.where({ id: note.id });
    expect(await db.one(query, { tier: "remote" })).toEqual(note);

    hold = true;
    const removal = db.e2ee.spaces.revoke(app.projects, project.id, account.account.id);
    let outcome: string | undefined;
    let rejection: unknown;
    const settled = removal.wait().then(
      () => {
        outcome = "accepted";
      },
      (error: unknown) => {
        outcome = "rejected";
        rejection = error;
      },
    );
    await paused;
    await db.disconnect();
    release();
    const removals = app.__e2ee_space_grants.where({
      operation: "remove",
      recipientId: account.account.id,
    });
    await expect
      .poll(async () => (await db!.all(removals, { tier: "local-first" })).length, {
        timeout: 5_000,
      })
      .toBe(1);
    expect(outcome).toBeUndefined();
    expect(await db.one(query, { tier: "local-first" })).toEqual(note);
    const write = db.update(app.notes, note.id, { title: "Still authorised" });
    await write.wait({ tier: "local" });
    expect(await db.one(query, { tier: "local-first" })).toEqual({
      ...note,
      title: "Still authorised",
    });

    await db.reconnect();
    await settled;
    expect(outcome).toBe("rejected");
    expect(rejection).toMatchObject({ code: "permission_denied" });
    await write.wait({ tier: "global" });
    expect(await db.all(removals, { tier: "remote" })).toEqual([]);
    expect(await db.one(query, { tier: "remote" })).toEqual({ ...note, title: "Still authorised" });
  } finally {
    release();
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);

it("does not grant access while an addition is pending or after Jazz rejects it", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const store = () => {
    let value: string | null = null;
    return {
      async read() {
        return value;
      },
      async update(transform: (current: string | null) => string) {
        value = transform(value);
      },
    };
  };
  let hold = false;
  let release!: () => void;
  const resumed = new Promise<void>((resolve) => {
    release = resolve;
  });
  let reached!: () => void;
  const paused = new Promise<void>((resolve) => {
    reached = resolve;
  });
  try {
    const restricted = definePermissions(app, ({ policy, session }) => {
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({
        authorAccountId: session.user.account,
        recipientId: session.user.account,
      });
    });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: { ...permissions, ...restricted },
    });
    const alice = await localAccountConfig(server.appId, server.url);
    const bob = await localAccountConfig(server.appId, server.url);
    const signer = await createBrowserDeviceSigner();
    const owner = await createDb({
      ...alice,
      e2ee: {
        app,
        store: store(),
        crypto: {
          deviceSigner: {
            ...signer,
            async sign(key, record) {
              const signature = await signer.sign(key, record);
              if (hold && new TextDecoder().decode(record).includes('"add"')) {
                hold = false;
                reached();
                await resumed;
              }
              return signature;
            },
          },
        },
      },
    });
    clients.push(owner);
    const reader = await createDb({ ...bob, e2ee: { app, store: store() } });
    clients.push(reader);
    await reader.e2ee.devices.list();
    const tx = owner.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Rejected addition" });
    const note = tx.insert(app.notes, { projectId: project.id, title: "Owner only" });
    await tx.commit().wait({ tier: "global" });
    // Finish optional delivery before testing the pending permission change.
    expect(await owner.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual({
      state: "ready",
    });
    const query = app.notes.where({ id: note.id });
    expect(await owner.one(query, { tier: "remote" })).toEqual(note);
    hold = true;
    const additions = app.__e2ee_space_grants.where({
      operation: "add",
      recipientId: bob.account.id,
    });
    expect(await owner.all(additions, { tier: "remote" })).toEqual([]);
    const grant = owner.e2ee.spaces.grant(app.projects, project.id, bob.account.id);
    let outcome: string | undefined;
    let rejection: unknown;
    const settled = grant.wait().then(
      () => {
        outcome = "accepted";
      },
      (error: unknown) => {
        outcome = "rejected";
        rejection = error;
      },
    );
    await paused;
    await owner.disconnect();
    release();
    await expect
      .poll(
        async () => {
          const rows = await owner.all(additions, { tier: "local-first" });
          return rows.length;
        },
        { timeout: 5_000 },
      )
      .toBe(1);
    expect(outcome).toBeUndefined();
    expect(await owner.one(query, { tier: "local-first" })).toEqual(note);
    await expect(reader.one(query, { tier: "remote" })).rejects.toMatchObject({
      code: "key-not-shared",
    });
    const write = owner.update(app.notes, note.id, { title: "Still owner only" });
    await write.wait({ tier: "local" });
    expect(await owner.one(query, { tier: "local-first" })).toEqual({
      ...note,
      title: "Still owner only",
    });
    await owner.reconnect();
    await settled;
    expect(outcome).toBe("rejected");
    expect(rejection).toMatchObject({ code: "permission_denied" });
    await write.wait({ tier: "global" });
    expect(await reader.all(additions, { tier: "remote" })).toEqual([]);
    await expect(reader.one(query, { tier: "remote" })).rejects.toMatchObject({
      code: "key-not-shared",
    });
    expect(await owner.one(query, { tier: "remote" })).toEqual({
      ...note,
      title: "Still owner only",
    });
  } finally {
    release();
    for (const client of clients) await client.shutdown();
    await server.stop();
  }
}, 60_000);

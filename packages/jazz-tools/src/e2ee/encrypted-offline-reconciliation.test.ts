import { expect, it } from "vitest";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createBrowserDeviceSigner } from "./browser.js";
import { app, permissions } from "./fixtures/encrypted-offline-fixture-schema.js";

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
      const tx = owner.beginSerializableTransaction();
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
    const tx = db.beginSerializableTransaction();
    const project = tx.insert(app.projects, { title: "Rejected removal" });
    const note = tx.insert(app.notes, { projectId: project.id, title: "Accepted data" });
    await tx.commit().wait({ tier: "global" });
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
    const tx = owner.beginSerializableTransaction();
    const project = tx.insert(app.projects, { title: "Rejected addition" });
    const note = tx.insert(app.notes, { projectId: project.id, title: "Owner only" });
    await tx.commit().wait({ tier: "global" });
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

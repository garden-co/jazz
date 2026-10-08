import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import {
  app,
  permissions,
  groupApp,
  groupPolicies,
} from "./fixtures/encrypted-offline-fixture-schema.js";

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
    const privateHistory = await first.all(app.__e2ee_account_successors, { tier: "global" });
    const publicHistory = await first.all(app.__e2ee_public_account_successors, { tier: "global" });
    expect(privateHistory.length).toBeGreaterThan(0);
    expect(publicHistory.length).toBeGreaterThan(0);
    await first.disconnect();
    const deletion = first.beginTransaction();
    for (const row of privateHistory) deletion.delete(app.__e2ee_account_successors, row.id);
    for (const row of publicHistory) deletion.delete(app.__e2ee_public_account_successors, row.id);
    await deletion.commit().wait({ tier: "local" });
    expect(await first.all(app.__e2ee_account_successors, { tier: "local" })).toEqual([]);
    expect(await first.all(app.__e2ee_public_account_successors, { tier: "local" })).toEqual([]);
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

it.each(["group", "space grant"] as const)(
  "does not reuse either independent offline cache after a sibling query observes %s removal",
  async (removal) => {
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
        schema: groupApp,
        permissions: { ...permissions, ...groupPolicies },
      });
      const alice = await localAccountConfig(server.appId, server.url);
      const bob = await localAccountConfig(server.appId, server.url);
      const owner = await createDb({ ...alice, e2ee: { app: groupApp, store: store() } });
      clients.push(owner);
      const preparationStore = store();
      const preparation = await createDb({
        ...bob,
        e2ee: { app: groupApp, store: preparationStore },
      });
      clients.push(preparation);
      await owner.e2ee.devices.list();
      const preparedDevices = await preparation.e2ee.devices.list();
      expect(preparedDevices).toHaveLength(1);
      expect(preparedDevices[0]).toMatchObject({ state: "active", keyReadiness: "verified" });
      const acceptedSnapshot = await preparationStore.read();
      expect(acceptedSnapshot).not.toBeNull();
      if (acceptedSnapshot === null) throw new Error("Missing accepted device fixture");
      const snapshot = JSON.parse(acceptedSnapshot, (key, value: unknown) => {
        // This guard needs identity/journal metadata, never decoded secrets.
        if ((key === "privateKey" || key === "signingPrivateKey") && Array.isArray(value)) {
          value.fill(0);
          return undefined;
        }
        return value;
      }) as {
        format: string;
        devices: { id: string }[];
        initializationJournalV1?: { outcome: string; promoted?: boolean }[];
      };
      expect(snapshot.format).toBe("jazz-e2ee-local-devices-v2");
      expect(snapshot.devices.map((device) => device.id)).toEqual([preparedDevices[0]!.id]);
      // Clone accepted identity only. Pending/unpromoted journal ownership is
      // deliberately excluded from this repeated-space-cache fixture.
      expect(
        (snapshot.initializationJournalV1 ?? []).every(
          (entry) => entry.outcome === "accepted" && entry.promoted === true,
        ),
      ).toBe(true);
      await preparation.shutdown();
      clients.pop();

      // Equal immutable bytes seed separate store closures, decoded key buffers,
      // Db runtimes and per-Db authority caches. No live preparation responder.
      const readStore = store(acceptedSnapshot);
      const writeStore = store(acceptedSnapshot);
      expect(readStore).not.toBe(writeStore);
      const readClient = await createDb({ ...bob, e2ee: { app: groupApp, store: readStore } });
      clients.push(readClient);
      const writeClient = await createDb({ ...bob, e2ee: { app: groupApp, store: writeStore } });
      clients.push(writeClient);
      expect(readClient).not.toBe(writeClient);
      for (const client of [readClient, writeClient]) {
        const devices = await client.e2ee.devices.list();
        expect(devices).toHaveLength(1);
        expect(devices[0]).toMatchObject({
          id: preparedDevices[0]!.id,
          state: "active",
          keyReadiness: "verified",
        });
      }
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
        tier: "global",
      });
      expect(space).not.toBeNull();
      const query = groupApp.notes.where({ id: note.id });
      const caches = [];
      for (const client of [readClient, writeClient]) {
        // Each branch warms its own cache before the same accepted removal.
        expect(await client.one(query, { tier: "global" })).toEqual(note);
        const before =
          removal === "group"
            ? await client.all(groupApp.__e2ee_group_membership, { tier: "global" })
            : await client.all(groupApp.__e2ee_space_grants, { tier: "global" });
        caches.push({ client, before });
      }

      if (removal === "group")
        await owner.e2ee.groups.remove(groupId!, { kind: "account", id: bob.account.id }).wait();
      else await owner.e2ee.spaces.revoke(groupApp.projects, project.id, bob.account.id).wait();
      for (const { client, before } of caches) {
        // Both caches observe the accepted removal independently through an
        // ordinary sibling query, without explicitly refreshing the space.
        const after =
          removal === "group"
            ? await client.all(groupApp.__e2ee_group_membership, { tier: "global" })
            : await client.all(groupApp.__e2ee_space_grants, { tier: "global" });
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
      }
      await Promise.all([readClient.disconnect(), writeClient.disconnect()]);
      // Launch both operations only after both runtimes are disconnected. The
      // writer never observes a read failure through its own cache or store.
      await Promise.all([
        expect(readClient.one(query, { tier: "local" })).rejects.toMatchObject({
          name: "E2eeDataError",
          code: expect.stringMatching(/^(key-unavailable|key-not-shared)$/),
        }),
        expect(
          writeClient
            .update(groupApp.notes, note.id, { title: "Must not be encrypted" })
            .wait({ tier: "local" }),
        ).rejects.toMatchObject({
          name: "E2eeDataError",
          code: expect.stringMatching(/^(key-unavailable|key-not-shared)$/),
        }),
      ]);
    } finally {
      for (const client of clients) await client.shutdown();
      await server.stop();
    }
  },
  60_000,
);

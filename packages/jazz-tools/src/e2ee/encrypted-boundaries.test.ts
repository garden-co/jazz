import { expect, it } from "vitest";
import { createBrowserCrypto } from "./browser.js";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it("automatically isolates private device records and keeps group, space and row authority independent", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const alice = await localAccountConfig(server.appId, server.url);
  const bob = await localAccountConfig(server.appId, server.url);
  const app = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), ownerId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"] }),
  });
  const policies = definePermissions(app, ({ policy, session, allOf }) => {
    const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
    policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_group_membership.allowInsert.where(
      allOf([{ authorAccountId: session.user.account }, { authorAccountId: alice.account.id }]),
    );
    policy.__e2ee_group_deliveries.allowRead.where(authenticated);
    policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_group_repairs.allowRead.where(authenticated);
    policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.notes.allowRead.always();
    policy.notes.allowInsert.where({ ownerId: session.user.account });
    policy.notes.allowUpdate
      .whereOld({ ownerId: session.user.account })
      .whereNew({ ownerId: session.user.account });
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where(() =>
      allOf([{ authorAccountId: session.user.account }, { authorAccountId: alice.account.id }]),
    );
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  });
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
      permissions: policies,
    });
    const creator = await createDb({ ...alice, e2ee: { app, store: store() } });
    clients.push(creator);
    const crypto = await createBrowserCrypto();
    let duringEncryption: (() => Promise<void>) | undefined;
    const borrowedKeys: Uint8Array[] = [];
    const recipient = await createDb({
      ...bob,
      e2ee: {
        app,
        store: store(),
        crypto: {
          ...crypto,
          cellCipher: {
            ...crypto.cellCipher,
            async encrypt(key, aad, plaintext) {
              if (duringEncryption) {
                const run = duringEncryption;
                duringEncryption = undefined;
                borrowedKeys.push(key);
                await run();
              }
              return crypto.cellCipher.encrypt(key, aad, plaintext);
            },
          },
        },
      },
    });
    clients.push(recipient);
    await creator.e2ee.devices.list();
    await recipient.e2ee.devices.list();
    const ownerRequests = await creator.all(app.__e2ee_device_requests, { tier: "remote" });
    const recipientRequests = await recipient.all(app.__e2ee_device_requests, { tier: "remote" });
    expect(ownerRequests).toHaveLength(1);
    expect(recipientRequests).toHaveLength(1);
    expect(recipientRequests[0]!.id).not.toBe(ownerRequests[0]!.id);
    expect(
      await recipient.all(app.__e2ee_device_requests.where({ id: ownerRequests[0]!.id }), {
        tier: "remote",
      }),
    ).toEqual([]);
    await expect(
      recipient.delete(app.__e2ee_device_requests, ownerRequests[0]!.id).wait({ tier: "global" }),
    ).rejects.toThrow(/permission/i);
    // Public keys are intentionally readable; enrolment handshakes are not.
    expect(
      await recipient.all(app.__e2ee_device_keys.where({ deviceId: ownerRequests[0]!.id }), {
        tier: "remote",
      }),
    ).toHaveLength(1);

    // Exercise automatically installed group policies before any space operation
    // can warm the group's managed schema or authority.
    const group = await creator.e2ee.groups.create().wait();
    expect(
      await recipient.all(app.__e2ee_groups.where({ id: group.id }), { tier: "remote" }),
    ).toHaveLength(1);
    await creator.e2ee.groups.add(group.id, { kind: "account", id: bob.account.id }).wait();
    expect(await creator.e2ee.explain({ groupId: group.id })).toEqual({ state: "ready" });
    expect(await recipient.e2ee.explain({ groupId: group.id })).toEqual({ state: "ready" });
    // Bob has the key, but Alice alone has membership administration permission.
    await expect(
      recipient.e2ee.groups.remove(group.id, { kind: "account", id: alice.account.id }).wait(),
    ).rejects.toThrow(/permission/i);
    expect(await creator.e2ee.explain({ groupId: group.id })).toEqual({ state: "ready" });

    const tx = creator.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Project" });
    const note = tx.insert(app.notes, {
      projectId: project.id,
      ownerId: alice.account.id,
      title: "Private title",
    });
    await tx.commit().wait({ tier: "global" });
    const query = app.notes.where({ id: note.id });
    // The server permits the row, but Bob has not received its encryption key.
    await expect(recipient.all(query, { tier: "remote" })).rejects.toMatchObject({
      name: "E2eeDataError",
      code: "key-not-shared",
    });
    await expect(
      recipient
        .insert(app.notes, {
          projectId: project.id,
          ownerId: bob.account.id,
          title: "No key yet",
        })
        .wait({ tier: "global" }),
    ).rejects.toMatchObject({
      name: "E2eeDataError",
      code: "key-not-shared",
    });
    await expect(
      recipient.update(app.notes, note.id, { title: "No key yet" }).wait({ tier: "global" }),
    ).rejects.toMatchObject({
      name: "E2eeDataError",
      code: "key-not-shared",
    });
    expect(await creator.all(app.notes, { tier: "remote" })).toEqual([note]);
    expect(await recipient.all(query.select("id", "ownerId"), { tier: "remote" })).toEqual([
      { id: note.id, ownerId: alice.account.id },
    ]);
    await creator.e2ee.spaces.grant(app.projects, project.id, bob.account.id).wait();
    expect(await recipient.one(query, { tier: "remote" })).toEqual(note);
    // Possessing the key lets Bob prepare ciphertext, not overwrite Alice's row.
    await expect(
      recipient.update(app.notes, note.id, { title: "Unauthorised" }).wait({ tier: "global" }),
    ).rejects.toThrow();
    expect(await creator.one(query, { tier: "remote" })).toEqual(note);
    const own = recipient.insert(app.notes, {
      projectId: project.id,
      ownerId: bob.account.id,
      title: "Bob's row",
    });
    await own.wait({ tier: "global" });
    expect(await creator.one(app.notes.where({ id: own.value.id }), { tier: "remote" })).toEqual(
      own.value,
    );
    // Pause both preparation kinds, then publish one revocation. Each batch
    // must independently recheck authority and roll back all of its changes.
    let resume!: () => void;
    const paused = new Promise<void>((resolve) => {
      resume = resolve;
    });
    let entered!: () => void;
    let timeout: ReturnType<typeof setTimeout>;
    const started = new Promise<void>((resolve, reject) => {
      entered = resolve;
      timeout = setTimeout(() => reject(new Error("Encrypted batches did not both start")), 10_000);
    });
    let preparations = 0;
    const pauseBatch = async () => {
      if (++preparations === 2) entered();
      else duringEncryption = pauseBatch;
      await paused;
    };
    duringEncryption = pauseBatch;
    const revoked = recipient.beginTransaction();
    const rejectedRows = Array.from({ length: 3 }, (_, index) =>
      revoked.insert(app.notes, {
        projectId: project.id,
        ownerId: bob.account.id,
        title: `Revoked batch ${index}`,
      }),
    );
    const updates = recipient.beginTransaction();
    for (let index = 0; index < 3; index++) {
      updates.update(app.notes, own.value.id, { title: `Revoked update ${index}` });
    }
    const rejected = Promise.all([
      expect(revoked.commit().wait({ tier: "global" })).rejects.toMatchObject({
        code: "key-not-shared",
      }),
      expect(updates.commit().wait({ tier: "global" })).rejects.toMatchObject({
        code: "key-not-shared",
      }),
    ]);
    rejected.catch(() => {});
    try {
      await started;
      await creator.e2ee.spaces.revoke(app.projects, project.id, bob.account.id).wait();
      // Later layers prepare from coherent retained authority. Establish that this
      // client knows the revocation before the cipher hands control back.
      expect(
        await recipient.e2ee.explain({ scope: app.projects, identifier: project.id }),
      ).toMatchObject({ state: "refused" });
    } finally {
      clearTimeout(timeout!);
      resume();
    }
    await rejected;
    expect(borrowedKeys).toHaveLength(2);
    expect(borrowedKeys.every((key) => key.every((byte) => byte === 0))).toBe(true);
    expect(
      await creator.all(app.notes.where({ id: { in: rejectedRows.map((row) => row.id) } }), {
        tier: "remote",
      }),
    ).toEqual([]);
    expect(await creator.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual({
      state: "ready",
    });
    expect(await creator.all(app.__e2ee_space_successors, { tier: "remote" })).toHaveLength(1);
    expect(await creator.one(query, { tier: "remote" })).toEqual(note);
    await creator.update(app.notes, note.id, { title: "New epoch" }).wait({ tier: "global" });
    // One result spans both the new and historical cell epochs.
    const epochs = await creator.all(app.notes, { tier: "remote" });
    expect(epochs.sort((a, b) => a.id.localeCompare(b.id))).toEqual(
      [{ ...note, title: "New epoch" }, own.value].sort((a, b) => a.id.localeCompare(b.id)),
    );
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 120_000);

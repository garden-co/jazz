import { expect, it, vi } from "vitest";
import { connect, createServer, type Socket } from "node:net";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import type { Db } from "../runtime/db.js";
import type { AccountStore } from "../accounts/persistence.js";
import { accountGeneratedHere, exportLocalFirstSecret } from "../accounts/enrollment.js";

const app = s.defineApp({
  plaintext: s.table({ title: s.string(), done: s.boolean() }, {}),
  projects: s.table({ title: s.string() }, {}),
  notes: s
    .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
    .encrypted({ space: "projectId", columns: ["body"] }),
  uploads: s
    .table({ projectId: s.uuid(), bytes: s.bytes() }, { project: s.rel("projects", "projectId") })
    .encrypted({ space: "projectId", columns: ["bytes"] }),
});
function privateStore(): AccountStore {
  let value: string | null = null;
  return {
    async read() {
      return value;
    },
    async update(transform) {
      value = transform(value);
    },
  };
}
async function transportGate(targetUrl: string) {
  const target = new URL(targetUrl);
  const sockets = new Set<Socket>();
  let blocked = false;
  const server = createServer((socket) => {
    if (blocked) {
      socket.destroy();
      return;
    }
    const upstream = connect({ host: target.hostname, port: Number(target.port) });
    for (const stream of [socket, upstream]) {
      sockets.add(stream);
      stream.on("error", () => {});
      stream.on("close", () => sockets.delete(stream));
    }
    socket.pipe(upstream).pipe(socket);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing transport gate address");
  return {
    url: `http://127.0.0.1:${address.port}`,
    block() {
      blocked = true;
      for (const socket of sockets) socket.destroy();
    },
    unblock() {
      blocked = false;
    },
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}

it.each([
  "unknown-recipient",
  "provisional-update",
  "rejected-founder",
  "plaintext-transaction",
  "imported-root-resumes-exact-founder-journal",
] as const)(
  "keeps automatic offline initialization fail-closed across %s",
  async (scenario) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const gate = await transportGate(server.url);
    const clients: Db[] = [];
    try {
      const warmAccount = await localAccountConfig(server.appId, gate.url);
      let founderAccount = await localAccountConfig(server.appId, gate.url);
      const permissions = definePermissions(app, ({ policy, session }) => {
        policy.projects.allowRead.always();
        policy.plaintext.allowRead.always();
        policy.plaintext.allowInsert.always();
        policy.projects.allowInsert.always();
        policy.notes.allowRead.always();
        policy.notes.allowInsert.always();
        policy.notes.allowUpdate.always();
        policy.uploads.allowRead.always();
        policy.uploads.allowInsert.always();
        policy.__e2ee_spaces.allowRead.always();
        policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
        policy.__e2ee_space_grants.allowRead.always();
        policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
        policy.__e2ee_space_deliveries.allowRead.always();
        policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      });
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      const warm = await createDb({ ...warmAccount, e2ee: { app, store: privateStore() } });
      clients.push(warm);
      await warm
        .insert(app.projects, { title: "Authenticated catalogue warm-up" })
        .wait({ tier: "global" });
      await warm.shutdown();
      clients.pop();
      gate.block();
      const founderStore = privateStore();
      let founder = await createDb({
        ...founderAccount,
        e2ee: { app, store: founderStore },
      });
      clients.push(founder);

      if (scenario === "plaintext-transaction") {
        await expect(
          founder.transaction(async (tx) => {
            expect(await tx.all(app.plaintext, { tier: "local" })).toEqual([]);
          }),
        ).rejects.toThrow();
        const onError = vi.fn();
        founder.onMutationError(onError);
        for (const kind of ["mergeable", "exclusive"] as const) {
          const tx =
            kind === "exclusive" ? founder.beginExclusiveTransaction() : founder.beginTransaction();
          tx.insert(app.plaintext, { title: "Must not publish", done: false });
          tx.upsert(app.plaintext, crypto.randomUUID(), { done: true });
          await expect(tx.commit().wait({ tier: "local" })).rejects.toThrow();
          expect(await founder.all(app.plaintext, { tier: "local" })).toEqual([]);
        }
        const valid = await founder
          .insert(app.plaintext, { title: "Ordinary local write", done: false })
          .wait({ tier: "local" });
        expect(await founder.all(app.plaintext, { tier: "local" })).toEqual([valid]);
        expect(onError).not.toHaveBeenCalled();
        return;
      }

      if (scenario === "unknown-recipient") {
        let pulls = 0;
        await expect(
          founder.streamingTransaction((plan) => {
            const project = plan.insert(
              app.projects,
              { title: "Unavailable recipient" },
              { initialRecipients: [crypto.randomUUID()] },
            );
            plan.insertStreaming(app.uploads, {
              projectId: project.id,
              bytes: (async function* () {
                pulls++;
                yield new Uint8Array([13, 21, 34]);
              })(),
            });
          }),
        ).rejects.toMatchObject({ code: "e2ee_initialization_not_ready", retryable: true });
        expect(pulls).toBe(0);
        expect(await founder.all(app.projects, { tier: "local" })).toEqual([]);
        expect(await founder.all(app.uploads, { tier: "local" })).toEqual([]);
        expect(await founder.all(app.__e2ee_spaces, { tier: "local" })).toEqual([]);
        return;
      }

      const tx = founder.beginExclusiveTransaction();
      const project = tx.insert(app.projects, { title: "Pending founder space" });
      const note = tx.insert(app.notes, {
        projectId: project.id,
        body: "Original offline message",
      });
      const commit = tx.commit();
      await commit.wait({ tier: "local" });
      const originalTxId = await commit.txId;
      expect(await founder.one(app.notes.where({ id: note.id }), { tier: "local" })).toEqual(note);
      const root = await founder.one(app.__e2ee_spaces.where({ identifier: project.id }), {
        tier: "local",
      });
      expect(root?.accountId).toBe(founderAccount.account.id);

      if (scenario === "imported-root-resumes-exact-founder-journal") {
        const secret = exportLocalFirstSecret(founderAccount.account);
        await founder.shutdown();
        clients.pop();
        founderAccount = await localAccountConfig(server.appId, gate.url, secret);
        expect(await accountGeneratedHere(founderAccount.account)).toBe(false);
        founder = await createDb({
          ...founderAccount,
          e2ee: { app, store: founderStore },
        });
        clients.push(founder);
        expect(await founder.one(app.notes.where({ id: note.id }), { tier: "local" })).toEqual(
          note,
        );
        return;
      }

      if (scenario === "provisional-update") {
        await founder
          .update(app.notes, note.id, { body: "Updated without replay" })
          .wait({ tier: "local" });
        expect(
          await founder.one(app.notes.where({ id: note.id }), { tier: "local" }),
        ).toMatchObject({ id: note.id, body: "Updated without replay" });
        const explicit = founder.beginTransaction();
        explicit.update(app.notes, note.id, { body: "Must not be staged" });
        await expect(explicit.commit().wait({ tier: "local" })).rejects.toMatchObject({
          code: "e2ee_provisional_requires_exclusive",
          retryable: true,
        });
        await explicit.rollback().catch(() => {});
        expect(
          await founder.one(app.notes.where({ id: note.id }), { tier: "local" }),
        ).toMatchObject({ body: "Updated without replay" });
        await expect(
          founder
            .update(app.notes, crypto.randomUUID(), { body: "Never an upsert" })
            .wait({ tier: "local" }),
        ).rejects.toThrow();
        expect(await founder.all(app.notes, { tier: "local" })).toEqual([
          { ...note, body: "Updated without replay" },
        ]);
        gate.unblock();
        await founder.reconnect();
        await commit.wait({ tier: "global" });
        expect(
          await founder.one(app.__e2ee_spaces.where({ identifier: project.id }), {
            tier: "global",
          }),
        ).toMatchObject({ id: root!.id, epochId: root!.epochId });
        expect(await commit.txId).toBe(originalTxId);
      } else {
        await founder.disconnect();
        gate.unblock();
        // A separate device wins the same account identity while this founder
        // remains explicitly offline; package-owned permissions stay intact.
        const winner = await createDb({
          ...founderAccount,
          e2ee: { app, store: privateStore() },
        });
        clients.push(winner);
        await winner.insert(app.projects, { title: "Winning founder" }).wait({ tier: "global" });
        const acceptedIdentity = await winner.one(
          app.__e2ee_account_identities.where({ id: founderAccount.account.id }),
          { tier: "global" },
        );
        expect(acceptedIdentity).not.toBeNull();
        await founder.reconnect();
        await expect(commit.wait({ tier: "global" })).rejects.toThrow();
        await expect(
          founder
            .insert(app.notes, { projectId: project.id, body: "Rejected descendant" })
            .wait({ tier: "local" }),
        ).rejects.toMatchObject({ code: "e2ee_initialization_not_ready" });
        expect(await commit.txId).toBe(originalTxId);
        expect(await founder.all(app.__e2ee_account_identities, { tier: "global" })).toEqual([
          acceptedIdentity,
        ]);
      }
    } finally {
      gate.unblock();
      await Promise.all(clients.map((client) => client.shutdown()));
      await gate.close();
      await server.stop();
    }
  },
  60_000,
);

it("never turns a provisional author's sealed envelope into recipient membership", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  try {
    const permissions = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
      policy.uploads.allowRead.always();
      policy.uploads.allowInsert.always();
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const ownerAccount = await localAccountConfig(server.appId, server.url);
    const recipientAccount = await localAccountConfig(server.appId, server.url);
    const owner = await createDb({ ...ownerAccount, e2ee: { app, store: privateStore() } });
    const recipient = await createDb({ ...recipientAccount, e2ee: { app, store: privateStore() } });
    clients.push(owner, recipient);
    await Promise.all([owner.e2ee.devices.list(), recipient.e2ee.devices.list()]);
    await owner
      .insert(
        app.projects,
        { title: "Known accepted recipients" },
        {
          initialRecipients: [ownerAccount.account.id, recipientAccount.account.id],
        },
      )
      .wait({ tier: "global" });
    await owner.disconnect();
    const tx = owner.beginExclusiveTransaction();
    const project = tx.insert(
      app.projects,
      { title: "Recipient only" },
      { initialRecipients: [recipientAccount.account.id] },
    );
    const note = tx.insert(app.notes, {
      projectId: project.id,
      body: "Only the selected recipient",
    });
    const pending = tx.commit();
    await pending.wait({ tier: "local" });
    await expect(
      owner.one(app.notes.where({ id: note.id }), { tier: "local" }),
    ).rejects.toMatchObject({ code: "key-not-shared" });
    await expect(
      owner
        .insert(app.notes, { projectId: project.id, body: "No implicit creator grant" })
        .wait({ tier: "local" }),
    ).rejects.toMatchObject({ code: "key-not-shared" });
    const root = await owner.one(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "local",
    });
    const grants = await owner.all(app.__e2ee_space_grants.where({ spaceId: root!.id }), {
      tier: "local",
    });
    expect(grants.map((grant) => grant.recipientId)).toEqual([recipientAccount.account.id]);
    await owner.reconnect();
    await pending.wait({ tier: "global" });
    expect(await recipient.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual(note);
    await expect(
      owner.one(app.notes.where({ id: note.id }), { tier: "global" }),
    ).rejects.toMatchObject({ code: "key-not-shared" });
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);

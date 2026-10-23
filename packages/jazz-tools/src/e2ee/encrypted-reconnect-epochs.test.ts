import { expect, it } from "vitest";
import { createServer, connect, type Socket } from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { groupSchema } from "./groups.js";

const app = s.defineApp({
  ...deviceRequestSchema,
  ...spaceSchema,
  ...groupSchema,
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
  // Ordinary row access remains open so the encryption lifecycle owns refusal.
  policy.notes.allowUpdate.always();
  policy.__e2ee_spaces.allowRead.always();
  policy.__e2ee_spaces.allowInsert.where({ creatorAccountId: session.user.account });
  policy.__e2ee_space_grants.allowRead.always();
  policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
  policy.__e2ee_space_deliveries.allowRead.always();
  policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  policy.__e2ee_space_successors.allowRead.always();
  policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
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
const permissions = { ...deviceRequestPermissions, ...policies };
const store = () => {
  let saved: string | null = null;
  return {
    async read() {
      return saved;
    },
    async update(transform: (value: string | null) => string) {
      saved = transform(saved);
    },
  };
};

// Both accounts use the same registry URL. Keep the owner's existing socket
// live while cutting the subsequently opened reader sockets and refusing new
// connections, including the reader's automatic transport retries.
async function reconnectGate(target: string) {
  const upstream = new URL(target);
  const sockets = new Set<Socket>();
  const readerSockets = new Set<Socket>();
  let capture = false;
  let blocked = false;
  const server = createServer((incoming) => {
    if (blocked) {
      incoming.destroy();
      return;
    }
    const outgoing = connect(Number(upstream.port), upstream.hostname);
    for (const socket of [incoming, outgoing]) {
      sockets.add(socket);
      if (capture) readerSockets.add(socket);
      socket.on("error", () => {
        incoming.destroy();
        outgoing.destroy();
      });
      socket.on("close", () => {
        sockets.delete(socket);
        readerSockets.delete(socket);
      });
    }
    incoming.pipe(outgoing).pipe(incoming);
    incoming.on("close", () => outgoing.destroy());
    outgoing.on("close", () => incoming.destroy());
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing proxy port");
  const url = new URL(target);
  url.hostname = "127.0.0.1";
  url.port = String(address.port);
  return {
    url: url.toString().replace(/\/$/, ""),
    captureReader() {
      capture = true;
    },
    block() {
      capture = false;
      blocked = true;
      const count = readerSockets.size;
      for (const socket of readerSockets) socket.destroy();
      return count;
    },
    unblock() {
      blocked = false;
    },
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

it.each(["retained account", "removed account", "removed nested member"] as const)(
  "refreshes missing epochs after an offline persistent reopen: %s",
  async (membership) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const gate = await reconnectGate(server.url);
    const directory = await mkdtemp(join(tmpdir(), "jazz-e2ee-reconnect-"));
    let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
    let reader: Awaited<ReturnType<typeof createJazzSession>> | undefined;
    let departing: Awaited<ReturnType<typeof createJazzSession>> | undefined;
    const common = {
      appId: server.appId,
      serverUrl: gate.url,
      app,
      permissions,
      initial: "local-first" as const,
    };
    const readerConfig = {
      ...common,
      driver: { type: "persistent" as const, dataPath: join(directory, "reader") },
      store: store(),
      e2ee: { app, store: store() },
    };
    try {
      await deploy({
        appId: server.appId,
        serverUrl: server.url,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      owner = await createJazzSession({
        ...common,
        driver: { type: "memory" },
        store: store(),
        e2ee: { app, store: store() },
      });
      const alice = owner.getSnapshot().client!.db;
      const aliceId = owner.getSnapshot().account!.id;
      await alice.e2ee.devices.list();
      let departingId: string | undefined;
      if (membership === "retained account") {
        departing = await createJazzSession({
          ...common,
          driver: { type: "memory" },
          store: store(),
          e2ee: { app, store: store() },
        });
        departingId = departing.getSnapshot().account!.id;
        await departing.getSnapshot().client!.db.e2ee.devices.list();
        expect(
          await alice.all(app.__e2ee_account_roots.where({ accountId: departingId }), {
            tier: "remote",
          }),
        ).toHaveLength(1);
      }
      gate.captureReader();
      reader = await createJazzSession(readerConfig);
      let bob = reader.getSnapshot().client!.db;
      const bobId = reader.getSnapshot().account!.id;
      await bob.e2ee.devices.list();
      // Initial recipients belong to the creation transaction's fixed snapshot.
      expect(
        await alice.all(app.__e2ee_account_roots.where({ accountId: bobId }), { tier: "remote" }),
      ).toHaveLength(1);
      let recipient = bobId;
      let child: string | undefined;
      if (membership === "removed nested member") {
        const parent = await alice.e2ee.groups.create().wait();
        child = (await alice.e2ee.groups.create().wait()).id;
        await alice.e2ee.groups.add(child, { kind: "account", id: bobId }).wait();
        await alice.e2ee.groups.add(parent.id, { kind: "group", id: child }).wait();
        recipient = parent.id;
      }
      const tx = alice.beginSerializableTransaction();
      const project = tx.insert(
        app.projects,
        { title: "Retained project" },
        {
          initialRecipients: departingId ? [aliceId, recipient, departingId] : [aliceId, recipient],
        },
      );
      const note = tx.insert(app.notes, { projectId: project.id, title: "Before removal" });
      await tx.commit().wait({ tier: "global" });
      // Complete the initial delivery before exercising an already-shared space.
      expect(await alice.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual({
        state: "ready",
      });
      const query = app.notes.where({ id: note.id });
      expect(await bob.one(query, { tier: "remote" })).toEqual(note);
      expect(await bob.one(app.projects.where({ id: project.id }), { tier: "remote" })).toEqual(
        project,
      );
      expect(gate.block()).toBeGreaterThan(0);
      if (child) await alice.e2ee.groups.remove(child, { kind: "account", id: bobId }).wait();
      else await alice.e2ee.spaces.revoke(app.projects, project.id, departingId ?? bobId).wait();
      expect(await alice.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual({
        state: "ready",
      });
      await alice.update(app.notes, note.id, { title: "After rotation" }).wait({ tier: "global" });
      await reader.close();
      reader = undefined;
      reader = await createJazzSession(readerConfig);
      bob = reader.getSnapshot().client!.db;
      expect(reader.getSnapshot().account!.id).toBe(bobId);
      expect(
        await bob.one(app.projects.where({ id: project.id }), { tier: "local-first" }),
      ).toEqual(project);
      gate.unblock();
      // Establish transport recovery without refreshing any encryption metadata.
      await expect
        .poll(
          () =>
            bob.one(app.projects.where({ id: project.id }), { tier: "remote" }).catch(() => null),
          { timeout: 15000, interval: 100 },
        )
        .toEqual(project);
      if (membership === "retained account") {
        expect(await bob.one(query, { tier: "remote" })).toEqual({
          ...note,
          title: "After rotation",
        });
      } else {
        await expect(bob.one(query, { tier: "remote" })).rejects.toMatchObject({
          name: "E2eeDataError",
          code: "key-not-shared",
        });
        await expect(
          bob.update(app.notes, note.id, { title: "Forbidden" }).wait({ tier: "global" }),
        ).rejects.toMatchObject({ name: "E2eeDataError", code: "key-not-shared" });
        expect(await alice.one(query, { tier: "remote" })).toEqual({
          ...note,
          title: "After rotation",
        });
      }
    } finally {
      gate.unblock();
      await reader?.close();
      await departing?.close();
      await owner?.close();
      await gate.close();
      await server.stop();
      await rm(directory, { recursive: true, force: true });
    }
  },
  // Native nested-group rotation needs the same full-CI budget as the other
  // group integration tests; all reads and revocation assertions remain bounded.
  120_000,
);

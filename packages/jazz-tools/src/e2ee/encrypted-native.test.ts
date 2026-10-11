import { afterAll, beforeAll, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";

function store() {
  let value: string | null = null;
  return {
    async read() {
      return value;
    },
    async update(transform: (value: string | null) => string) {
      value = transform(value);
    },
  };
}

async function createFixture() {
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string(), bytes: s.bytes() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title", "bytes"] }),
  });
  const policies = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
    policy.notes.allowUpdate.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  });
  const permissions = { ...deviceRequestPermissions, ...policies };
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
  let wasm: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    owner = await createJazzSession({
      appId: server.appId,
      serverUrl: server.url,
      app,
      permissions,
      driver: { type: "memory" },
      initial: "local-first",
      e2ee: { app, store: store() },
    });
    const native = owner.getSnapshot().client!.db;
    const nativeId = owner.getSnapshot().account!.id;
    const account = await localAccountConfig(server.appId, server.url);
    wasm = await createDb({ ...account, e2ee: { app, store: store() } });
    await native.e2ee.devices.list();
    await wasm.e2ee.devices.list();
    return {
      app,
      native,
      nativeId,
      wasm,
      account,
      server,
      async close() {
        await wasm?.shutdown();
        await owner?.close();
        await server.stop();
      },
    };
  } catch (error) {
    await wasm?.shutdown();
    await owner?.close();
    await server.stop();
    throw error;
  }
}

// Each mode and writer direction creates a fresh scope. Only the accepted
// accounts are shared; the automatic startup suites exercise cold enrolment.
let fixture: Awaited<ReturnType<typeof createFixture>>;
beforeAll(async () => {
  fixture = await createFixture();
}, 60_000);
afterAll(async () => {
  await fixture?.close();
});

it.each(["grant", "initial", "initial-upsert"])(
  "reads and updates encrypted cells across native Node and WASM (%s)",
  async (sharing) => {
    const { app, native, nativeId, wasm, account } = fixture;
    for (const [writer, reader, readerId] of [
      [native, wasm, account.account.id],
      [wasm, native, nativeId],
    ] as const) {
      const tx = writer.beginExclusiveTransaction();
      const recipients = [readerId];
      const project =
        sharing === "initial-upsert"
          ? { id: crypto.randomUUID(), title: "Project" }
          : tx.insert(
              app.projects,
              { title: "Project" },
              sharing !== "grant" ? { initialRecipients: recipients } : undefined,
            );
      if (sharing === "initial-upsert") {
        tx.upsert(
          app.projects,
          project.id,
          { title: project.title },
          { initialRecipients: recipients },
        );
      }
      recipients.length = 0;
      const note = tx.insert(app.notes, {
        projectId: project.id,
        title: "Private title",
        bytes: new Uint8Array([1, 2, 255]),
      });
      const query = app.notes.where({ id: note.id });
      expect(await tx.one(query, { tier: "local" })).toEqual(note);
      expect(await tx.one(query.select("id", "title"), { tier: "local" })).toEqual({
        id: note.id,
        title: "Private title",
      });
      await tx.commit().wait({ tier: "global" });
      if (sharing !== "grant") {
        await expect(
          writer
            .upsert(
              app.projects,
              project.id,
              { title: "Must not change" },
              { initialRecipients: [readerId] },
            )
            .wait({ tier: "global" }),
        ).rejects.toThrow("initialRecipients cannot change an existing encryption scope");
        expect(
          await writer.one(app.projects.where({ id: project.id }), { tier: "global" }),
        ).toEqual(project);
      }
      if (sharing === "grant") {
        await writer.e2ee.spaces.grant(app.projects, project.id, readerId).wait();
      } else {
        expect(
          await writer.e2ee.explain({ scope: app.projects, identifier: project.id }),
        ).toMatchObject({ state: "refused" });
        const root = await writer.one(app.__e2ee_spaces.where({ identifier: project.id }), {
          tier: "global",
        });
        expect(
          (
            await writer.all(app.__e2ee_space_grants.where({ spaceId: root!.id }), {
              tier: "global",
            })
          ).map((grant) => grant.recipientId),
        ).toEqual([readerId]);
      }
      expect(await reader.one(query, { tier: "global" })).toEqual(note);
      expect(await reader.one(query.select("id", "title"), { tier: "global" })).toEqual({
        id: note.id,
        title: "Private title",
      });
      await reader
        .update(app.notes, note.id, { title: "Changed", bytes: new Uint8Array([4, 5]) })
        .wait({ tier: "global" });
      if (sharing === "grant") {
        // Existing-space bursts borrow one key, then recheck authority before
        // publication. Both writer runtimes must preserve every row and byte.
        const batch = reader.beginTransaction();
        const added = Array.from({ length: 3 }, (_, index) =>
          batch.insert(app.notes, {
            projectId: project.id,
            title: `Batched ${index}`,
            bytes: new Uint8Array([index, 255]),
          }),
        );
        await batch.commit().wait({ tier: "global" });
        const batchRows = await writer.all(
          app.notes.where({ id: { in: added.map((row) => row.id) } }),
          { tier: "global" },
        );
        expect(batchRows.sort((a, b) => a.id.localeCompare(b.id))).toEqual(
          added.sort((a, b) => a.id.localeCompare(b.id)),
        );
        expect(await writer.one(query, { tier: "global" })).toEqual({
          ...note,
          title: "Changed",
          bytes: new Uint8Array([4, 5]),
        });
      } else {
        await expect(writer.one(query, { tier: "global" })).rejects.toMatchObject({
          code: "key-not-shared",
        });
      }
    }
  },
  60_000,
);

// Both runtimes capture authority before the same newly enrolled recipient.
// Each still attempts an independent scope and proves atomic rollback.
it("rejects a late initial recipient on native and WASM without publishing any rows", async () => {
  const { app, native, wasm, server } = fixture;
  const writers = [native, wasm];
  const transactions = writers.map((writer) => writer.beginExclusiveTransaction());
  const pending = new Set(transactions);
  let recipient: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    for (const tx of transactions) {
      expect(
        await tx.one(app.projects.where({ id: crypto.randomUUID() }), { tier: "global" }),
      ).toBeNull();
    }
    const account = await localAccountConfig(server.appId, server.url);
    recipient = await createDb({ ...account, e2ee: { app, store: store() } });
    await recipient.e2ee.devices.list();
    for (const [index, tx] of transactions.entries()) {
      const writer = writers[index]!;
      const recipients = [account.account.id];
      const project = tx.insert(
        app.projects,
        { title: "Late recipient" },
        { initialRecipients: recipients },
      );
      recipients.length = 0;
      const note = tx.insert(app.notes, {
        projectId: project.id,
        title: "Must not publish",
        bytes: new Uint8Array([1, 2, 255]),
      });
      const result = tx.commit();
      pending.delete(tx);
      await expect(result.wait({ tier: "global" })).rejects.toThrow(
        "E2EE initial recipient account is unavailable",
      );
      expect(
        await writer.one(app.projects.where({ id: project.id }), { tier: "global" }),
      ).toBeNull();
      expect(await writer.one(app.notes.where({ id: note.id }), { tier: "global" })).toBeNull();
      expect(
        await writer.all(app.__e2ee_spaces.where({ identifier: project.id }), { tier: "global" }),
      ).toEqual([]);
    }
  } finally {
    await Promise.all([...pending].map((tx) => tx.rollback()));
    await recipient?.shutdown();
  }
}, 60_000);

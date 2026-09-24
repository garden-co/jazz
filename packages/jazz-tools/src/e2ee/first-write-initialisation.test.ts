import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createBrowserKeyEnvelope } from "./browser.js";
import type { KeyEnvelope } from "./types.js";

it("grants the first encrypted writer, not the pre-existing project's creator", async () => {
  const before = { projects: s.table({ title: s.string() }, {}) };
  const after = {
    ...before,
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] }),
  };
  const oldApp = s.defineApp(before);
  const app = s.defineApp(after);
  const migration = s.defineMigration({ from: before, to: after, createTables: { notes: true } });
  const oldPermissions = definePermissions(oldApp, ({ policy }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  });
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
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let creator: Awaited<ReturnType<typeof createDb>> | undefined;
  let writer: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    const target = { serverUrl: server.url, appId: server.appId, adminSecret: server.adminSecret };
    await deploy({ ...target, schema: oldApp, permissions: oldPermissions });
    const alice = await localAccountConfig(server.appId, server.url);
    creator = await createDb(alice);
    const project = await creator
      .insert(oldApp.projects, { title: "Created before encryption" })
      .wait({ tier: "global" });
    await creator.shutdown();
    creator = undefined;

    await deploy({ ...target, schema: app, permissions, migration });
    creator = await createDb({ ...alice, e2ee: { app, store: store() } });
    const bob = await localAccountConfig(server.appId, server.url);
    writer = await createDb({ ...bob, e2ee: { app, store: store() } });
    const note = await writer
      .insert(app.notes, { projectId: project.id, body: "Bob's first encrypted note" })
      .wait({ tier: "global" });

    expect(await writer.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual(note);
    const roots = await writer.all(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "global",
    });
    expect(roots).toHaveLength(1);
    expect(roots[0]).toMatchObject({ accountId: bob.account.id });
    const grants = await writer.all(app.__e2ee_space_grants.where({ spaceId: roots[0]!.id }), {
      tier: "global",
    });
    expect(grants).toHaveLength(1);
    expect(grants[0]).toMatchObject({ operation: "add", recipientId: bob.account.id });
    // Jazz allows Alice to read the row; creation history must not supply its key.
    await expect(
      creator.one(app.notes.where({ id: note.id }), { tier: "global" }),
    ).rejects.toMatchObject({
      code: "key-not-shared",
    });
    expect(await creator.one(app.projects.where({ id: project.id }), { tier: "global" })).toEqual(
      project,
    );
  } finally {
    await writer?.shutdown();
    await creator?.shutdown();
    await server.stop();
  }
}, 60_000);

it.each([
  "insert",
  "upsert",
  "exclusive",
  "mergeable",
  "deny-data",
  "deny-root",
  "deny-grant",
  "crypto-error",
  "hidden-existing",
  "hidden-existing-updatable",
] as const)(
  "keeps first encrypted writes atomic (%s)",
  async (mode) => {
    const before = { projects: s.table({ title: s.string() }, {}) };
    const after = {
      ...before,
      events: s.table({ message: s.string() }, {}),
      notes: s
        .table(
          { projectId: s.uuid(), body: s.string() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({ space: "projectId", columns: ["body"] }),
    };
    const oldApp = s.defineApp(before);
    const app = s.defineApp(after);
    const migration = s.defineMigration({
      from: before,
      to: after,
      createTables: { notes: true, events: true },
    });
    const oldPermissions = definePermissions(oldApp, ({ policy }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
    });
    const permissions = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.events.allowRead.always();
      policy.events.allowInsert.always();
      policy.notes.allowRead.always();
      if (mode !== "deny-data") policy.notes.allowInsert.always();
      if (mode.startsWith("hidden-existing"))
        policy.__e2ee_spaces.allowRead.where({ accountId: session.user.account });
      else policy.__e2ee_spaces.allowRead.always();
      if (mode === "hidden-existing-updatable") policy.__e2ee_spaces.allowUpdate.always();
      if (mode !== "deny-root")
        policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      if (mode !== "deny-grant")
        policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let creator: Awaited<ReturnType<typeof createDb>> | undefined;
    let writer: Awaited<ReturnType<typeof createDb>> | undefined;
    let outsider: Awaited<ReturnType<typeof createDb>> | undefined;
    let saved: string | null = null;
    const store = {
      async read() {
        return saved;
      },
      async update(transform: (current: string | null) => string) {
        saved = transform(saved);
      },
    };
    try {
      const target = {
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
      };
      await deploy({ ...target, schema: oldApp, permissions: oldPermissions });
      creator = await createDb(await localAccountConfig(server.appId, server.url));
      const project = await creator
        .insert(oldApp.projects, { title: "Legacy project" })
        .wait({ tier: "global" });
      await creator.shutdown();
      creator = undefined;
      await deploy({ ...target, schema: app, permissions, migration });
      const account = await localAccountConfig(server.appId, server.url);
      let failWrapping = false;
      const keys = mode === "crypto-error" ? await createBrowserKeyEnvelope() : undefined;
      const keyEnvelope: KeyEnvelope | undefined = keys && {
        ...keys,
        async wrap(secret, context, plaintext) {
          if (failWrapping) throw new Error("adapter-private-material");
          return keys.wrap(secret, context, plaintext);
        },
      };
      writer = await createDb({ ...account, e2ee: { app, store, crypto: { keyEnvelope } } });
      if (mode === "crypto-error") {
        await writer.e2ee.devices.list();
        failWrapping = true;
      }
      const data = { projectId: project.id, body: "Original value" };
      let noteId: string;
      let completed: Promise<unknown>;
      if (mode === "exclusive" || mode === "mergeable") {
        const tx =
          mode === "exclusive" ? writer.beginExclusiveTransaction() : writer.beginTransaction();
        tx.insert(app.events, { message: "Same transaction" });
        noteId = tx.insert(app.notes, data).id;
        completed = tx.commit().wait({ tier: "local" });
      } else if (mode === "upsert") {
        noteId = crypto.randomUUID();
        completed = writer.upsert(app.notes, noteId, data).wait({ tier: "local" });
      } else {
        const write = writer.insert(app.notes, data);
        noteId = write.value.id;
        completed = write.wait({ tier: "local" });
      }
      // A deferred retry must retain the original values and provisional ID.
      data.body = "Changed after the mutation returned";
      if (mode === "mergeable" || mode.startsWith("deny-") || mode === "crypto-error") {
        if (mode === "mergeable")
          await expect(completed).rejects.toThrow("beginExclusiveTransaction()");
        else if (mode === "crypto-error")
          await expect(completed).rejects.toMatchObject({
            code: "key-unavailable",
            message: "The encryption key is currently unavailable",
          });
        else await expect(completed).rejects.toMatchObject({ code: "permission_denied" });
        expect(await writer.all(app.notes.select("id"), { tier: "global" })).toEqual([]);
        expect(await writer.all(app.events, { tier: "global" })).toEqual([]);
        expect(await writer.all(app.__e2ee_spaces, { tier: "global" })).toEqual([]);
        expect(await writer.all(app.__e2ee_space_grants, { tier: "global" })).toEqual([]);
        expect(await writer.all(app.__e2ee_space_deliveries, { tier: "global" })).toEqual([]);
        return;
      }
      await completed;
      const expected = { id: noteId, projectId: project.id, body: "Original value" };
      expect(await writer.one(app.notes.where({ id: noteId }), { tier: "global" })).toEqual(
        expected,
      );
      expect(await writer.all(app.events, { tier: "global" })).toHaveLength(
        mode === "exclusive" ? 1 : 0,
      );
      expect(await writer.all(app.__e2ee_spaces, { tier: "global" })).toHaveLength(1);
      if (mode.startsWith("hidden-existing")) {
        let outsiderSaved: string | null = null;
        outsider = await createDb({
          ...(await localAccountConfig(server.appId, server.url)),
          e2ee: {
            app,
            store: {
              async read() {
                return outsiderSaved;
              },
              async update(transform) {
                outsiderSaved = transform(outsiderSaved);
              },
            },
          },
        });
        await outsider.e2ee.devices.list();
        expect(await outsider.all(app.__e2ee_spaces, { tier: "global" })).toEqual([]);
        await expect(
          outsider
            .insert(app.notes, { projectId: project.id, body: "Must not create another space" })
            .wait({ tier: "global" }),
        ).rejects.toThrow();
        expect(await outsider.all(app.__e2ee_spaces, { tier: "global" })).toEqual([]);
        expect(await writer.all(app.__e2ee_spaces, { tier: "global" })).toHaveLength(1);
        expect(await writer.all(app.notes.select("id"), { tier: "global" })).toEqual([
          { id: noteId },
        ]);
        expect(await writer.one(app.notes.where({ id: noteId }), { tier: "global" })).toEqual(
          expected,
        );
        return;
      }
      // Once initialised, an explicit mergeable transaction must still work.
      const existing = writer.beginTransaction();
      const second = existing.insert(app.notes, { projectId: project.id, body: "Existing space" });
      await existing.commit().wait({ tier: "global" });
      expect(await writer.one(app.notes.where({ id: second.id }), { tier: "global" })).toEqual(
        second,
      );
      expect(await writer.all(app.__e2ee_spaces, { tier: "global" })).toHaveLength(1);
      if (mode === "insert") {
        await writer.shutdown();
        writer = await createDb({ ...account, e2ee: { app, store } });
        expect(await writer.one(app.notes.where({ id: noteId }), { tier: "global" })).toEqual(
          expected,
        );
      }
    } finally {
      await outsider?.shutdown();
      await writer?.shutdown();
      await creator?.shutdown();
      await server.stop();
    }
  },
  60_000,
);

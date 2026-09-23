import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

async function createFixture() {
  const app = s.defineApp({
    events: s.table({ message: s.string() }, {}),
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), body: s.string(), bytes: s.bytes() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["body", "bytes"] })
      .indexOnly(["projectId"]),
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.events.allowRead.always();
    policy.events.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.projects.allowRead.always();
    policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.notes.allowRead.always();
    policy.notes.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.notes.allowUpdate.where({ "$createdBy.account": session.user.account });
    policy.notes.allowDelete.where({ "$createdBy.account": session.user.account });
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  const additionalClients: Awaited<ReturnType<typeof createDb>>[] = [];
  let retained: string | null = null;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    db = await createDb({
      ...account,
      e2ee: {
        app,
        store: {
          async read() {
            return retained;
          },
          async update(transform: (current: string | null) => string) {
            retained = transform(retained);
          },
        },
      },
    });
    return {
      app,
      db,
      account,
      async openOtherAccount() {
        const other = await localAccountConfig(server.appId, server.url);
        let saved: string | null = null;
        const client = await createDb({
          ...other,
          e2ee: {
            app,
            store: {
              async read() {
                return saved;
              },
              async update(transform: (current: string | null) => string) {
                saved = transform(saved);
              },
            },
          },
        });
        additionalClients.push(client);
        return { client, account: other };
      },
      async close() {
        await Promise.all([
          db?.shutdown(),
          ...additionalClients.map((client) => client.shutdown()),
        ]);
        await server.stop();
      },
    };
  } catch (error) {
    await Promise.all([db?.shutdown(), ...additionalClients.map((client) => client.shutdown())]);
    await server.stop();
    throw error;
  }
}

it("keeps encrypted row scope stable across pending deletes and upserts", async () => {
  const fixture = await createFixture();
  const { app, db } = fixture;
  try {
    const initial = db.beginExclusiveTransaction();
    const project = initial.insert(app.projects, { title: "First scope" });
    const other = initial.insert(app.projects, { title: "Second scope" });
    const note = initial.insert(app.notes, {
      projectId: project.id,
      body: "Original content",
      bytes: new Uint8Array([1, 2]),
    });
    await initial.commit().wait({ tier: "global" });

    const crossScope = db.beginTransaction();
    crossScope.delete(app.notes, note.id);
    crossScope.upsert(app.notes, note.id, {
      projectId: other.id,
      body: "Changed scope",
      bytes: new Uint8Array([3, 4]),
    });
    await expect(crossScope.commit().wait({ tier: "global" })).rejects.toThrow();
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual(note);
    expect(
      await db.one(app.notes.includeDeleted().where({ id: note.id }), { tier: "global" }),
    ).toEqual(note);

    const sameScope = db.beginTransaction();
    sameScope.delete(app.notes, note.id);
    sameScope.upsert(app.notes, note.id, {
      projectId: project.id,
      body: "Updated while deleted",
      bytes: new Uint8Array([5, 6]),
    });
    await sameScope.commit().wait({ tier: "global" });
    expect(
      await db.one(app.notes.includeDeleted().where({ id: note.id }), { tier: "global" }),
    ).toMatchObject({
      id: note.id,
      projectId: project.id,
      body: "Updated while deleted",
    });

    const missingId = crypto.randomUUID();
    const missingUpdatedAt = Date.UTC(2025, 0, 1);
    const missing = db.beginTransaction();
    missing.upsert(
      app.notes,
      missingId,
      {
        projectId: project.id,
        body: "Created by missing-ID upsert",
        bytes: new Uint8Array([7, 8]),
      },
      { updatedAt: missingUpdatedAt },
    );
    await missing.commit().wait({ tier: "global" });
    expect(await db.one(app.notes.where({ id: missingId }), { tier: "global" })).toEqual({
      id: missingId,
      projectId: project.id,
      body: "Created by missing-ID upsert",
      bytes: new Uint8Array([7, 8]),
    });
    expect(
      await db.one(app.notes.where({ id: missingId }).select("$updatedAt"), { tier: "global" }),
    ).toEqual({ id: missingId, $updatedAt: new Date(missingUpdatedAt) });
  } finally {
    await fixture.close();
  }
}, 120_000);

it("commits an encrypted write when an exclusive transaction resumes after yielding", async () => {
  const fixture = await createFixture();
  const { app, db } = fixture;
  try {
    const tx = db.beginExclusiveTransaction();
    await Promise.resolve();
    expect(await tx.all(app.events, { tier: "local" })).toEqual([]);
    const project = tx.insert(app.projects, { title: "Resumed scope" });
    const note = tx.insert(app.notes, {
      projectId: project.id,
      body: "Prepared after yielding",
      bytes: new Uint8Array([9]),
    });
    await tx.commit().wait({ tier: "global" });
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual(note);
  } finally {
    await fixture.close();
  }
}, 120_000);

it("anchors encrypted application snapshots at public begin rather than the first operation", async () => {
  const fixture = await createFixture();
  const { app, db } = fixture;
  try {
    const before = db.insert(app.events, { message: "Before begin" }).value;
    const tx = db.beginExclusiveTransaction();
    try {
      db.insert(app.events, { message: "After begin" });
      expect(await tx.all(app.events, { tier: "local" })).toEqual([before]);
    } finally {
      await tx.rollback();
    }
  } finally {
    await fixture.close();
  }
}, 120_000);

it("keeps encrypted bytes unchanged when insert input and preview buffers are mutated", async () => {
  const fixture = await createFixture();
  const { app, db } = fixture;
  try {
    const projectTx = db.beginExclusiveTransaction();
    const project = projectTx.insert(app.projects, { title: "Byte scope" });
    await projectTx.commit().wait({ tier: "global" });

    const inputBytes = new Uint8Array([11, 22]);
    const write = db.insert(app.notes, {
      projectId: project.id,
      body: "Db insert snapshot",
      bytes: inputBytes,
    });
    inputBytes[0] = 91;
    write.value.bytes[1] = 92;
    await write.wait({ tier: "global" });
    expect(
      (await db.one(app.notes.where({ id: write.value.id }), { tier: "global" }))?.bytes,
    ).toEqual(new Uint8Array([11, 22]));

    const tx = db.beginExclusiveTransaction();
    const txInputBytes = new Uint8Array([33, 44]);
    const preview = tx.insert(app.notes, {
      projectId: project.id,
      body: "Transaction insert snapshot",
      bytes: txInputBytes,
    });
    txInputBytes[0] = 93;
    preview.bytes[1] = 94;
    await tx.commit().wait({ tier: "global" });
    expect((await db.one(app.notes.where({ id: preview.id }), { tier: "global" }))?.bytes).toEqual(
      new Uint8Array([33, 44]),
    );
  } finally {
    await fixture.close();
  }
}, 120_000);

it("uses the public begin snapshot for cold and late initial recipients", async () => {
  const fixture = await createFixture();
  const { app, db } = fixture;
  try {
    const cold = await fixture.openOtherAccount();
    const coldTx = db.beginExclusiveTransaction();
    const coldProject = coldTx.insert(
      app.projects,
      { title: "Cold recipient scope" },
      { initialRecipients: [cold.account.account.id] },
    );
    const coldNote = coldTx.insert(app.notes, {
      projectId: coldProject.id,
      body: "Available at begin",
      bytes: new Uint8Array([1]),
    });
    await coldTx.commit().wait({ tier: "global" });
    expect(await cold.client.one(app.notes.where({ id: coldNote.id }), { tier: "global" })).toEqual(
      coldNote,
    );

    const lateTx = db.beginExclusiveTransaction();
    expect(
      await lateTx.one(app.projects.where({ id: crypto.randomUUID() }), { tier: "global" }),
    ).toBeNull();
    const late = await fixture.openOtherAccount();
    const lateProject = lateTx.insert(
      app.projects,
      { title: "Late recipient scope" },
      { initialRecipients: [late.account.account.id] },
    );
    lateTx.insert(app.notes, {
      projectId: lateProject.id,
      body: "Unavailable after begin",
      bytes: new Uint8Array([2]),
    });
    await expect(lateTx.commit().wait({ tier: "global" })).rejects.toThrow(
      "E2EE initial recipient account is unavailable",
    );
    expect(await db.one(app.projects.where({ id: lateProject.id }), { tier: "global" })).toBeNull();
  } finally {
    await fixture.close();
  }
}, 120_000);

it("reads plaintext-only projections in exclusive transactions without the local key store", async () => {
  const fixture = await createFixture();
  const { app, db, account } = fixture;
  let observer: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    const initial = db.beginExclusiveTransaction();
    const project = initial.insert(app.projects, { title: "Plaintext scope" });
    const note = initial.insert(app.notes, {
      projectId: project.id,
      body: "Encrypted body",
      bytes: new Uint8Array([55]),
    });
    await initial.commit().wait({ tier: "global" });
    const storeError = new Error("Local key store unavailable");
    let reads = 0;
    let updates = 0;
    let unavailable = false;
    let retained: string | null = null;
    observer = await createDb({
      ...account,
      e2ee: {
        app,
        store: {
          async read() {
            reads += 1;
            if (unavailable) throw storeError;
            return retained;
          },
          async update(transform: (current: string | null) => string) {
            updates += 1;
            if (unavailable) throw storeError;
            retained = transform(retained);
          },
        },
      },
    });
    const startupStoreAccesses = { reads, updates };
    unavailable = true;

    const plain = app.notes.where({ id: note.id }).select("id", "projectId");
    expect(await observer.one(plain, { tier: "global" })).toEqual({
      id: note.id,
      projectId: project.id,
    });
    expect({ reads, updates }).toEqual(startupStoreAccesses);

    const exclusive = observer.beginExclusiveTransaction();
    try {
      expect(await exclusive.one(plain, { tier: "global" })).toEqual({
        id: note.id,
        projectId: project.id,
      });
      expect({ reads, updates }).toEqual(startupStoreAccesses);
    } finally {
      await exclusive.rollback();
    }
  } finally {
    await observer?.shutdown();
    await fixture.close();
  }
}, 120_000);

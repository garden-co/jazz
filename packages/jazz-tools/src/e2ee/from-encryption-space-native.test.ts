import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { TypedTableQueryBuilder } from "../typed-app.js";
import { permissionSpaceColumn } from "./current-membership-names.js";
import { prefetchPublicMembershipHistory } from "./public-membership.js";
import { e2eeAccountRelation } from "./account-relation.js";

async function fixture(
  denyMemberUpdates = false,
  spaceHelpers = false,
  ownNotesOnly = false,
  groupHelpers: boolean | "create-only" | "admin-only" | "delegated" = false,
) {
  const app = s.defineApp({
    projects: s.table(
      { ownerId: s.uuid(), title: s.string() },
      { owner: e2eeAccountRelation("ownerId") },
    ),
    projectOwners: s.table(
      { projectId: s.uuid(), accountId: s.uuid() },
      {
        project: s.rel("projects", "projectId"),
        account: e2eeAccountRelation(),
      },
    ),
    groupOwners: s.table(
      { groupId: s.uuid(), accountId: s.uuid() },
      { group: s.rel("__e2ee_groups", "groupId"), account: e2eeAccountRelation() },
    ),
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] }),
    drafts: s
      .table(
        { projectId: s.uuid(), body: s.string(), payload: s.bytes().optional() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["body", "payload"] }),
    files: s
      .table(
        { projectId: s.uuid(), payload: s.bytes() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["payload"] }),
  });
  const permissions = definePermissions(app, ({ policy: p, session, allOf, anyOf, e2ee }) => {
    p.projects.allowRead.always();
    p.projects.allowInsert.where({
      ownerId: session.user.account,
      "$createdBy.account": session.user.account,
    });
    p.notes.fromEncryptionSpace(
      ownNotesOnly ? { write: { "$createdBy.account": session.user.account } } : undefined,
    );
    p.files.fromEncryptionSpace();
    p.drafts.allowRead.always();
    p.drafts.allowInsert.always();
    p.drafts.allowUpdate.always();
    p.projectOwners.allowRead.always();
    p.projectOwners.allowInsert.where((binding) =>
      allOf([
        { accountId: session.user.account, "$createdBy.account": session.user.account },
        p.projects.existsIncludingCreated.where({
          id: binding.projectId,
          ownerId: binding.accountId,
        }),
      ]),
    );
    if (spaceHelpers) {
      p.notes.space.allowRead.always();
      p.notes.space.allowInsert.where((space) =>
        p.projectOwners.existsIncludingCreated.where({
          accountId: space.creatorAccountId,
          projectId: space.identifier,
        }),
      );
      p.notes.space.allowUpdate.where((space) =>
        p.projectOwners.existsIncludingCreated.where({
          accountId: session.user.account,
          projectId: space.identifier,
        }),
      );
    } else {
      p.__e2ee_spaces.allowRead.always();
      p.__e2ee_spaces.allowInsert.where((space) =>
        allOf([
          { creatorAccountId: session.user.account, "$createdBy.account": session.user.account },
          p.projectOwners.existsIncludingCreated.where({
            accountId: space.creatorAccountId,
            projectId: space.identifier,
          }),
        ]),
      );
      p.__e2ee_space_grants.allowRead.always();
      p.__e2ee_space_grants.allowInsert.where((grant) =>
        allOf([
          { authorAccountId: session.user.account },
          p.__e2ee_spaces.existsIncludingCreated.where({
            id: grant.spaceId,
            creatorAccountId: session.user.account,
          }),
        ]),
      );
      p.__e2ee_space_successors.allowRead.always();
      p.__e2ee_space_successors.allowInsert.where((row) =>
        p.__e2ee_spaces.existsIncludingCreated.where({
          id: row.spaceId,
          creatorAccountId: session.user.account,
        }),
      );
      p.__e2ee_space_deliveries.allowRead.always();
      p.__e2ee_space_deliveries.allowInsert.where((row) =>
        p.__e2ee_spaces.existsIncludingCreated.where({
          id: row.spaceId,
          creatorAccountId: session.user.account,
        }),
      );
    }
    if (groupHelpers) {
      if (groupHelpers !== "admin-only") e2ee.groups.allowInsert.always();
      if (groupHelpers === "delegated") {
        p.groupOwners.allowRead.always();
        p.groupOwners.allowInsert.where((binding) =>
          p.__e2ee_groups.exists.where({
            id: binding.groupId,
            accountId: session.user.account,
          }),
        );
        p.groupOwners.allowDelete.where((binding) =>
          p.__e2ee_groups.exists.where({
            id: binding.groupId,
            accountId: session.user.account,
          }),
        );
        e2ee.groups.allowUpdate.where((group) =>
          anyOf([
            { creatorAccountId: session.user.account },
            p.groupOwners.existsIncludingCreated.where({
              groupId: group.id,
              accountId: session.user.account,
            }),
          ]),
        );
      } else if (groupHelpers !== "create-only") {
        e2ee.groups.allowUpdate.where({ creatorAccountId: session.user.account });
      }
    } else {
      p.__e2ee_groups.allowInsert.where({
        accountId: session.user.account,
        "$createdBy.account": session.user.account,
      });
      p.__e2ee_group_membership.allowInsert.where((row) =>
        p.__e2ee_groups.exists.where({ id: row.groupId, accountId: session.user.account }),
      );
      p.__e2ee_group_successors.allowInsert.where((row) =>
        p.__e2ee_groups.exists.where({ id: row.groupId, accountId: session.user.account }),
      );
      p.__e2ee_group_deliveries.allowRead.always();
      p.__e2ee_group_deliveries.allowInsert.where((row) =>
        p.__e2ee_groups.exists.where({ id: row.groupId, accountId: session.user.account }),
      );
      p.__e2ee_group_repairs.allowRead.always();
      p.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
    }
    if (!spaceHelpers) {
      p.__e2ee_v1_space_members_projects.allowInsert.where((member) =>
        p.projects.existsIncludingCreated.where({
          id: member.scopeId,
          ownerId: session.user.account,
        }),
      );
      if (!denyMemberUpdates)
        p.__e2ee_v1_space_members_projects.allowUpdate.where((member) =>
          p.projects.exists.where({ id: member.scopeId, ownerId: session.user.account }),
        );
    }
    if (!groupHelpers) {
      p.__e2ee_v1_group_members.allowInsert.where((member) =>
        p.__e2ee_groups.existsIncludingCreated.where({
          id: member.groupId,
          accountId: session.user.account,
        }),
      );
      p.__e2ee_v1_group_members.allowUpdate.where((member) =>
        p.__e2ee_groups.exists.where({ id: member.groupId, accountId: session.user.account }),
      );
    }
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const close = async () => {
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  };
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const open = async () => {
      const account = await localAccountConfig(server.appId, server.url);
      let saved: string | null = null;
      const db = await createDb({
        ...account,
        driver: { type: "memory" },
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
        },
      });
      clients.push(db);
      await db.e2ee.devices.list();
      await expect
        .poll(
          async () =>
            (
              await db.all(app.__e2ee_account_roots.where({ accountId: account.account.id }), {
                tier: "remote",
              })
            ).length,
          { timeout: 10_000 },
        )
        .toBe(1);
      return { db, id: account.account.id };
    };
    const owner = await open();
    const reader = await open();
    const createProject = async (title: string, initialRecipients?: readonly string[]) => {
      for (const id of initialRecipients ?? [])
        await prefetchPublicMembershipHistory(owner.db, id, app);
      const tx = owner.db.beginSerializableTransaction();
      const project = tx.insert(app.projects, { ownerId: owner.id, title }, { initialRecipients });
      tx.insert(app.projectOwners, { projectId: project.id, accountId: owner.id });
      await tx.commit().wait({ tier: "global" });
      return project;
    };
    return { app, owner, reader, createProject, close };
  } catch (error) {
    await close();
    throw error;
  }
}

it("restricts member writes to their own notes and retains membership checks after revocation", async () => {
  const { app, owner, reader, createProject, close } = await fixture(false, false, true);
  try {
    const project = await createProject("Shared project");
    const aliceNote = await owner.db
      .insert(app.notes, {
        projectId: project.id,
        body: "Alice's note",
      })
      .wait({ tier: "global" });
    await owner.db.e2ee.spaces.grant(app.projects, project.id, reader.id).wait();
    expect(
      await reader.db.one(app.notes.where({ id: aliceNote.id }), { tier: "remote" }),
    ).toMatchObject({ body: "Alice's note" });
    await expect(
      reader.db.update(app.notes, aliceNote.id, { body: "Bob's edit" }).wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      reader.db.delete(app.notes, aliceNote.id).wait({ tier: "global" }),
    ).rejects.toThrow();
    const bobNote = await reader.db
      .insert(app.notes, {
        projectId: project.id,
        body: "Bob's note",
      })
      .wait({ tier: "global" });
    await reader.db.update(app.notes, bobNote.id, { body: "Bob's edit" }).wait({ tier: "global" });
    await owner.db.e2ee.spaces.revoke(app.projects, project.id, reader.id).wait();
    // Ownership alone must not allow a write after space membership is removed.
    // Physical access exercises the deployed policy independently of SDK key checks.
    const physicalNotes = new TypedTableQueryBuilder("notes", { ...app.wasmSchema });
    await expect(
      reader.db
        .update(physicalNotes, bobNote.id, { body: new Uint8Array([1]) })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      reader.db.delete(physicalNotes, bobNote.id).wait({ tier: "global" }),
    ).rejects.toThrow();
    await owner.db.e2ee.spaces.grant(app.projects, project.id, reader.id).wait();
    await reader.db.delete(app.notes, bobNote.id).wait({ tier: "global" });
  } finally {
    await close();
  }
}, 120_000);

it("derives CRUD from direct and nested membership, retracts live rows and rejects mismatched scopes", async () => {
  const { app, owner, reader, createProject, close } = await fixture();
  let stop: (() => void) | undefined;
  try {
    const project = await createProject("Shared project");
    const other = await createProject("Other project");
    const note = await owner.db
      .insert(app.notes, { projectId: project.id, body: "Private" })
      .wait({ tier: "global" });
    expect(await reader.db.all(app.notes.select("id"), { tier: "remote" })).toEqual([]);
    // A physical write proves ordinary server policies reject non-members even
    // when the caller bypasses SDK encryption preparation.
    const physicalNotes = new TypedTableQueryBuilder("notes", { ...app.wasmSchema });
    await expect(
      reader.db
        .insert(physicalNotes, {
          projectId: project.id,
          [permissionSpaceColumn]: project.id,
          body: new Uint8Array([1]),
        })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await owner.db.e2ee.spaces.grant(app.projects, project.id, reader.id).wait();
    expect(await reader.db.one(app.notes.where({ id: note.id }), { tier: "remote" })).toMatchObject(
      { body: "Private" },
    );
    const created = await reader.db
      .insert(app.notes, { projectId: project.id, body: "Reader write" })
      .wait({ tier: "global" });
    await reader.db.update(app.notes, created.id, { body: "Updated" }).wait({ tier: "global" });
    expect(
      await reader.db.one(app.notes.where({ id: created.id }), { tier: "remote" }),
    ).toMatchObject({ body: "Updated" });
    await reader.db.delete(app.notes, created.id).wait({ tier: "global" });
    await expect(
      owner.db
        .insert(app.__e2ee_v1_space_members_projects, {
          scopeId: project.id,
          accessId: other.id,
          accountId: reader.id,
        })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      owner.db
        .insert(physicalNotes, {
          projectId: project.id,
          [permissionSpaceColumn]: other.id,
          body: new Uint8Array([1]),
        })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    const snapshots: { id: string }[][] = [];
    let failure: Error | undefined;
    stop = reader.db.subscribe(
      app.notes.where({ projectId: project.id }).select("id"),
      {
        onUpdate: (rows) => {
          snapshots.push(rows);
        },
        onError: (error) => {
          failure = error;
        },
      },
      { tier: "remote" },
    );
    const visible = async (rows: { id: string }[]) =>
      expect.poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 }).toEqual(rows);
    await visible([{ id: note.id }]);
    await owner.db.e2ee.spaces.revoke(app.projects, project.id, reader.id).wait();
    await visible([]);
    await owner.db.e2ee.spaces.grant(app.projects, project.id, reader.id).wait();
    await visible([{ id: note.id }]);
    await owner.db.e2ee.spaces.revoke(app.projects, project.id, reader.id).wait();
    await visible([]);
    const parent = await owner.db.e2ee.groups.create().wait();
    const child = await owner.db.e2ee.groups.create().wait();
    await expect(owner.db.e2ee.groups.add(parent.id, crypto.randomUUID()).wait()).rejects.toThrow(
      "E2EE group recipient not found",
    );
    await expect(
      owner.db.e2ee.groups.remove(parent.id, crypto.randomUUID()).wait(),
    ).rejects.toThrow("E2EE group recipient not found");
    await expect(owner.db.e2ee.groups.add(parent.id, "invalid").wait()).rejects.toThrow(
      "Invalid E2EE group member selector",
    );
    await owner.db.e2ee.groups.add(parent.id, child.id).wait();
    await owner.db.e2ee.groups.add(child.id, reader.id).wait();
    await owner.db.e2ee.spaces.grant(app.projects, project.id, parent.id).wait();
    await visible([{ id: note.id }]);
    const nested = await reader.db
      .insert(app.notes, { projectId: project.id, body: "Nested write" })
      .wait({ tier: "global" });
    await reader.db
      .update(app.notes, nested.id, { body: "Nested update" })
      .wait({ tier: "global" });
    expect(
      await reader.db.one(app.notes.where({ id: nested.id }), { tier: "remote" }),
    ).toMatchObject({ body: "Nested update" });
    await reader.db.delete(app.notes, nested.id).wait({ tier: "global" });
    await visible([{ id: note.id }]);
    await owner.db.e2ee.groups.remove(parent.id, child.id).wait();
    await visible([]);
    await owner.db.e2ee.groups.add(parent.id, child.id).wait();
    await visible([{ id: note.id }]);
    await owner.db.e2ee.groups.remove(child.id, reader.id).wait();
    await visible([]);
  } finally {
    stop?.();
    await close();
  }
}, 120_000);

it("fails missing and unaccepted space writes and atomically rejects denied membership changes", async () => {
  const { app, owner, reader, createProject, close } = await fixture(true);
  try {
    await expect(
      owner.db
        .insert(app.notes, { projectId: crypto.randomUUID(), body: "Missing" })
        .wait({ tier: "global" }),
    ).rejects.toMatchObject({ code: "space-not-initialised" });
    for (const upsert of [false, true]) {
      const tx = owner.db.beginSerializableTransaction();
      const project = tx.insert(app.projects, { ownerId: owner.id, title: "Unaccepted" });
      tx.insert(app.projectOwners, { projectId: project.id, accountId: owner.id });
      if (upsert)
        tx.upsert(app.notes, crypto.randomUUID(), { projectId: project.id, body: "Unaccepted" });
      else tx.insert(app.notes, { projectId: project.id, body: "Unaccepted" });
      await expect(tx.commit().wait({ tier: "global" })).rejects.toMatchObject({
        code: "space-not-initialised",
      });
      expect(
        await owner.db.all(app.projects.where({ id: project.id }), { tier: "remote" }),
      ).toEqual([]);
    }
    const project = await createProject("Accepted");
    const note = await owner.db
      .insert(app.notes, { projectId: project.id, body: "Still shared" })
      .wait({ tier: "global" });
    await owner.db.e2ee.spaces.grant(app.projects, project.id, reader.id).wait();
    const before = await owner.db.all(app.__e2ee_space_grants, { tier: "remote" });
    await expect(
      owner.db.e2ee.spaces.revoke(app.projects, project.id, reader.id).wait(),
    ).rejects.toThrow();
    expect(await owner.db.all(app.__e2ee_space_grants, { tier: "remote" })).toEqual(before);
    expect(await reader.db.one(app.notes.where({ id: note.id }), { tier: "remote" })).toMatchObject(
      { body: "Still shared" },
    );
  } finally {
    await close();
  }
}, 120_000);

it("preserves atomic first writes and streams for explicit-policy siblings in the same scope", async () => {
  const { app, owner, close } = await fixture();
  try {
    const tx = owner.db.beginSerializableTransaction();
    const project = tx.insert(app.projects, { ownerId: owner.id, title: "Atomic explicit policy" });
    tx.insert(app.projectOwners, { projectId: project.id, accountId: owner.id });
    tx.insert(app.drafts, { projectId: project.id, body: "First" });
    tx.insert(app.drafts, { projectId: project.id, body: "Second" });
    tx.upsert(app.drafts, crypto.randomUUID(), { projectId: project.id, body: "Third" });
    await tx.commit().wait({ tier: "global" });
    expect(
      (await owner.db.all(app.drafts.where({ projectId: project.id }), { tier: "remote" }))
        .map((row) => row.body)
        .sort(),
    ).toEqual(["First", "Second", "Third"]);
    await owner.db
      .insert(app.notes, { projectId: project.id, body: "Now accepted" })
      .wait({ tier: "global" });
    const upload = await owner.db.streamingTransaction((plan) => {
      const streamed = plan.insert(app.projects, {
        ownerId: owner.id,
        title: "Streaming explicit policy",
      });
      plan.insert(app.projectOwners, { projectId: streamed.id, accountId: owner.id });
      return plan.insertStreaming(app.drafts, {
        projectId: streamed.id,
        body: "Streamed",
        payload: bytes(),
      });
    });
    const row = await upload.wait({ tier: "global" });
    expect(await owner.db.one(app.drafts.where({ id: row.id }), { tier: "remote" })).toMatchObject({
      body: "Streamed",
      payload: new Uint8Array([1, 2, 3]),
    });
  } finally {
    await close();
  }
}, 120_000);

it("uses exact initial recipients and rejects missing spaces before consuming a stream", async () => {
  const { app, owner, reader, createProject, close } = await fixture();
  try {
    const project = await createProject("Creator excluded", [reader.id]);
    const upload = await reader.db.streamingTransaction((plan) =>
      plan.insertStreaming(app.files, { projectId: project.id, payload: bytes() }),
    );
    const row = await upload.wait({ tier: "global" });
    expect(await reader.db.one(app.files.where({ id: row.id }), { tier: "remote" })).toMatchObject({
      payload: new Uint8Array([1, 2, 3]),
    });
    expect(await owner.db.all(app.files.select("id"), { tier: "remote" })).toEqual([]);
    let consumed = false;
    await expect(
      reader.db.streamingTransaction((plan) =>
        plan.insertStreaming(app.files, {
          projectId: crypto.randomUUID(),
          payload: (async function* () {
            consumed = true;
            yield new Uint8Array([1]);
          })(),
        }),
      ),
    ).rejects.toMatchObject({ code: "space-not-initialised" });
    expect(consumed).toBe(false);
  } finally {
    await close();
  }
}, 120_000);

async function* bytes() {
  yield new Uint8Array([1, 2, 3]);
}

it.each([true, "delegated"] as const)(
  "enforces current group administration independently of space creation (%s)",
  async (mode) => {
    const { app, owner, reader, close } = await fixture(false, true, false, mode);
    try {
      const group = await owner.db.e2ee.groups.create().wait();
      await expect(reader.db.e2ee.groups.add(group.id, reader.id).wait()).rejects.toThrow();
      await owner.db.e2ee.groups.add(group.id, reader.id).wait();
      expect(
        await reader.db.all(
          app.__e2ee_v1_group_members.where({ groupId: group.id, accountId: reader.id }),
          { tier: "remote" },
        ),
      ).toHaveLength(1);
      await expect(reader.db.e2ee.groups.remove(group.id, owner.id).wait()).rejects.toThrow();
      if (mode === "delegated") {
        await expect(
          reader.db
            .insert(app.groupOwners, { groupId: group.id, accountId: reader.id })
            .wait({ tier: "global" }),
        ).rejects.toThrow();
        const delegation = await owner.db
          .insert(app.groupOwners, { groupId: group.id, accountId: reader.id })
          .wait({ tier: "global" });
        await reader.db.e2ee.groups.remove(group.id, owner.id).wait();
        await reader.db.e2ee.groups.add(group.id, owner.id).wait();
        await owner.db.delete(app.groupOwners, delegation.id).wait({ tier: "global" });
        await expect(reader.db.e2ee.groups.remove(group.id, owner.id).wait()).rejects.toThrow();
      }
      await owner.db.e2ee.groups.remove(group.id, reader.id).wait();
      expect(
        await reader.db.all(
          app.__e2ee_v1_group_members.where({ groupId: group.id, accountId: reader.id }),
          { tier: "remote" },
        ),
      ).toEqual([]);
    } finally {
      await close();
    }
  },
  120_000,
);

it.each(["create-only", "admin-only"] as const)(
  "refuses group creation when only %s is configured",
  async (mode) => {
    const { app, owner, close } = await fixture(false, true, false, mode);
    try {
      const group = owner.db.e2ee.groups.create();
      await expect(group.wait()).rejects.toThrow();
      expect(
        await owner.db.all(app.__e2ee_v1_group_members.where({ groupId: group.id }), {
          tier: "remote",
        }),
      ).toEqual([]);
    } finally {
      await close();
    }
  },
  120_000,
);

it("applies and revokes application-defined group administration without granting it through key possession", async () => {
  const { app, owner, reader, createProject, close } = await fixture(
    false,
    true,
    false,
    "delegated",
  );
  try {
    const project = await createProject("Delegated group administration");
    const note = await owner.db
      .insert(app.notes, { projectId: project.id, body: "Shared with the group" })
      .wait({ tier: "global" });
    const group = await owner.db.e2ee.groups.create().wait();
    await owner.db.e2ee.groups.add(group.id, reader.id).wait();
    await owner.db.e2ee.spaces.grant(app.projects, project.id, group.id).wait();
    // This case exercises group-only access; the founder's direct grant is independent.
    await owner.db.e2ee.spaces.revoke(app.projects, project.id, owner.id).wait();
    await expect(reader.db.e2ee.groups.remove(group.id, owner.id).wait()).rejects.toThrow();
    // Group members cannot appoint themselves as administrators.
    await expect(
      reader.db
        .insert(app.groupOwners, { groupId: group.id, accountId: reader.id })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    const delegation = await owner.db
      .insert(app.groupOwners, { groupId: group.id, accountId: reader.id })
      .wait({ tier: "global" });
    await reader.db.e2ee.groups.remove(group.id, owner.id).wait();
    expect(await owner.db.all(app.notes.where({ id: note.id }), { tier: "remote" })).toEqual([]);
    await reader.db.e2ee.groups.add(group.id, owner.id).wait();
    // Settle group key changes while the scope administrator is available.
    expect(await owner.db.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual({
      state: "ready",
    });
    expect(await owner.db.one(app.notes.where({ id: note.id }), { tier: "remote" })).toMatchObject({
      body: "Shared with the group",
    });
    await owner.db.delete(app.groupOwners, delegation.id).wait({ tier: "global" });
    // Removing administration preserves membership and its decryption key.
    expect(await reader.db.one(app.notes.where({ id: note.id }), { tier: "remote" })).toMatchObject(
      {
        body: "Shared with the group",
      },
    );
    await expect(reader.db.e2ee.groups.remove(group.id, owner.id).wait()).rejects.toThrow();
    await owner.db.e2ee.groups.remove(group.id, reader.id).wait();
    expect(await reader.db.all(app.notes.where({ id: note.id }), { tier: "remote" })).toEqual([]);
  } finally {
    await close();
  }
}, 120_000);

it("enforces owner-managed group administration across nested sharing, recovery and revocation", async () => {
  const { app, owner, reader, createProject, close } = await fixture(false, true, false, true);
  try {
    const project = await createProject("Group helper");
    const note = await owner.db
      .insert(app.notes, { projectId: project.id, body: "Private" })
      .wait({ tier: "global" });
    const parent = await owner.db.e2ee.groups.create().wait();
    const child = await owner.db.e2ee.groups.create().wait();
    await expect(reader.db.e2ee.groups.add(child.id, reader.id).wait()).rejects.toThrow();
    await owner.db.e2ee.groups.add(parent.id, child.id).wait();
    await owner.db.e2ee.groups.add(child.id, reader.id).wait();
    await owner.db.e2ee.spaces.grant(app.projects, project.id, parent.id).wait();
    expect(await reader.db.one(app.notes.where({ id: note.id }), { tier: "remote" })).toMatchObject(
      { body: "Private" },
    );
    // Possession of a group key must not grant administration authority.
    await expect(reader.db.e2ee.groups.remove(child.id, owner.id).wait()).rejects.toThrow();
    const root = await owner.db.one(app.__e2ee_groups.where({ id: child.id }), { tier: "remote" });
    await expect(
      owner.db
        .update(app.__e2ee_groups, child.id, { epochId: crypto.randomUUID() })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    const { id: _id, ...values } = root!;
    await expect(
      reader.db.insert(app.__e2ee_groups, values).wait({ tier: "global" }),
    ).rejects.toThrow();
    const member = (
      await owner.db.all(
        app.__e2ee_v1_group_members.where({
          groupId: child.id,
          accountId: reader.id,
        }),
        { tier: "remote" },
      )
    )[0]!;
    // SQLite updates require a locally observed current row. Load the member
    // as the reader so this assertion reaches the authority's permission check.
    await reader.db.one(app.__e2ee_v1_group_members.where({ id: member.id }), { tier: "remote" });
    await expect(
      reader.db
        .update(app.__e2ee_v1_group_members, member.id, { accountId: owner.id })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    const readerGroup = await reader.db.e2ee.groups.create().wait();
    const readerMember = (
      await reader.db.all(app.__e2ee_v1_group_members.where({ groupId: readerGroup.id }), {
        tier: "remote",
      })
    )[0]!;
    // An administrator cannot move an edge into a group they do not administer.
    await expect(
      owner.db
        .update(app.__e2ee_v1_group_members, member.id, { groupId: readerGroup.id })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      reader.db
        .update(app.__e2ee_v1_group_members, readerMember.id, { groupId: child.id })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await owner.db.e2ee.recovery.create().wait();
    expect(
      (
        await owner.db.all(app.__e2ee_group_recovery_deliveries.where({ groupId: child.id }), {
          tier: "remote",
        })
      ).length,
    ).toBeGreaterThan(0);
    await owner.db.e2ee.groups.remove(child.id, reader.id).wait();
    expect(await reader.db.all(app.notes.where({ id: note.id }), { tier: "remote" })).toEqual([]);
  } finally {
    await close();
  }
}, 120_000);

it("authorises space administration through the scoped helper while keeping roots immutable", async () => {
  const { app, owner, reader, createProject, close } = await fixture(false, true);
  try {
    const project = await createProject("Helper administration");
    const note = await owner.db
      .insert(app.notes, { projectId: project.id, body: "Private" })
      .wait({ tier: "global" });
    await expect(
      reader.db.e2ee.spaces.grant(app.projects, project.id, reader.id).wait(),
    ).rejects.toThrow();
    await owner.db.e2ee.spaces.grant(app.projects, project.id, reader.id).wait();
    expect(await reader.db.one(app.notes.where({ id: note.id }), { tier: "remote" })).toMatchObject(
      { body: "Private" },
    );
    await expect(
      reader.db.e2ee.spaces.revoke(app.projects, project.id, owner.id).wait(),
    ).rejects.toThrow();
    const root = await owner.db.one(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "remote",
    });
    expect(root).not.toBeNull();
    await expect(
      owner.db
        .update(app.__e2ee_spaces, root!.id, { epochId: crypto.randomUUID() })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    const { id: _id, ...values } = root!;
    await expect(
      owner.db
        .insert(app.__e2ee_spaces, { ...values, scopeId: crypto.randomUUID() })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await owner.db.e2ee.spaces.revoke(app.projects, project.id, reader.id).wait();
    expect(await reader.db.all(app.notes.where({ id: note.id }), { tier: "remote" })).toEqual([]);
  } finally {
    await close();
  }
}, 120_000);

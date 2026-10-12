import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, groupSchema, spaceSchema } from "./managed-schema.js";
import {
  uploadApp,
  legacyUploadApp,
  uploadFixture,
  uploadBytes,
} from "./fixtures/encrypted-upload-fixture.js";

it("publishes an encrypted file and metadata that a granted recipient can read", async () => {
  const app = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    files: s
      .table(
        { projectId: s.uuid(), payload: s.bytes(), name: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["payload", "name"] }),
  });
  const physical = s.defineApp({
    ...deviceRequestSchema,
    ...groupSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
    files: s.table(
      { projectId: s.uuid(), payload: s.bytes(), name: s.bytes() },
      { project: s.rel("projects", "projectId") },
    ),
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
    // Public ciphertext reads deliberately prove encryption independently of RLS.
    policy.files.allowRead.always();
    policy.files.allowInsert.where({ "$createdBy.account": session.user.account });
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
  const clients: Db[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const accounts = await Promise.all([
      localAccountConfig(server.appId, server.url),
      localAccountConfig(server.appId, server.url),
      localAccountConfig(server.appId, server.url),
    ]);
    for (const [index, account] of accounts.entries()) {
      let retained: string | null = null;
      clients.push(
        await createDb({
          ...account,
          ...(index < 2
            ? {
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
              }
            : {}),
        }),
      );
    }
    const [alice, bob, observer] = clients;
    const project = alice!.insert(app.projects, { title: "Encrypted attachments" });
    await project.wait({ tier: "global" });
    await alice!.e2ee.spaces.grant(app.projects, project.value.id, accounts[1]!.account.id).wait();
    const payload = Uint8Array.from({ length: 131_091 }, (_, index) => index % 251);
    const inserted = await alice!.insertStreaming(app.files, {
      projectId: project.value.id,
      name: "private-photo.png",
      payload: (async function* () {
        yield payload.subarray(0, 31);
        yield payload.subarray(31, 70_000);
        yield payload.subarray(70_000);
      })(),
    });
    await inserted.wait({ tier: "global" });
    expect(await bob!.one(app.files.where({ id: inserted.value.id }), { tier: "global" })).toEqual({
      id: inserted.value.id,
      projectId: project.value.id,
      name: "private-photo.png",
      payload,
    });
    const stored = await observer!.one(physical.files.where({ id: inserted.value.id }), {
      tier: "global",
    });
    expect(stored).not.toBeNull();
    expect(stored!.payload).not.toEqual(payload);
    expect(new TextDecoder().decode(stored!.name)).not.toContain("private-photo.png");
    await expect(
      observer!.one(app.files.where({ id: inserted.value.id }), { tier: "global" }),
    ).rejects.toThrow();
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);

it.each([false, true])(
  "atomically publishes a declared scope and two files with its exact recipient seed (native=%s)",
  async (native) => {
    const fixture = await uploadFixture(native, false, true);
    try {
      const recipient = await fixture.getRecipient();
      const payload = Uint8Array.from({ length: 131_091 }, (_, index) => index % 251);
      const result = await fixture.owner.streamingTransaction((plan) => {
        const scope = plan.insert(
          uploadApp.projects,
          { title: "Only recipient can read" },
          {
            initialRecipients: [recipient.id],
          },
        );
        const image = plan.insertStreaming(uploadApp.files, {
          projectId: scope.id,
          name: "image.png",
          payload: uploadBytes(payload),
        });
        const empty = plan.insertStreaming(uploadApp.files, {
          projectId: scope.id,
          name: "empty",
          payload: uploadBytes(new Uint8Array()),
        });
        return { scope: scope.id, image: image.id, empty: empty.id };
      });
      const ids = await result.wait({ tier: "global" });
      expect(
        await recipient.db.one(uploadApp.files.where({ id: ids.image }), { tier: "global" }),
      ).toEqual({
        id: ids.image,
        projectId: ids.scope,
        name: "image.png",
        payload,
      });
      expect(
        await recipient.db.one(uploadApp.files.where({ id: ids.empty }), { tier: "global" }),
      ).toEqual({
        id: ids.empty,
        projectId: ids.scope,
        name: "empty",
        payload: new Uint8Array(),
      });
      const roots = await fixture.owner.all(
        uploadApp.__e2ee_spaces.where({ identifier: ids.scope }),
        { tier: "global" },
      );
      expect(roots).toHaveLength(1);
      const grants = await fixture.owner.all(
        uploadApp.__e2ee_space_grants.where({ spaceId: roots[0]!.id }),
        { tier: "global" },
      );
      expect(grants.map((grant) => grant.recipientId)).toEqual([recipient.id]);
      await expect(
        fixture.owner.one(uploadApp.files.where({ id: ids.image }), { tier: "global" }),
      ).rejects.toThrow();
    } finally {
      await fixture.close();
    }
  },
  60_000,
);

it.each([false, true])(
  "initialises a legacy scope and replaces cell/stream ciphertext without replay (native=%s)",
  async (native) => {
    const fixture = await uploadFixture(native);
    try {
      const legacy = await fixture.getLegacy();
      const scope = await legacy
        .insert(legacyUploadApp.projects, { title: "Legacy" })
        .wait({ tier: "global" });
      expect(await fixture.owner.all(uploadApp.__e2ee_spaces, { tier: "global" })).toEqual([]);
      const uploaded = await fixture.owner.insertStreaming(uploadApp.files, {
        projectId: scope.id,
        name: "first",
        payload: uploadBytes(new Uint8Array([1, 2, 3])),
      });
      await uploaded.wait({ tier: "global" });
      expect(
        await fixture.owner.one(uploadApp.files.where({ id: uploaded.value.id }), {
          tier: "global",
        }),
      ).toEqual({
        id: uploaded.value.id,
        projectId: scope.id,
        name: "first",
        payload: new Uint8Array([1, 2, 3]),
      });
      const ordinary = await fixture.owner
        .insert(uploadApp.files, {
          projectId: scope.id,
          name: "cell",
          payload: new Uint8Array([4]),
        })
        .wait({ tier: "global" });
      await (
        await fixture.owner.updateStreaming(uploadApp.files, ordinary.id, {
          name: "stream",
          payload: uploadBytes(new Uint8Array([5, 6])),
        })
      ).wait({ tier: "global" });
      expect(
        await fixture.owner.one(uploadApp.files.where({ id: ordinary.id }), { tier: "global" }),
      ).toEqual({
        id: ordinary.id,
        projectId: scope.id,
        name: "stream",
        payload: new Uint8Array([5, 6]),
      });
      await fixture.owner
        .update(uploadApp.files, ordinary.id, { payload: new Uint8Array([7]), name: "cell again" })
        .wait({ tier: "global" });
      await (
        await fixture.owner.upsertStreaming(uploadApp.files, uploaded.value.id, {
          payload: uploadBytes(new Uint8Array()),
          name: "empty replacement",
        })
      ).wait({ tier: "global" });
      expect(
        await fixture.owner.one(uploadApp.files.where({ id: ordinary.id }), { tier: "global" }),
      ).toEqual({
        id: ordinary.id,
        projectId: scope.id,
        name: "cell again",
        payload: new Uint8Array([7]),
      });
      expect(
        await fixture.owner.one(uploadApp.files.where({ id: uploaded.value.id }), {
          tier: "global",
        }),
      ).toEqual({
        id: uploaded.value.id,
        projectId: scope.id,
        name: "empty replacement",
        payload: new Uint8Array(),
      });
      expect(
        await fixture.owner.all(uploadApp.__e2ee_spaces.where({ identifier: scope.id }), {
          tier: "global",
        }),
      ).toHaveLength(1);
      if (!native) {
        // A new declared scope also checks insert-then-upsert order within one plan.
        const result = await fixture.owner.streamingTransaction((plan) => {
          const scope = plan.insert(uploadApp.projects, { title: "Ordered plan" });
          const file = plan.insertStreaming(uploadApp.files, {
            projectId: scope.id,
            name: "superseded",
            payload: uploadBytes(new Uint8Array([1])),
          });
          plan.upsertStreaming(uploadApp.files, file.id, {
            name: "final",
            payload: uploadBytes(new Uint8Array([2, 3])),
          });
          return { id: file.id, projectId: scope.id };
        });
        const row = await result.wait({ tier: "global" });
        expect(
          await fixture.owner.all(uploadApp.files.where({ projectId: row.projectId }), {
            tier: "global",
          }),
        ).toEqual([{ ...row, name: "final", payload: new Uint8Array([2, 3]) }]);
      }
    } finally {
      await fixture.close();
    }
  },
  60_000,
);

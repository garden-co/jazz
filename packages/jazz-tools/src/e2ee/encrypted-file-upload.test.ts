import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, groupSchema, spaceSchema } from "./managed-schema.js";
import {
  createJazzSession,
  type JazzClient as NativeJazzClient,
} from "../backend/create-jazz-session.js";
import type { JazzSession } from "../session/state.js";

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

const uploadApp = s.defineApp({
  projects: s.table({ title: s.string() }, {}),
  files: s
    .table(
      { projectId: s.uuid(), payload: s.bytes(), name: s.string() },
      { project: s.rel("projects", "projectId") },
    )
    .encrypted({ space: "projectId", columns: ["payload", "name"] }),
});
// Same deployed physical schema/identities, without automatic scope initialisation.
const legacyUploadApp = s.defineApp({
  ...deviceRequestSchema,
  ...groupSchema,
  ...spaceSchema,
  projects: s.table({ title: s.string() }, {}),
  files: s.table(
    { projectId: s.uuid(), payload: s.bytes(), name: s.bytes() },
    { project: s.rel("projects", "projectId") },
  ),
});

async function uploadFixture(native = false, denied = false) {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const permissions = definePermissions(uploadApp, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.files.allowRead.always();
    if (!denied) policy.files.allowInsert.always();
    policy.files.allowUpdate.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
  });
  const clients: Db[] = [];
  let session: JazzSession<NativeJazzClient> | undefined;
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
  const close = async () => {
    await Promise.all(clients.map((db) => db.shutdown()));
    await session?.close();
    await server.stop();
  };
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: uploadApp,
      permissions,
    });
    const ownerAccount = await localAccountConfig(server.appId, server.url);
    const recipientAccount = await localAccountConfig(server.appId, server.url);
    let owner: Db;
    let ownerId: string;
    if (native) {
      session = await createJazzSession({
        appId: server.appId,
        serverUrl: server.url,
        app: uploadApp,
        permissions,
        driver: { type: "memory" },
        initial: "local-first",
        e2ee: { app: uploadApp, store: store() },
      });
      owner = session.getSnapshot().client!.db;
      ownerId = session.getSnapshot().account!.id;
    } else {
      owner = await createDb({ ...ownerAccount, e2ee: { app: uploadApp, store: store() } });
      ownerId = ownerAccount.account.id;
      clients.push(owner);
    }
    const recipient = await createDb({
      ...recipientAccount,
      e2ee: { app: uploadApp, store: store() },
    });
    clients.push(recipient);
    const legacy = await createDb(ownerAccount);
    clients.push(legacy);
    await owner.e2ee.devices.list();
    await recipient.e2ee.devices.list();
    return { owner, ownerId, recipient, recipientId: recipientAccount.account.id, legacy, close };
  } catch (error) {
    await close();
    throw error;
  }
}

function uploadBytes(bytes: Uint8Array) {
  return (async function* () {
    for (let offset = 0; offset < bytes.length; offset += 31_337)
      yield bytes.subarray(offset, offset + 31_337);
  })();
}

it.each([false, true])(
  "atomically publishes a declared scope and two files with its exact recipient seed (native=%s)",
  async (native) => {
    const fixture = await uploadFixture(native);
    try {
      const payload = Uint8Array.from({ length: 131_091 }, (_, index) => index % 251);
      const result = await fixture.owner.streamingTransaction((plan) => {
        const scope = plan.insert(
          uploadApp.projects,
          { title: "Only recipient can read" },
          {
            initialRecipients: [fixture.recipientId],
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
        await fixture.recipient.one(uploadApp.files.where({ id: ids.image }), { tier: "global" }),
      ).toEqual({
        id: ids.image,
        projectId: ids.scope,
        name: "image.png",
        payload,
      });
      expect(
        await fixture.recipient.one(uploadApp.files.where({ id: ids.empty }), { tier: "global" }),
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
      expect(grants.map((grant) => grant.recipientId)).toEqual([fixture.recipientId]);
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
      const scope = await fixture.legacy
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
    } finally {
      await fixture.close();
    }
  },
  60_000,
);

it.each(["rotation", "removal"] as const)(
  "rejects %s during a blocked source without replay or an accepted file",
  async (change) => {
    const fixture = await uploadFixture();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    try {
      const scope = await fixture.owner
        .insert(
          uploadApp.projects,
          { title: "Shared" },
          {
            initialRecipients: [fixture.ownerId, fixture.recipientId],
          },
        )
        .wait({ tier: "global" });
      let pulls = 0;
      const writer = change === "rotation" ? fixture.owner : fixture.recipient;
      const pending = writer
        .insertStreaming(uploadApp.files, {
          projectId: scope.id,
          name: "stale epoch",
          payload: (async function* () {
            pulls++;
            entered();
            yield new Uint8Array([1]);
            await gate;
            yield new Uint8Array([2]);
          })(),
        })
        .then((write) => write.wait({ tier: "global" }));
      const rejected = expect(pending).rejects.toThrow();
      await started;
      await fixture.owner.e2ee.spaces
        .revoke(uploadApp.projects, scope.id, fixture.recipientId)
        .wait();
      release();
      await rejected;
      expect(pulls).toBe(1);
      expect(await fixture.owner.all(uploadApp.files.select("id"), { tier: "global" })).toEqual([]);
    } finally {
      release();
      await fixture.close();
    }
  },
  60_000,
);

it.each(["source-failure", "denied"] as const)(
  "publishes neither new scope nor controls on %s",
  async (failure) => {
    const fixture = await uploadFixture(false, failure === "denied");
    try {
      let pulls = 0;
      const pending = fixture.owner
        .streamingTransaction((plan) => {
          const scope = plan.insert(uploadApp.projects, { title: "Must remain absent" });
          plan.insertStreaming(uploadApp.files, {
            projectId: scope.id,
            name: "first sibling",
            payload: uploadBytes(new Uint8Array([1, 2])),
          });
          return plan.insertStreaming(uploadApp.files, {
            projectId: scope.id,
            name: "second sibling",
            payload: (async function* () {
              pulls++;
              yield new Uint8Array([3]);
              if (failure === "source-failure") throw new Error("fixture source failed");
            })(),
          });
        })
        .then((write) => write.wait({ tier: "global" }));
      await expect(pending).rejects.toThrow(
        failure === "source-failure" ? "fixture source failed" : undefined,
      );
      expect(pulls).toBe(1);
      expect(await fixture.owner.all(uploadApp.projects, { tier: "global" })).toEqual([]);
      expect(await fixture.owner.all(uploadApp.files.select("id"), { tier: "global" })).toEqual([]);
      expect(await fixture.owner.all(uploadApp.__e2ee_spaces, { tier: "global" })).toEqual([]);
      expect(await fixture.owner.all(uploadApp.__e2ee_space_grants, { tier: "global" })).toEqual(
        [],
      );
    } finally {
      await fixture.close();
    }
  },
  60_000,
);

it("rejects keyless writers and every invalid declaration before pulling any source", async () => {
  const fixture = await uploadFixture();
  try {
    const scope = await fixture.owner
      .insert(uploadApp.projects, { title: "Private" })
      .wait({ tier: "global" });
    let pulls = 0;
    const source = () =>
      (async function* () {
        pulls++;
        yield new Uint8Array([1]);
      })();
    await expect(
      fixture.recipient.insertStreaming(uploadApp.files, {
        projectId: scope.id,
        name: "not granted",
        payload: source(),
      }),
    ).rejects.toThrow();
    await expect(
      fixture.owner.streamingTransaction((plan) => {
        plan.insertStreaming(uploadApp.files, {
          projectId: scope.id,
          name: "valid first",
          payload: source(),
        });
        plan.updateStreaming(uploadApp.files, "invalid UUID", { payload: source() });
      }),
    ).rejects.toThrow();
    await expect(
      fixture.owner.updateStreaming(
        uploadApp.files,
        crypto.randomUUID(),
        {
          payload: source(),
        },
        { branch: { projectId: scope.id } },
      ),
    ).rejects.toThrow("root-view");
    expect(pulls).toBe(0);
    expect(await fixture.owner.all(uploadApp.files.select("id"), { tier: "global" })).toEqual([]);
  } finally {
    await fixture.close();
  }
}, 60_000);

it.each(["resolved", "pending"] as const)(
  "rejects an invalid byte source despite %s cancellation and leaves staged siblings unpublished",
  async (cancellation) => {
    const fixture = await uploadFixture();
    try {
      const scope = await fixture.owner
        .insert(uploadApp.projects, { title: "Cancellation" })
        .wait({ tier: "global" });
      let cancelled = false;
      const source = new ReadableStream<Uint8Array | string>(
        {
          pull(controller) {
            controller.enqueue("not bytes");
          },
          cancel() {
            cancelled = true;
            if (cancellation === "pending") return new Promise<void>(() => {});
          },
        },
        { highWaterMark: 0 },
      );
      await expect(
        fixture.owner.streamingTransaction((plan) => {
          plan.insertStreaming(uploadApp.files, {
            projectId: scope.id,
            name: "sibling",
            payload: uploadBytes(new Uint8Array([1])),
          });
          plan.insertStreaming(uploadApp.files, {
            projectId: scope.id,
            name: "invalid",
            payload: source,
          });
        }),
      ).rejects.toThrow();
      expect(cancelled).toBe(true);
      expect(await fixture.owner.all(uploadApp.files.select("id"), { tier: "global" })).toEqual([]);
      const replacement = await fixture.owner.insertStreaming(uploadApp.files, {
        projectId: scope.id,
        name: "subsequent upload",
        payload: uploadBytes(new Uint8Array([8, 9])),
      });
      await replacement.wait({ tier: "global" });
      expect(
        await fixture.owner.one(uploadApp.files.where({ id: replacement.value.id }), {
          tier: "global",
        }),
      ).toEqual({
        id: replacement.value.id,
        projectId: scope.id,
        name: "subsequent upload",
        payload: new Uint8Array([8, 9]),
      });
    } finally {
      await fixture.close();
    }
  },
  60_000,
);

it("rejects a later keyless space before pulling an earlier eligible source", async () => {
  const fixture = await uploadFixture();
  try {
    const shared = await fixture.owner
      .insert(
        uploadApp.projects,
        { title: "Shared" },
        {
          initialRecipients: [fixture.ownerId, fixture.recipientId],
        },
      )
      .wait({ tier: "global" });
    const privateScope = await fixture.owner
      .insert(uploadApp.projects, { title: "Private" })
      .wait({ tier: "global" });
    let pulls = 0;
    const source = () =>
      (async function* () {
        pulls++;
        yield new Uint8Array([1]);
      })();
    await expect(
      fixture.recipient.streamingTransaction((plan) => {
        plan.insertStreaming(uploadApp.files, {
          projectId: shared.id,
          name: "eligible",
          payload: source(),
        });
        plan.insertStreaming(uploadApp.files, {
          projectId: privateScope.id,
          name: "keyless",
          payload: source(),
        });
      }),
    ).rejects.toThrow();
    expect(pulls).toBe(0);
    expect(await fixture.owner.all(uploadApp.files.select("id"), { tier: "global" })).toEqual([]);
  } finally {
    await fixture.close();
  }
}, 60_000);

it("keeps a legacy scope but rejects its first file and initial controls together on permission denial", async () => {
  const fixture = await uploadFixture(false, true);
  try {
    const scope = await fixture.legacy
      .insert(legacyUploadApp.projects, { title: "Legacy remains" })
      .wait({ tier: "global" });
    const pending = fixture.owner
      .insertStreaming(uploadApp.files, {
        projectId: scope.id,
        name: "denied first file",
        payload: uploadBytes(new Uint8Array([1, 2, 3])),
      })
      .then((write) => write.wait({ tier: "global" }));
    await expect(pending).rejects.toThrow();
    expect(await fixture.owner.all(uploadApp.projects, { tier: "global" })).toEqual([scope]);
    expect(await fixture.owner.all(uploadApp.files.select("id"), { tier: "global" })).toEqual([]);
    expect(await fixture.owner.all(uploadApp.__e2ee_spaces, { tier: "global" })).toEqual([]);
    expect(await fixture.owner.all(uploadApp.__e2ee_space_grants, { tier: "global" })).toEqual([]);
  } finally {
    await fixture.close();
  }
}, 60_000);

it("applies declaration order when a later stream replaces an earlier staged file", async () => {
  const fixture = await uploadFixture();
  try {
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
    expect(await fixture.owner.all(uploadApp.files, { tier: "global" })).toEqual([
      { ...row, name: "final", payload: new Uint8Array([2, 3]) },
    ]);
  } finally {
    await fixture.close();
  }
}, 60_000);

it("keeps both existing-space keys borrowed until their staged files reach the granted recipient", async () => {
  const fixture = await uploadFixture();
  try {
    const first = await fixture.owner
      .insert(uploadApp.projects, { title: "First space" })
      .wait({ tier: "global" });
    const second = await fixture.owner
      .insert(uploadApp.projects, { title: "Second space" })
      .wait({ tier: "global" });
    await fixture.owner.e2ee.spaces.grant(uploadApp.projects, first.id, fixture.recipientId).wait();
    await fixture.owner.e2ee.spaces
      .grant(uploadApp.projects, second.id, fixture.recipientId)
      .wait();
    const firstBytes = Uint8Array.from({ length: 131_091 }, (_, index) => index % 251);
    const secondBytes = Uint8Array.from({ length: 131_093 }, (_, index) => 255 - (index % 251));
    const result = await fixture.owner.streamingTransaction((plan) => {
      const firstFile = plan.insertStreaming(uploadApp.files, {
        projectId: first.id,
        name: "first.png",
        payload: uploadBytes(firstBytes),
      });
      const secondFile = plan.insertStreaming(uploadApp.files, {
        projectId: second.id,
        name: "second.png",
        payload: uploadBytes(secondBytes),
      });
      return { first: firstFile.id, second: secondFile.id };
    });
    const ids = await result.wait({ tier: "global" });
    expect(
      await fixture.recipient.one(uploadApp.files.where({ id: ids.first }), { tier: "global" }),
    ).toEqual({
      id: ids.first,
      projectId: first.id,
      name: "first.png",
      payload: firstBytes,
    });
    expect(
      await fixture.recipient.one(uploadApp.files.where({ id: ids.second }), { tier: "global" }),
    ).toEqual({
      id: ids.second,
      projectId: second.id,
      name: "second.png",
      payload: secondBytes,
    });
  } finally {
    await fixture.close();
  }
}, 60_000);

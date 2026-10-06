import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import type { AccountDbConfig } from "../accounts/context.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { spaceSchema } from "./spaces.js";
import { createNativeCrypto } from "./native.js";

const app = s.defineApp({
  ...deviceRequestSchema,
  ...groupSchema,
  ...spaceSchema,
  projects: s.table({ title: s.string() }, {}),
});

async function fixture(allowGrantDeletion = false) {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients = new Set<Db>();
  const cleanup = async () => {
    try {
      await Promise.all([...clients].map((client) => client.shutdown()));
    } finally {
      await server.stop();
    }
  };
  try {
    const policies = definePermissions(app, ({ policy, session }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.projects.allowRead.where(authenticated);
      policy.projects.allowInsert.where(authenticated);
      policy.__e2ee_spaces.allowRead.where(authenticated);
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.where(authenticated);
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      if (allowGrantDeletion)
        policy.__e2ee_space_grants.allowDelete.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.where(authenticated);
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.where(authenticated);
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_recovery_deliveries.allowRead.where(authenticated);
      policy.__e2ee_space_recovery_deliveries.allowInsert.where({
        senderAccountId: session.user.account,
      });
      policy.__e2ee_groups.allowRead.where(authenticated);
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowRead.where(authenticated);
      policy.__e2ee_group_membership.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_successors.allowRead.where(authenticated);
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_group_recovery_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_recovery_deliveries.allowInsert.where({
        senderAccountId: session.user.account,
      });
      policy.__e2ee_group_repairs.allowRead.where(authenticated);
      policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
    });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...policies },
    });
    const native = await createNativeCrypto();
    const decoder = new TextDecoder();
    const verifierError = new Error("Selected space root verifier unavailable");
    let failingIdentifier: string | undefined;
    const open = async (account: AccountDbConfig, observe = false) => {
      let saved: string | null = null;
      const db = await createDb({
        ...account,
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
          crypto: {
            ...native,
            deviceSigner: {
              ...native.deviceSigner,
              async verify(publicKey, record, signature) {
                const text = decoder.decode(record);
                if (
                  observe &&
                  failingIdentifier &&
                  text.includes("jazz.e2ee.space.v1") &&
                  text.includes('["root",') &&
                  text.includes(failingIdentifier)
                )
                  throw verifierError;
                return native.deviceSigner.verify(publicKey, record, signature);
              },
            },
          },
        },
      });
      clients.add(db);
      return db;
    };
    const ownerAccount = await localAccountConfig(server.appId, server.url);
    const recipientAccount = await localAccountConfig(server.appId, server.url);
    const owner = await open(ownerAccount);
    const recipient = await open(recipientAccount, true);
    await owner.e2ee.devices.list();
    await recipient.e2ee.devices.list();
    const parent = await owner.e2ee.groups.create().wait();
    const child = await owner.e2ee.groups.create().wait();
    await owner.e2ee.groups
      .add(child.id, { kind: "account", id: recipientAccount.account.id })
      .wait();
    await owner.e2ee.groups.add(parent.id, { kind: "group", id: child.id }).wait();
    const project = async (title: string, recipientId = ownerAccount.account.id) => {
      const row = await owner.insert(app.projects, { title }).wait({ tier: "global" });
      await owner.e2ee.spaces.grant(app.projects, row.id, recipientId).wait();
      return { scope: app.projects, identifier: row.id };
    };
    const close = async (db: Db) => {
      await db.shutdown();
      clients.delete(db);
    };
    return {
      owner,
      recipient,
      recipientAccount,
      parent,
      child,
      project,
      open,
      close,
      cleanup,
      verifierError,
      failRoot(identifier?: string) {
        failingIdentifier = identifier;
      },
    };
  } catch (error) {
    await cleanup();
    throw error;
  }
}

it.each(["status", "create", "use"] as const)(
  "discovers only recipient spaces during recovery %s without losing direct or nested access",
  async (operation) => {
    const f = await fixture();
    try {
      const direct = await f.project("Direct noncreator access", f.recipientAccount.account.id);
      const inherited = await f.project("Nested recipient access", f.parent.id);
      expect(await f.recipient.e2ee.explain(direct)).toEqual({ state: "ready" });
      expect(await f.recipient.e2ee.explain(inherited)).toEqual({ state: "ready" });
      const { material } = await f.recipient.e2ee.recovery.create().wait();
      const before = await f.recipient.e2ee.recovery.status(material);
      expect(before.spaces).toMatchObject({ validation: "checked" });
      if (before.spaces.validation !== "checked") throw new Error("Space recovery was not checked");
      expect(before.spaces.paths).toHaveLength(2);
      expect(before.spaces.paths).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ identifier: direct.identifier, validation: "validated" }),
          expect.objectContaining({ identifier: inherited.identifier, validation: "validated" }),
        ]),
      );

      // This ordinary, canonical space is readable, but grants no access to the recipient.
      const unrelated = await f.project("Unrelated space");
      f.failRoot(unrelated.identifier);
      await expect(f.recipient.e2ee.explain(unrelated)).rejects.toBe(f.verifierError);
      const observer =
        operation === "create" ? f.recipient : await f.open(f.recipientAccount, true);
      if (operation === "use") {
        // Recovery must work without a live holder delivering keys to the new device.
        await f.close(f.owner);
        await f.close(f.recipient);
      }
      const recover = async () => {
        if (operation === "status") return observer.e2ee.recovery.status(material);
        if (operation === "create") return observer.e2ee.recovery.create().wait();
        return observer.e2ee.recovery.use(material).wait();
      };
      const result = await recover();
      const checkedMaterial = result && "material" in result ? result.material : material;
      const after = await observer.e2ee.recovery.status(checkedMaterial);
      expect(after.spaces).toEqual(before.spaces);
      if (operation !== "status") {
        expect(await observer.e2ee.explain(direct)).toEqual({ state: "ready" });
        expect(await observer.e2ee.explain(inherited)).toEqual({ state: "ready" });
      }

      if (operation === "status") {
        // Fresh read-only recovery must propagate a required root's operational fault;
        // a live Spaces cache need not reverify after only adapter closure state changes.
        const requiredReader = await f.open(f.recipientAccount, true);
        f.failRoot(inherited.identifier);
        await expect(requiredReader.e2ee.recovery.status(material)).rejects.toBe(f.verifierError);
        f.failRoot();
        expect((await requiredReader.e2ee.recovery.status(material)).spaces).toEqual(before.spaces);
      }
    } finally {
      await f.cleanup();
    }
  },
  180_000,
);

it("requires an undelivered nested recipient space and excludes it after membership removal", async () => {
  const f = await fixture();
  try {
    const { material } = await f.recipient.e2ee.recovery.create().wait();
    const target = await f.project("Undelivered inherited access");
    // Ordinary administration permits this grant, but its author has no space key to deliver.
    await f.recipient.e2ee.spaces.grant(app.projects, target.identifier, f.parent.id).wait();
    const missing = await f.recipient.e2ee.recovery.status(material);
    expect(missing.spaces).toMatchObject({
      validation: "checked",
      paths: [
        {
          identifier: target.identifier,
          validation: "unavailable",
          reason: "missing-recovery-delivery",
        },
      ],
    });
    if (missing.spaces.validation !== "checked") throw new Error("Space recovery was not checked");
    expect(missing.spaces.paths).toHaveLength(1);
    await expect(f.recipient.e2ee.recovery.use(material).wait()).rejects.toMatchObject({
      code: "recovery-space-delivery-unavailable",
    });
    await expect(f.recipient.e2ee.recovery.create().wait()).rejects.toThrow(
      "Space key unavailable while creating recovery",
    );

    expect(await f.owner.e2ee.explain(target)).toEqual({ state: "ready" });
    const protectedRecovery = await f.recipient.e2ee.recovery.create().wait();
    expect(await f.recipient.e2ee.recovery.status(protectedRecovery.material)).toMatchObject({
      spaces: {
        validation: "checked",
        paths: [{ identifier: target.identifier, validation: "validated" }],
      },
    });
    await f.recipient.e2ee.groups.leave(f.child.id).wait();
    expect(await f.recipient.e2ee.recovery.status(protectedRecovery.material)).toMatchObject({
      spaces: { validation: "checked", paths: [] },
    });
    const removedRecovery = await f.recipient.e2ee.recovery.create().wait();
    await f.recipient.e2ee.recovery.use(removedRecovery.material).wait();
    expect(await f.recipient.e2ee.explain(target)).toMatchObject({
      state: "refused",
      reason: "not-a-space-recipient",
    });
  } finally {
    await f.cleanup();
  }
}, 180_000);

it("fails closed when the only initial group-recipient grant is deleted", async () => {
  const f = await fixture(true);
  try {
    const target = await f.project("Deleted initial group grant", f.parent.id);
    expect(await f.owner.e2ee.explain(target)).toEqual({ state: "ready" });
    const { material } = await f.owner.e2ee.recovery.create().wait();
    const root = await f.owner.one(app.__e2ee_spaces.where({ identifier: target.identifier }), {
      tier: "global",
    });
    expect(root).not.toBeNull();
    const grants = await f.owner.all(app.__e2ee_space_grants.where({ spaceId: root!.id }), {
      tier: "global",
    });
    expect(grants).toEqual([
      expect.objectContaining({
        id: root!.initialGrantId,
        recipientKind: "group",
        recipientId: f.parent.id,
      }),
    ]);
    await f.owner.delete(app.__e2ee_space_grants, root!.initialGrantId).wait({ tier: "global" });
    // Inspect from the deleting client, with no cross-client propagation race.
    expect(
      await f.owner.all(app.__e2ee_space_grants.where({ spaceId: root!.id }), {
        tier: "global",
      }),
    ).toEqual([]);
    await expect
      .soft(f.owner.e2ee.recovery.status(material))
      .rejects.toThrow("Invalid or unsupported E2EE space membership");
    await expect
      .soft(f.owner.e2ee.recovery.use(material).wait())
      .rejects.toThrow("Invalid or unsupported E2EE space membership");
    await expect
      .soft(f.owner.e2ee.recovery.create().wait())
      .rejects.toThrow("Invalid or unsupported E2EE space membership");
  } finally {
    await f.cleanup();
  }
}, 180_000);

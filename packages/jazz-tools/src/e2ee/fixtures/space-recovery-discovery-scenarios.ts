import { schema as s } from "../../schema-namespace.js";
import { definePermissions } from "../../permissions/index.js";
import { createDb } from "../../runtime/default-create-db.js";
import type { Db } from "../../runtime/db.js";
import type { AccountDbConfig } from "../../accounts/context.js";
import { localAccountConfig } from "../../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from ".././device-requests.js";
import { groupSchema } from ".././groups.js";
import { spaceSchema } from ".././spaces.js";
import { createNativeCrypto } from ".././native.js";

export const app = s.defineApp({
  ...deviceRequestSchema,
  ...groupSchema,
  ...spaceSchema,
  projects: s.table({ title: s.string() }, {}),
});

export async function fixture(allowGrantDeletion = false) {
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

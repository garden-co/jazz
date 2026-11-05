import { schema as s } from "../../schema-namespace.js";
import { definePermissions } from "../../permissions/index.js";
import { createDb } from "../../runtime/default-create-db.js";
import type { Db } from "../../runtime/db.js";
import { localAccountConfig } from "../../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../../testing/index.js";
import { deviceRequestSchema, groupSchema, spaceSchema } from "../managed-schema.js";
import {
  createJazzSession,
  type JazzClient as NativeJazzClient,
} from "../../backend/create-jazz-session.js";
import type { JazzSession } from "../../session/state.js";
export const uploadApp = s.defineApp({
  projects: s.table({ title: s.string() }, {}),
  files: s
    .table(
      { projectId: s.uuid(), payload: s.bytes(), name: s.string() },
      { project: s.rel("projects", "projectId") },
    )
    .encrypted({ space: "projectId", columns: ["payload", "name"] }),
});
// Same deployed physical schema/identities, without automatic scope initialisation.
export const legacyUploadApp = s.defineApp({
  ...deviceRequestSchema,
  ...groupSchema,
  ...spaceSchema,
  projects: s.table({ title: s.string() }, {}),
  files: s.table(
    { projectId: s.uuid(), payload: s.bytes(), name: s.bytes() },
    { project: s.rel("projects", "projectId") },
  ),
});

export async function uploadFixture(native = false, denied = false, withRecipient = false) {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const permissions = definePermissions(uploadApp, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.files.allowRead.always();
    if (!denied) policy.files.allowInsert.always();
    policy.files.allowUpdate.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ creatorAccountId: session.user.account });
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
    const recipientAccount = withRecipient
      ? await localAccountConfig(server.appId, server.url)
      : undefined;
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
    const recipient = recipientAccount
      ? await createDb({ ...recipientAccount, e2ee: { app: uploadApp, store: store() } })
      : undefined;
    if (recipient) clients.push(recipient);
    const getRecipient = async () => {
      if (!recipient || !recipientAccount) throw new Error("Recipient fixture was not requested");
      return { db: recipient, id: recipientAccount.account.id };
    };
    let legacy: Promise<Db> | undefined;
    const getLegacy = () =>
      (legacy ??= (async () => {
        const db = await createDb(ownerAccount);
        clients.push(db);
        return db;
      })());
    await owner.e2ee.devices.list();
    if (recipient) await recipient.e2ee.devices.list();
    return { owner, ownerId, getRecipient, getLegacy, close };
  } catch (error) {
    await close();
    throw error;
  }
}

export function uploadBytes(bytes: Uint8Array) {
  return (async function* () {
    for (let offset = 0; offset < bytes.length; offset += 31_337)
      yield bytes.subarray(offset, offset + 31_337);
  })();
}

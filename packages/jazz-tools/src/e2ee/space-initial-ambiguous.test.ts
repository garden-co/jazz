import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { spaceSchema } from "./spaces.js";
import { createBrowserKeyEnvelope, createBrowserDeviceSigner } from "./browser.js";
import { groupContext, groupRootBytes } from "./group-format.js";

it("rejects an initial recipient ID that resolves to both an accepted account and group", async () => {
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...groupSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
  });
  const policies = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
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
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
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
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: {
        ...deviceRequestPermissions,
        projects: policies.projects!,
        __e2ee_spaces: policies.__e2ee_spaces!,
        __e2ee_space_grants: policies.__e2ee_space_grants!,
        __e2ee_space_deliveries: policies.__e2ee_space_deliveries!,
        __e2ee_space_successors: policies.__e2ee_space_successors!,
        __e2ee_groups: policies.__e2ee_groups!,
        __e2ee_group_membership: policies.__e2ee_group_membership!,
        __e2ee_group_successors: policies.__e2ee_group_successors!,
        __e2ee_group_deliveries: policies.__e2ee_group_deliveries!,
        __e2ee_group_repairs: policies.__e2ee_group_repairs!,
      },
    });
    const alice = await localAccountConfig(server.appId, server.url);
    const bob = await localAccountConfig(server.appId, server.url);
    const creator = await createDb({ ...alice, e2ee: { app, store: store() } });
    const bobStore = store();
    const recipient = await createDb({ ...bob, e2ee: { app, store: bobStore } });
    clients.push(creator, recipient);
    await creator.e2ee.devices.list();
    await recipient.e2ee.devices.list();
    const stored = JSON.parse((await bobStore.read())!).devices[0];
    const identity = await recipient.one(
      app.__e2ee_account_identities.where({ id: bob.account.id }),
      {
        tier: "edge",
      },
    );
    const keys = await createBrowserKeyEnvelope();
    const signer = await createBrowserDeviceSigner();
    const secret = crypto.getRandomValues(new Uint8Array(32));
    const signingKey = Uint8Array.from(stored.signingPrivateKey);
    try {
      // Both namespaces contain real, accepted, device-signed identities.
      // The ordinary Jazz policy permits this deliberately colliding group ID.
      const coordinates = {
        id: bob.account.id,
        accountId: bob.account.id,
        deviceId: stored.id as string,
        accountEpochId: identity!.epochId,
        epochId: crypto.randomUUID(),
        mechanism: keys.mechanism.id,
        version: keys.mechanism.version,
      };
      const context = groupContext(stored.scope, coordinates, "verification");
      const verification = await keys.wrap(secret, context, new Uint8Array(32));
      const confirmed = await keys.unwrap(secret, context, verification);
      try {
        expect(confirmed).toEqual(new Uint8Array(32));
      } finally {
        confirmed.fill(0);
      }
      const root = { ...coordinates, verification };
      const bytes = groupRootBytes(stored.scope, root);
      const signature = await signer.sign(signingKey, bytes);
      expect(await signer.verify(Uint8Array.from(stored.signingPublicKey), bytes, signature)).toBe(
        true,
      );
      const { id, ...values } = root;
      await recipient
        .insert(app.__e2ee_groups, { ...values, signature }, { id })
        .wait({ tier: "global" });
    } finally {
      secret.fill(0);
      signingKey.fill(0);
      stored.privateKey.fill(0);
      stored.signingPrivateKey.fill(0);
    }
    expect(
      await creator.one(app.__e2ee_groups.where({ id: bob.account.id }), {
        tier: "edge",
      }),
    ).not.toBeNull();
    expect(
      await creator.one(app.__e2ee_account_roots.where({ accountId: bob.account.id }), {
        tier: "edge",
      }),
    ).not.toBeNull();
    const project = await creator
      .insert(app.projects, { title: "Ambiguous recipient scope" })
      .wait({ tier: "global" });
    await expect(
      creator.e2ee.spaces.grant(app.projects, project.id, bob.account.id).wait(),
    ).rejects.toThrow("Ambiguous E2EE space recipient ID");
    expect(
      await creator.all(app.__e2ee_spaces.where({ identifier: project.id }), {
        tier: "edge",
      }),
    ).toEqual([]);
    expect(await creator.all(app.__e2ee_space_grants, { tier: "edge" })).toEqual([]);
    expect(await creator.all(app.__e2ee_space_deliveries, { tier: "edge" })).toEqual([]);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);

import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createBrowserDeviceSigner } from "./browser.js";
import { spaceGrantBytes } from "./space-format.js";

it("seals an empty space and ignores a later signed creator regrant", async () => {
  const app = s.defineApp({
    ...deviceRequestSchema,
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
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let owner: Awaited<ReturnType<typeof createDb>> | undefined;
  let saved: string | null = null;
  let signingKey: Uint8Array | undefined;
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
      },
    });
    const account = await localAccountConfig(server.appId, server.url);
    owner = await createDb({
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
      },
    });
    const [device] = await owner.e2ee.devices.list();
    const project = await owner
      .insert(app.projects, { title: "Sealed scope" })
      .wait({ tier: "global" });
    await owner.e2ee.spaces.grant(app.projects, project.id, account.account.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    expect(await owner.e2ee.explain(target)).toEqual({ state: "ready" });
    const root = (await owner.one(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "edge",
    }))!;
    await owner.e2ee.spaces.revoke(app.projects, project.id, account.account.id).wait();
    expect(await owner.e2ee.explain(target)).toEqual({ state: "refused", reason: "space-sealed" });
    await owner.e2ee.spaces.revoke(app.projects, project.id, account.account.id).wait();
    await expect(
      owner.e2ee.spaces.grant(app.projects, project.id, account.account.id).wait(),
    ).rejects.toThrow();
    expect(
      await owner.all(app.__e2ee_space_grants.where({ spaceId: root.id }), { tier: "edge" }),
    ).toHaveLength(2);
    expect(
      await owner.all(app.__e2ee_space_successors.where({ spaceId: root.id }), { tier: "edge" }),
    ).toEqual([]);

    const stored = JSON.parse(saved!).devices[0];
    signingKey = Uint8Array.from(stored.signingPrivateKey);
    const signer = await createBrowserDeviceSigner();
    const record = {
      id: crypto.randomUUID(),
      spaceId: root.id,
      epochId: root.epochId,
      authorAccountId: account.account.id,
      authorDeviceId: device!.id,
      authorEpochId: root.accountEpochId,
      operation: "add",
      recipientKind: "account",
      recipientId: account.account.id,
      recipientEpochId: root.accountEpochId,
    };
    const bytes = spaceGrantBytes(stored.scope, root, record);
    const signature = await signer.sign(signingKey, bytes);
    expect(await signer.verify(Uint8Array.from(stored.signingPublicKey), bytes, signature)).toBe(
      true,
    );
    expect((await owner.e2ee.devices.list()).find((row) => row.id === device!.id)).toMatchObject({
      state: "active",
    });
    const { id, ...values } = record;
    await owner
      .insert(app.__e2ee_space_grants, { ...values, signature }, { id })
      .wait({ tier: "global" });
    expect(
      await owner.all(app.__e2ee_space_grants.where({ spaceId: root.id }), { tier: "edge" }),
    ).toHaveLength(3);
    expect(await owner.e2ee.explain(target)).toEqual({ state: "refused", reason: "space-sealed" });
    expect(
      await owner.all(app.__e2ee_spaces.where({ identifier: project.id }), { tier: "edge" }),
    ).toHaveLength(1);
    expect(
      await owner.all(app.__e2ee_space_successors.where({ spaceId: root.id }), { tier: "edge" }),
    ).toEqual([]);
  } finally {
    signingKey?.fill(0);
    await owner?.shutdown();
    await server.stop();
  }
}, 60_000);

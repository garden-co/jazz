import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";

it("shares scoped keys in both directions between native Node and WASM accounts", async () => {
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
  });
  const permissions = {
    ...deviceRequestPermissions,
    projects: policies.projects!,
    __e2ee_spaces: policies.__e2ee_spaces!,
    __e2ee_space_grants: policies.__e2ee_space_grants!,
    __e2ee_space_deliveries: policies.__e2ee_space_deliveries!,
  };
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
  let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
  let wasm: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    owner = await createJazzSession({
      appId: server.appId,
      serverUrl: server.url,
      app,
      permissions,
      driver: { type: "memory" },
      initial: "local-first",
      e2ee: { app, store: store() },
    });
    const native = owner.getSnapshot().client!.db;
    const nativeAccount = owner.getSnapshot().account!;
    const other = await localAccountConfig(server.appId, server.url);
    expect(nativeAccount.id).not.toBe(other.account.id);
    wasm = await createDb({ ...other, e2ee: { app, store: store() } });
    await native.e2ee.devices.list();
    await wasm.e2ee.devices.list();

    for (const [creator, creatorId, recipient, recipientId, title] of [
      [native, nativeAccount.id, wasm, other.account.id, "Native scope"],
      [wasm, other.account.id, native, nativeAccount.id, "WASM scope"],
    ] as const) {
      const project = await creator.insert(app.projects, { title }).wait({ tier: "global" });
      await creator.e2ee.spaces.grant(app.projects, project.id, creatorId).wait();
      const target = { scope: app.projects, identifier: project.id };
      expect(await recipient.e2ee.explain(target)).toMatchObject({ state: "refused" });
      const grant = creator.e2ee.spaces.grant(app.projects, project.id, recipientId);
      expect(grant).not.toBeInstanceOf(Promise);
      await grant.wait();
      expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });
      expect(await recipient.e2ee.explain(target)).toEqual({ state: "ready" });
    }
  } finally {
    await wasm?.shutdown();
    await owner?.close();
    await server.stop();
  }
}, 60_000);

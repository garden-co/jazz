import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";

it("recovers native Node spaces before and after revoking the original device", async () => {
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
  });
  const policies = definePermissions(app, ({ policy, session }) => {
    const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
    policy.projects.allowRead.where(authenticated);
    policy.projects.allowInsert.where(authenticated);
    policy.__e2ee_spaces.allowRead.where(authenticated);
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.where(authenticated);
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.where(authenticated);
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.where(authenticated);
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_recovery_deliveries.allowRead.where(authenticated);
    policy.__e2ee_space_recovery_deliveries.allowInsert.where({
      senderAccountId: session.user.account,
    });
  });
  const permissions = { ...deviceRequestPermissions, ...policies };
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
  const accountStore = store();
  const sessions: Awaited<ReturnType<typeof createJazzSession>>[] = [];
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const open = async (deviceStore: ReturnType<typeof store>) => {
      const session = await createJazzSession({
        appId: server.appId,
        serverUrl: server.url,
        app,
        permissions,
        driver: { type: "memory" },
        initial: "local-first",
        store: accountStore,
        e2ee: { app, store: deviceStore },
      });
      sessions.push(session);
      return session;
    };
    const first = await open(store());
    const db = first.getSnapshot().client!.db;
    const accountId = first.getSnapshot().account!.id;
    const [creator] = await db.e2ee.devices.list();
    expect(creator).toBeDefined();
    const project = await db
      .insert(app.projects, { title: "Native recovery" })
      .wait({ tier: "global" });
    await db.e2ee.spaces.grant(app.projects, project.id, accountId).wait();
    const target = { scope: app.projects, identifier: project.id };
    const { material } = await db.e2ee.recovery.create().wait();
    await first.close();

    const replacementStore = store();
    const second = await open(replacementStore);
    expect(second.getSnapshot().account!.id).toBe(accountId);
    const replacement = second.getSnapshot().client!.db;
    const before = await replacement.e2ee.recovery.status(material);
    expect(before.spaces).toMatchObject({
      validation: "checked",
      paths: [{ identifier: project.id, validation: "validated" }],
    });
    expect(await replacementStore.read()).toBeNull();
    const pending = (await replacement.e2ee.devices.list()).find((row) => row.state === "pending");
    expect(pending).toBeDefined();
    expect(await replacement.e2ee.explain(target)).toMatchObject({ state: "refused" });
    await replacement.e2ee.recovery.use(material).wait();
    expect(await replacement.e2ee.explain(target)).toEqual({ state: "ready" });
    await replacement.e2ee.devices.revoke(creator!.id).wait();
    expect(await replacement.e2ee.explain(target)).toEqual({ state: "ready" });
    const rotated = await replacement.e2ee.recovery.status(material);
    expect(rotated.account.epochId).not.toBe(before.account.epochId);
    expect(rotated.spaces.validation).toBe("checked");
    if (before.spaces.validation !== "checked" || rotated.spaces.validation !== "checked")
      throw new Error("Space recovery was not checked");
    expect(rotated.spaces.paths).toHaveLength(1);
    expect(rotated.spaces.paths[0]!.validation).toBe("validated");
    expect(rotated.spaces.paths[0]!.epochId).not.toBe(before.spaces.paths[0]!.epochId);
    await second.close();

    // No key holder remains online. Recovery must reach the accepted successor,
    // not merely decrypt the original root epoch with obsolete material.
    const third = await open(store());
    const fresh = third.getSnapshot().client!.db;
    await fresh.e2ee.devices.list();
    await fresh.e2ee.recovery.use(material).wait();
    expect(await fresh.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await fresh.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: creator!.id, state: "revoked" }),
    );
    const final = await fresh.e2ee.recovery.status(material);
    expect(final.spaces).toEqual(rotated.spaces);
  } finally {
    await Promise.all(sessions.map((session) => session.close()));
    await server.stop();
  }
}, 120_000);

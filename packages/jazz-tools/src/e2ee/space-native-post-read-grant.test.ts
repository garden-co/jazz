import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createNativeCrypto } from "./native.js";

it("grants a native space after reading its encrypted rows", async () => {
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string(), position: s.float() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
  });
  const policies = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
    policy.notes.allowUpdate.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
  });
  const permissions = {
    ...deviceRequestPermissions,
    projects: policies.projects!,
    notes: policies.notes!,
    __e2ee_spaces: policies.__e2ee_spaces!,
    __e2ee_space_grants: policies.__e2ee_space_grants!,
    __e2ee_space_deliveries: policies.__e2ee_space_deliveries!,
    __e2ee_space_successors: policies.__e2ee_space_successors!,
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
  const sessions: Awaited<ReturnType<typeof createJazzSession>>[] = [];
  const crypto = await createNativeCrypto();
  let tokens = 0;
  let verifications = 0;
  const measuredCrypto = {
    ...crypto,
    equalityIndex: {
      ...crypto.equalityIndex!,
      async token(...args: Parameters<NonNullable<typeof crypto.equalityIndex>["token"]>) {
        tokens++;
        return crypto.equalityIndex!.token(...args);
      },
    },
    deviceSigner: {
      ...crypto.deviceSigner!,
      async verify(...args: Parameters<NonNullable<typeof crypto.deviceSigner>["verify"]>) {
        verifications++;
        return crypto.deviceSigner!.verify(...args);
      },
    },
  };
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    for (let index = 0; index < 2; index++) {
      sessions.push(
        await createJazzSession({
          appId: server.appId,
          serverUrl: server.url,
          app,
          permissions,
          driver: { type: "memory" },
          initial: "local-first",
          store: store(),
          e2ee: { app, store: store(), crypto: measuredCrypto },
        }),
      );
    }
    const owner = sessions[0]!.getSnapshot().client!.db;
    const other = sessions[1]!.getSnapshot().client!.db;
    const otherId = sessions[1]!.getSnapshot().account!.id;
    expect(otherId).not.toBe(sessions[0]!.getSnapshot().account!.id);
    await owner.e2ee.devices.list();
    await other.e2ee.devices.list();
    console.info("E2EE_SEARCH_PERF", JSON.stringify({ phase: "devices-ready" }));
    const tx = owner.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "History measurement" });
    const expected = Array.from({ length: 8 }, (_, position) =>
      tx.insert(app.notes, { projectId: project.id, title: "Wanted", position }),
    );
    await tx.commit().wait({ tier: "global" });
    console.info("E2EE_SEARCH_PERF", JSON.stringify({ phase: "initial-data-accepted" }));
    const target = { scope: app.projects, identifier: project.id };
    const query = app.notes.where({ projectId: project.id, title: "Wanted" }).orderBy("position");
    const root = await owner.one(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "global",
    });
    expect(root).toBeDefined();
    expect(
      await owner.all(app.__e2ee_space_successors.where({ spaceId: root!.id }), { tier: "global" }),
    ).toEqual([]);
    for (let sample = 0; sample < 4; sample++)
      expect(await owner.all(query, { tier: "global" })).toEqual(expected);
    await owner.e2ee.spaces.grant(app.projects, project.id, otherId).wait();
    expect(await other.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await other.all(query, { tier: "global" })).toEqual(expected);
    console.info("E2EE_POST_READ_GRANT", JSON.stringify({ tokens, verifications }));
  } finally {
    await Promise.all(sessions.map((session) => session.close()));
    await server.stop();
  }
}, 120_000);

import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it("initialises a legacy space in the browser without replacing a hidden existing space", async () => {
  const server = await getJazzServerInfo(`e2ee-first-write-${crypto.randomUUID()}`);
  const before = { projects: s.table({ title: s.string() }, {}) };
  const after = {
    ...before,
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] }),
  };
  const oldApp = s.defineApp(before);
  const app = s.defineApp(after);
  const oldPermissions = definePermissions(oldApp, ({ policy }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
    policy.__e2ee_spaces.allowRead.where({ accountId: session.user.account });
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_spaces.allowUpdate.always();
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    await deploy({ ...server, schema: oldApp, permissions: oldPermissions });
    const creator = await createDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      account: await acquireBrowserTestAccount({ ...server, key: "creator" }),
      driver: { type: "memory" },
    });
    clients.push(creator);
    const project = await creator
      .insert(oldApp.projects, { title: "Before encryption" })
      .wait({ tier: "global" });
    await deploy({
      ...server,
      schema: app,
      permissions,
      migration: s.defineMigration({ from: before, to: after, createTables: { notes: true } }),
    });
    for (const key of ["writer", "outsider"]) {
      let saved: string | null = null;
      clients.push(
        await createDb({
          appId: server.appId,
          serverUrl: server.serverUrl,
          account: await acquireBrowserTestAccount({ ...server, key }),
          driver: { type: "memory" },
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
        }),
      );
    }
    const writer = clients[1]!;
    const outsider = clients[2]!;
    const note = await writer
      .insert(app.notes, { projectId: project.id, body: "First browser write" })
      .wait({ tier: "global" });
    expect(await writer.all(app.notes, { tier: "global" })).toEqual([note]);
    const roots = await writer.all(app.__e2ee_spaces, { tier: "global" });
    expect(roots).toHaveLength(1);
    await outsider.e2ee.devices.list();
    expect(await outsider.all(app.__e2ee_spaces, { tier: "global" })).toEqual([]);
    await expect(
      outsider
        .insert(app.notes, { projectId: project.id, body: "Must be rejected" })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    expect(await outsider.all(app.__e2ee_spaces, { tier: "global" })).toEqual([]);
    expect(await writer.all(app.__e2ee_spaces, { tier: "global" })).toEqual(roots);
    expect(await writer.all(app.notes, { tier: "global" })).toEqual([note]);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await stopJazzServer(server.serverUrl);
  }
}, 60_000);

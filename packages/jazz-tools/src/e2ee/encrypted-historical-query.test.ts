import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createBrowserCrypto } from "./browser.js";

it("returns matching old and current epochs together, and rejects incomplete key history", async () => {
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string(), done: s.boolean() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
  });
  const policies = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
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
      permissions: { ...deviceRequestPermissions, ...policies },
    });
    const crypto = await createBrowserCrypto();
    let failHistory = false;
    let deniedHistory = 0;
    const alice = await localAccountConfig(server.appId, server.url);
    const bob = await localAccountConfig(server.appId, server.url);
    const owner = await createDb({
      ...alice,
      e2ee: {
        app,
        store: store(),
        crypto: {
          ...crypto,
          keyEnvelope: {
            ...crypto.keyEnvelope,
            async unwrap(key, context, envelope) {
              // The public crypto format identifies predecessor wraps with successor role "history".
              const text = new TextDecoder().decode(context);
              if (failHistory && text.includes('"successor"') && text.includes('"history"')) {
                deniedHistory++;
                throw new Error("Historical key unavailable");
              }
              return crypto.keyEnvelope.unwrap(key, context, envelope);
            },
          },
        },
      },
    });
    clients.push(owner);
    const reader = await createDb({ ...bob, e2ee: { app, store: store() } });
    clients.push(reader);
    await owner.e2ee.devices.list();
    await reader.e2ee.devices.list();
    const tx = owner.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Epochs" });
    const old = tx.insert(app.notes, { projectId: project.id, title: "Wanted", done: false });
    await tx.commit().wait({ tier: "global" });
    await owner.e2ee.spaces.grant(app.projects, project.id, bob.account.id).wait();
    const query = app.notes.where({ projectId: project.id, title: "Wanted" }).orderBy("done");
    const snapshots: unknown[][] = [];
    let failure: Error | undefined;
    const stop = owner.subscribe(
      query,
      {
        onUpdate: (rows) => snapshots.push(rows),
        onError: (error) => {
          failure = error;
        },
      },
      { tier: "global" },
    );
    try {
      await expect.poll(() => failure ?? snapshots.at(-1), { timeout: 30_000 }).toEqual([old]);
      await owner.e2ee.spaces.revoke(app.projects, project.id, bob.account.id).wait();
      expect(await owner.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual({
        state: "ready",
      });
      const current = owner.insert(app.notes, {
        projectId: project.id,
        title: "Wanted",
        done: true,
      });
      await current.wait({ tier: "global" });
      const expected = [old, current.value];
      expect(await owner.all(query, { tier: "global" })).toEqual(expected);
      expect(await owner.all(query.limit(1), { tier: "global" })).toEqual([old]);
      expect(await owner.all(query.offset(1).limit(1), { tier: "global" })).toEqual([
        current.value,
      ]);
      await expect.poll(() => failure ?? snapshots.at(-1), { timeout: 30_000 }).toEqual(expected);
      expect(
        snapshots.every((rows) => rows.some((row) => (row as { id: string }).id === old.id)),
      ).toBe(true);
      expect(failure).toBeUndefined();
    } finally {
      stop();
    }
    failHistory = true;
    await expect(owner.all(query, { tier: "global" })).rejects.toMatchObject({
      name: "E2eeDataError",
    });
    expect(deniedHistory).toBeGreaterThan(0);
    const incomplete: unknown[][] = [];
    let historyFailure: Error | undefined;
    const stopIncomplete = owner.subscribe(
      query,
      {
        onUpdate: (rows) => incomplete.push(rows),
        onError: (error) => {
          historyFailure = error;
        },
      },
      { tier: "global" },
    );
    try {
      await expect.poll(() => historyFailure?.name, { timeout: 30_000 }).toBe("E2eeDataError");
      expect(incomplete).toEqual([]);
    } finally {
      stopIncomplete();
    }
  } finally {
    for (const client of clients) await client.shutdown();
    await server.stop();
  }
}, 120_000);

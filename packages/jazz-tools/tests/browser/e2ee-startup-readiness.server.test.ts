import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

function defineEncryptedFixture() {
  const app = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] }),
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.notes.allowRead.always();
    policy.notes.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
  });
  return { app, permissions };
}

it("restores local encrypted readiness at startup after persistent reopen while offline", async () => {
  const server = await getJazzServerInfo(`e2ee-offline-startup-${crypto.randomUUID()}`);
  const { app, permissions } = defineEncryptedFixture();
  const storageKey = `e2ee-device-${crypto.randomUUID()}`;
  const dbName = `e2ee-offline-startup-${crypto.randomUUID()}`;
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let stopped = false;
  try {
    await deploy({ ...server, schema: app, permissions });
    const config = {
      appId: server.appId,
      serverUrl: server.serverUrl,
      account: await acquireBrowserTestAccount(server),
      driver: { type: "persistent" as const, dbName },
      e2ee: {
        app,
        store: {
          async read() {
            return localStorage.getItem(storageKey);
          },
          async update(transform: (current: string | null) => string) {
            await navigator.locks.request(storageKey, () =>
              localStorage.setItem(storageKey, transform(localStorage.getItem(storageKey))),
            );
          },
        },
      },
    };
    db = await createDb(config);
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Accepted local scope" });
    const note = tx.insert(app.notes, { projectId: project.id, body: "Retained locally" });
    await tx.commit().wait({ tier: "global" });
    await db.shutdown();
    db = undefined;
    await stopJazzServer(server.serverUrl);
    stopped = true;

    db = await createDb(config);
    await db.disconnect();
    expect(await db.e2ee.explain({ scope: app.projects, identifier: project.id })).toEqual({
      state: "ready",
    });
    expect(await db.one(app.notes.where({ id: note.id }), { tier: "local" })).toEqual(note);
  } finally {
    await db?.shutdown();
    localStorage.removeItem(storageKey);
    if (!stopped) await stopJazzServer(server.serverUrl);
  }
}, 60_000);

it("does not treat missing retained keys or accepted local history as offline readiness", async () => {
  const server = await getJazzServerInfo(`e2ee-offline-evidence-${crypto.randomUUID()}`);
  const { app, permissions } = defineEncryptedFixture();
  const retainedKey = `e2ee-device-${crypto.randomUUID()}`;
  const absentKey = `e2ee-device-${crypto.randomUUID()}`;
  const dbName = `e2ee-offline-evidence-${crypto.randomUUID()}`;
  const emptyDbName = `e2ee-offline-empty-history-${crypto.randomUUID()}`;
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let unexpectedlyOpened: Awaited<ReturnType<typeof createDb>> | undefined;
  let stopped = false;
  try {
    await deploy({ ...server, schema: app, permissions });
    const account = await acquireBrowserTestAccount(server);
    const config = {
      appId: server.appId,
      serverUrl: server.serverUrl,
      account,
      driver: { type: "persistent" as const, dbName },
      e2ee: {
        app,
        store: {
          async read() {
            return localStorage.getItem(retainedKey);
          },
          async update(transform: (current: string | null) => string) {
            await navigator.locks.request(retainedKey, () =>
              localStorage.setItem(retainedKey, transform(localStorage.getItem(retainedKey))),
            );
          },
        },
      },
    };
    db = await createDb(config);
    const tx = db.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Local evidence" });
    tx.insert(app.notes, { projectId: project.id, body: "Key required" });
    await tx.commit().wait({ tier: "global" });
    await db.shutdown();
    db = undefined;
    await stopJazzServer(server.serverUrl);
    stopped = true;

    await expect(
      createDb({
        ...config,
        e2ee: {
          app,
          store: {
            async read() {
              return localStorage.getItem(absentKey);
            },
            async update(transform: (current: string | null) => string) {
              await navigator.locks.request(absentKey, () =>
                localStorage.setItem(absentKey, transform(localStorage.getItem(absentKey))),
              );
            },
          },
        },
      }).then((opened) => {
        unexpectedlyOpened = opened;
        return "unexpectedly opened";
      }),
    ).rejects.toThrow();

    await expect(
      createDb({
        ...config,
        driver: { type: "persistent" as const, dbName: emptyDbName },
      }).then((opened) => {
        unexpectedlyOpened = opened;
        return "unexpectedly opened";
      }),
    ).rejects.toThrow();
  } finally {
    await unexpectedlyOpened?.shutdown();
    await db?.shutdown();
    localStorage.removeItem(retainedKey);
    localStorage.removeItem(absentKey);
    if (!stopped) await stopJazzServer(server.serverUrl);
  }
}, 60_000);

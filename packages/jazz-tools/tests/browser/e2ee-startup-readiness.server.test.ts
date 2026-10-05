import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { E2eeHistoryUnavailable } from "../../src/e2ee/history-reader.js";
import { attachE2ee, prepareE2eeStartup } from "../../src/e2ee/lifecycle.js";
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

it("refuses encrypted readiness without retained keys or accepted history after confirmed disconnect", async () => {
  const server = await getJazzServerInfo(`e2ee-offline-evidence-${crypto.randomUUID()}`);
  const { app, permissions } = defineEncryptedFixture();
  const retainedKey = `e2ee-device-${crypto.randomUUID()}`;
  const absentKey = `e2ee-device-${crypto.randomUUID()}`;
  const dbName = `e2ee-offline-evidence-${crypto.randomUUID()}`;
  const emptyDbName = `e2ee-offline-empty-history-${crypto.randomUUID()}`;
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let emptyHistoryOwner: Awaited<ReturnType<typeof createDb>> | undefined;
  const foregrounds: Awaited<ReturnType<typeof createDb>>[] = [];
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
    await db.disconnect();
    emptyHistoryOwner = await createDb({
      ...config,
      e2ee: undefined,
      driver: { type: "persistent" as const, dbName: emptyDbName },
    });
    await emptyHistoryOwner.all(app.projects, { tier: "local" });
    await emptyHistoryOwner.disconnect();
    await stopJazzServer(server.serverUrl);
    stopped = true;

    // Initialize real followers before testing the startup readiness gate.
    // Constructor enrollment without keys may otherwise wait for online authority.
    const withoutKeys = await createDb({ ...config, e2ee: undefined });
    foregrounds.push(withoutKeys);
    await withoutKeys.all(app.projects, { tier: "local" });
    attachE2ee(
      withoutKeys,
      account,
      {
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
      "dev",
    );
    await expect(prepareE2eeStartup(withoutKeys)).rejects.toBeInstanceOf(E2eeHistoryUnavailable);

    const withoutHistory = await createDb({
      ...config,
      e2ee: undefined,
      driver: { type: "persistent" as const, dbName: emptyDbName },
    });
    foregrounds.push(withoutHistory);
    await withoutHistory.all(app.projects, { tier: "local" });
    attachE2ee(withoutHistory, account, config.e2ee, "dev");
    await expect(prepareE2eeStartup(withoutHistory)).rejects.toBeInstanceOf(E2eeHistoryUnavailable);
  } finally {
    await Promise.all(foregrounds.map((foreground) => foreground.shutdown()));
    await db?.shutdown();
    await emptyHistoryOwner?.shutdown();
    localStorage.removeItem(retainedKey);
    localStorage.removeItem(absentKey);
    if (!stopped) await stopJazzServer(server.serverUrl);
  }
}, 60_000);

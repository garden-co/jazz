import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createBrowserKeyEnvelope } from "./browser.js";

function signal() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

it("accepts only one simultaneous first encrypted writer even when root updates are permitted", async () => {
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
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    // The absence precondition, not a denied update policy, must stop the loser.
    policy.__e2ee_spaces.allowUpdate.always();
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const reached = [signal(), signal()];
  const resume = signal();
  let armed = false;
  try {
    const target = { serverUrl: server.url, appId: server.appId, adminSecret: server.adminSecret };
    await deploy({ ...target, schema: oldApp, permissions: oldPermissions });
    const creator = await createDb(await localAccountConfig(server.appId, server.url));
    clients.push(creator);
    const project = await creator
      .insert(oldApp.projects, { title: "Legacy scope" })
      .wait({ tier: "global" });
    await deploy({
      ...target,
      schema: app,
      permissions,
      migration: s.defineMigration({ from: before, to: after, createTables: { notes: true } }),
    });
    const keys = await createBrowserKeyEnvelope();
    const accounts = await Promise.all([
      localAccountConfig(server.appId, server.url),
      localAccountConfig(server.appId, server.url),
    ]);
    const writers: Awaited<ReturnType<typeof createDb>>[] = [];
    for (const [index, account] of accounts.entries()) {
      let saved: string | null = null;
      const db = await createDb({
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
          crypto: {
            keyEnvelope: {
              ...keys,
              async wrap(secret, context, plaintext) {
                if (armed) {
                  reached[index]!.resolve();
                  await resume.promise;
                }
                return keys.wrap(secret, context, plaintext);
              },
            },
          },
        },
      });
      clients.push(db);
      writers.push(db);
      await db.e2ee.devices.list();
    }
    armed = true;
    const writes = writers.map((db, index) =>
      db.insert(app.notes, {
        projectId: project.id,
        body: `Writer ${index}`,
      }),
    );
    const pending = writes.map((write) => write.wait({ tier: "global" }));
    const settled = Promise.allSettled(pending);
    await Promise.race([
      Promise.all(reached.map((gate) => gate.promise)),
      ...pending.map(async (operation) => {
        await operation;
        throw new Error("Write completed before both absence reads were held");
      }),
    ]);
    armed = false;
    resume.resolve();
    const outcomes = await settled;
    expect(outcomes.filter((outcome) => outcome.status === "fulfilled")).toHaveLength(1);
    const winnerIndex = outcomes.findIndex((outcome) => outcome.status === "fulfilled");
    const loserIndex = 1 - winnerIndex;
    expect(outcomes[loserIndex]).toMatchObject({
      status: "rejected",
      reason: { code: "exclusive_conflict" },
    });
    const winner = writers[winnerIndex]!;
    const loser = writers[loserIndex]!;
    const roots = await winner.all(app.__e2ee_spaces, { tier: "global" });
    expect(roots).toHaveLength(1);
    expect(roots[0]).toMatchObject({ accountId: accounts[winnerIndex]!.account.id });
    expect(await winner.all(app.__e2ee_space_grants, { tier: "global" })).toMatchObject([
      { spaceId: roots[0]!.id, recipientId: accounts[winnerIndex]!.account.id },
    ]);
    expect(await winner.all(app.notes, { tier: "global" })).toEqual([writes[winnerIndex]!.value]);
    await expect(
      loser.one(app.notes.where({ id: writes[winnerIndex]!.value.id }), { tier: "global" }),
    ).rejects.toMatchObject({ code: "key-not-shared" });
    await expect(
      loser
        .insert(app.notes, { projectId: project.id, body: "No retained provisional key" })
        .wait({ tier: "global" }),
    ).rejects.toMatchObject({ code: "key-not-shared" });
    expect(await winner.all(app.notes.select("id"), { tier: "global" })).toEqual([
      { id: writes[winnerIndex]!.value.id },
    ]);
  } finally {
    armed = false;
    resume.resolve();
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);

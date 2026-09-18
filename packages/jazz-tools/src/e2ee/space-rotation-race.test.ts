import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createBrowserKeyEnvelope } from "./browser.js";

function signal() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

it("accepts one competing rotation and distributes only its epoch to remaining accounts", async () => {
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
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const reached = [signal(), signal()];
  const resume = signal();
  let armed = false;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...policies },
    });
    const accounts = await Promise.all(
      Array.from({ length: 3 }, () => localAccountConfig(server.appId, server.url)),
    );
    expect(new Set(accounts.map((account) => account.account.id)).size).toBe(3);
    const keys = await createBrowserKeyEnvelope();
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
                if (armed && index < 2) {
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
      await db.e2ee.devices.list();
    }
    const owner = clients[0]!;
    const removed = clients[2]!;
    const project = await owner
      .insert(app.projects, { title: "Contended rotation" })
      .wait({ tier: "global" });
    for (const account of accounts)
      await owner.e2ee.spaces.grant(app.projects, project.id, account.account.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    for (const client of clients)
      expect(await client.e2ee.explain(target)).toEqual({ state: "ready" });
    const [root] = await owner.all(app.__e2ee_spaces, { tier: "edge" });
    expect(root).toBeDefined();
    // The departing account can record its removal under this fixture's policy,
    // but cannot produce a successor; neither remaining holder has loaded it yet.
    await removed.e2ee.spaces.revoke(app.projects, project.id, accounts[2]!.account.id).wait();
    expect(await owner.all(app.__e2ee_space_successors, { tier: "edge" })).toEqual([]);
    const remaining = clients.slice(0, 2);
    armed = true;
    const pending = remaining.map((client) => client.e2ee.explain(target));
    const settled = Promise.allSettled(pending);
    await Promise.race([
      Promise.all(reached.map((gate) => gate.promise)),
      ...pending.map(async (operation) => {
        await operation;
        throw new Error("Rotation completed before both predecessor reads were held");
      }),
    ]);
    armed = false;
    resume.resolve();
    const outcomes = await settled;
    expect(outcomes.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    expect(outcomes.filter((result) => result.status === "rejected")).toHaveLength(1);
    const rejected = outcomes.find((result) => result.status === "rejected");
    if (rejected?.status !== "rejected") throw new Error("Missing rejected rotation");
    expect(rejected.reason).toMatchObject({ code: "exclusive_conflict" });
    const successors = await owner.all(app.__e2ee_space_successors, { tier: "edge" });
    expect(successors).toHaveLength(1);
    expect(successors[0]).toMatchObject({ spaceId: root!.id, predecessor: root!.epochId });
    expect(successors[0]!.epochId).not.toBe(root!.epochId);
    for (const client of remaining)
      expect(await client.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await removed.e2ee.explain(target)).toMatchObject({ state: "refused" });
    expect(await owner.all(app.__e2ee_space_successors, { tier: "edge" })).toEqual(successors);
    const deliveries = await owner.all(app.__e2ee_space_deliveries, { tier: "edge" });
    const current = deliveries.filter((row) => row.epochId === successors[0]!.epochId);
    expect(current.map((row) => row.recipientAccountId).sort()).toEqual(
      accounts
        .slice(0, 2)
        .map((account) => account.account.id)
        .sort(),
    );
    expect(
      deliveries.every(
        (row) => row.epochId === root!.epochId || row.epochId === successors[0]!.epochId,
      ),
    ).toBe(true);
  } finally {
    resume.resolve();
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 180_000);

import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createBrowserCrypto } from "./browser.js";

it("replays successive space rotations after regrant and delivers history to a newly approved device", async () => {
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
      permissions: {
        ...deviceRequestPermissions,
        projects: policies.projects!,
        __e2ee_spaces: policies.__e2ee_spaces!,
        __e2ee_space_grants: policies.__e2ee_space_grants!,
        __e2ee_space_deliveries: policies.__e2ee_space_deliveries!,
        __e2ee_space_successors: policies.__e2ee_space_successors!,
      },
    });
    const alice = await localAccountConfig(server.appId, server.url);
    const bob = await localAccountConfig(server.appId, server.url);
    const creator = await createDb({ ...alice, e2ee: { app, store: store() } });
    const removed = await createDb({ ...bob, e2ee: { app, store: store() } });
    clients.push(creator, removed);
    const [first] = await creator.e2ee.devices.list();
    const [bobDevice] = await removed.e2ee.devices.list();
    const remaining = await createDb({ ...alice, e2ee: { app, store: store() } });
    clients.push(remaining);
    const request = (await remaining.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    await creator.e2ee.devices.approve(request.id).wait();
    const project = await creator
      .insert(app.projects, { title: "Revocable scope" })
      .wait({ tier: "global" });
    await creator.e2ee.spaces.grant(app.projects, project.id, alice.account.id).wait();
    await creator.e2ee.spaces.grant(app.projects, project.id, bob.account.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    expect(await removed.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await remaining.e2ee.explain(target)).toEqual({ state: "ready" });
    const root = await creator.one(app.__e2ee_spaces.where({ identifier: project.id }), {
      tier: "edge",
    });
    const revoke = creator.e2ee.spaces.revoke(app.projects, project.id, bob.account.id);
    expect(revoke).not.toBeInstanceOf(Promise);
    await revoke.wait();
    expect(await removed.e2ee.explain(target)).toMatchObject({ state: "refused" });
    expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(await remaining.e2ee.explain(target)).toEqual({ state: "ready" });
    const successors = await creator.all(app.__e2ee_space_successors.where({ spaceId: root!.id }), {
      tier: "edge",
    });
    expect(successors).toHaveLength(1);
    expect(successors[0]!.predecessor).toBe(root!.epochId);
    expect(successors[0]!.epochId).not.toBe(root!.epochId);
    const deliveries = await creator.all(
      app.__e2ee_space_deliveries.where({ spaceId: root!.id, epochId: successors[0]!.epochId }),
      { tier: "edge" },
    );
    expect(deliveries.map((row) => row.recipientDeviceId).sort()).toEqual(
      [first!.id, request.id].sort(),
    );
    expect(deliveries.some((row) => row.recipientDeviceId === bobDevice!.id)).toBe(false);

    // Re-adding Bob shares the current history, but does not create a new epoch.
    await creator.e2ee.spaces.grant(app.projects, project.id, bob.account.id).wait();
    expect(await removed.e2ee.explain(target)).toEqual({ state: "ready" });
    expect(
      await creator.all(app.__e2ee_space_successors.where({ spaceId: root!.id }), {
        tier: "edge",
      }),
    ).toHaveLength(1);

    // A different remaining device authors the next rotation.
    await remaining.e2ee.spaces.revoke(app.projects, project.id, bob.account.id).wait();
    expect(await removed.e2ee.explain(target)).toMatchObject({
      state: "refused",
      reason: "not-a-space-recipient",
    });
    expect(await creator.e2ee.explain(target)).toEqual({ state: "ready" });
    const history = await creator.all(app.__e2ee_space_successors.where({ spaceId: root!.id }), {
      tier: "edge",
    });
    expect(history).toHaveLength(2);
    const latest = history.find((row) => row.predecessor === successors[0]!.epochId)!;
    expect(latest).toBeDefined();
    expect(latest.authorDeviceId).toBe(request.id);
    expect(new Set([root!.epochId, ...history.map((row) => row.epochId)]).size).toBe(3);
    const latestDeliveries = await creator.all(
      app.__e2ee_space_deliveries.where({ spaceId: root!.id, epochId: latest.epochId }),
      { tier: "edge" },
    );
    expect(latestDeliveries.map((row) => row.recipientDeviceId).sort()).toEqual(
      [first!.id, request.id].sort(),
    );

    // This device has never seen either old epoch. Its latest delivery must
    // suffice to validate the complete predecessor chain.
    const crypto = await createBrowserCrypto();
    const historyError = new Error("History provider unavailable");
    let failHistory = true;
    let historySteps = 0;
    const newcomer = await createDb({
      ...alice,
      e2ee: {
        app,
        store: store(),
        crypto: {
          ...crypto,
          keyEnvelope: {
            ...crypto.keyEnvelope,
            async unwrap(key, context, envelope) {
              if (
                failHistory &&
                new TextDecoder().decode(context).includes("history") &&
                ++historySteps === 2
              )
                throw historyError;
              return crypto.keyEnvelope.unwrap(key, context, envelope);
            },
          },
        },
      },
    });
    clients.push(newcomer);
    const newRequest = (await newcomer.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    await remaining.e2ee.devices.approve(newRequest.id).wait();
    expect(await remaining.e2ee.explain(target)).toEqual({ state: "ready" });
    // The non-author device must propagate a failure after opening one predecessor.
    await expect(newcomer.e2ee.explain(target)).rejects.toBe(historyError);
    failHistory = false;
    expect(await newcomer.e2ee.explain(target)).toEqual({ state: "ready" });
    const newcomerDeliveries = await creator.all(
      app.__e2ee_space_deliveries.where({ spaceId: root!.id, recipientDeviceId: newRequest.id }),
      { tier: "edge" },
    );
    expect(newcomerDeliveries.map((row) => row.epochId)).toEqual([latest.epochId]);
    expect(await removed.e2ee.explain(target)).toMatchObject({ state: "refused" });
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 180_000);

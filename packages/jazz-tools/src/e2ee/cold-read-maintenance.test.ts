import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createBrowserKeyEnvelope } from "./browser.js";
import { spaceSchema } from "./spaces.js";
import { deviceRequestSchema } from "./device-requests.js";

it("delivers a cold encrypted subscription while optional device delivery is pending", async () => {
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), title: s.string() },
        { project: s.rel("projects", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
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
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  let release = () => {};
  const delivery = new Promise<void>((resolve) => {
    release = resolve;
  });
  let blockDelivery = false;
  let waiting = false;
  let stop: (() => void) | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const alice = await localAccountConfig(server.appId, server.url);
    const bob = await localAccountConfig(server.appId, server.url);
    const owner = await createDb({ ...alice, e2ee: { app, store: store() } });
    clients.push(owner);
    const envelope = await createBrowserKeyEnvelope();
    const contextDecoder = new TextDecoder();
    const reader = await createDb({
      ...bob,
      e2ee: {
        app,
        store: store(),
        crypto: {
          keyEnvelope: {
            ...envelope,
            async seal(recipient, context, secret) {
              if (blockDelivery && contextDecoder.decode(context).includes("jazz.e2ee.space.v1")) {
                waiting = true;
                await delivery;
              }
              return envelope.seal(recipient, context, secret);
            },
          },
        },
      },
    });
    clients.push(reader);
    await owner.e2ee.devices.list();
    await reader.e2ee.devices.list();
    const tx = owner.beginExclusiveTransaction();
    const project = tx.insert(app.projects, { title: "Shared project" });
    const note = tx.insert(app.notes, { projectId: project.id, title: "Shared note" });
    await tx.commit().wait({ tier: "global" });
    await owner.e2ee.spaces.grant(app.projects, project.id, bob.account.id).wait();
    // Only the instrumented reader may backfill the new device. The owner's
    // background reconciliation must not satisfy delivery before our barrier.
    await owner.shutdown();
    // Approval may start background backfill before its wait resolves. Hold
    // space envelopes now, without blocking the account approval envelopes.
    blockDelivery = true;
    const secondDevice = await createDb({ ...bob, e2ee: { app, store: store() } });
    clients.push(secondDevice);
    const pending = (await secondDevice.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    await reader.e2ee.devices.approve(pending.id).wait();
    const snapshots: unknown[][] = [];
    let failure: Error | undefined;
    stop = reader.subscribe(
      app.notes.where({ projectId: project.id, title: note.title }),
      {
        onUpdate: (rows) => snapshots.push(rows),
        onError: (error) => {
          failure = error;
        },
      },
      { tier: "global" },
    );
    await expect.poll(() => waiting, { timeout: 10_000 }).toBe(true);
    // Delivery is deliberately still pending when the verified result arrives.
    await expect.poll(() => failure ?? snapshots.at(-1), { timeout: 10_000 }).toEqual([note]);
  } finally {
    stop?.();
    blockDelivery = false;
    release();
    for (const client of clients) await client.shutdown();
    await server.stop();
  }
}, 60_000);

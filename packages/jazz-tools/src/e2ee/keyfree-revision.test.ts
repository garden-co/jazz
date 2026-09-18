import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeCrypto } from "./native.js";
import { deviceRequestApp as app, deviceRequestPermissions } from "./device-requests.js";

it("includes an accepted key-free approval when rotation precedes its delivery", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  let hold = false;
  let release!: () => void;
  const resumed = new Promise<void>((resolve) => {
    release = resolve;
  });
  let reached!: () => void;
  const paused = new Promise<void>((resolve) => {
    reached = resolve;
  });
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: deviceRequestPermissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    const adapters = await createNativeCrypto();
    const open = async (pause: boolean) => {
      let saved: string | null = null;
      const db = await createDb({
        ...account,
        e2ee: {
          store: {
            async read() {
              return saved;
            },
            async update(transform) {
              saved = transform(saved);
            },
          },
          crypto: {
            ...adapters,
            keyEnvelope: {
              ...adapters.keyEnvelope,
              async seal(recipient, context, secret) {
                if (pause && hold && new TextDecoder().decode(context).includes("delivery")) {
                  reached();
                  await resumed;
                }
                return adapters.keyEnvelope.seal(recipient, context, secret);
              },
            },
          },
        },
      });
      clients.push(db);
      return db;
    };
    const first = await open(true);
    const [creator] = await first.e2ee.devices.list();
    const second = await open(false);
    const secondId = (await second.e2ee.devices.list()).find((d) => d.id !== creator!.id)!.id;
    await first.e2ee.devices.approve(secondId).wait();
    const third = await open(false);
    const thirdId = (await third.e2ee.devices.list()).find(
      (d) => d.id !== creator!.id && d.id !== secondId,
    )!.id;
    hold = true;
    const approval = first.e2ee.devices.approve(thirdId);
    await paused;
    const [challenge] = await second.all(
      app.__e2ee_device_challenges.where({ deviceId: thirdId }),
      { tier: "global" },
    );
    expect(
      await second.all(app.__e2ee_device_approvals.where({ id: challenge!.id }), {
        tier: "global",
      }),
    ).toHaveLength(1);
    expect(
      await second.all(app.__e2ee_device_deliveries.where({ id: challenge!.id }), {
        tier: "global",
      }),
    ).toHaveLength(0);
    await second.e2ee.devices.revoke(creator!.id).wait();
    const [successor] = await second.all(app.__e2ee_account_successors, { tier: "global" });
    expect(JSON.parse(new TextDecoder().decode(successor!.revision))).toContain(challenge!.id);
    expect(JSON.parse(new TextDecoder().decode(successor!.membership))).toContain(thirdId);
    release();
    await approval.wait();
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: thirdId, state: "active" }),
    );
  } finally {
    release();
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 30_000);

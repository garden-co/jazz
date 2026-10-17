import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeCrypto } from "./native.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("rejects an approval prepared before its author was revoked", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  let release!: () => void;
  const resumed = new Promise<void>((resolve) => {
    release = resolve;
  });
  let reached!: () => void;
  const paused = new Promise<void>((resolve) => {
    reached = resolve;
  });
  let hold = false;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: deviceRequestApp,
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
            deviceSigner: {
              ...adapters.deviceSigner,
              async sign(key, record) {
                const signed = await adapters.deviceSigner.sign(key, record);
                if (
                  pause &&
                  hold &&
                  new TextDecoder().decode(record).includes("approval-signature:")
                ) {
                  reached();
                  await resumed;
                }
                return signed;
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
    await second.e2ee.devices.revoke(creator!.id).wait();
    release();
    await expect(approval.wait()).rejects.toThrow(/stale|revok|conflict/i);
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: thirdId, state: "pending" }),
    );
  } finally {
    release();
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 30_000);

import { expect } from "vitest";
import type { Db } from "../../runtime/db.js";
import { createDb } from "../../runtime/default-create-db.js";
import { localAccountConfig } from "../../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../../testing/index.js";
import { deviceRequestApp as app, deviceRequestPermissions } from "../device-requests.js";
import { createNativeCrypto } from "../native.js";
import type { CryptoAdapters } from "../types.js";

// Prepared history always uses real approval, recovery roots and rotation.
// Each case opens a fresh client and adapters; callers choose whether to share history.
export async function createRotatedRecovery() {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  const shutdown = async () => {
    try {
      await Promise.all(clients.map((client) => client.shutdown()));
    } finally {
      await server.stop();
    }
  };
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: deviceRequestPermissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    const native = await createNativeCrypto();
    const open = async (crypto = native) => {
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
          crypto,
        },
      });
      clients.push(db);
      return { db, saved: () => saved };
    };
    const { db: owner } = await open();
    const [creator] = await owner.e2ee.devices.list();
    const { db: second } = await open();
    const removed = (await second.e2ee.devices.list()).find((row) => row.id !== creator!.id)!;
    await owner.e2ee.devices.approve(removed.id).wait();
    const { material } = await owner.e2ee.recovery.create().wait();
    await owner.e2ee.recovery.create().wait();
    await owner.e2ee.devices.revoke(removed.id).wait();
    // No other live responder can consume a one-shot fault or alter membership.
    await second.shutdown();
    await owner.shutdown();
    return { native, open, material, creatorId: creator!.id, removedId: removed.id, shutdown };
  } catch (error) {
    await shutdown();
    throw error;
  }
}

export type RotatedRecovery = Awaited<ReturnType<typeof createRotatedRecovery>>;

export async function withRotatedRecovery(
  preparedHistory: RotatedRecovery | undefined,
  adapt: (native: CryptoAdapters) => CryptoAdapters,
  run: (fixture: {
    client: Db;
    material: string;
    creatorId: string;
    removedId: string;
    saved: () => string | null;
  }) => Promise<void>,
) {
  const history = preparedHistory ?? (await createRotatedRecovery());
  try {
    const { db: client, saved } = await history.open(adapt(history.native));
    try {
      expect(await client.all(app.__e2ee_recovery_protectors, { tier: "remote" })).toHaveLength(2);
      await run({ client, saved, ...history });
    } finally {
      await client.shutdown();
    }
  } finally {
    if (!preparedHistory) await history.shutdown();
  }
}

// Observe persisted enrolment/publication through the same Db queries available
// to an application, rather than spying on recovery's private implementation.
export async function recoveryRecords(client: Db) {
  return Promise.all([
    client.all(app.__e2ee_device_requests, { tier: "remote" }),
    client.all(app.__e2ee_device_challenges, { tier: "remote" }),
    client.all(app.__e2ee_device_approvals, { tier: "remote" }),
    client.all(app.__e2ee_public_device_approvals, { tier: "remote" }),
    client.all(app.__e2ee_device_deliveries, { tier: "remote" }),
  ]);
}

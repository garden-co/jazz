import { expect, it, vi } from "vitest";
import type { Db, E2eeTransactionScope } from "../runtime/db.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { DeviceApproval } from "./device-approval.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";
import { createNativeCrypto } from "./native.js";

it("refreshes a warm snapshot when another device revokes its reader before acceptance", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  let resume!: () => void;
  const resumed = new Promise<void>((resolve) => {
    resume = resolve;
  });
  let captured!: () => void;
  const snapshotCaptured = new Promise<void>((resolve) => {
    captured = resolve;
  });
  // Delay an actual covered read, preserving its rows, metadata and transaction.
  const prototype = DeviceApproval.prototype as unknown as {
    readSnapshot(this: { db: Db }, tx: E2eeTransactionScope): Promise<unknown>;
  };
  const original = prototype.readSnapshot;
  let gate = false;
  let first: Db | undefined;
  const snapshots = vi
    .spyOn(prototype, "readSnapshot")
    .mockImplementation(async function (this: { db: Db }, tx) {
      const result = await original.call(this, tx);
      if (this.db === first && gate) {
        gate = false;
        captured();
        await resumed;
      }
      return result;
    });
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: deviceRequestApp,
      permissions: deviceRequestPermissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    const crypto = await createNativeCrypto();
    const open = async () => {
      let stored: string | null = null;
      const db = await createDb({
        ...account,
        e2ee: {
          crypto,
          store: {
            async read() {
              return stored;
            },
            async update(transform) {
              stored = transform(stored);
            },
          },
        },
      });
      clients.push(db);
      return db;
    };
    first = await open();
    const [creator] = await first.e2ee.devices.list();
    const second = await open();
    const pending = (await second.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    await first.e2ee.devices.approve(pending.id).wait();
    gate = true;
    const listing = first.e2ee.devices.list();
    listing.catch(() => {});
    await snapshotCaptured;
    await second.e2ee.devices.revoke(creator!.id).wait();
    resume();
    expect(await listing).toContainEqual(
      expect.objectContaining({ id: creator!.id, state: "revoked", keyReadiness: "not-verified" }),
    );
    await expect(first.e2ee.devices.approve(pending.id).wait()).rejects.toThrow(/revok/i);
  } finally {
    resume();
    snapshots.mockRestore();
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 60_000);

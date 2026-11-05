import { expect, it, vi } from "vitest";
import type { Db, E2eeTransactionScope } from "../runtime/db.js";
import { PersistedWriteRejectedError } from "../runtime/client.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { DeviceApproval } from "./device-approval.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";
import { createNativeCrypto } from "./native.js";

it("retries a conflicted background response from fresh authority without hiding signer failures", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  let first: Db;
  let target: Db | undefined;
  let invalidate = 0;
  let reportFailure!: (error: unknown) => void;
  let failedResponse = new Promise<unknown>((resolve) => {
    reportFailure = resolve;
  });
  const prototype = DeviceApproval.prototype as unknown as {
    readSnapshot(this: { db: Db }, tx: E2eeTransactionScope): Promise<unknown>;
    respond(this: { db: Db }, challengeId: string, snapshot?: unknown): Promise<void>;
  };
  const readSnapshot = prototype.readSnapshot;
  const respond = prototype.respond;
  const reads = vi
    .spyOn(prototype, "readSnapshot")
    .mockImplementation(async function (this: { db: Db }, tx) {
      const result = await readSnapshot.call(this, tx);
      if (this.db === target && invalidate > 0) {
        invalidate--;
        // Change an actually covered predicate after its read, before acceptance.
        // Requests need no handshake, so these writes cannot start other responders.
        const request = (
          await first.all(deviceRequestApp.__e2ee_device_requests, {
            tier: "remote",
          })
        )[0]!;
        await first
          .insert(deviceRequestApp.__e2ee_device_requests, {
            publicKey: request.publicKey,
            mechanism: request.mechanism,
            version: request.version,
            challenge: request.challenge,
            signingPublicKey: request.signingPublicKey,
            signingMechanism: request.signingMechanism,
            signingVersion: request.signingVersion,
          })
          .wait({ tier: "global" });
      }
      return result;
    });
  const responses = vi
    .spyOn(prototype, "respond")
    .mockImplementation(async function (this: { db: Db }, challengeId, snapshot) {
      try {
        await respond.call(this, challengeId, snapshot);
      } catch (error) {
        if (this.db === target) reportFailure(error);
        throw error;
      }
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
    const signerFailure = new Error("Device signer is unavailable");
    let failSigning = false;
    const open = async (faultySigner = false) => {
      let stored: string | null = null;
      const db = await createDb({
        ...account,
        e2ee: {
          crypto: {
            ...crypto,
            deviceSigner: {
              ...crypto.deviceSigner,
              async sign(key, message) {
                if (faultySigner && failSigning) throw signerFailure;
                return crypto.deviceSigner.sign(key, message);
              },
            },
          },
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
    await first.e2ee.devices.list();
    const second = await open();
    const pending = (await second.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    target = second;
    invalidate = 3;
    const approval = first.e2ee.devices.approve(pending.id).wait();
    approval.catch(() => {});
    const conflict = await failedResponse;
    expect(conflict).toBeInstanceOf(PersistedWriteRejectedError);
    expect(conflict).toMatchObject({ code: "serialization_conflict" });
    expect(invalidate).toBe(0);
    // The background catch runs after the observed rejection.
    await Promise.resolve();
    await expect(second.e2ee.devices.list()).resolves.toContainEqual(
      expect.objectContaining({ id: pending.id, state: "pending" }),
    );
    await approval;
    expect(await second.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active", keyReadiness: "verified" }),
    );

    const beforeThird = new Set((await first.e2ee.devices.list()).map((device) => device.id));
    const third = await open(true);
    const enrolled = (await third.e2ee.devices.list()).find(
      (device) => !beforeThird.has(device.id),
    )!;
    expect(enrolled).toBeDefined();
    target = third;
    failedResponse = new Promise<unknown>((resolve) => {
      reportFailure = resolve;
    });
    failSigning = true;
    const retry = first.e2ee.devices.approve(enrolled.id).wait();
    retry.catch(() => {});
    expect(await failedResponse).toBe(signerFailure);
    await Promise.resolve();
    failSigning = false;
    await expect(third.e2ee.devices.list()).rejects.toBe(signerFailure);
    await third.e2ee.devices.list();
    await retry;
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: enrolled.id, state: "active", keyReadiness: "verified" }),
    );
  } finally {
    reads.mockRestore();
    responses.mockRestore();
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 60_000);

import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeCrypto, createNativeKeyEnvelope } from "./native.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";
import { accountEpochContext, firstAccountEpoch } from "./first-epoch.js";
import { readAccountMembership } from "./public-membership.js";
import {
  encodeEpochIds,
  encodePublicApprovalRevision,
  publicSuccessorSigningBytes,
} from "./account-successor.js";

it("rejects a correctly sealed but substituted account epoch key", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let record: string | null = null;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: deviceRequestApp,
      permissions: deviceRequestPermissions,
    });
    const keys = await createNativeKeyEnvelope();
    db = await createDb({
      ...(await localAccountConfig(server.appId, server.url)),
      e2ee: {
        store: {
          async read() {
            return record;
          },
          async update(transform) {
            record = transform(record);
          },
        },
        crypto: {
          keyEnvelope: {
            ...keys,
            async seal(publicKey, context, key) {
              // Fault injection at the BYOC seam: valid crypto delivers the wrong
              // secret. This is not a test of the adapter's cryptographic strength.
              const substitute = new TextDecoder()
                .decode(context)
                .includes("__e2ee_account_identities");
              return keys.seal(publicKey, context, substitute ? new Uint8Array(32).fill(7) : key);
            },
          },
        },
      },
    });
    await expect(db.e2ee.devices.list()).rejects.toThrow();
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 15_000);

it("rejects an initial identity referring to another account's device request", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: deviceRequestApp,
      permissions: deviceRequestPermissions,
    });
    const alice = await createDb(await localAccountConfig(server.appId, server.url));
    const bobConfig = await localAccountConfig(server.appId, server.url);
    const bob = await createDb(bobConfig);
    clients.push(alice, bob);
    const request = await alice
      .insert(deviceRequestApp.__e2ee_device_requests, {
        publicKey: new Uint8Array(32).fill(1),
        signingPublicKey: new Uint8Array(32).fill(1),
        signingMechanism: "jazz.sodium.sign",
        signingVersion: 1,
        mechanism: "jazz.sodium.key",
        version: 1,
        challenge: new Uint8Array(32).fill(2),
      })
      .wait({ tier: "global" });
    await expect(
      bob
        .insert(
          deviceRequestApp.__e2ee_account_identities,
          {
            deviceId: request.id,
            epochId: crypto.randomUUID(),
            envelope: new Uint8Array([1]),
            verification: new Uint8Array([2]),
          },
          { id: bobConfig.account.id },
        )
        .wait({ tier: "global" }),
    ).rejects.toThrow(/authori|permission/i);
    await expect(
      bob.all(deviceRequestApp.__e2ee_account_identities, { tier: "remote" }),
    ).resolves.toEqual([]);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 15_000);

it("accepts only one first device and never reinitialises an existing account for a new device", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: deviceRequestApp,
      permissions: deviceRequestPermissions,
    });
    const config = await localAccountConfig(server.appId, server.url);
    const keyEnvelope = await createNativeKeyEnvelope();
    const openNewDevice = async () => {
      let record: string | null = null;
      const db = await createDb({
        ...config,
        e2ee: {
          crypto: { keyEnvelope },
          store: {
            async read() {
              return record;
            },
            async update(transform) {
              record = transform(record);
            },
          },
        },
      });
      clients.push(db);
      return db;
    };
    const [first, second] = await Promise.all([openNewDevice(), openNewDevice()]);
    await Promise.all([first.e2ee.devices.list(), second.e2ee.devices.list()]);
    const devices = await first.e2ee.devices.list();
    expect(devices).toHaveLength(2);
    expect(devices.filter((device) => device.state === "active")).toHaveLength(1);
    expect(devices.filter((device) => device.state === "pending")).toHaveLength(1);
    const activeId = devices.find((device) => device.state === "active")!.id;
    const identities = deviceRequestApp.__e2ee_account_identities;
    const original = await first.one(identities.where({ id: config.account.id }), {
      tier: "remote",
    });
    expect(original).not.toBeNull();
    const settled = await first.exclusiveTransaction(async (tx) => ({
      identities: await tx.allSettledForE2ee(identities.where({ id: config.account.id })),
      roots: await tx.allSettledForE2ee(
        deviceRequestApp.__e2ee_account_roots.where({ accountId: config.account.id }),
      ),
    }));
    const history = await settled.wait({ tier: "global" });
    expect(history.identities.rows).toHaveLength(1);
    expect(history.roots.rows).toHaveLength(1);
    expect(history.roots.rows[0]).toMatchObject({
      deviceId: activeId,
      epochId: original!.epochId,
      ledgerVersion: 1,
    });
    // Concurrent first-device producers publish one identity/root activation,
    // not an identity followed by an independently accepted projection.
    expect(history.roots.settlements[0]!.transactionId).toBe(
      history.identities.settlements[0]!.transactionId,
    );
    expect(history.roots.settlements[0]!.position).toBe(
      history.identities.settlements[0]!.position,
    );
    expect(
      (await second.e2ee.devices.list())
        .filter((device) => device.state === "active")
        .map((device) => device.id),
    ).toEqual([activeId]);
    for (const client of [first, second]) {
      const replacement = {
        deviceId: activeId,
        epochId: crypto.randomUUID(),
        envelope: new Uint8Array([1]),
        verification: new Uint8Array([2]),
      };
      for (const replace of [
        () => client.insert(identities, replacement, { id: config.account.id }),
        () => client.upsert(identities, config.account.id, replacement),
        () => client.restore(identities, config.account.id, replacement),
        () => client.update(identities, config.account.id, replacement),
        () => client.delete(identities, config.account.id),
      ]) {
        await expect(async () => replace().wait({ tier: "global" })).rejects.toThrow(
          /already exists|authori|policy denied|not deleted|not_deleted|conflict/i,
        );
      }
    }
    await expect(
      first.one(identities.where({ id: config.account.id }), { tier: "remote" }),
    ).resolves.toEqual(original);
    await first.shutdown();
    await second.shutdown();
    const third = await openNewDevice();
    const afterLoss = await third.e2ee.devices.list();
    expect(afterLoss).toHaveLength(3);
    expect(
      afterLoss.filter((device) => device.state === "active").map((device) => device.id),
    ).toEqual([activeId]);
    expect(afterLoss.filter((device) => device.state === "pending")).toHaveLength(2);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 15_000);

it("repairs a missing initial root without granting authority to pre-root successors", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Db | undefined;
  const secrets: Uint8Array[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: deviceRequestApp,
      permissions: deviceRequestPermissions,
    });
    const config = await localAccountConfig(server.appId, server.url);
    db = await createDb(config);
    const adapters = await createNativeCrypto();
    const pair = await adapters.keyEnvelope.createKeyPair();
    secrets.push(pair.privateKey);
    const signing = await adapters.deviceSigner.createKeyPair();
    secrets.push(signing.privateKey);
    const device = {
      ...pair,
      signing,
      id: crypto.randomUUID(),
      challenge: crypto.getRandomValues(new Uint8Array(32)),
    };
    const columns = {
      publicKey: device.publicKey,
      mechanism: adapters.keyEnvelope.mechanism.id,
      version: adapters.keyEnvelope.mechanism.version,
      signingPublicKey: signing.publicKey,
      signingMechanism: adapters.deviceSigner.mechanism.id,
      signingVersion: adapters.deviceSigner.mechanism.version,
    };
    await db
      .insert(
        deviceRequestApp.__e2ee_device_requests,
        {
          ...columns,
          challenge: device.challenge,
        },
        { id: device.id },
      )
      .wait({ tier: "global" });
    await db
      .insert(deviceRequestApp.__e2ee_device_keys, {
        ...columns,
        deviceId: device.id,
      })
      .wait({ tier: "global" });
    const application = "missing-initial-root";
    const epochId = crypto.randomUUID();
    const secret = crypto.getRandomValues(new Uint8Array(32));
    secrets.push(secret);
    const identity = await db
      .insert(
        deviceRequestApp.__e2ee_account_identities,
        {
          deviceId: device.id,
          epochId,
          ledgerVersion: 1,
          envelope: await adapters.keyEnvelope.seal(
            device.publicKey,
            accountEpochContext(application, config.account.id, epochId, device.id),
            secret,
          ),
          verification: await adapters.keyEnvelope.wrap(
            secret,
            accountEpochContext(application, config.account.id, epochId, "", "verification"),
            new Uint8Array(32),
          ),
        },
        { id: config.account.id },
      )
      .wait({ tier: "global" });
    const successor = {
      id: crypto.randomUUID(),
      accountId: config.account.id,
      predecessor: epochId,
      epochId: crypto.randomUUID(),
      signerId: device.id,
      removedDeviceId: device.id,
      membership: encodeEpochIds([]),
      revision: encodePublicApprovalRevision([]),
    };
    const { id, ...successorColumns } = successor;
    await db
      .insert(
        deviceRequestApp.__e2ee_public_account_successors,
        {
          ...successorColumns,
          signature: await adapters.deviceSigner.sign(
            signing.privateKey,
            publicSuccessorSigningBytes(application, successor),
          ),
        },
        { id },
      )
      .wait({ tier: "global" });
    expect(await db.all(deviceRequestApp.__e2ee_account_roots, { tier: "global" })).toEqual([]);
    expect(
      await firstAccountEpoch(
        db,
        config.account.id,
        application,
        device,
        adapters.keyEnvelope,
        () => {},
      ),
    ).toBe(device.id);
    expect(
      await db.one(deviceRequestApp.__e2ee_account_identities.where({ id: config.account.id }), {
        tier: "global",
      }),
    ).toEqual(identity);
    const state = await readAccountMembership(
      db,
      config.account.id,
      application,
      adapters.deviceSigner,
    );
    expect(state.epochId).toBe(epochId);
    expect(state.active).toEqual(new Set([device.id]));
    expect(state.successorIds).toEqual(new Set());
    // Reopening is idempotent and never republishes an already present root.
    await firstAccountEpoch(
      db,
      config.account.id,
      application,
      device,
      adapters.keyEnvelope,
      () => {},
    );
    expect(await db.all(deviceRequestApp.__e2ee_account_roots, { tier: "global" })).toHaveLength(1);
  } finally {
    for (const secret of secrets) secret.fill(0);
    await db?.shutdown();
    await server.stop();
  }
}, 15_000);

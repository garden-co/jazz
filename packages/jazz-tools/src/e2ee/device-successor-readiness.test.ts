import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeCrypto } from "./native.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("keeps accepted device membership visible when rotated key delivery fails authentication", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  let corruptSuccessorDelivery = false;
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
    const open = async (faultyDelivery = false) => {
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
              async open(device, context, envelope) {
                const secret = await adapters.keyEnvelope.open(device, context, envelope);
                if (
                  faultyDelivery &&
                  corruptSuccessorDelivery &&
                  new TextDecoder().decode(context).includes("jazz.e2ee.account-successor.v1")
                ) {
                  secret[0] = secret[0]! ^ 1;
                }
                return secret;
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
    const second = await open();
    const removed = (await second.e2ee.devices.list()).find((device) => device.id !== creator!.id)!;
    await first.e2ee.devices.approve(removed.id).wait();
    await first.e2ee.devices.revoke(removed.id).wait();
    corruptSuccessorDelivery = true;
    expect(await first.e2ee.devices.list()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: creator!.id, state: "active", keyReadiness: "not-verified" }),
        expect.objectContaining({ id: removed.id, state: "revoked", keyReadiness: "not-verified" }),
      ]),
    );
    const third = await open();
    const pending = (await third.e2ee.devices.list()).find((device) => device.state === "pending")!;
    await expect(first.e2ee.devices.approve(pending.id).wait()).rejects.toThrow();
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "pending" }),
    );
    corruptSuccessorDelivery = false;
    expect(await first.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: creator!.id, state: "active", keyReadiness: "verified" }),
    );
    await first.e2ee.devices.approve(pending.id).wait();
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active", keyReadiness: "verified" }),
    );
  } finally {
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 60_000);

it("rejects device listing with the original signer error during successor history authentication", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  const signerError = new Error("Successor history signer unavailable");
  let failHistoryVerification = false;
  let successorHistoryUnwrapped = false;
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
    const decoder = new TextDecoder();
    const open = async (faultyVerifier = false) => {
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
              async unwrap(key, context, envelope) {
                const secret = await adapters.keyEnvelope.unwrap(key, context, envelope);
                if (faultyVerifier && failHistoryVerification) {
                  const decoded = decoder.decode(context);
                  if (
                    decoded.includes("jazz.e2ee.account-successor.v1") &&
                    decoded.includes("history")
                  ) {
                    successorHistoryUnwrapped = true;
                  }
                }
                return secret;
              },
            },
            deviceSigner: {
              ...adapters.deviceSigner,
              async verify(publicKey, record, signature) {
                if (faultyVerifier && failHistoryVerification && successorHistoryUnwrapped)
                  throw signerError;
                return adapters.deviceSigner.verify(publicKey, record, signature);
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
    const second = await open();
    const removed = (await second.e2ee.devices.list()).find((device) => device.id !== creator!.id)!;
    await first.e2ee.devices.approve(removed.id).wait();
    await first.e2ee.devices.revoke(removed.id).wait();

    failHistoryVerification = true;
    await expect(first.e2ee.devices.list()).rejects.toBe(signerError);
    expect(successorHistoryUnwrapped).toBe(true);

    failHistoryVerification = false;
    successorHistoryUnwrapped = false;
    expect(await first.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: creator!.id, state: "active", keyReadiness: "verified" }),
    );
  } finally {
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 60_000);

it("reports signer failure when a shared device reconciles an accepted proof", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  const signerError = new Error("Accepted proof signer unavailable");
  let releaseResponses!: () => void;
  const responsesReady = new Promise<void>((resolve) => {
    releaseResponses = resolve;
  });
  let reachedVerifier!: () => void;
  const verifierFailed = new Promise<void>((resolve) => {
    reachedVerifier = resolve;
  });
  let responses = 0;
  let rejectProofVerification = true;
  const returnedSecrets: Uint8Array[] = [];
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
    const store = () => {
      let saved: string | null = null;
      return {
        async read() {
          return saved;
        },
        async update(transform: (current: string | null) => string) {
          saved = transform(saved);
        },
      };
    };
    const open = async (localStore = store(), responder = false) => {
      let reconcilingProof = false;
      const db = await createDb({
        ...account,
        e2ee: {
          store: localStore,
          crypto: {
            ...adapters,
            keyEnvelope: {
              ...adapters.keyEnvelope,
              async open(device, context, envelope) {
                const secret = await adapters.keyEnvelope.open(device, context, envelope);
                if (
                  responder &&
                  new TextDecoder().decode(context).includes("jazz.e2ee.device-approval.v1")
                )
                  returnedSecrets.push(secret);
                return secret;
              },
              async unwrap(key, context, envelope) {
                const secret = await adapters.keyEnvelope.unwrap(key, context, envelope);
                const decoded = new TextDecoder().decode(context);
                if (
                  responder &&
                  decoded.includes("jazz.e2ee.device-approval.v1") &&
                  decoded.includes("proof")
                )
                  reconcilingProof = true;
                return secret;
              },
              async wrap(key, context, plaintext) {
                if (responder && new TextDecoder().decode(context).includes("proof")) {
                  if (++responses === 2) releaseResponses();
                  await responsesReady;
                }
                return adapters.keyEnvelope.wrap(key, context, plaintext);
              },
            },
            deviceSigner: {
              ...adapters.deviceSigner,
              async verify(publicKey, context, signature) {
                if (
                  responder &&
                  rejectProofVerification &&
                  reconcilingProof &&
                  new TextDecoder().decode(context).includes("proof-signature")
                ) {
                  reconcilingProof = false;
                  reachedVerifier();
                  throw signerError;
                }
                return adapters.deviceSigner.verify(publicKey, context, signature);
              },
            },
          },
        },
      });
      clients.push(db);
      return db;
    };
    const creator = await open();
    await creator.e2ee.devices.list();
    const sharedStore = store();
    const second = await open(sharedStore, true);
    const pending = (await second.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    const shared = await open(sharedStore, true);
    await shared.e2ee.devices.list();
    await creator.e2ee.devices.approve(pending.id).wait();
    await verifierFailed;
    const reported: unknown[] = [];
    await expect
      .poll(async () => {
        const results = await Promise.allSettled([
          second.e2ee.devices.list(),
          shared.e2ee.devices.list(),
        ]);
        for (const result of results)
          if (result.status === "rejected") reported.push(result.reason);
        return reported;
      })
      .toContain(signerError);
    expect(responses).toBe(2);
    expect(returnedSecrets.every((secret) => secret.every((byte) => byte === 0))).toBe(true);
    rejectProofVerification = false;
    for (const client of [second, shared])
      expect(await client.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: pending.id, state: "active", keyReadiness: "verified" }),
      );
  } finally {
    releaseResponses();
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 60_000);

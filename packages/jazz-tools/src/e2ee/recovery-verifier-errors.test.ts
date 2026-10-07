import { expect, it } from "vitest";
import type { Db } from "../runtime/db.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp as app, deviceRequestPermissions } from "./device-requests.js";
import { E2eeRecoveryError } from "./index.js";
import { createNativeCrypto } from "./native.js";
import type { CryptoAdapters } from "./types.js";

// Each case has its own server, account and adapters. An accepted prior approval
// followed by rotation makes recovery authenticate a real successor's ancestry.
async function withRotatedRecovery(
  adapt: (native: CryptoAdapters) => CryptoAdapters,
  run: (fixture: {
    client: Db;
    material: string;
    creatorId: string;
    removedId: string;
    saved: () => string | null;
  }) => Promise<void>,
) {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
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
    const { db: client, saved } = await open(adapt(native));
    expect(await client.all(app.__e2ee_recovery_protectors, { tier: "remote" })).toHaveLength(2);
    await run({ client, material, creatorId: creator!.id, removedId: removed.id, saved });
  } finally {
    try {
      await Promise.all(clients.map((client) => client.shutdown()));
    } finally {
      await server.stop();
    }
  }
}

// Observe persisted enrolment/publication through the same Db queries available
// to an application, rather than spying on recovery's private implementation.
async function recoveryRecords(client: Db) {
  return Promise.all([
    client.all(app.__e2ee_device_requests, { tier: "remote" }),
    client.all(app.__e2ee_device_challenges, { tier: "remote" }),
    client.all(app.__e2ee_device_approvals, { tier: "remote" }),
    client.all(app.__e2ee_public_device_approvals, { tier: "remote" }),
    client.all(app.__e2ee_device_deliveries, { tier: "remote" }),
  ]);
}

for (const operation of ["status", "use"] as const) {
  for (const input of ["explicit", "protected"] as const) {
    it.each(["plain", "public-code-collision"] as const)(
      `aborts ${input} recovery ${operation} on a one-shot %s ancestry verifier error`,
      async (kind) => {
        const failure =
          kind === "plain"
            ? new Error("Successor history signer unavailable")
            : new E2eeRecoveryError("recovery-material-unusable");
        const decoder = new TextDecoder();
        let armed = false;
        let historyUnwrapped = false;
        let injected = 0;
        let protectorsOpened = 0;
        const returnedSecrets: Uint8Array[] = [];
        await withRotatedRecovery(
          (native) => ({
            ...native,
            cellCipher: {
              ...native.cellCipher,
              async decrypt(key, context, envelope) {
                const plaintext = await native.cellCipher.decrypt(key, context, envelope);
                if (decoder.decode(context).includes("jazz.e2ee.local-recovery-protection.v1"))
                  protectorsOpened++;
                return plaintext;
              },
            },
            keyEnvelope: {
              ...native.keyEnvelope,
              async open(device, context, envelope) {
                const secret = await native.keyEnvelope.open(device, context, envelope);
                if (armed && decoder.decode(context).includes("__e2ee_recovery_deliveries"))
                  returnedSecrets.push(secret);
                return secret;
              },
              async unwrap(key, context, envelope) {
                const secret = await native.keyEnvelope.unwrap(key, context, envelope);
                const decoded = decoder.decode(context);
                if (
                  armed &&
                  decoded.includes("jazz.e2ee.account-successor.v1") &&
                  decoded.includes("history")
                ) {
                  historyUnwrapped = true;
                  returnedSecrets.push(secret);
                }
                return secret;
              },
            },
            deviceSigner: {
              ...native.deviceSigner,
              async verify(publicKey, record, signature) {
                if (armed && historyUnwrapped) {
                  armed = false;
                  injected++;
                  throw failure;
                }
                return native.deviceSigner.verify(publicKey, record, signature);
              },
            },
          }),
          async ({ client, material, creatorId, removedId, saved }) => {
            // Prepare use before arming; status must not enrol at all.
            const beforeDevices = operation === "use" ? await client.e2ee.devices.list() : null;
            const before = await recoveryRecords(client);
            const savedBefore = saved();
            armed = true;
            const value = input === "explicit" ? material : undefined;
            const result =
              operation === "status"
                ? client.e2ee.recovery.status(value)
                : client.e2ee.recovery.use(value).wait();
            await expect(result).rejects.toBe(failure);
            expect(historyUnwrapped).toBe(true);
            expect(injected).toBe(1);
            expect(protectorsOpened).toBe(input === "protected" ? 1 : 0);
            expect(returnedSecrets).toHaveLength(2);
            expect(returnedSecrets.every((secret) => secret.every((byte) => byte === 0))).toBe(
              true,
            );
            expect(await recoveryRecords(client)).toEqual(before);
            expect(saved()).toBe(savedBefore);
            if (beforeDevices) expect(await client.e2ee.devices.list()).toEqual(beforeDevices);
            else expect(saved()).toBeNull();

            // The same real material is usable once the operational fault ends.
            if (operation === "status") {
              expect(await client.e2ee.recovery.status(value)).toMatchObject({
                configured: true,
                account: { validation: "validated", activeDeviceIds: [creatorId] },
              });
              expect(await recoveryRecords(client)).toEqual(before);
              expect(saved()).toBeNull();
            } else {
              const pending = beforeDevices!.find((row) => row.state === "pending")!;
              await client.e2ee.recovery.use(value).wait();
              expect(await client.e2ee.devices.list()).toEqual(
                expect.arrayContaining([
                  expect.objectContaining({ id: creatorId, state: "active" }),
                  expect.objectContaining({ id: removedId, state: "revoked" }),
                  expect.objectContaining({
                    id: pending.id,
                    state: "active",
                    keyReadiness: "verified",
                  }),
                ]),
              );
            }
          },
        );
      },
      60_000,
    );
  }

  it.each(["rejected-open", "thrown-open", "thrown-confirmation", "thrown-history"] as const)(
    `sanitises %s and still falls back to a usable protector during ${operation}`,
    async (failureAt) => {
      const sensitive = "private-envelope-diagnostic";
      const failure = new Error(sensitive, { cause: { secret: sensitive } });
      const decoder = new TextDecoder();
      let rejectNextDelivery = false;
      let injected = 0;
      let protectorsOpened = 0;
      await withRotatedRecovery(
        (native) => ({
          ...native,
          cellCipher: {
            ...native.cellCipher,
            async decrypt(key, context, envelope) {
              const plaintext = await native.cellCipher.decrypt(key, context, envelope);
              if (decoder.decode(context).includes("jazz.e2ee.local-recovery-protection.v1"))
                protectorsOpened++;
              return plaintext;
            },
          },
          keyEnvelope: {
            ...native.keyEnvelope,
            open(device, context, envelope) {
              if (
                rejectNextDelivery &&
                (failureAt === "rejected-open" || failureAt === "thrown-open") &&
                decoder.decode(context).includes("__e2ee_recovery_deliveries")
              ) {
                rejectNextDelivery = false;
                injected++;
                if (failureAt === "thrown-open") throw failure;
                return Promise.reject(failure);
              }
              return native.keyEnvelope.open(device, context, envelope);
            },
            unwrap(key, context, envelope) {
              if (
                rejectNextDelivery &&
                (failureAt === "thrown-confirmation" || failureAt === "thrown-history")
              ) {
                const decoded = decoder.decode(context);
                if (
                  decoded.includes("jazz.e2ee.account-successor.v1") &&
                  decoded.includes(failureAt === "thrown-confirmation" ? "verification" : "history")
                ) {
                  rejectNextDelivery = false;
                  injected++;
                  throw failure;
                }
              }
              return native.keyEnvelope.unwrap(key, context, envelope);
            },
          },
        }),
        async ({ client, material, creatorId, saved }) => {
          const devices = operation === "use" ? await client.e2ee.devices.list() : null;
          const before = await recoveryRecords(client);
          rejectNextDelivery = true;
          const error = await (
            operation === "status"
              ? client.e2ee.recovery.status(material)
              : client.e2ee.recovery.use(material).wait()
          ).then(
            () => undefined,
            (error: unknown) => error,
          );
          expect(error).toBeInstanceOf(E2eeRecoveryError);
          expect(error).toMatchObject({
            name: "E2eeRecoveryError",
            code: "recovery-delivery-unusable",
          });
          expect(error).not.toBe(failure);
          expect(error).not.toHaveProperty("cause");
          expect(String(error)).not.toContain(sensitive);
          expect(await recoveryRecords(client)).toEqual(before);

          // Reject whichever protector is encountered first; row ordering cannot
          // affect the test. Its sole current-epoch delivery is unusable, but the
          // other independently registered root still recovers successfully.
          rejectNextDelivery = true;
          if (operation === "status") {
            expect(await client.e2ee.recovery.status()).toMatchObject({
              configured: true,
              account: { validation: "validated", activeDeviceIds: [creatorId] },
            });
            expect(saved()).toBeNull();
            expect(await recoveryRecords(client)).toEqual(before);
          } else {
            const pending = devices!.find((row) => row.state === "pending")!;
            await client.e2ee.recovery.use().wait();
            expect(await client.e2ee.devices.list()).toContainEqual(
              expect.objectContaining({
                id: pending.id,
                state: "active",
                keyReadiness: "verified",
              }),
            );
          }
          expect(injected).toBe(2);
          expect(protectorsOpened).toBe(2);
        },
      );
    },
    60_000,
  );
}

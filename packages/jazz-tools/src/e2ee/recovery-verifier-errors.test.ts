import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { E2eeRecoveryError } from "./index.js";
import {
  createRotatedRecovery,
  recoveryRecords,
  withRotatedRecovery,
  type RotatedRecovery,
} from "./fixtures/recovery-verifier-fixture.js";

describe.each(["status", "use"] as const)("recovery %s verifier boundaries", (operation) => {
  let statusHistory: RotatedRecovery | undefined;
  beforeAll(async () => {
    if (operation === "status") statusHistory = await createRotatedRecovery();
  }, 60_000);
  afterAll(async () => {
    if (operation === "status") {
      await statusHistory?.shutdown();
      statusHistory = undefined;
    }
  });
  for (const input of ["explicit", "protected"] as const) {
    it(`aborts ${input} recovery ${operation} on plain and code-collision ancestry errors`, async () => {
      let failure: Error = new Error("Successor history signer unavailable");
      const decoder = new TextDecoder();
      let armed = false;
      let historyUnwrapped = false;
      let injected = 0;
      let protectorsOpened = 0;
      const returnedSecrets: Uint8Array[] = [];
      await withRotatedRecovery(
        statusHistory,
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
          const value = input === "explicit" ? material : undefined;
          for (const kind of ["plain", "public-code-collision"] as const) {
            failure =
              kind === "plain"
                ? new Error("Successor history signer unavailable")
                : new E2eeRecoveryError("recovery-material-unusable");
            historyUnwrapped = false;
            injected = 0;
            protectorsOpened = 0;
            returnedSecrets.length = 0;
            armed = true;
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
          }

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
    }, 60_000);
  }
});

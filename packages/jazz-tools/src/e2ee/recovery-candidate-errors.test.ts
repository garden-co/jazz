import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { E2eeRecoveryError } from "./index.js";
import {
  createRotatedRecovery,
  recoveryRecords,
  withRotatedRecovery,
  type RotatedRecovery,
} from "./fixtures/recovery-verifier-fixture.js";

describe.each(["status", "use"] as const)("recovery %s candidate boundaries", (operation) => {
  let history: RotatedRecovery | undefined;
  beforeAll(async () => {
    history = await createRotatedRecovery();
  }, 60_000);
  afterAll(async () => {
    await history?.shutdown();
    history = undefined;
  });

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
        history,
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
          // Every use case starts with its own pending device. Prior cases may
          // have recovered other devices, but cannot satisfy this one's checks.
          if (devices) expect(devices.filter((row) => row.state === "pending")).toHaveLength(1);
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
            const accepted = await client.e2ee.devices.list();
            expect(accepted).toEqual(
              expect.arrayContaining(
                devices!
                  .filter((row) => row.state === "active" || row.state === "revoked")
                  .map(({ id, state }) => expect.objectContaining({ id, state })),
              ),
            );
            const records = await recoveryRecords(client);
            for (let index = 0; index < before.length; index++) {
              expect(records[index]).toEqual(expect.arrayContaining<unknown>(before[index]!));
            }
            expect(accepted).toContainEqual(
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
});

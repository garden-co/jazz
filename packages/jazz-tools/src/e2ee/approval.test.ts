import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeKeyEnvelope } from "./native.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it.each([false, true])(
  "approves the exact device despite untrusted records and shared contexts: %s",
  async (adversarial) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    const store = () => {
      let value: string | null = null;
      return {
        async read() {
          return value;
        },
        async update(transform: (current: string | null) => string) {
          value = transform(value);
        },
      };
    };
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: deviceRequestApp,
        permissions: deviceRequestPermissions,
      });
      const account = await localAccountConfig(server.appId, server.url);
      const adapter = await createNativeKeyEnvelope();
      let proofCalls = 0;
      let lastProof: Uint8Array | undefined;
      let replay = false;
      let deliveries = 0;
      let forgeDelivery = false;
      let bothResponding!: () => void;
      const responsesReady = new Promise<void>((resolve) => {
        bothResponding = resolve;
      });
      const keyEnvelope = {
        ...adapter,
        async seal(publicKey: Uint8Array, context: Uint8Array, value: Uint8Array) {
          if (new TextDecoder().decode(context).includes("delivery")) {
            deliveries++;
            if (forgeDelivery) {
              const grants = await second.all(deviceRequestApp.__e2ee_device_approvals, {
                tier: "edge",
              });
              const sent = await second.all(deviceRequestApp.__e2ee_device_deliveries, {
                tier: "edge",
              });
              const grant = grants.find(
                (item) => !sent.some((delivery) => delivery.id === item.id),
              )!;
              await second
                .insert(
                  deviceRequestApp.__e2ee_device_deliveries,
                  {
                    challengeId: grant.id,
                    envelope: new Uint8Array([1]),
                    verification: new Uint8Array([1]),
                  },
                  { id: grant.id },
                )
                .wait({ tier: "global" });
            }
          }
          return adapter.seal(publicKey, context, value);
        },
        async wrap(key: Uint8Array, context: Uint8Array, value: Uint8Array) {
          if (new TextDecoder().decode(context).includes("proof")) {
            if (replay) return lastProof!.slice();
            if (adversarial) {
              if (++proofCalls >= 2) bothResponding();
              await responsesReady;
            }
            const proof = await adapter.wrap(key, context, value);
            lastProof = proof.slice();
            return proof;
          }
          return adapter.wrap(key, context, value);
        },
      };
      const open = async (localStore = store()) => {
        const db = await createDb({
          ...account,
          e2ee: { store: localStore, crypto: { keyEnvelope } },
        });
        clients.push(db);
        return db;
      };
      const first = await open();
      const [initial] = await first.e2ee.devices.list();
      const secondStore = store();
      const second = await open(secondStore);
      const pending = (await second.e2ee.devices.list()).find(
        (device) => device.state === "pending",
      )!;
      const third = await open();
      const another = (await third.e2ee.devices.list()).find(
        (device) => device.id !== initial!.id && device.id !== pending.id,
      )!;
      await expect(second.e2ee.devices.approve(another.id).wait()).rejects.toThrow(/active|key/i);
      let shared: Awaited<ReturnType<typeof open>> | undefined;
      if (adversarial) {
        shared = await open(secondStore);
        await shared.e2ee.devices.list();
        const identity = await first.one(deviceRequestApp.__e2ee_account_identities, {
          tier: "edge",
        });
        const forged = await second
          .insert(deviceRequestApp.__e2ee_device_challenges, {
            deviceId: pending.id,
            epochId: identity!.epochId,
            envelope: new Uint8Array([1]),
          })
          .wait({ tier: "global" });
        await second
          .insert(
            deviceRequestApp.__e2ee_device_approvals,
            {
              challengeId: forged.id,
              verification: new Uint8Array([1]),
              signerId: initial!.id,
              signature: new Uint8Array([1]),
            },
            { id: forged.id },
          )
          .wait({ tier: "global" });
        await second
          .insert(
            deviceRequestApp.__e2ee_device_deliveries,
            {
              challengeId: forged.id,
              envelope: new Uint8Array([1]),
              verification: new Uint8Array([1]),
            },
            { id: forged.id },
          )
          .wait({ tier: "global" });
        expect(await first.e2ee.devices.list()).toContainEqual(
          expect.objectContaining({ id: pending.id, state: "pending" }),
        );
        expect(await second.e2ee.devices.list()).toContainEqual(
          expect.objectContaining({ id: pending.id, state: "pending" }),
        );
      }
      forgeDelivery = adversarial;
      const approval = first.e2ee.devices.approve(pending.id);
      expect(approval).not.toBeInstanceOf(Promise);
      if (adversarial) {
        await expect(approval.wait()).rejects.toThrow();
        expect(await first.e2ee.devices.list()).toContainEqual(
          expect.objectContaining({
            id: pending.id,
            state: "active",
            keyReadiness: "not-verified",
          }),
        );
        forgeDelivery = false;
        await first.e2ee.devices.approve(pending.id).wait();
      } else await approval.wait();
      expect(await second.e2ee.devices.list()).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: pending.id, state: "active", keyReadiness: "verified" }),
          expect.objectContaining({ id: another.id, state: "pending" }),
        ]),
      );
      await second.shutdown();
      if (shared) {
        expect(await shared.e2ee.devices.list()).toContainEqual(
          expect.objectContaining({ id: pending.id, state: "active" }),
        );
        await shared.shutdown();
      }
      const reopened = await open(secondStore);
      expect(await reopened.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: pending.id, state: "active" }),
      );
      replay = true;
      const beforeRejectedProof = deliveries;
      await expect(reopened.e2ee.devices.approve(another.id).wait()).rejects.toThrow();
      expect(deliveries).toBe(beforeRejectedProof);
      expect(await third.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: another.id, state: "pending" }),
      );
      replay = false;
      await reopened.e2ee.devices.approve(another.id).wait();
      expect(await third.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: another.id, state: "active" }),
      );
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  60000,
);

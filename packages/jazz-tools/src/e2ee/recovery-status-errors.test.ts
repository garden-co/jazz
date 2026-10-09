import { afterAll, beforeAll, expect, it } from "vitest";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp as app, deviceRequestPermissions } from "./device-requests.js";
import { createNativeCrypto } from "./native.js";
import { deviceRequestSchema, E2eeRecoveryError } from "./index.js";

async function prepareRecovery() {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  try {
    const deployment = {
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
    };
    await deploy({ ...deployment, permissions: deviceRequestPermissions });
    const account = await localAccountConfig(server.appId, server.url);
    const adapters = await createNativeCrypto();
    let saved: string | null = null;
    const owner = await createDb({
      ...account,
      e2ee: {
        crypto: adapters,
        store: {
          async read() {
            return saved;
          },
          async update(transform) {
            saved = transform(saved);
          },
        },
      },
    });
    try {
      const { material } = await owner.e2ee.recovery.create().wait();
      const requests = await owner.all(app.__e2ee_device_requests, { tier: "remote" });
      return { server, deployment, account, adapters, material, requests };
    } finally {
      await owner.shutdown();
    }
  } catch (error) {
    await server.stop();
    throw error;
  }
}

let history: Awaited<ReturnType<typeof prepareRecovery>> | undefined;
beforeAll(async () => {
  history = await prepareRecovery();
}, 60_000);
afterAll(async () => {
  await history?.server.stop();
  history = undefined;
});

it.each(["delivery-missing", "protector-missing", "protector-unusable"])(
  "reports recovery %s without exposing adapter errors or enrolling a device",
  async (fault) => {
    const { deployment, account, adapters, material, requests } = history!;
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    try {
      const sensitive = "private-adapter-error-marker";
      let failDecrypt = fault === "protector-unusable";
      const open = async (inspect = false) => {
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
              cellCipher: {
                ...adapters.cellCipher,
                async decrypt(key, context, envelope) {
                  if (inspect && failDecrypt) throw new Error(sensitive);
                  return adapters.cellCipher.decrypt(key, context, envelope);
                },
              },
            },
          },
        });
        clients.push(db);
        return { db, saved: () => saved };
      };
      // Each case installs its own read policy before opening a fresh observer.
      // Restoring the normal policy also isolates the adapter-only fault case.
      let permissions = deviceRequestPermissions;
      if (fault !== "protector-unusable") {
        // Missing means unavailable to this client, including policy-filtered records.
        const denied =
          fault === "delivery-missing"
            ? "__e2ee_recovery_deliveries"
            : "__e2ee_recovery_protectors";
        permissions = definePermissions(app, ({ policy }) => {
          for (const name of Object.keys(
            deviceRequestSchema,
          ) as (keyof typeof deviceRequestSchema)[]) {
            if (name === denied) policy[name].allowRead.never();
            else policy[name].allowRead.always();
          }
        });
      }
      await deploy({ ...deployment, permissions });
      const observer = await open(true);
      expect(await observer.db.all(app.__e2ee_recovery_roots, { tier: "remote" })).toHaveLength(1);
      const error = await observer.db.e2ee.recovery
        .status(fault === "delivery-missing" ? material : undefined)
        .then(
          () => undefined,
          (error: unknown) => error,
        );
      expect(error).toMatchObject({ name: "E2eeRecoveryError", code: `recovery-${fault}` });
      expect(error).toBeInstanceOf(E2eeRecoveryError);
      expect(String(error)).not.toContain(sensitive);
      expect(error).not.toHaveProperty("cause");
      expect(observer.saved()).toBeNull();
      expect(await observer.db.all(app.__e2ee_device_requests, { tier: "remote" })).toEqual(
        requests,
      );
      if (fault === "protector-unusable") {
        failDecrypt = false;
        expect(await observer.db.e2ee.recovery.status()).toMatchObject({
          configured: true,
          account: { validation: "validated" },
        });
        expect(observer.saved()).toBeNull();
      }
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
    }
  },
  60000,
);

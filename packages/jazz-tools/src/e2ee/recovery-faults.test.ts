import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";
import { createNativeCrypto } from "./native.js";
import { E2eeRecoveryError } from "./index.js";

it.each(["private-signature", "device-envelope", "delivery-verification"])(
  "rejects a faulty recovery %s before publishing it and permits retry",
  async (fault) => {
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
      const account = await localAccountConfig(server.appId, server.url);
      const adapters = await createNativeCrypto();
      let corrupt = false;
      let injected = 0;
      const open = async () => {
        let saved: string | null = null;
        const client = await createDb({
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
              deviceSigner: {
                ...adapters.deviceSigner,
                async sign(key, record) {
                  const signature = await adapters.deviceSigner.sign(key, record);
                  if (
                    corrupt &&
                    fault === "private-signature" &&
                    new TextDecoder().decode(record).includes("approval-signature:")
                  ) {
                    injected++;
                    signature[0] ^= 1;
                  }
                  return signature;
                },
              },
              keyEnvelope: {
                ...adapters.keyEnvelope,
                async seal(key, context, value) {
                  if (
                    corrupt &&
                    fault === "device-envelope" &&
                    new TextDecoder().decode(context).includes("delivery")
                  ) {
                    injected++;
                    return new Uint8Array([1]);
                  }
                  return adapters.keyEnvelope.seal(key, context, value);
                },
                async wrap(key, context, value) {
                  if (
                    corrupt &&
                    fault === "delivery-verification" &&
                    new TextDecoder().decode(context).includes("delivery-verification")
                  ) {
                    injected++;
                    return new Uint8Array([1]);
                  }
                  return adapters.keyEnvelope.wrap(key, context, value);
                },
              },
            },
          },
        });
        clients.push(client);
        return client;
      };
      const first = await open();
      const [creator] = await first.e2ee.devices.list();
      const { material } = await first.e2ee.recovery.create().wait();
      await first.shutdown();
      const second = await open();
      const pending = (await second.e2ee.devices.list()).find((row) => row.id !== creator!.id)!;
      corrupt = true;
      await expect(second.e2ee.recovery.use(material).wait()).rejects.toThrow();
      expect(injected).toBeGreaterThan(0);
      const table =
        fault === "private-signature"
          ? deviceRequestApp.__e2ee_device_approvals
          : deviceRequestApp.__e2ee_device_deliveries;
      expect(await second.all<{ id: string }>(table, { tier: "edge" })).toEqual([]);
      corrupt = false;
      await second.e2ee.recovery.use(material).wait();
      expect(await second.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: pending.id, state: "active", keyReadiness: "verified" }),
      );
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  60_000,
);

it.each(["parser", "recipient-open", "signing"])(
  "sanitises recovery material %s failures without enrolling the device",
  async (fault) => {
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
      const account = await localAccountConfig(server.appId, server.url);
      const adapters = await createNativeCrypto();
      const sensitive = "private-recovery-import-diagnostic";
      const adapterError = new Error(sensitive, { cause: { privateKey: sensitive } });
      let failImport = false;
      const isImport = (context: Uint8Array) =>
        new TextDecoder().decode(context).startsWith("jazz.e2ee.recovery-material-check.v1\0");
      const open = async () => {
        let saved: string | null = null;
        const client = await createDb({
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
                async open(key, context, envelope) {
                  if (failImport && fault === "recipient-open" && isImport(context))
                    throw adapterError;
                  return adapters.keyEnvelope.open(key, context, envelope);
                },
              },
              deviceSigner: {
                ...adapters.deviceSigner,
                async sign(key, record) {
                  if (failImport && fault === "signing" && isImport(record)) throw adapterError;
                  return adapters.deviceSigner.sign(key, record);
                },
              },
            },
          },
        });
        clients.push(client);
        return client;
      };
      const owner = await open();
      const [creator] = await owner.e2ee.devices.list();
      const { material } = await owner.e2ee.recovery.create().wait();
      await owner.shutdown();
      const client = await open();
      const before = await client.e2ee.devices.list();
      const pending = before.find((row) => row.id !== creator!.id)!;
      const approvals = await client.all(deviceRequestApp.__e2ee_device_approvals, {
        tier: "edge",
      });
      const deliveries = await client.all(deviceRequestApp.__e2ee_device_deliveries, {
        tier: "edge",
      });
      failImport = true;
      const error = await client.e2ee.recovery
        .use(fault === "parser" ? `{"privateKey":"${sensitive}"` : material)
        .wait()
        .then(
          () => undefined,
          (error: unknown) => error,
        );
      expect(error).toBeInstanceOf(E2eeRecoveryError);
      expect(error).toMatchObject({ code: "recovery-material-unusable" });
      expect(error).not.toBe(adapterError);
      expect(String(error)).not.toContain(sensitive);
      expect(error).not.toHaveProperty("cause");
      expect(await client.e2ee.devices.list()).toEqual(before);
      expect(await client.all(deviceRequestApp.__e2ee_device_approvals, { tier: "edge" })).toEqual(
        approvals,
      );
      expect(await client.all(deviceRequestApp.__e2ee_device_deliveries, { tier: "edge" })).toEqual(
        deliveries,
      );
      failImport = false;
      await client.e2ee.recovery.use(material).wait();
      expect(await client.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: pending.id, state: "active", keyReadiness: "verified" }),
      );
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      await server.stop();
    }
  },
  60_000,
);

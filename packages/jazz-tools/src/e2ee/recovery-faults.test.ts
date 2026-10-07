import { expect, it, vi } from "vitest";
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
      expect(await second.all<{ id: string }>(table, { tier: "remote" })).toEqual([]);
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

it("sanitises recovery import failures without enrolling the device and permits retry", async () => {
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
    let fault: "parser" | "recipient-open" | "signing" = "parser";
    let injected = 0;
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
                if (failImport && fault === "recipient-open" && isImport(context)) {
                  injected++;
                  throw adapterError;
                }
                return adapters.keyEnvelope.open(key, context, envelope);
              },
            },
            deviceSigner: {
              ...adapters.deviceSigner,
              async sign(key, record) {
                if (failImport && fault === "signing" && isImport(record)) {
                  injected++;
                  throw adapterError;
                }
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
      tier: "remote",
    });
    const deliveries = await client.all(deviceRequestApp.__e2ee_device_deliveries, {
      tier: "remote",
    });
    for (const nextFault of ["parser", "recipient-open", "signing"] as const) {
      fault = nextFault;
      injected = 0;
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
      expect(
        await client.all(deviceRequestApp.__e2ee_device_approvals, { tier: "remote" }),
      ).toEqual(approvals);
      expect(
        await client.all(deviceRequestApp.__e2ee_device_deliveries, { tier: "remote" }),
      ).toEqual(deliveries);
      expect(injected).toBe(fault === "parser" ? 0 : 1);
      failImport = false;
    }
    await client.e2ee.recovery.use(material).wait();
    expect(await client.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active", keyReadiness: "verified" }),
    );
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);

it("publishes neither recovery approval when the public approval write is refused", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
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
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: deviceRequestApp,
      permissions: deviceRequestPermissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    const owner = await createDb({ ...account, e2ee: { store: store() } });
    const recovering = await createDb({ ...account, e2ee: { store: store() } });
    clients.push(owner, recovering);
    const [creator] = await owner.e2ee.devices.list();
    const { material } = await owner.e2ee.recovery.create().wait();
    const pending = (await recovering.e2ee.devices.list()).find((row) => row.id !== creator!.id)!;
    const privateBefore = await recovering.all(deviceRequestApp.__e2ee_device_approvals, {
      tier: "remote",
    });
    const publicBefore = await recovering.all(deviceRequestApp.__e2ee_public_device_approvals, {
      tier: "remote",
    });
    const originalTransaction = recovering.transaction.bind(recovering);
    let injected = 0;
    const transactionFault = vi.spyOn(recovering, "transaction").mockImplementation((callback) =>
      originalTransaction((tx) =>
        callback(
          new Proxy(tx, {
            get(target, property) {
              if (property === "insert") {
                const insert = Reflect.get(target, property, target) as (
                  ...args: unknown[]
                ) => unknown;
                return (...args: unknown[]) => {
                  const [table, data, options] = args;
                  if (table === deviceRequestApp.__e2ee_public_device_approvals) {
                    injected++;
                    return insert.call(
                      target,
                      table,
                      {
                        ...(data as Record<string, unknown>),
                        deviceId: crypto.randomUUID(),
                      },
                      options,
                    );
                  }
                  return insert.apply(target, args);
                };
              }
              const value = Reflect.get(target, property, target);
              return typeof value === "function" ? value.bind(target) : value;
            },
          }) as typeof tx,
        ),
      ),
    );

    await expect(recovering.e2ee.recovery.use(material).wait()).rejects.toThrow();
    expect(injected).toBe(1);
    expect(
      await recovering.all(deviceRequestApp.__e2ee_device_approvals, { tier: "remote" }),
    ).toEqual(privateBefore);
    expect(
      await recovering.all(deviceRequestApp.__e2ee_public_device_approvals, { tier: "remote" }),
    ).toEqual(publicBefore);

    transactionFault.mockRestore();
    await recovering.e2ee.recovery.use(material).wait();
    expect(await recovering.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active", keyReadiness: "verified" }),
    );
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);

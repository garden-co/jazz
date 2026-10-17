import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";
import { createNativeCrypto } from "./native.js";
import { E2eeRecoveryError } from "./index.js";

let server: Awaited<ReturnType<typeof startLocalJazzServer>> | undefined;
let account: Awaited<ReturnType<typeof localAccountConfig>>;
let adapters: Awaited<ReturnType<typeof createNativeCrypto>>;
let material: string;

beforeAll(async () => {
  server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  await deploy({
    serverUrl: server.url,
    appId: server.appId,
    adminSecret: server.adminSecret,
    schema: deviceRequestApp,
    permissions: deviceRequestPermissions,
  });
  account = await localAccountConfig(server.appId, server.url);
  adapters = await createNativeCrypto();
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
    await owner.e2ee.devices.list();
    ({ material } = await owner.e2ee.recovery.create().wait());
  } finally {
    await owner.shutdown();
  }
}, 60_000);

afterAll(async () => {
  await server?.stop();
});

it.each(["success", "failure"] as const)(
  "joins a background recovery proof with %s and leaves subsequent recovery usable",
  async (outcome) => {
    let saved: string | null = null;
    let challengeId: string | undefined;
    let proofCalls = 0;
    let heldKey: Uint8Array | undefined;
    let armed = true;
    let proofEntered!: () => void;
    let releaseProof!: () => void;
    let foregroundReleased!: () => void;
    const proofStarted = new Promise<void>((resolve) => {
      proofEntered = resolve;
    });
    const proofRelease = new Promise<void>((resolve) => {
      releaseProof = resolve;
    });
    const foregroundReady = new Promise<void>((resolve) => {
      foregroundReleased = resolve;
    });
    const sentinel = new Error("Recovery proof provider unavailable");
    const decoder = new TextDecoder();
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
            async wrap(key, context, plaintext) {
              const text = decoder.decode(context);
              if (armed && challengeId && text.includes(challengeId) && text.includes("proof")) {
                proofCalls++;
                heldKey = key;
                proofEntered();
                await proofRelease;
                if (outcome === "failure") throw sentinel;
              }
              return adapters.keyEnvelope.wrap(key, context, plaintext);
            },
          },
        },
      },
    });
    const insert = client.insert.bind(client);
    const insertion = vi.spyOn(client, "insert").mockImplementation((table, data, options) => {
      const result = insert(table, data, options);
      if (table !== deviceRequestApp.__e2ee_device_challenges || challengeId) return result;
      challengeId = (result.value as { id: string }).id;
      const wait = result.wait.bind(result);
      // Preserve real global acceptance. Delay only its caller until the
      // subscription is demonstrably responding to the accepted challenge.
      vi.spyOn(result, "wait").mockImplementation(
        (waitOptions) =>
          new Promise((resolve, reject) => {
            wait(waitOptions).then(async (accepted) => {
              await proofStarted;
              // Queue useRecovery's continuation first: it reaches respond()
              // before the test resumes and releases the held provider.
              resolve(accepted);
              foregroundReleased();
            }, reject);
          }),
      );
      return result;
    });
    try {
      const before = await client.e2ee.devices.list();
      const pending = before.filter((device) => device.state === "pending");
      expect(pending).toHaveLength(1);
      const recovery = client.e2ee.recovery.use(material).wait();
      const completed =
        outcome === "failure"
          ? expect(recovery).rejects.toBe(sentinel)
          : expect(recovery).resolves.toBeUndefined();
      await foregroundReady;
      expect(proofCalls).toBe(1);
      expect(heldKey?.some((byte) => byte !== 0)).toBe(true);
      releaseProof();
      await completed;
      expect(proofCalls).toBe(1);
      expect(heldKey?.every((byte) => byte === 0)).toBe(true);
      armed = false;
      // A shared rejection belongs to the waiting call, not its next retry.
      // Successful responses likewise leave no promise that can mask new work.
      await client.e2ee.recovery.use(material).wait();
      expect(await client.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({
          id: pending[0]!.id,
          state: "active",
          keyReadiness: "verified",
        }),
      );
    } finally {
      armed = false;
      releaseProof();
      insertion.mockRestore();
      await client.shutdown();
    }
  },
  60_000,
);

// Cases share accepted history but open and close their own pending device.
// Earlier recovered devices must never satisfy a later case's success checks.

it.each(["device-envelope", "delivery-verification"])(
  "rejects a faulty recovery %s before publishing it and permits retry",
  async (fault) => {
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    try {
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
      const second = await open();
      const before = await second.e2ee.devices.list();
      const pendingDevices = before.filter((row) => row.state === "pending");
      expect(pendingDevices).toHaveLength(1);
      const pending = pendingDevices[0]!;
      const deliveries = await second.all(deviceRequestApp.__e2ee_device_deliveries, {
        tier: "remote",
      });
      corrupt = true;
      await expect(second.e2ee.recovery.use(material).wait()).rejects.toThrow();
      expect(injected).toBeGreaterThan(0);
      expect(
        await second.all(deviceRequestApp.__e2ee_device_deliveries, { tier: "remote" }),
      ).toEqual(deliveries);
      expect(await second.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: pending.id, state: "active", keyReadiness: "not-verified" }),
      );
      corrupt = false;
      await second.e2ee.recovery.use(material).wait();
      const accepted = await second.e2ee.devices.list();
      expect(accepted).toContainEqual(
        expect.objectContaining({ id: pending.id, state: "active", keyReadiness: "verified" }),
      );
      expect(accepted).toEqual(
        expect.arrayContaining(
          before
            .filter((row) => row.id !== pending.id)
            .map(({ id, state }) => expect.objectContaining({ id, state })),
        ),
      );
      expect(
        await second.all(deviceRequestApp.__e2ee_device_deliveries, { tier: "remote" }),
      ).toEqual(expect.arrayContaining(deliveries));
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
    }
  },
  60_000,
);

it("keeps failed imports and approval attempts unpublished, sanitises import errors and permits retry", async () => {
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    const sensitive = "private-recovery-import-diagnostic";
    const adapterError = new Error(sensitive, { cause: { privateKey: sensitive } });
    let failImport = false;
    let fault: "parser" | "recipient-open" | "signing" | "private-signature" = "parser";
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
                const signature = await adapters.deviceSigner.sign(key, record);
                if (
                  failImport &&
                  fault === "private-signature" &&
                  new TextDecoder().decode(record).includes("approval-signature:")
                ) {
                  injected++;
                  signature[0] ^= 1;
                }
                return signature;
              },
            },
          },
        },
      });
      clients.push(client);
      return client;
    };
    const client = await open();
    const before = await client.e2ee.devices.list();
    const pendingDevices = before.filter((row) => row.state === "pending");
    expect(pendingDevices).toHaveLength(1);
    const pending = pendingDevices[0]!;
    const approvals = await client.all(deviceRequestApp.__e2ee_device_approvals, {
      tier: "remote",
    });
    const deliveries = await client.all(deviceRequestApp.__e2ee_device_deliveries, {
      tier: "remote",
    });
    const publicApprovals = await client.all(deviceRequestApp.__e2ee_public_device_approvals, {
      tier: "remote",
    });
    expect(pending).toMatchObject({ state: "pending" });
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
      expect(
        await client.all(deviceRequestApp.__e2ee_public_device_approvals, { tier: "remote" }),
      ).toEqual(publicApprovals);
      expect(injected).toBe(fault === "parser" ? 0 : 1);
      failImport = false;
    }
    // Material validation has passed; a corrupt private approval still cannot publish.
    fault = "private-signature";
    injected = 0;
    failImport = true;
    await expect(client.e2ee.recovery.use(material).wait()).rejects.toThrow();
    expect(injected).toBeGreaterThan(0);
    expect(await client.all(deviceRequestApp.__e2ee_device_approvals, { tier: "remote" })).toEqual(
      approvals,
    );
    expect(
      await client.all(deviceRequestApp.__e2ee_public_device_approvals, { tier: "remote" }),
    ).toEqual(publicApprovals);
    expect(await client.all(deviceRequestApp.__e2ee_device_deliveries, { tier: "remote" })).toEqual(
      deliveries,
    );
    expect(await client.e2ee.devices.list()).toEqual(before);
    failImport = false;

    const originalTransaction = client.transaction.bind(client);
    injected = 0;
    const transactionFault = vi.spyOn(client, "transaction").mockImplementation((callback) =>
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

    try {
      await expect(client.e2ee.recovery.use(material).wait()).rejects.toThrow();
      expect(injected).toBe(1);
      expect(
        await client.all(deviceRequestApp.__e2ee_device_approvals, { tier: "remote" }),
      ).toEqual(approvals);
      expect(
        await client.all(deviceRequestApp.__e2ee_public_device_approvals, { tier: "remote" }),
      ).toEqual(publicApprovals);

      expect(await client.e2ee.devices.list()).toEqual(before);
      expect(
        await client.all(deviceRequestApp.__e2ee_device_deliveries, { tier: "remote" }),
      ).toEqual(deliveries);
    } finally {
      transactionFault.mockRestore();
    }
    // Every preceding failure leaves this device able to retry with the original material.
    await client.e2ee.recovery.use(material).wait();
    const accepted = await client.e2ee.devices.list();
    expect(accepted).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active", keyReadiness: "verified" }),
    );
    expect(accepted).toEqual(
      expect.arrayContaining(
        before
          .filter((row) => row.id !== pending.id)
          .map(({ id, state }) => expect.objectContaining({ id, state })),
      ),
    );
    expect(await client.all(deviceRequestApp.__e2ee_device_deliveries, { tier: "remote" })).toEqual(
      expect.arrayContaining(deliveries),
    );
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
  }
}, 60_000);

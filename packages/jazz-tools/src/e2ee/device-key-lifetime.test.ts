import { expect, it } from "vitest";
import type { AccountStore } from "../accounts/persistence.js";
import type { Db } from "../runtime/db.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";
import { createNativeCrypto } from "./native.js";
import type { CryptoAdapters } from "./types.js";

type KeyStep = "recipient-generation" | "signing-generation" | "open" | "sign";
type KeyGate = {
  arrived: Promise<Uint8Array>;
  release(): void;
  arm(step: KeyStep): void;
  hold(step: KeyStep, key: Uint8Array): Promise<void>;
};

function keyGate(): KeyGate {
  let release!: () => void;
  let arrive!: (key: Uint8Array) => void;
  const resumed = new Promise<void>((resolve) => {
    release = resolve;
  });
  const arrived = new Promise<Uint8Array>((resolve) => {
    arrive = resolve;
  });
  let target: KeyStep | undefined;
  return {
    arrived,
    release,
    arm(step: KeyStep) {
      target = step;
    },
    async hold(step: KeyStep, key: Uint8Array) {
      if (step !== target) return;
      target = undefined;
      arrive(key);
      await resumed;
    },
  };
}

function observedCrypto(adapters: CryptoAdapters, gate: KeyGate, used: Uint8Array[]) {
  return {
    ...adapters,
    keyEnvelope: {
      ...adapters.keyEnvelope,
      async createKeyPair() {
        const pair = await adapters.keyEnvelope.createKeyPair();
        used.push(pair.privateKey);
        await gate.hold("recipient-generation", pair.privateKey);
        return pair;
      },
      async open(...args: Parameters<CryptoAdapters["keyEnvelope"]["open"]>) {
        used.push(args[0].privateKey);
        const opened = await adapters.keyEnvelope.open(...args);
        await gate.hold("open", args[0].privateKey);
        return opened;
      },
    },
    deviceSigner: {
      ...adapters.deviceSigner,
      async createKeyPair() {
        const pair = await adapters.deviceSigner.createKeyPair();
        used.push(pair.privateKey);
        await gate.hold("signing-generation", pair.privateKey);
        return pair;
      },
      async sign(...args: Parameters<CryptoAdapters["deviceSigner"]["sign"]>) {
        used.push(args[0]);
        const signature = await adapters.deviceSigner.sign(...args);
        await gate.hold("sign", args[0]);
        return signature;
      },
    },
  };
}

function memoryStore(): AccountStore {
  let saved: string | null = null;
  return {
    async read() {
      return saved;
    },
    async update(transform) {
      saved = transform(saved);
    },
  };
}

it.each([
  ["generated", "open"],
  ["generated", "sign"],
  ["restored", "open"],
  ["restored", "sign"],
] as const)(
  "wipes %s initialization keys while %s is suspended",
  async (origin, step) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Db[] = [];
    const gate = keyGate();
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
      const store = memoryStore();
      if (origin === "restored") {
        const original = await createDb({ ...account, e2ee: { store, crypto: adapters } });
        clients.push(original);
        await original.e2ee.devices.list();
        await original.shutdown();
      }
      const saved = await store.read();
      const db = await createDb({
        ...account,
        e2ee: { store, crypto: observedCrypto(adapters, gate, []) },
      });
      clients.push(db);
      gate.arm(step);
      const listing = db.e2ee.devices.list();
      const rejected = expect(listing).rejects.toThrow(/closed|shutting down/);
      const key = await gate.arrived;
      expect(key.some((byte) => byte !== 0)).toBe(true);
      await db.shutdown();
      expect(key.every((byte) => byte === 0)).toBe(true);
      gate.release();
      await rejected;
      expect(await store.read()).toBe(saved);
      const reopened = await createDb({ ...account, e2ee: { store, crypto: adapters } });
      clients.push(reopened);
      expect(await reopened.e2ee.devices.list()).toEqual([
        expect.objectContaining({ state: "active" }),
      ]);
    } finally {
      gate.release();
      await Promise.all(clients.map((db) => db.shutdown()));
      await server.stop();
    }
  },
  30_000,
);

it.each(["recipient-generation", "signing-generation"] as const)(
  "wipes a key returned by late %s without publishing a device",
  async (step) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Db[] = [];
    const gate = keyGate();
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
      const store = memoryStore();
      const used: Uint8Array[] = [];
      const db = await createDb({
        ...account,
        e2ee: { store, crypto: observedCrypto(adapters, gate, used) },
      });
      clients.push(db);
      gate.arm(step);
      const rejected = expect(db.e2ee.devices.list()).rejects.toThrow(/closed|shutting down/);
      const key = await gate.arrived;
      await db.shutdown();
      expect(
        used.filter((held) => held !== key).every((held) => held.every((byte) => byte === 0)),
      ).toBe(true);
      // The provider still owns this not-yet-returned result. Adoption must erase it on return.
      gate.release();
      await rejected;
      expect(key.every((byte) => byte === 0)).toBe(true);
      expect(used.every((held) => held.every((byte) => byte === 0))).toBe(true);
      expect(await store.read()).toBeNull();
      const observer = await createDb({ ...account, e2ee: { store, crypto: adapters } });
      clients.push(observer);
      expect(
        await observer.all(deviceRequestApp.__e2ee_device_requests, { tier: "global" }),
      ).toEqual([]);
    } finally {
      gate.release();
      await Promise.all(clients.map((db) => db.shutdown()));
      await server.stop();
    }
  },
  30_000,
);

it.each(["listing", "responder", "approval", "revocation"] as const)(
  "wipes the established %s operation's key copy before its adapter resumes",
  async (operation) => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const clients: Db[] = [];
    const gate = keyGate();
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
      const used: Uint8Array[] = [];
      const firstStore = memoryStore();
      const secondStore = memoryStore();
      const first = await createDb({
        ...account,
        e2ee: {
          store: firstStore,
          crypto: operation === "responder" ? adapters : observedCrypto(adapters, gate, used),
        },
      });
      clients.push(first);
      const [creator] = await first.e2ee.devices.list();
      const second = await createDb({
        ...account,
        e2ee: {
          store: secondStore,
          crypto: operation === "responder" ? observedCrypto(adapters, gate, used) : adapters,
        },
      });
      clients.push(second);
      const pending = (await second.e2ee.devices.list()).find(
        (device) => device.id !== creator!.id,
      )!;
      if (operation === "revocation") await first.e2ee.devices.approve(pending.id).wait();
      gate.arm(operation === "listing" || operation === "responder" ? "open" : "sign");
      const completion =
        operation === "listing"
          ? first.e2ee.devices.list()
          : operation === "revocation"
            ? first.e2ee.devices.revoke(pending.id).wait()
            : first.e2ee.devices.approve(pending.id).wait();
      const rejected = expect(completion).rejects.toThrow(/closed|shutting down|cancel/i);
      const key = await gate.arrived;
      expect(key.some((byte) => byte !== 0)).toBe(true);
      const closing = operation === "responder" ? second : first;
      const store = operation === "responder" ? secondStore : firstStore;
      const saved = await store.read();
      await closing.shutdown();
      expect(key.every((byte) => byte === 0)).toBe(true);
      gate.release();
      // The approver's proof wait is deliberately not cancelled by a remote device's shutdown.
      // Cancelling that local wait is not a join on the remote responder.
      // The responder assertion above covers immediate erasure while suspended.
      if (operation === "responder") await first.shutdown();
      await rejected;
      expect(await store.read()).toBe(saved);
      const reopened = await createDb({ ...account, e2ee: { store, crypto: adapters } });
      clients.push(reopened);
      expect(await reopened.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({
          id: operation === "responder" ? pending.id : creator!.id,
          state: operation === "responder" ? "pending" : "active",
        }),
      );
    } finally {
      gate.release();
      await Promise.all(clients.map((db) => db.shutdown()));
      await server.stop();
    }
  },
  30_000,
);

it("releases completed approver copies promptly and retains usable session keys", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
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
    const used: Uint8Array[] = [];
    const crypto = observedCrypto(adapters, keyGate(), used);
    const first = await createDb({ ...account, e2ee: { store: memoryStore(), crypto } });
    clients.push(first);
    const [creator] = await first.e2ee.devices.list();
    expect(used.length).toBeGreaterThan(0);
    expect(used.every((key) => key.every((byte) => byte === 0))).toBe(true);
    // Approval completion does not join the recipient's independent proof wait.
    // Observe only this approver's copies at its public completion boundaries.
    const second = await createDb({
      ...account,
      e2ee: { store: memoryStore(), crypto: adapters },
    });
    clients.push(second);
    const pending = (await second.e2ee.devices.list()).find((device) => device.id !== creator!.id)!;
    await first.e2ee.devices.approve(pending.id).wait();
    expect(used.every((key) => key.every((byte) => byte === 0))).toBe(true);
    await first.e2ee.devices.revoke(pending.id).wait();
    expect(used.every((key) => key.every((byte) => byte === 0))).toBe(true);
    expect(await first.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: creator!.id, state: "active" }),
    );
    expect(used.every((key) => key.every((byte) => byte === 0))).toBe(true);
  } finally {
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 30_000);

it("preserves a durable winner across retried store callbacks and shutdown before update returns", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  let durable: string | null = null;
  let release!: () => void;
  let committed!: () => void;
  const resumed = new Promise<void>((resolve) => {
    release = resolve;
  });
  const updated = new Promise<void>((resolve) => {
    committed = resolve;
  });
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
    const winnerStore = memoryStore();
    const winner = await createDb({ ...account, e2ee: { store: winnerStore, crypto: adapters } });
    clients.push(winner);
    const devices = await winner.e2ee.devices.list();
    const winnerRecord = (await winnerStore.read())!;
    const used: Uint8Array[] = [];
    const store: AccountStore = {
      async read() {
        return durable;
      },
      async update(transform) {
        // A transactional host discards its first attempt after another context wins.
        transform(null);
        durable = transform(winnerRecord);
        // Retrying even an unchanged winner must not reuse a wiped previous selection.
        durable = transform(durable);
        committed();
        await resumed;
      },
    };
    const contender = await createDb({
      ...account,
      e2ee: {
        store,
        crypto: observedCrypto(adapters, keyGate(), used),
      },
    });
    clients.push(contender);
    const rejected = expect(contender.e2ee.devices.list()).rejects.toThrow(/closed|shutting down/);
    await updated;
    await contender.shutdown();
    expect(used.length).toBeGreaterThan(0);
    expect(used.every((key) => key.every((byte) => byte === 0))).toBe(true);
    expect(durable).toBe(winnerRecord);
    release();
    await rejected;
    const reopened = await createDb({ ...account, e2ee: { store, crypto: adapters } });
    clients.push(reopened);
    expect(await reopened.e2ee.devices.list()).toEqual(devices);
    expect(durable).toBe(winnerRecord);
  } finally {
    release();
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 30_000);

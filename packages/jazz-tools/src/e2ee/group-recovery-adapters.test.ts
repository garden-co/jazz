import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createAccountManager } from "../accounts/create-account-manager.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { schema as s } from "../schema-namespace.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestPermissions, deviceRequestSchema } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { withGroupTopologyPermissions } from "./group-topology.js";
import { createNativeCrypto } from "./native.js";
import type { JazzCrypto } from "./types.js";

type Recovery = { rootId: string; material: string };
const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
const decoder = new TextDecoder();
const exhausted = "No authenticated group recovery delivery for the current epoch";

async function fixture() {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients = new Set<Db>();
  const clearStores: (() => void)[] = [];
  const recoveries: Recovery[] = [];
  const memoryStore = () => {
    let saved: string | null = null;
    clearStores.push(() => {
      saved = null;
    });
    return {
      async read() {
        return saved;
      },
      async update(transform: (value: string | null) => string) {
        saved = transform(saved);
      },
    };
  };
  const close = async (db: Db) => {
    await db.shutdown();
    clients.delete(db);
  };
  const cleanup = async () => {
    const failures: unknown[] = [];
    for (const db of [...clients].reverse()) {
      try {
        await close(db);
      } catch (error) {
        failures.push(error);
      }
    }
    try {
      await server.stop();
    } catch (error) {
      failures.push(error);
    }
    for (const clear of clearStores) clear();
    for (const recovery of recoveries) recovery.material = "";
    if (failures.length) throw new AggregateError(failures, "Recovery fixture cleanup failed");
  };
  try {
    expect(["127.0.0.1", "localhost", "[::1]"]).toContain(new URL(server.url).hostname);
    const policies = definePermissions(app, ({ policy, session }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_group_membership.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_group_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_group_recovery_deliveries.allowRead.where(authenticated);
      policy.__e2ee_group_recovery_deliveries.allowInsert.where({
        senderAccountId: session.user.account,
      });
      policy.__e2ee_group_repairs.allowRead.where(authenticated);
      policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
    });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: withGroupTopologyPermissions(app, { ...deviceRequestPermissions, ...policies }),
    });
    const native = await createNativeCrypto();
    const manager = await createAccountManager({
      appId: server.appId,
      serverUrl: server.url,
      store: memoryStore(),
    });
    const account = manager.createLocalFirst();
    const open = async (crypto: JazzCrypto = native, store = memoryStore(), enrol = true) => {
      const db = await createDb({
        appId: server.appId,
        serverUrl: server.url,
        account,
        driver: { type: "memory" },
        e2ee: { app, store, crypto },
      });
      clients.add(db);
      if (enrol) await db.e2ee.devices.list();
      return db;
    };
    const recovery = async (db: Db): Promise<Recovery> => {
      const before = new Set(
        (await db.all(app.__e2ee_recovery_roots, { tier: "global" })).map((row) => row.id),
      );
      const created = await db.e2ee.recovery.create().wait();
      const result = { rootId: "", material: created.material };
      recoveries.push(result);
      const added = (await db.all(app.__e2ee_recovery_roots, { tier: "global" })).filter(
        (row) => !before.has(row.id),
      );
      expect(added).toHaveLength(1);
      result.rootId = added[0]!.id;
      return result;
    };
    return { native, open, close, recovery, memoryStore, cleanup };
  } catch (error) {
    await cleanup();
    throw error;
  }
}

async function recoveryPath(db: Db, recovery: Recovery, groupId: string) {
  const status = await db.e2ee.recovery.status(recovery.material);
  expect(status.account.validation).toBe("validated");
  expect(status.account.validatedRootId).toBe(recovery.rootId);
  expect(status.groups.validation).toBe("checked");
  if (status.groups.validation !== "checked") throw new Error("Group coverage was not checked");
  const path = status.groups.paths.find((entry) => entry.groupId === groupId);
  if (!path) throw new Error("Expected group is absent from recovery coverage");
  return path;
}

async function readyStatus(db: Db, recovery: Recovery, groupId: string) {
  expect(await recoveryPath(db, recovery, groupId)).toMatchObject({ validation: "validated" });
}

async function capture<T>(action: () => Promise<T>) {
  try {
    return { ok: true as const, value: await action() };
  } catch (error) {
    return { ok: false as const, error };
  }
}

async function prepareAdapterMatrix() {
  const f = await fixture();
  try {
    const ownerStore = f.memoryStore();
    const owner = await f.open(f.native, ownerStore);
    // Cover automatic protection and later backfill on the same group.
    const early = await f.recovery(owner);
    const { id: groupId } = await owner.e2ee.groups.create().wait();
    const roots = [early, await f.recovery(owner)];
    const [root] = roots;
    if (!root) throw new Error("Missing recovery root");
    const protectors = await owner.all(app.__e2ee_recovery_protectors, { tier: "global" });
    expect(protectors).toHaveLength(2);
    for (const entry of roots) {
      expect(protectors.some((row) => row.rootId === entry.rootId)).toBe(true);
      const deliveries = await owner.all(
        app.__e2ee_group_recovery_deliveries.where({ groupId, recoveryRootId: entry.rootId }),
        { tier: "global" },
      );
      expect(deliveries).toHaveLength(1);
    }
    // One cold native control covers automatic protector discovery for this history.
    // Explicit cold recovery is covered by group-recovery-membership.test.ts
    // and group-nested-recovery.test.ts.
    const recoveredStore = f.memoryStore();
    const normal = await f.open(f.native, recoveredStore);
    await readyStatus(normal, root, groupId);
    await normal.e2ee.recovery.use().wait();
    expect(await normal.e2ee.explain({ groupId })).toEqual({ state: "ready" });
    const ownerRecord = (await ownerStore.read())!;
    const recoveredRecord = (await recoveredStore.read())!;
    await f.close(normal);
    await f.close(owner);
    return { f, groupId, root, roots, ownerRecord, recoveredRecord };
  } catch (error) {
    await f.cleanup();
    throw error;
  }
}

describe("group recovery adapter classification and fallback", () => {
  let prepared: Awaited<ReturnType<typeof prepareAdapterMatrix>>;
  beforeAll(async () => {
    prepared = await prepareAdapterMatrix();
  }, 180_000);
  afterAll(async () => {
    if (prepared) await prepared.f.cleanup();
  });
  it.each(["status", "use", "explain"] as const)(
    "classifies synchronous and rejected group envelope faults identically through %s",
    async (surface) => {
      const { f, groupId, root, ownerRecord, recoveredRecord } = prepared;
      let client: Db | undefined;
      try {
        const envelopeFault = new Error("Synthetic group envelope fault");
        const signerFault = new Error("Synthetic group recovery signer fault");
        let kind: "sync" | "async" | undefined;
        let method: "open" | "unwrap" = "open";
        let hits = 0;
        let signerArmed = false;
        let signerHits = 0;
        const matches = (operation: "open" | "unwrap", context: Uint8Array) => {
          if (method !== operation) return false;
          const text = decoder.decode(context);
          if (operation === "unwrap")
            return text.includes("__e2ee_groups") && text.includes("verification");
          return surface === "explain"
            ? text.includes("jazz.e2ee.group.v1") && text.includes('["delivery",')
            : text.includes("__e2ee_group_recovery_deliveries");
        };
        const adapters: JazzCrypto = {
          ...f.native,
          keyEnvelope: {
            ...f.native.keyEnvelope,
            // Non-async on purpose: a Promise-returning adapter can throw before returning.
            open(pair, context, envelope) {
              if (kind && matches("open", context)) {
                hits++;
                if (kind === "sync") throw envelopeFault;
                return Promise.reject(envelopeFault);
              }
              return f.native.keyEnvelope.open(pair, context, envelope);
            },
            unwrap(key, context, envelope) {
              if (kind && matches("unwrap", context)) {
                hits++;
                if (kind === "sync") throw envelopeFault;
                return Promise.reject(envelopeFault);
              }
              return f.native.keyEnvelope.unwrap(key, context, envelope);
            },
          },
          deviceSigner: {
            ...f.native.deviceSigner,
            verify(publicKey, bytes, signature) {
              if (
                signerArmed &&
                decoder.decode(bytes).includes("__e2ee_group_recovery_deliveries")
              ) {
                signerHits++;
                return Promise.reject(signerFault);
              }
              return f.native.deviceSigner.verify(publicKey, bytes, signature);
            },
          },
        };
        // Reopen the clean control's recovered device for explicit-use faults.
        // Each case gets its own client/store; status observers remain unenrolled.
        // Cold explicit recovery is also covered by group-recovery-membership.test.ts.
        const store = f.memoryStore();
        if (surface === "explain") await store.update(() => ownerRecord);
        else if (surface === "use") await store.update(() => recoveredRecord);
        const active = (client = await f.open(adapters, store, surface !== "status"));
        if (surface === "status") expect(await store.read()).toBeNull();
        if (surface === "explain") {
          expect(await active.e2ee.explain({ groupId })).toEqual({ state: "ready" });
        } else {
          await readyStatus(active, root, groupId);
        }
        const invoke = async () => {
          if (surface === "status") return recoveryPath(active, root, groupId);
          if (surface === "use") return active.e2ee.recovery.use(root.material).wait();
          return active.e2ee.explain({ groupId });
        };
        const attempt = async (fault: "sync" | "async") => {
          kind = fault;
          hits = 0;
          try {
            const result = await capture(invoke);
            expect(hits, `${surface}/${method}/${fault}`).toBeGreaterThan(0);
            return result;
          } finally {
            kind = undefined;
          }
        };
        // Both read-only status methods share one unenrolled observer. A healthy
        // check after each pair is also the next method's positive control.
        const methods = surface === "status" ? (["open", "unwrap"] as const) : (["open"] as const);
        for (const operation of methods) {
          method = operation;
          const rejected = await attempt("async");
          const thrown = await attempt("sync");
          // Check signer propagation once, independently of both envelope methods.
          if (surface === "status" && method === "open") {
            signerArmed = true;
            try {
              await expect(active.e2ee.recovery.status(root.material)).rejects.toBe(signerFault);
              expect(signerHits).toBeGreaterThan(0);
            } finally {
              signerArmed = false;
            }
          }
          if (surface === "explain")
            expect(await active.e2ee.explain({ groupId })).toEqual({ state: "ready" });
          else await readyStatus(active, root, groupId);
          if (surface === "status") {
            expect(await store.read()).toBeNull();
            expect(rejected).toMatchObject({
              ok: true,
              value: { validation: "unavailable", reason: "unusable-recovery-delivery" },
            });
            expect(thrown).toEqual(rejected);
          } else {
            expect(rejected.ok).toBe(false);
            expect(thrown.ok).toBe(false);
            if (rejected.ok || thrown.ok) throw new Error("Faulty group envelope was accepted");
            const message = surface === "use" ? exhausted : "Unable to authenticate E2EE group key";
            expect(rejected.error).toBeInstanceOf(Error);
            expect(rejected.error).toMatchObject({ message });
            expect(thrown.error).toMatchObject({ message });
            expect(thrown.error).not.toBe(envelopeFault);
            expect(rejected.error).not.toBe(envelopeFault);
            if (surface === "explain") {
              expect((rejected.error as Error).cause).toBe(envelopeFault);
              expect((thrown.error as Error).cause).toBe(envelopeFault);
            }
            expect(thrown.error).toEqual(rejected.error);
          }
        }
      } finally {
        if (client) await f.close(client);
      }
    },
    180000,
  );

  it("tries another protected recovery root when the first group delivery is unusable", async () => {
    const { f, groupId, roots } = prepared;
    let observer: Db | undefined;
    try {
      let armed = false;
      let failedRoot: string | undefined;
      let failures = 0;
      const attempts: string[] = [];
      const active = (observer = await f.open({
        ...f.native,
        keyEnvelope: {
          ...f.native.keyEnvelope,
          async open(pair, context, envelope) {
            const text = decoder.decode(context);
            if (armed && text.includes("__e2ee_group_recovery_deliveries")) {
              const root = roots.find((entry) => text.includes(entry.rootId));
              if (!root) throw new Error("Group recovery context names an unknown root");
              attempts.push(root.rootId);
              // Learn the actual first candidate; database row ordering is not a contract.
              if (failedRoot === undefined) {
                failedRoot = root.rootId;
                failures++;
                throw new Error("Synthetic first encountered group envelope rejection");
              }
            }
            return f.native.keyEnvelope.open(pair, context, envelope);
          },
        },
      }));
      for (const root of roots) await readyStatus(active, root, groupId);
      armed = true;
      const implicit = await capture(() => active.e2ee.recovery.use().wait());
      armed = false;
      const implicitReadiness = implicit.ok ? await active.e2ee.explain({ groupId }) : undefined;
      expect(failures).toBe(1);
      expect(failedRoot).toBeDefined();
      const other = roots.find((root) => root.rootId !== failedRoot);
      if (!other) throw new Error("Missing independent second protector");
      expect(attempts).toContain(other.rootId);
      expect(implicit.ok).toBe(true);
      expect(implicitReadiness).toEqual({ state: "ready" });
    } finally {
      if (observer) await f.close(observer);
    }
  }, 180000);
});

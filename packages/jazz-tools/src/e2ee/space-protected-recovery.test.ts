import { expect, it } from "vitest";
import { createAccountManager } from "../accounts/create-account-manager.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { schema as s } from "../schema-namespace.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestPermissions, deviceRequestSchema } from "./device-requests.js";
import { E2eeRecoveryError } from "./index.js";
import { createNativeCrypto } from "./native.js";
import { spaceSchema } from "./spaces.js";
import type { JazzCrypto } from "./types.js";

const app = s.defineApp({
  ...deviceRequestSchema,
  ...spaceSchema,
  projects: s.table({ title: s.string() }, {}),
});
const decoder = new TextDecoder();
const exhausted = "No authenticated space recovery delivery for the current epoch";
type Recovery = { rootId: string; material: string };

function isSpaceRecovery(bytes: Uint8Array) {
  const text = decoder.decode(bytes);
  return text.includes("jazz.e2ee.space.v1") && text.includes("recovery-delivery");
}

async function fixture() {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients = new Set<Db>();
  const clearStores: (() => void)[] = [];
  const roots: Recovery[] = [];
  const store = () => {
    let saved: string | null = null;
    clearStores.push(() => {
      saved = null;
    });
    return {
      async read() {
        return saved;
      },
      async update(transform: (current: string | null) => string) {
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
    for (const root of roots) root.material = "";
    if (failures.length)
      throw new AggregateError(failures, "Space recovery fixture cleanup failed");
  };
  try {
    const policies = definePermissions(app, ({ policy, session }) => {
      const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
      policy.projects.allowRead.where(authenticated);
      policy.projects.allowInsert.where(authenticated);
      policy.__e2ee_spaces.allowRead.where(authenticated);
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.where(authenticated);
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.where(authenticated);
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.where(authenticated);
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_recovery_deliveries.allowRead.where(authenticated);
      policy.__e2ee_space_recovery_deliveries.allowInsert.where({
        senderAccountId: session.user.account,
      });
    });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...policies },
    });
    const native = await createNativeCrypto();
    const manager = await createAccountManager({
      appId: server.appId,
      serverUrl: server.url,
      store: store(),
    });
    const account = manager.createLocalFirst();
    const open = async (crypto: JazzCrypto = native) => {
      const db = await createDb({
        appId: server.appId,
        serverUrl: server.url,
        account,
        driver: { type: "memory" },
        e2ee: { app, store: store(), crypto },
      });
      clients.add(db);
      await db.e2ee.devices.list();
      return db;
    };
    const owner = await open();
    const project = await owner.insert(app.projects, { title: "Protected space" }).wait({
      tier: "global",
    });
    await owner.e2ee.spaces.grant(app.projects, project.id, account.id).wait();
    const target = { scope: app.projects, identifier: project.id };
    expect(await owner.e2ee.explain(target)).toEqual({ state: "ready" });
    for (let index = 0; index < 2; index++) {
      const before = new Set(
        (await owner.all(app.__e2ee_recovery_roots, { tier: "global" })).map((row) => row.id),
      );
      const created = await owner.e2ee.recovery.create().wait();
      const root = { rootId: "", material: created.material };
      roots.push(root);
      const added = (await owner.all(app.__e2ee_recovery_roots, { tier: "global" })).filter(
        (row) => !before.has(row.id),
      );
      expect(added).toHaveLength(1);
      root.rootId = added[0]!.id;
      const deliveries = await owner.all(
        app.__e2ee_space_recovery_deliveries.where({ recoveryRootId: root.rootId }),
        { tier: "global" },
      );
      expect(deliveries).toHaveLength(1);
    }
    const protectors = await owner.all(app.__e2ee_recovery_protectors, { tier: "global" });
    expect(new Set(protectors.map((row) => row.rootId))).toEqual(
      new Set(roots.map((root) => root.rootId)),
    );
    await close(owner);
    return { native, roots, target, open, cleanup };
  } catch (error) {
    await cleanup();
    throw error;
  }
}

async function capture(action: () => Promise<unknown>) {
  try {
    await action();
    return { ok: true as const };
  } catch (error) {
    return { ok: false as const, error };
  }
}

it("tries another protected recovery root when the first space delivery is unusable", async () => {
  const f = await fixture();
  try {
    let armed = false;
    let failedRoot: string | undefined;
    const attempts: string[] = [];
    const opened: string[] = [];
    const returnedSecrets: Uint8Array[] = [];
    const observer = await f.open({
      ...f.native,
      keyEnvelope: {
        ...f.native.keyEnvelope,
        async open(pair, context, envelope) {
          if (armed && isSpaceRecovery(context)) {
            const root = f.roots.find((entry) => decoder.decode(context).includes(entry.rootId));
            if (!root) throw new Error("Space recovery context names an unknown root");
            attempts.push(root.rootId);
            // Capture encounter order: neither root UUID nor row order chooses the failure.
            failedRoot ??= root.rootId;
            if (root.rootId === failedRoot)
              throw new Error("Synthetic first encountered space envelope rejection");
            const secret = await f.native.keyEnvelope.open(pair, context, envelope);
            opened.push(root.rootId);
            returnedSecrets.push(secret);
            return secret;
          }
          return f.native.keyEnvelope.open(pair, context, envelope);
        },
      },
    });
    for (const root of f.roots) {
      expect(await observer.e2ee.recovery.status(root.material)).toMatchObject({
        account: { validatedRootId: root.rootId },
        spaces: { validation: "checked", paths: [{ validation: "validated" }] },
      });
    }
    armed = true;
    const implicit = await capture(() => observer.e2ee.recovery.use().wait());
    armed = false;
    const readiness = implicit.ok ? await observer.e2ee.explain(f.target) : undefined;
    expect(failedRoot).toBeDefined();
    const other = f.roots.find((root) => root.rootId !== failedRoot);
    if (!other) throw new Error("Missing independent second protector");
    // Prove the independent root works even on the baseline that aborts implicit use.
    await observer.e2ee.recovery.use(other.material).wait();
    expect(await observer.e2ee.explain(f.target)).toEqual({ state: "ready" });
    if (!implicit.ok) throw implicit.error;
    expect(readiness).toEqual({ state: "ready" });
    expect(attempts).toContain(other.rootId);
    expect(opened).toContain(other.rootId);
    expect(returnedSecrets.length).toBeGreaterThan(0);
    expect(returnedSecrets.every((secret) => secret.every((byte) => byte === 0))).toBe(true);
  } finally {
    await f.cleanup();
  }
}, 60_000);

it("reports fixed space recovery exhaustion for explicit material", async () => {
  const f = await fixture();
  try {
    const failure = new Error("private-space-envelope-diagnostic", {
      cause: { secret: "private-space-envelope-diagnostic" },
    });
    let armed = false;
    let rejected = 0;
    const observer = await f.open({
      ...f.native,
      keyEnvelope: {
        ...f.native.keyEnvelope,
        async open(pair, context, envelope) {
          if (armed && isSpaceRecovery(context)) {
            rejected++;
            throw failure;
          }
          return f.native.keyEnvelope.open(pair, context, envelope);
        },
      },
    });
    armed = true;
    const result = await capture(() => observer.e2ee.recovery.use(f.roots[0]!.material).wait());
    armed = false;
    expect(rejected).toBeGreaterThan(0);
    await observer.e2ee.recovery.use(f.roots[0]!.material).wait();
    expect(await observer.e2ee.explain(f.target)).toEqual({ state: "ready" });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("Expected explicit space recovery exhaustion");
    expect(result.error).toBeInstanceOf(E2eeRecoveryError);
    expect(result.error).toMatchObject({
      code: "recovery-space-delivery-unavailable",
      message: exhausted,
    });
    expect(result.error).not.toBe(failure);
    expect(result.error).not.toHaveProperty("cause");
    expect(String(result.error)).not.toContain("private-space-envelope-diagnostic");
  } finally {
    await f.cleanup();
  }
}, 60_000);

it("aborts protected space recovery on a public recovery-code collision without trying another protector", async () => {
  const f = await fixture();
  try {
    const failure = new E2eeRecoveryError("recovery-space-delivery-unavailable");
    let armed = false;
    let injected = 0;
    let protectorsOpened = 0;
    let failedRoot: string | undefined;
    const observer = await f.open({
      ...f.native,
      cellCipher: {
        ...f.native.cellCipher,
        async decrypt(key, context, envelope) {
          const plaintext = await f.native.cellCipher.decrypt(key, context, envelope);
          if (armed && decoder.decode(context).includes("jazz.e2ee.local-recovery-protection.v1"))
            protectorsOpened++;
          return plaintext;
        },
      },
      deviceSigner: {
        ...f.native.deviceSigner,
        async verify(publicKey, record, signature) {
          if (armed && injected === 0 && isSpaceRecovery(record)) {
            failedRoot = f.roots.find((root) =>
              decoder.decode(record).includes(root.rootId),
            )?.rootId;
            injected++;
            throw failure;
          }
          return f.native.deviceSigner.verify(publicKey, record, signature);
        },
      },
    });
    armed = true;
    const result = await capture(() => observer.e2ee.recovery.use().wait());
    armed = false;
    expect(injected).toBe(1);
    expect(failedRoot).toBeDefined();
    expect(result).toEqual({ ok: false, error: failure });
    if (result.ok) throw new Error("Expected operational verifier failure");
    expect(result.error).toBe(failure);
    expect(protectorsOpened).toBe(1);
    // A one-shot operational failure must not be hidden by this valid later root.
    const other = f.roots.find((root) => root.rootId !== failedRoot);
    if (!other) throw new Error("Missing independent second protector");
    await observer.e2ee.recovery.use(other.material).wait();
    expect(await observer.e2ee.explain(f.target)).toEqual({ state: "ready" });
  } finally {
    await f.cleanup();
  }
}, 60_000);

import { expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createNativeCrypto } from "./native.js";
import { Spaces } from "./space-lifecycle.js";

it("rejects stale space recovery batches before staging and stops delivery after device revocation", async () => {
  const app = s.defineApp({
    ...deviceRequestSchema,
    ...spaceSchema,
    projects: s.table({ title: s.string() }, {}),
  });
  const policies = definePermissions(app, ({ policy, session }) => {
    const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
    policy.projects.allowRead.where(authenticated);
    policy.projects.allowInsert.where(authenticated);
    policy.__e2ee_spaces.allowRead.where(authenticated);
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.where(authenticated);
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.where(authenticated);
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.where(authenticated);
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_space_recovery_deliveries.allowRead.where(authenticated);
    policy.__e2ee_space_recovery_deliveries.allowInsert.where({
      senderAccountId: session.user.account,
    });
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  const secrets: Uint8Array[] = [];
  let recovering: Db | undefined;
  let readsUntilChange = 0;
  let beforeAcceptance: (() => Promise<void>) | undefined;
  let beforeOpen: (() => Promise<void>) | undefined;
  let acceptanceChanges = 0;
  let openChanges = 0;
  // Observe the real covered read without replacing its rows or authority.
  type SpaceReader = { db: Db; readRecords(...args: unknown[]): Promise<unknown> };
  const prototype = Spaces.prototype as unknown as SpaceReader;
  const original = prototype.readRecords;
  const reads = vi.spyOn(prototype, "readRecords").mockImplementation(async function (
    this: SpaceReader,
    ...args: Parameters<typeof original>
  ) {
    const result = await original.apply(this, args);
    if (this.db === recovering && beforeAcceptance && --readsUntilChange === 0) {
      // Both spaces have been read in the same still-unaccepted batch.
      // Change authority before that transaction can be accepted globally.
      const change = beforeAcceptance;
      beforeAcceptance = undefined;
      acceptanceChanges++;
      await change();
    }
    return result;
  });
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: { ...deviceRequestPermissions, ...policies },
    });
    const account = await localAccountConfig(server.appId, server.url);
    const crypto = await createNativeCrypto();
    const open = async (measure = false) => {
      let saved: string | null = null;
      const store = {
        async read() {
          return saved;
        },
        async update(transform: (current: string | null) => string) {
          saved = transform(saved);
        },
      };
      const db = await createDb({
        ...account,
        e2ee: {
          app,
          store,
          crypto: {
            ...crypto,
            keyEnvelope: {
              ...crypto.keyEnvelope,
              async open(pair, context, envelope) {
                const recovery =
                  measure && new TextDecoder().decode(context).includes('["recovery-delivery",');
                if (recovery && beforeOpen) {
                  const change = beforeOpen;
                  beforeOpen = undefined;
                  openChanges++;
                  await change();
                }
                const secret = await crypto.keyEnvelope.open(pair, context, envelope);
                if (recovery) secrets.push(secret);
                return secret;
              },
            },
          },
        },
      });
      clients.push(db);
      return { db, store };
    };
    const { db: owner } = await open();
    const project = async () => {
      const row = await owner
        .insert(app.projects, { title: "Recovery boundary" })
        .wait({ tier: "global" });
      await owner.e2ee.spaces.grant(app.projects, row.id, account.account.id).wait();
      return row;
    };
    // The race requires protected keys, not a recovery-creation backfill.
    const retired = await owner.e2ee.recovery.create().wait();
    const first = await project();
    const second = await project();
    const replacement = await open(true);
    recovering = replacement.db;
    const pending = (await recovering.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    expect(pending).toBeDefined();
    const staged = async () =>
      JSON.parse((await replacement.store.read())!).recoveredSpaceKeysV1 ?? [];
    const delivered = () =>
      owner.all(app.__e2ee_space_deliveries.where({ recipientDeviceId: pending.id }), {
        tier: "global",
      });

    readsUntilChange = 2;
    beforeAcceptance = () => owner.e2ee.recovery.revoke(JSON.parse(retired.material).rootId).wait();
    await expect(recovering.e2ee.recovery.status(retired.material)).rejects.toThrow();
    expect(acceptanceChanges).toBe(1);
    expect(secrets).toHaveLength(0);
    expect(await staged()).toEqual([]);
    expect(await delivered()).toEqual([]);

    const current = await owner.e2ee.recovery.create().wait();
    const beforeRemoval = await delivered();
    readsUntilChange = 2;
    beforeAcceptance = () =>
      owner.e2ee.spaces.revoke(app.projects, first.id, account.account.id).wait();
    await expect(recovering.e2ee.recovery.use(current.material).wait()).rejects.toThrow();
    expect(acceptanceChanges).toBe(2);
    expect(secrets).toHaveLength(0);
    expect(await staged()).toEqual([]);
    expect(await delivered()).toEqual(beforeRemoval);

    // This attempt accepts the remaining space's snapshot. Revocation while its
    // key opens must still prevent delivery and successful recovery completion.
    beforeOpen = () => owner.e2ee.devices.revoke(pending.id).wait();
    const beforeRevocation = await delivered();
    await expect(recovering.e2ee.recovery.use(current.material).wait()).rejects.toThrow(
      /active|revok/i,
    );
    expect(openChanges).toBe(1);
    expect(secrets).toHaveLength(1);
    expect(secrets[0]!.every((byte) => byte === 0)).toBe(true);
    expect(await delivered()).toEqual(beforeRevocation);
    expect(
      await recovering.e2ee.explain({ scope: app.projects, identifier: second.id }),
    ).toMatchObject({
      state: "refused",
    });
  } finally {
    reads.mockRestore();
    for (const secret of secrets) secret.fill(0);
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 120_000);

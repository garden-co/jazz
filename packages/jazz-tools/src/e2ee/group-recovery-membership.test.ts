import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import type { AccountDbConfig } from "../accounts/context.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { groupSchema } from "./groups.js";
import { withGroupTopologyPermissions } from "./group-topology.js";
import { createNativeCrypto } from "./native.js";
import type { CryptoAdapters } from "./types.js";
import { createBrowserCrypto } from "./browser.js";
import { groupMembershipBytes } from "./group-format.js";

const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
const policies = (ownerOnly: boolean) =>
  definePermissions(app, ({ policy, session, allOf }) => {
    const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
    policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_group_membership.allowInsert.where((row) =>
      ownerOnly
        ? allOf([
            { authorAccountId: session.user.account },
            policy.__e2ee_groups.exists.where({ id: row.groupId, accountId: session.user.account }),
          ])
        : { authorAccountId: session.user.account },
    );
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

type Fixture = {
  native: CryptoAdapters;
  account(): Promise<AccountDbConfig>;
  stored(db: Db): Promise<string | null>;
  open(account: AccountDbConfig, crypto?: CryptoAdapters, enrolled?: boolean): Promise<Db>;
};

async function withFixture(
  run: (fixture: Fixture) => Promise<void>,
  ownerOnly = false,
): Promise<void> {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Db[] = [];
  const stores = new Map<Db, ReturnType<typeof store>>();
  const store = () => {
    let saved: string | null = null;
    const result = {
      async read() {
        return saved;
      },
      async update(transform: (current: string | null) => string) {
        saved = transform(saved);
      },
      clear() {
        saved = null;
      },
    };
    return result;
  };
  try {
    expect(["127.0.0.1", "localhost", "[::1]"]).toContain(new URL(server.url).hostname);
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: withGroupTopologyPermissions(app, {
        ...deviceRequestPermissions,
        ...policies(ownerOnly),
      }),
    });
    const native = await createNativeCrypto();
    await run({
      native,
      account: () => localAccountConfig(server.appId, server.url),
      stored: (db) => stores.get(db)!.read(),
      async open(account, crypto = native, enrolled = true) {
        const saved = store();
        const db = await createDb({
          ...account,
          ...(enrolled ? { e2ee: { app, store: saved, crypto } } : {}),
        });
        stores.set(db, saved);
        clients.push(db);
        if (enrolled) await db.e2ee.devices.list();
        return db;
      },
    });
  } finally {
    const closed = await Promise.allSettled(clients.map((client) => client.shutdown()));
    try {
      await server.stop();
    } finally {
      for (const saved of stores.values()) saved.clear();
    }
    for (const result of closed) {
      if (result.status === "rejected") throw result.reason;
    }
  }
}

async function readyGroup(db: Db): Promise<string> {
  const group = db.e2ee.groups.create();
  await group.wait();
  expect(await db.e2ee.explain({ groupId: group.id })).toEqual({ state: "ready" });
  return group.id;
}

async function expectRecoveryPath(db: Db, material: string, groupId: string): Promise<void> {
  const status = await db.e2ee.recovery.status(material);
  expect(status.account.validation).toBe("validated");
  expect(status.groups.validation).toBe("checked");
  if (status.groups.validation !== "checked") throw new Error("Recovery groups were not checked");
  expect(status.groups.paths).toContainEqual(
    expect.objectContaining({ groupId, validation: "validated" }),
  );
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

it("isolates pre-enrolment authors, recipients and recovery deliveries from legitimate groups", async () => {
  await withFixture(async ({ account, open, native, stored }) => {
    const browser = await createBrowserCrypto();
    const ownerAccount = await account();
    const owner = await open(ownerAccount, browser);
    const groupId = await readyGroup(owner);
    const before = new Set(
      (await owner.all(app.__e2ee_recovery_roots, { tier: "global" })).map((row) => row.id),
    );
    const { material } = await owner.e2ee.recovery.create().wait();
    const added = (await owner.all(app.__e2ee_recovery_roots, { tier: "global" })).filter(
      (row) => !before.has(row.id),
    );
    expect(added).toHaveLength(1);
    const recoveryRootId = added[0]!.id;
    const mallory = await open(await account(), browser);
    const other = mallory.e2ee.groups.create();
    await other.wait();
    const root = await mallory.one(app.__e2ee_groups.where({ id: other.id }), { tier: "global" });
    expect(root).toBeDefined();

    const futureAccount = await account();
    const writer = await open(futureAccount, native, false);
    expect(
      await writer.all(app.__e2ee_account_roots.where({ accountId: futureAccount.account.id }), {
        tier: "global",
      }),
    ).toEqual([]);
    const unrelatedId = crypto.randomUUID();
    const unrelatedEpochId = crypto.randomUUID();
    const unrelatedDeviceId = crypto.randomUUID();
    await writer
      .insert(
        app.__e2ee_groups,
        {
          accountId: futureAccount.account.id,
          deviceId: unrelatedDeviceId,
          accountEpochId: crypto.randomUUID(),
          epochId: unrelatedEpochId,
          mechanism: root!.mechanism,
          version: root!.version,
          verification: root!.verification,
          signature: new Uint8Array(64),
        },
        { id: unrelatedId },
      )
      .wait({ tier: "global" });
    // An otherwise valid administrator signature still cannot enrol a recipient
    // whose account root has not been accepted at the time of the proposal.
    const device = JSON.parse((await stored(mallory))!).devices[0];
    const privateKey = Uint8Array.from(device.signingPrivateKey);
    const record = {
      id: crypto.randomUUID(),
      groupId: other.id,
      epochId: root!.epochId,
      authorAccountId: root!.accountId,
      authorDeviceId: root!.deviceId,
      authorEpochId: root!.accountEpochId,
      operation: "add",
      memberKind: "account",
      memberId: futureAccount.account.id,
    };
    try {
      const bytes = groupMembershipBytes(device.scope, record);
      const signature = await browser.deviceSigner.sign(privateKey, bytes);
      expect(
        await browser.deviceSigner.verify(
          Uint8Array.from(device.signingPublicKey),
          bytes,
          signature,
        ),
      ).toBe(true);
      const { id, ...values } = record;
      await mallory
        .insert(app.__e2ee_group_membership, { ...values, signature }, { id })
        .wait({ tier: "global" });
    } finally {
      privateKey.fill(0);
    }
    expect(await owner.e2ee.explain({ groupId })).toEqual({ state: "ready" });
    expect(await mallory.e2ee.explain({ groupId: other.id })).toEqual({ state: "ready" });

    // Later real enrolment cannot authorise either the earlier root or add.
    const recipient = await open(futureAccount, browser);
    expect(await owner.e2ee.explain({ groupId })).toEqual({ state: "ready" });
    expect(await mallory.e2ee.explain({ groupId: other.id })).toEqual({ state: "ready" });
    expect(await recipient.e2ee.explain({ groupId: other.id })).toMatchObject({ state: "refused" });
    expect(
      await mallory.all(
        app.__e2ee_group_deliveries.where({
          groupId: other.id,
          recipientAccountId: futureAccount.account.id,
        }),
        { tier: "global" },
      ),
    ).toEqual([]);

    // Recovery uses a fresh store, with no original key-holding client online.
    await Promise.all([owner, mallory, recipient].map((db) => db.shutdown()));
    const recovered = await open(ownerAccount);
    await expectRecoveryPath(recovered, material, groupId);
    await recovered.e2ee.recovery.use(material).wait();
    expect(await recovered.e2ee.explain({ groupId })).toEqual({ state: "ready" });

    const deliveryId = crypto.randomUUID();
    await writer
      .insert(
        app.__e2ee_group_recovery_deliveries,
        {
          groupId: unrelatedId,
          epochId: unrelatedEpochId,
          senderAccountId: futureAccount.account.id,
          senderDeviceId: unrelatedDeviceId,
          recipientAccountId: ownerAccount.account.id,
          recoveryRootId,
          envelope: Uint8Array.of(1),
          signature: new Uint8Array(64),
        },
        { id: deliveryId },
      )
      .wait({ tier: "global" });
    expect(
      await recovered.one(app.__e2ee_group_recovery_deliveries.where({ id: deliveryId }), {
        tier: "global",
      }),
    ).toMatchObject({ groupId: unrelatedId, recoveryRootId });

    const outcome = await recovered.e2ee.recovery
      .use(material)
      .wait()
      .then(
        () => "fulfilled",
        () => "rejected",
      );
    expect(await recovered.e2ee.explain({ groupId })).toEqual({ state: "ready" });
    expect(outcome).toBe("fulfilled");
  }, true);
}, 60000);

it("rejects recovery creation when a required ready group loses membership", async () => {
  await withFixture(async ({ account, open, native }) => {
    const entered = deferred();
    const released = deferred();
    let armed = false;
    let hits = 0;
    const adapters: CryptoAdapters = {
      ...native,
      keyEnvelope: {
        ...native.keyEnvelope,
        async open(pair, context, envelope) {
          if (
            armed &&
            new TextDecoder().decode(context).startsWith("jazz.e2ee.recovery-material-check.v1\0")
          ) {
            armed = false;
            hits++;
            entered.resolve();
            await released.promise;
          }
          return native.keyEnvelope.open(pair, context, envelope);
        },
      },
    };
    const ownerAccount = await account();
    const owner = await open(ownerAccount, adapters);
    const administratorAccount = await account();
    const administrator = await open(administratorAccount);
    const groupId = await readyGroup(owner);
    await owner.e2ee.groups
      .add(groupId, { kind: "account", id: administratorAccount.account.id })
      .wait();
    expect(await administrator.e2ee.explain({ groupId })).toEqual({ state: "ready" });
    const control = await owner.e2ee.recovery.create().wait();
    await expectRecoveryPath(owner, control.material, groupId);
    expect(await owner.e2ee.explain({ groupId })).toEqual({ state: "ready" });

    armed = true;
    let completed = false;
    // Observe both settlements immediately, so an early failure cannot hang gate setup.
    const creating = owner.e2ee.recovery
      .create()
      .wait()
      .then(
        () => {
          completed = true;
          return { status: "fulfilled" as const };
        },
        (error: unknown) => {
          completed = true;
          return { status: "rejected" as const, error };
        },
      );
    try {
      const first = await Promise.race([
        entered.promise.then(() => "gate"),
        creating.then(() => "completed"),
      ]);
      expect(first).toBe("gate");
      expect(hits).toBe(1);
      expect(completed).toBe(false);
      // Final recovery-path verification starts after protection collected ready groups.
      // Public removal waits for global acceptance before the native adapter resumes.
      await administrator.e2ee.groups
        .remove(groupId, { kind: "account", id: ownerAccount.account.id })
        .wait();
      expect(await owner.e2ee.explain({ groupId })).toEqual({
        state: "refused",
        reason: "not-a-group-member",
      });
      released.resolve();
      const outcome = await creating;
      expect(await owner.e2ee.explain({ groupId })).toEqual({
        state: "refused",
        reason: "not-a-group-member",
      });
      expect(outcome.status).toBe("rejected");
      if (outcome.status === "rejected") {
        expect(outcome.error).toBeInstanceOf(Error);
        expect(outcome.error).toMatchObject({
          message: expect.stringMatching(
            /(?:required|group).*(?:unavailable|refused|member|lost)|(?:unavailable|refused).*(?:required|group)/i,
          ),
        });
      }
    } finally {
      released.resolve();
      await creating;
    }
  });
}, 60000);

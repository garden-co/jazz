import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeCrypto } from "./native.js";
import { readAccountMembership } from "./public-membership.js";
import { recoveryRootBytes } from "./recovery-format.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("rejects racing recovery registration, retries rotation and retains independent recovery", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  const delivered: { recipient: Uint8Array; secret: Uint8Array }[] = [];
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
    let beforeRotationSeal: (() => Promise<void>) | undefined;
    let corruptHistory = false;
    let injectedHistory = 0;
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
            keyEnvelope: {
              ...adapters.keyEnvelope,
              async unwrap(key, context, envelope) {
                if (
                  inspect &&
                  corruptHistory &&
                  new TextDecoder().decode(context).includes("history")
                ) {
                  injectedHistory++;
                  return new Uint8Array(32).fill(9);
                }
                return adapters.keyEnvelope.unwrap(key, context, envelope);
              },
              async seal(recipient, context, secret) {
                if (new TextDecoder().decode(context).includes("jazz.e2ee.account-successor.v1")) {
                  const action = beforeRotationSeal;
                  beforeRotationSeal = undefined;
                  await action?.();
                }
                delivered.push({ recipient: recipient.slice(), secret: secret.slice() });
                return adapters.keyEnvelope.seal(recipient, context, secret);
              },
            },
          },
        },
      });
      clients.push(db);
      return { db, readStore: () => saved };
    };
    const { db: first } = await open();
    const [creator] = await first.e2ee.devices.list();
    const { db: second } = await open();
    const removed = (await second.e2ee.devices.list()).find((d) => d.id !== creator!.id)!;
    await first.e2ee.devices.approve(removed.id).wait();
    const oldSecrets = delivered.map((item) => item.secret.slice());
    const before = delivered.length;
    const removal = first.e2ee.devices.revoke(removed.id);
    expect(removal).not.toBeInstanceOf(Promise);
    await removal.wait();
    const rotations = delivered.slice(before);
    expect(rotations).toHaveLength(1);
    expect(rotations[0]!.recipient).toEqual(creator!.publicKey);
    expect(rotations[0]!.secret).toHaveLength(32);
    for (const old of oldSecrets) {
      expect(rotations[0]!.secret).not.toEqual(old);
      old.fill(0);
    }
    for (const db of [first, second])
      expect(await db.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: removed.id, state: "revoked" }),
      );
    const { db: third, readStore: thirdStore } = await open();
    const pending = (await third.e2ee.devices.list()).find(
      (d) => d.id !== creator!.id && d.id !== removed.id,
    )!;
    await expect(second.e2ee.devices.approve(pending.id).wait()).rejects.toThrow(
      /revok|active|eligible/i,
    );
    await first.e2ee.devices.approve(pending.id).wait();
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active" }),
    );

    // Continue the already-approved device workflow into recovery. The recovery
    // root's author and the remaining approver are both revoked in turn, and
    // every earlier client is closed before each fresh device recovers.
    const privateSuccessors = await first.all(deviceRequestApp.__e2ee_account_successors, {
      tier: "remote",
    });
    const publicSuccessors = await first.all(deviceRequestApp.__e2ee_public_account_successors, {
      tier: "remote",
    });
    let material: string | undefined;
    beforeRotationSeal = async () => {
      ({ material } = await first.e2ee.recovery.create().wait());
    };
    await expect(third.e2ee.devices.revoke(creator!.id).wait()).rejects.toThrow(/conflict|stale/i);
    expect(material).toBeTypeOf("string");
    expect(await first.all(deviceRequestApp.__e2ee_account_successors, { tier: "remote" })).toEqual(
      privateSuccessors,
    );
    expect(
      await first.all(deviceRequestApp.__e2ee_public_account_successors, { tier: "remote" }),
    ).toEqual(publicSuccessors);
    expect(await first.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: creator!.id, state: "active" }),
    );
    const roots = await first.all(deviceRequestApp.__e2ee_recovery_roots, { tier: "remote" });
    expect(roots).toHaveLength(1);
    const statusBeforeRotation = await first.e2ee.recovery.status(material!);
    await third.e2ee.devices.revoke(creator!.id).wait();
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: creator!.id, state: "revoked" }),
    );
    await Promise.all(clients.map((db) => db.shutdown()));

    const { db: recovered, readStore: recoveryStore } = await open(true);
    const recovering = (await recovered.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    expect(recovering).toBeDefined();
    // Inspect the rotated account from this pending device before recovering it.
    // Neither failed nor successful status may activate it or change its store.
    const requestsBeforeStatus = await recovered.all(deviceRequestApp.__e2ee_device_requests, {
      tier: "remote",
    });
    const approvalsBeforeStatus = await recovered.all(
      deviceRequestApp.__e2ee_public_device_approvals,
      {
        tier: "remote",
      },
    );
    const storeBeforeStatus = recoveryStore();
    const expectReadOnlyStatus = async () => {
      expect(
        await recovered.all(deviceRequestApp.__e2ee_device_requests, { tier: "remote" }),
      ).toEqual(requestsBeforeStatus);
      expect(
        await recovered.all(deviceRequestApp.__e2ee_public_device_approvals, { tier: "remote" }),
      ).toEqual(approvalsBeforeStatus);
      expect(recoveryStore()).toBe(storeBeforeStatus);
      expect(await recovered.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: recovering.id, state: "pending" }),
      );
    };
    corruptHistory = true;
    await expect(recovered.e2ee.recovery.status(material!)).rejects.toMatchObject({
      code: "recovery-delivery-unusable",
      message: "No authenticated recovery delivery for the current account epoch",
    });
    expect(injectedHistory).toBeGreaterThan(0);
    corruptHistory = false;
    await expectReadOnlyStatus();
    const checked = await recovered.e2ee.recovery.status(material!);
    expect(checked.account).toMatchObject({
      validation: "validated",
      activeDeviceIds: [pending.id],
      validatedRootId: statusBeforeRotation.account.validatedRootId,
    });
    expect(checked.account.epochId).not.toBe(statusBeforeRotation.account.epochId);
    expect(checked.groups.validation).toBe("not-checked");
    await expectReadOnlyStatus();
    await recovered.e2ee.recovery.use(material!).wait();
    expect(await recovered.e2ee.devices.list()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: recovering.id, state: "active" }),
        expect.objectContaining({ id: creator!.id, state: "revoked" }),
        expect.objectContaining({ id: removed.id, state: "revoked" }),
      ]),
    );
    expect(await recovered.all(deviceRequestApp.__e2ee_recovery_roots, { tier: "remote" })).toEqual(
      roots,
    );
    // The approved, non-founding device can register authority while active.
    // Its retained signing key must not confer authority after revocation.
    const observer = await createDb(await localAccountConfig(server.appId, server.url));
    clients.push(observer);
    const device = JSON.parse(thirdStore()!).devices[0];
    const signingKey = Uint8Array.from(device.signingPrivateKey);
    const recoverySigner = await adapters.deviceSigner.createKeyPair();
    const recoveryKeys = await adapters.keyEnvelope.createKeyPair();
    recoverySigner.privateKey.fill(0);
    recoveryKeys.privateKey.fill(0);
    const membership = () =>
      readAccountMembership(observer, account.account.id, device.scope, adapters.deviceSigner);
    const register = async (epochId: string) => {
      const root = {
        id: globalThis.crypto.randomUUID(),
        accountId: account.account.id,
        epochId,
        signerId: pending.id,
        signingPublicKey: recoverySigner.publicKey,
        signingMechanism: adapters.deviceSigner.mechanism.id,
        signingVersion: adapters.deviceSigner.mechanism.version,
        publicKey: recoveryKeys.publicKey,
        mechanism: adapters.keyEnvelope.mechanism.id,
        version: adapters.keyEnvelope.mechanism.version,
      };
      const signature = await adapters.deviceSigner.sign(
        signingKey,
        recoveryRootBytes(device.scope, root),
      );
      const { id, ...columns } = root;
      // Raw account writes publish the signature even though its original
      // signing client is offline. Public replay decides its authority.
      await recovered
        .insert(deviceRequestApp.__e2ee_recovery_roots, { ...columns, signature }, { id })
        .wait({ tier: "global" });
      return id;
    };
    let rootsAfterRegistrations: typeof roots;
    try {
      const initial = await membership();
      expect(initial.active.has(pending.id)).toBe(true);
      const accepted = await register(initial.epochId);
      await recovered.e2ee.devices.revoke(pending.id).wait();
      const revoked = await membership();
      expect([...revoked.revoked]).toContain(pending.id);
      const rejected = await register(revoked.epochId);
      const result = await membership();
      expect(result.recoveryRoots.map((root) => root.id).sort()).toEqual(
        [...roots.map((root) => root.id), accepted].sort(),
      );
      expect(result.recoveryRoots.map((root) => root.id)).not.toContain(rejected);
      expect(
        await observer.all(deviceRequestApp.__e2ee_account_identities, { tier: "remote" }),
      ).toEqual([]);
      rootsAfterRegistrations = await recovered.all(deviceRequestApp.__e2ee_recovery_roots, {
        tier: "remote",
      });
      expect(rootsAfterRegistrations.map((root) => root.id).sort()).toEqual(
        [...roots.map((root) => root.id), accepted, rejected].sort(),
      );
      expect(
        rootsAfterRegistrations.filter((root) => roots.some((prior) => prior.id === root.id)),
      ).toEqual(roots);
    } finally {
      signingKey.fill(0);
    }
    await Promise.all(clients.map((db) => db.shutdown()));

    const { db: reopened } = await open();
    const last = (await reopened.e2ee.devices.list()).find((device) => device.state === "pending")!;
    expect(last).toBeDefined();
    await reopened.e2ee.recovery.use(material!).wait();
    expect(await reopened.e2ee.devices.list()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: last.id, state: "active" }),
        expect.objectContaining({ id: creator!.id, state: "revoked" }),
        expect.objectContaining({ id: removed.id, state: "revoked" }),
        expect.objectContaining({ id: pending.id, state: "revoked" }),
      ]),
    );
    expect(await reopened.all(deviceRequestApp.__e2ee_recovery_roots, { tier: "remote" })).toEqual(
      rootsAfterRegistrations,
    );
  } finally {
    for (const item of delivered) item.secret.fill(0);
    await Promise.all(clients.map((db) => db.shutdown()));
    await server.stop();
  }
}, 60_000);

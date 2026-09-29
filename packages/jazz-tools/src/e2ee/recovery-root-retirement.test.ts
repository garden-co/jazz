import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import type { Db } from "../runtime/db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeCrypto } from "./native.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";
import { publicSuccessorSigningBytes } from "./account-successor.js";
import { readAccountMembership } from "./public-membership.js";
import { publicDeviceApprovalBytes } from "./public-device-approval.js";

type MemoryStore = {
  store: {
    read(): Promise<string | null>;
    update(transform: (current: string | null) => string): Promise<void>;
  };
};

function memoryStore(): MemoryStore {
  let saved: string | null = null;
  return {
    store: {
      async read() {
        return saved;
      },
      async update(transform: (current: string | null) => string) {
        saved = transform(saved);
      },
    },
  };
}

it("retires one recovery root with an epoch rotation and preserves other authority", async () => {
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
    const crypto = await createNativeCrypto();
    const stores: MemoryStore[] = [];
    const open = async () => {
      const local = memoryStore();
      stores.push(local);
      const client = await createDb({
        ...account,
        e2ee: { store: local.store, crypto },
      });
      clients.push(client);
      return client;
    };

    const owner = await open();
    const [creator] = await owner.e2ee.devices.list();
    const compromised = await owner.e2ee.recovery.create().wait();
    const retained = await owner.e2ee.recovery.create().wait();
    const compromisedRootId = JSON.parse(compromised.material).rootId as string;
    const retainedRootId = JSON.parse(retained.material).rootId as string;
    const initial = await owner.e2ee.recovery.status(compromised.material);
    const ownerStore = JSON.parse((await stores[0]!.store.read())!) as {
      devices: { id: string; scope: string; signingPrivateKey: number[] }[];
    };
    const ownerDevice = ownerStore.devices.find((device) => device.id === creator!.id)!;
    const deviceKey = Uint8Array.from(ownerDevice.signingPrivateKey);
    const roots = await owner.all(deviceRequestApp.__e2ee_recovery_roots, { tier: "edge" });
    // Even an invalid registration row makes a shared authority position unordered.
    const candidateRoot = {
      ...roots.find((root) => root.id === compromisedRootId)!,
      id: globalThis.crypto.randomUUID(),
      signature: Uint8Array.of(0),
    };
    const unorderedSuccessor = {
      id: globalThis.crypto.randomUUID(),
      accountId: account.account.id,
      predecessor: initial.account.epochId!,
      epochId: globalThis.crypto.randomUUID(),
      signerId: creator!.id,
      action: "retire-recovery-root" as const,
      retiredRecoveryRootId: compromisedRootId,
      membership: new TextEncoder().encode(JSON.stringify(initial.account.activeDeviceIds)),
      revision: new TextEncoder().encode(
        JSON.stringify(
          (await owner.all(deviceRequestApp.__e2ee_public_device_approvals, { tier: "edge" }))
            .filter((row) => row.epochId === initial.account.epochId)
            .map((row) => row.id)
            .sort(),
        ),
      ),
    };
    try {
      const bytes = publicSuccessorSigningBytes(ownerDevice.scope, unorderedSuccessor);
      const signature = await crypto.deviceSigner.sign(deviceKey, bytes);
      const positionedWrite = await owner.transaction((tx) => {
        const { id: candidateRootId, ...candidateRootColumns } = candidateRoot;
        tx.insert(deviceRequestApp.__e2ee_recovery_roots, candidateRootColumns, {
          id: candidateRootId,
        });
        const { id, ...columns } = unorderedSuccessor;
        tx.insert(
          deviceRequestApp.__e2ee_public_account_successors,
          { ...columns, signature },
          { id },
        );
      });
      await positionedWrite.wait({ tier: "global" });
    } finally {
      deviceKey.fill(0);
    }
    const samePosition = await owner.e2ee.recovery.status(retained.material);
    expect(samePosition.account.epochId).toBe(initial.account.epochId);
    expect(samePosition.account.recoveryRootIds).not.toContain(candidateRoot.id);

    const second = await open();
    const secondDevice = (await second.e2ee.devices.list()).find(
      (device) => device.id !== creator!.id,
    )!;
    await owner.e2ee.devices.approve(secondDevice.id).wait();
    const recoveryMemberClient = await open();
    const recoveryMember = (await recoveryMemberClient.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    await recoveryMemberClient.e2ee.recovery.use(compromised.material).wait();
    expect(await recoveryMemberClient.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: recoveryMember.id, state: "active" }),
    );
    const before = await owner.e2ee.recovery.status(compromised.material);
    expect(before.account.activeDeviceIds).toContain(recoveryMember.id);

    const historicalDeliveries = await owner.all(deviceRequestApp.__e2ee_recovery_deliveries, {
      tier: "edge",
    });
    expect(historicalDeliveries.some((delivery) => delivery.rootId === compromisedRootId)).toBe(
      true,
    );
    expect(historicalDeliveries.some((delivery) => delivery.rootId === retainedRootId)).toBe(true);
    await owner.e2ee.recovery.revoke(compromisedRootId).wait();

    const after = await owner.e2ee.recovery.status(retained.material);
    expect(after.account.epochId).not.toBe(before.account.epochId);
    expect(after.account.activeDeviceIds).toEqual(before.account.activeDeviceIds);
    expect(after.account.activeDeviceIds).toContain(recoveryMember.id);
    expect(after.account.recoveryRootIds).toEqual([retainedRootId]);
    expect(after.account.validatedRootId).toBe(retainedRootId);

    const deliveries = await owner.all(deviceRequestApp.__e2ee_recovery_deliveries, {
      tier: "edge",
    });
    expect(deliveries).toEqual(expect.arrayContaining(historicalDeliveries));
    expect(
      deliveries.some(
        (delivery) =>
          delivery.rootId === compromisedRootId && delivery.epochId === after.account.epochId,
      ),
    ).toBe(false);
    expect(
      deliveries.some(
        (delivery) =>
          delivery.rootId === retainedRootId && delivery.epochId === after.account.epochId,
      ),
    ).toBe(true);
    await expect(owner.e2ee.recovery.status(compromised.material)).rejects.toMatchObject({
      code: "recovery-root-mismatch",
    });

    const recovering = await open();
    const pending = (await recovering.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    const compromisedRecord = JSON.parse(compromised.material) as {
      signingPrivateKey: number[];
    };
    const recoveringStore = JSON.parse((await stores[stores.length - 1]!.store.read())!) as {
      devices: { id: string; signingPrivateKey: number[] }[];
    };
    const recoveringDeviceKey = Uint8Array.from(
      recoveringStore.devices.find((device) => device.id === pending.id)!.signingPrivateKey,
    );
    const recoveryKey = Uint8Array.from(compromisedRecord.signingPrivateKey);
    const forged = {
      id: globalThis.crypto.randomUUID(),
      accountId: account.account.id,
      epochId: after.account.epochId!,
      deviceId: pending.id,
      signerId: pending.id,
      recoveryRootId: compromisedRootId,
    };
    try {
      const bytes = publicDeviceApprovalBytes(ownerDevice.scope, forged);
      const signature = await crypto.deviceSigner.sign(recoveringDeviceKey, bytes);
      const recoverySignature = await crypto.deviceSigner.sign(recoveryKey, bytes);
      const { id, ...approvalColumns } = forged;
      await owner
        .insert(
          deviceRequestApp.__e2ee_public_device_approvals,
          { ...approvalColumns, signature, recoverySignature },
          { id },
        )
        .wait({ tier: "global" });
    } finally {
      recoveringDeviceKey.fill(0);
      recoveryKey.fill(0);
    }
    expect(await recovering.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "pending" }),
    );
    await expect(recovering.e2ee.recovery.use(compromised.material).wait()).rejects.toMatchObject({
      code: "recovery-root-mismatch",
    });
    expect(await recovering.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "pending" }),
    );
    await recovering.e2ee.recovery.use(retained.material).wait();
    expect(await recovering.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active" }),
    );
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);

it("does not accept a recovery approval at the retirement transaction position", async () => {
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
    const crypto = await createNativeCrypto();
    const stores: MemoryStore[] = [];
    const open = async () => {
      const local = memoryStore();
      stores.push(local);
      const client = await createDb({ ...account, e2ee: { store: local.store, crypto } });
      clients.push(client);
      return client;
    };
    const owner = await open();
    const [creator] = await owner.e2ee.devices.list();
    const { material } = await owner.e2ee.recovery.create().wait();
    const rootId = JSON.parse(material).rootId as string;
    const pendingClient = await open();
    const pending = (await pendingClient.e2ee.devices.list()).find(
      (device) => device.id !== creator!.id,
    )!;
    const rootMaterial = JSON.parse(material) as { signingPrivateKey: number[] };
    const identity = await owner.one(
      deviceRequestApp.__e2ee_account_identities.where({ id: account.account.id }),
      { tier: "edge" },
    );
    const ownerStore = JSON.parse((await stores[0]!.store.read())!) as {
      devices: { id: string; scope: string; signingPrivateKey: number[] }[];
    };
    const ownerDevice = ownerStore.devices.find((device) => device.id === creator!.id)!;
    const pendingStore = JSON.parse((await stores[1]!.store.read())!) as {
      devices: { id: string; scope: string; signingPrivateKey: number[] }[];
    };
    const pendingDeviceKey = Uint8Array.from(
      pendingStore.devices.find((device) => device.id === pending.id)!.signingPrivateKey,
    );
    const ownerDeviceKey = Uint8Array.from(ownerDevice.signingPrivateKey);
    const rootSignatureKey = Uint8Array.from(rootMaterial.signingPrivateKey);
    const approval = {
      id: globalThis.crypto.randomUUID(),
      accountId: account.account.id,
      epochId: identity!.epochId,
      deviceId: pending.id,
      signerId: pending.id,
      recoveryRootId: rootId,
    };
    const successor = {
      id: globalThis.crypto.randomUUID(),
      accountId: account.account.id,
      predecessor: identity!.epochId,
      epochId: globalThis.crypto.randomUUID(),
      signerId: creator!.id,
      action: "retire-recovery-root" as const,
      retiredRecoveryRootId: rootId,
      membership: new TextEncoder().encode(JSON.stringify([creator!.id])),
      revision: new TextEncoder().encode("[]"),
    };
    try {
      const approvalBytes = publicDeviceApprovalBytes(ownerDevice.scope, approval);
      const approvalSignature = await crypto.deviceSigner.sign(pendingDeviceKey, approvalBytes);
      const recoverySignature = await crypto.deviceSigner.sign(rootSignatureKey, approvalBytes);
      const successorBytes = publicSuccessorSigningBytes(ownerDevice.scope, successor);
      const successorSignature = await crypto.deviceSigner.sign(ownerDeviceKey, successorBytes);
      const samePositionWrite = await owner.transaction((tx) => {
        const { id: approvalId, ...approvalColumns } = approval;
        tx.insert(
          deviceRequestApp.__e2ee_public_device_approvals,
          { ...approvalColumns, signature: approvalSignature, recoverySignature },
          { id: approvalId },
        );
        const { id: successorId, ...successorColumns } = successor;
        tx.insert(
          deviceRequestApp.__e2ee_public_account_successors,
          { ...successorColumns, signature: successorSignature },
          { id: successorId },
        );
      });
      await samePositionWrite.wait({ tier: "global" });
      const replayed = await readAccountMembership(
        owner,
        account.account.id,
        ownerDevice.scope,
        crypto.deviceSigner,
      );
      expect(replayed.epochId).toBe(successor.epochId);
      expect(replayed.active.has(pending.id)).toBe(false);
      expect(replayed.active.has(creator!.id)).toBe(true);
    } finally {
      rootSignatureKey.fill(0);
      pendingDeviceKey.fill(0);
      ownerDeviceKey.fill(0);
    }
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);

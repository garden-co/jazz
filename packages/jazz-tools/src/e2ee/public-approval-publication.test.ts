import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createNativeDeviceSigner } from "./native.js";
import { publicDeviceApprovalBytes } from "./public-device-approval.js";
import {
  deviceRequestApp as app,
  deviceRequestPermissions as permissions,
} from "./device-requests.js";

it("publishes a verifiable key-free approval while keeping the handshake account-private", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    let creatorStore: string | null = null;
    const owner = await createDb({
      ...account,
      e2ee: {
        store: {
          async read() {
            return creatorStore;
          },
          async update(transform) {
            creatorStore = transform(creatorStore);
          },
        },
      },
    });
    clients.push(owner);
    const [creator] = await owner.e2ee.devices.list();
    let secondStore: string | null = null;
    const second = await createDb({
      ...account,
      e2ee: {
        store: {
          async read() {
            return secondStore;
          },
          async update(transform) {
            secondStore = transform(secondStore);
          },
        },
      },
    });
    clients.push(second);
    const pending = (await second.e2ee.devices.list()).find(
      (device) => device.state === "pending",
    )!;
    const observer = await createDb(await localAccountConfig(server.appId, server.url));
    clients.push(observer);
    expect(await observer.all(app.__e2ee_public_device_approvals, { tier: "edge" })).toEqual([]);
    await owner.e2ee.devices.approve(pending.id).wait();
    const rows = await observer.all(app.__e2ee_public_device_approvals, { tier: "edge" });
    expect(rows).toHaveLength(1);
    const approval = rows[0]!;
    const settled = await owner.exclusiveTransaction(async (tx) => ({
      public: await tx.allSettledForE2ee(app.__e2ee_public_device_approvals),
      private: await tx.allSettledForE2ee(app.__e2ee_device_approvals),
    }));
    const history = await settled.wait({ tier: "global" });
    expect(history.public.settlements).toHaveLength(1);
    expect(history.private.settlements).toHaveLength(1);
    expect(history.public.settlements[0]!.transactionId).toBe(
      history.private.settlements[0]!.transactionId,
    );
    expect(approval).toMatchObject({
      accountId: account.account.id,
      signerId: creator!.id,
      deviceId: pending.id,
    });
    for (const name of ["envelope", "verification", "proof", "challenge"])
      expect(approval).not.toHaveProperty(name);
    const keys = await observer.all(app.__e2ee_device_keys, { tier: "edge" });
    const signerKey = keys.find((row) => row.deviceId === creator!.id)!;
    // The host-owned store's documented scope tuple binds the signature's application.
    const scope = JSON.parse(creatorStore!).devices[0].scope as string;
    const signer = await createNativeDeviceSigner();
    expect(
      await signer.verify(
        signerKey.signingPublicKey,
        publicDeviceApprovalBytes(scope, approval),
        approval.signature,
      ),
    ).toBe(true);
    const { readAccountMembership } = await import("./public-membership.js");
    const membership = await readAccountMembership(observer, account.account.id, scope, signer);
    expect([...membership.active].sort()).toEqual([creator!.id, pending.id].sort());
    expect([...membership.revoked]).toEqual([]);
    expect(await observer.all(app.__e2ee_device_challenges, { tier: "edge" })).toEqual([]);
    expect(await observer.all(app.__e2ee_device_deliveries, { tier: "edge" })).toEqual([]);
    const { id, ...columns } = approval;
    await expect(
      observer.insert(app.__e2ee_public_device_approvals, columns).wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      owner
        .update(app.__e2ee_public_device_approvals, id, {
          deviceId: creator!.id,
        })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      owner.delete(app.__e2ee_public_device_approvals, id).wait({ tier: "global" }),
    ).rejects.toThrow();
    // Raw candidates are policy-admissible even when their signed format is invalid.
    const hostileId = "77777777-7777-1777-8777-777777777777";
    await second
      .insert(
        app.__e2ee_public_device_approvals,
        { ...columns, signature: Uint8Array.of(0) },
        { id: hostileId },
      )
      .wait({ tier: "global" });
    await owner.e2ee.devices.revoke(pending.id).wait();
    const successors = await observer.all(app.__e2ee_public_account_successors, { tier: "edge" });
    expect(successors).toHaveLength(1);
    const successor = successors[0]!;
    expect(successor).toMatchObject({
      accountId: account.account.id,
      predecessor: approval.epochId,
      signerId: creator!.id,
      removedDeviceId: pending.id,
      membership: new TextEncoder().encode(JSON.stringify([creator!.id])),
      revision: new TextEncoder().encode(JSON.stringify([approval.id, hostileId].sort())),
    });
    for (const name of ["verification", "history", "deliveries"])
      expect(successor).not.toHaveProperty(name);
    const { publicSuccessorSigningBytes } = await import("./account-successor.js");
    expect(
      await signer.verify(
        signerKey.signingPublicKey,
        publicSuccessorSigningBytes(scope, successor),
        successor.signature,
      ),
    ).toBe(true);
    const rotation = await owner.exclusiveTransaction(async (tx) => ({
      public: await tx.allSettledForE2ee(app.__e2ee_public_account_successors),
      private: await tx.allSettledForE2ee(app.__e2ee_account_successors),
    }));
    const rotated = await rotation.wait({ tier: "global" });
    expect(rotated.public.settlements).toHaveLength(1);
    expect(rotated.private.settlements).toHaveLength(1);
    expect(rotated.public.settlements[0]!.transactionId).toBe(
      rotated.private.settlements[0]!.transactionId,
    );
    expect(await observer.all(app.__e2ee_account_successors, { tier: "edge" })).toEqual([]);
    const after = await readAccountMembership(observer, account.account.id, scope, signer);
    expect([...after.active]).toEqual([creator!.id]);
    expect([...after.revoked]).toEqual([pending.id]);
    expect(after.epochId).toBe(successor.epochId);
    const { id: successorId, ...successorColumns } = successor;
    await expect(
      observer
        .insert(app.__e2ee_public_account_successors, successorColumns)
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      owner
        .update(app.__e2ee_public_account_successors, successorId, {
          removedDeviceId: creator!.id,
        })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      owner.delete(app.__e2ee_public_account_successors, successorId).wait({ tier: "global" }),
    ).rejects.toThrow();
    // A genuinely signed public-only removal must not leave local operations
    // using the earlier private membership as if both histories still agreed.
    const publicOnly = {
      id: crypto.randomUUID(),
      accountId: account.account.id,
      predecessor: successor.epochId,
      epochId: crypto.randomUUID(),
      signerId: creator!.id,
      removedDeviceId: creator!.id,
      membership: new TextEncoder().encode("[]"),
      revision: new TextEncoder().encode("[]"),
    };
    const privateSigningKey = Uint8Array.from(
      JSON.parse(creatorStore!).devices[0].signingPrivateKey,
    );
    let publicOnlySignature: Uint8Array;
    try {
      publicOnlySignature = await signer.sign(
        privateSigningKey,
        publicSuccessorSigningBytes(scope, publicOnly),
      );
    } finally {
      privateSigningKey.fill(0);
    }
    const { id: publicOnlyId, ...publicOnlyColumns } = publicOnly;
    await owner
      .insert(
        app.__e2ee_public_account_successors,
        { ...publicOnlyColumns, signature: publicOnlySignature },
        { id: publicOnlyId },
      )
      .wait({ tier: "global" });
    expect([
      ...(await readAccountMembership(observer, account.account.id, scope, signer)).active,
    ]).toEqual([]);
    await expect(owner.e2ee.devices.list()).rejects.toThrow(/incomplete|disagree/i);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 30_000);

import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("inspects recovery registration without enrolling a device or claiming recoverability", async () => {
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
    const untouched = store();
    const observer = await createDb({ ...account, e2ee: { store: untouched } });
    clients.push(observer);
    expect(await observer.e2ee.recovery.status()).toEqual({
      configured: false,
      account: {
        epochId: null,
        activeDeviceIds: [],
        recoveryRootIds: [],
        validation: "not-checked",
      },
      groups: { validation: "not-checked" },
      spaces: { validation: "not-checked" },
    });
    expect(await untouched.read()).toBeNull();
    expect(await observer.all(deviceRequestApp.__e2ee_device_requests, { tier: "edge" })).toEqual(
      [],
    );
    expect(await observer.all(deviceRequestApp.__e2ee_account_roots, { tier: "edge" })).toEqual([]);

    const owner = await createDb({ ...account, e2ee: { store: store() } });
    clients.push(owner);
    const [creator] = await owner.e2ee.devices.list();
    const unconfigured = await observer.e2ee.recovery.status();
    expect(unconfigured.configured).toBe(false);
    expect(unconfigured.account.activeDeviceIds).toEqual([creator!.id]);
    expect(unconfigured.account.epochId).toEqual(expect.any(String));
    const { material } = await owner.e2ee.recovery.create().wait();
    const rootId: string = JSON.parse(material).rootId;
    expect(await observer.e2ee.recovery.status(material)).toMatchObject({
      configured: true,
      account: { validation: "validated", validatedRootId: rootId },
      groups: { validation: "not-checked" },
      spaces: { validation: "not-checked" },
    });
    await expect(observer.e2ee.recovery.status("not recovery material")).rejects.toMatchObject({
      code: "recovery-material-unusable",
    });
    const otherRoot = JSON.stringify({ ...JSON.parse(material), rootId: crypto.randomUUID() });
    await expect(observer.e2ee.recovery.status(otherRoot)).rejects.toMatchObject({
      code: "recovery-root-mismatch",
      message: "Recovery material does not match an accepted recovery root",
    });
    const requests = await owner.all(deviceRequestApp.__e2ee_device_requests, { tier: "edge" });
    await owner.shutdown();

    const configured = await observer.e2ee.recovery.status();
    expect(configured).toMatchObject({
      configured: true,
      account: {
        epochId: unconfigured.account.epochId,
        activeDeviceIds: [creator!.id],
        validation: "validated",
        validatedRootId: rootId,
      },
      groups: { validation: "not-checked" },
      spaces: { validation: "not-checked" },
    });
    const roots = await observer.all(deviceRequestApp.__e2ee_recovery_roots, { tier: "edge" });
    expect(configured.account.recoveryRootIds).toEqual(roots.map((root) => root.id));
    expect(await untouched.read()).toBeNull();
    expect(await observer.all(deviceRequestApp.__e2ee_device_requests, { tier: "edge" })).toEqual(
      requests,
    );
    for (const table of [
      deviceRequestApp.__e2ee_device_challenges,
      deviceRequestApp.__e2ee_device_proofs,
      deviceRequestApp.__e2ee_device_approvals,
      deviceRequestApp.__e2ee_public_device_approvals,
    ])
      expect(await observer.all<{ id: string }>(table, { tier: "edge" })).toEqual([]);
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60000);

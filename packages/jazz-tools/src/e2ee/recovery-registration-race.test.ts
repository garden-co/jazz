import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";
import { createNativeCrypto } from "./native.js";

it("rejects a rotation that races recovery registration and preserves the root on retry", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const clients: Awaited<ReturnType<typeof createDb>>[] = [];
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
                if (new TextDecoder().decode(context).includes("jazz.e2ee.account-successor.v1")) {
                  const action = beforeRotationSeal;
                  beforeRotationSeal = undefined;
                  await action?.();
                }
                return adapters.keyEnvelope.seal(key, context, value);
              },
            },
          },
        },
      });
      clients.push(client);
      return client;
    };
    const first = await open();
    const [creator] = await first.e2ee.devices.list();
    const second = await open();
    const other = (await second.e2ee.devices.list()).find((row) => row.id !== creator!.id)!;
    await first.e2ee.devices.approve(other.id).wait();
    let material: string | undefined;
    beforeRotationSeal = async () => {
      ({ material } = await first.e2ee.recovery.create().wait());
    };
    await expect(second.e2ee.devices.revoke(creator!.id).wait()).rejects.toThrow(/conflict|stale/i);
    expect(material).toBeTypeOf("string");
    expect(await first.all(deviceRequestApp.__e2ee_account_successors, { tier: "edge" })).toEqual(
      [],
    );
    expect(
      await first.all(deviceRequestApp.__e2ee_public_account_successors, { tier: "edge" }),
    ).toEqual([]);
    expect(await first.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: creator!.id, state: "active" }),
    );
    await second.e2ee.devices.revoke(creator!.id).wait();
    await Promise.all([first.shutdown(), second.shutdown()]);
    const third = await open();
    const pending = (await third.e2ee.devices.list()).find((row) => row.state === "pending")!;
    await third.e2ee.recovery.use(material!).wait();
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active" }),
    );
    expect(await third.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: creator!.id, state: "revoked" }),
    );
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await server.stop();
  }
}, 60_000);

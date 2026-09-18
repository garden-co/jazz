import { expect, it } from "vitest";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { deviceRequestApp, deviceRequestPermissions } from "../../src/e2ee/device-requests.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, getJazzServerJwtForUser, stopJazzServer } from "./testing-server.js";
import { createAccountManager } from "../../src/accounts/create-account-manager.js";

it.each(["material", "protected", "external"])(
  "recovers, reopens and retains recovery with browser crypto (%s)",
  async (mode) => {
    const protectedMaterial = mode === "protected";
    const server = await getJazzServerInfo(`e2ee-recovery-${crypto.randomUUID()}`);
    const clients: Awaited<ReturnType<typeof createDb>>[] = [];
    const keys = [crypto.randomUUID(), crypto.randomUUID(), crypto.randomUUID()];
    try {
      await deploy({ ...server, schema: deviceRequestApp, permissions: deviceRequestPermissions });
      const account =
        mode === "external"
          ? await (
              await createAccountManager({ appId: server.appId, serverUrl: server.serverUrl })
            ).registerJWT(
              await getJazzServerJwtForUser(crypto.randomUUID(), undefined, server.appId),
            )
          : await acquireBrowserTestAccount(server);
      const open = async (key: string) => {
        const client = await createDb({
          appId: server.appId,
          serverUrl: server.serverUrl,
          account,
          driver: { type: "memory" },
          e2ee: {
            store: {
              async read() {
                return localStorage.getItem(key);
              },
              async update(transform) {
                await navigator.locks.request(key, () => {
                  localStorage.setItem(key, transform(localStorage.getItem(key)));
                });
              },
            },
          },
        });
        clients.push(client);
        return client;
      };
      const first = await open(keys[0]!);
      const [creator] = await first.e2ee.devices.list();
      const { material } = await first.e2ee.recovery.create().wait();
      if (mode === "external") {
        expect(
          await first.all(deviceRequestApp.__e2ee_recovery_protectors, { tier: "edge" }),
        ).toEqual([]);
      }
      await first.shutdown();
      const second = await open(keys[1]!);
      await expect(second.e2ee.recovery.status("not recovery material")).rejects.toMatchObject({
        name: "E2eeRecoveryError",
        code: "recovery-material-unusable",
      });
      if (mode === "external") {
        expect(await second.e2ee.recovery.status()).toMatchObject({
          configured: true,
          account: { validation: "not-checked" },
        });
      }
      const checked = await second.e2ee.recovery.status(protectedMaterial ? undefined : material);
      expect(checked).toMatchObject({
        configured: true,
        account: {
          validation: "validated",
          activeDeviceIds: [creator!.id],
          validatedRootId: JSON.parse(material).rootId,
        },
      });
      expect(localStorage.getItem(keys[1]!)).toBeNull();
      const pending = (await second.e2ee.devices.list()).find((row) => row.state === "pending")!;
      if (mode === "external") {
        await expect(second.e2ee.recovery.use().wait()).rejects.toThrow(
          /local_first_recovery_unavailable/,
        );
        expect(await second.e2ee.devices.list()).toContainEqual(
          expect.objectContaining({ id: pending.id, state: "pending" }),
        );
      }
      await second.e2ee.recovery.use(protectedMaterial ? undefined : material).wait();
      expect(await second.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: pending.id, state: "active" }),
      );
      await second.shutdown();
      const reopened = await open(keys[1]!);
      expect(await reopened.e2ee.devices.list()).toContainEqual(
        expect.objectContaining({ id: pending.id, state: "active" }),
      );
      await reopened.e2ee.devices.revoke(creator!.id).wait();
      await reopened.shutdown();
      const third = await open(keys[2]!);
      const fresh = (await third.e2ee.devices.list()).find((row) => row.state === "pending")!;
      const rotated = await third.e2ee.recovery.status(protectedMaterial ? undefined : material);
      expect(rotated.account).toMatchObject({
        validation: "validated",
        activeDeviceIds: [pending.id],
      });
      expect(rotated.account.epochId).not.toBe(checked.account.epochId);
      await third.e2ee.recovery.use(protectedMaterial ? undefined : material).wait();
      expect(await third.e2ee.devices.list()).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: fresh.id, state: "active" }),
          expect.objectContaining({ id: creator!.id, state: "revoked" }),
        ]),
      );
    } finally {
      await Promise.all(clients.map((client) => client.shutdown()));
      for (const key of keys) localStorage.removeItem(key);
      await stopJazzServer(server.serverUrl);
    }
  },
  60_000,
);

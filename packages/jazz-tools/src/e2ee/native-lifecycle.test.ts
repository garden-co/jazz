import { expect, it } from "vitest";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { createDb } from "../runtime/default-create-db.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import {
  deviceRequestApp as app,
  deviceRequestPermissions as permissions,
} from "./device-requests.js";

const keyStore = () => {
  let value: string | null = null;
  return {
    async read() {
      return value;
    },
    async update(transform: (stored: string | null) => string) {
      value = transform(value);
    },
  };
};

it("uses native Node session lifecycle with a WASM device and retains revocation", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
  let other: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    await deploy({
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    owner = await createJazzSession({
      appId: server.appId,
      serverUrl: server.url,
      app,
      permissions,
      driver: { type: "memory" },
      initial: "local-first",
      e2ee: { store: keyStore() },
    });
    const native = owner.getSnapshot().client!.db;
    const [creator] = await native.e2ee.devices.list();
    const account = owner.getSnapshot().account!;
    other = await createDb({
      appId: server.appId,
      serverUrl: server.url,
      account,
      driver: { type: "memory" },
      e2ee: { store: keyStore() },
    });
    const pending = (await other.e2ee.devices.list()).find((device) => device.id !== creator!.id)!;
    await native.e2ee.devices.approve(pending.id).wait();
    expect(await other.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "active" }),
    );
    await native.e2ee.devices.revoke(pending.id).wait();
    expect(await other.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: pending.id, state: "revoked" }),
    );
    expect(await native.e2ee.devices.list()).toContainEqual(
      expect.objectContaining({ id: creator!.id, state: "active" }),
    );
  } finally {
    await other?.shutdown();
    await owner?.close();
    await server.stop();
  }
}, 30_000);

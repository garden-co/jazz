import { expect, it } from "vitest";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp as app, deviceRequestPermissions } from "./device-requests.js";
import { createNativeDeviceSigner } from "./native.js";

it.skipIf(process.env.JAZZ_E2EE_HISTORY_PERF !== "1")(
  "reuses unchanged public account approvals and invalidates them after revocation",
  async () => {
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const sessions: Awaited<ReturnType<typeof createJazzSession>>[] = [];
    const memoryStore = () => {
      let value: string | null = null;
      return {
        async read() {
          return value;
        },
        async update(transform: (current: string | null) => string) {
          value = transform(value);
        },
      };
    };
    const accountStore = memoryStore();
    const signer = await createNativeDeviceSigner();
    let approvalSignature: string | undefined;
    let approvalVerifications = 0;
    const measuredSigner = {
      ...signer,
      async verify(...args: Parameters<typeof signer.verify>) {
        if (Buffer.from(args[2]).toString("base64") === approvalSignature) approvalVerifications++;
        return signer.verify(...args);
      },
    };
    const open = async () => {
      const session = await createJazzSession({
        appId: server.appId,
        serverUrl: server.url,
        app,
        permissions: deviceRequestPermissions,
        driver: { type: "memory" },
        initial: "local-first",
        store: accountStore,
        e2ee: { app, store: memoryStore(), crypto: { deviceSigner: measuredSigner } },
      });
      sessions.push(session);
      return session.getSnapshot().client!.db;
    };
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: deviceRequestPermissions,
      });
      const owner = await open();
      await owner.e2ee.devices.list();
      const newcomer = await open();
      const pending = (await newcomer.e2ee.devices.list()).find(
        (device) => device.state === "pending",
      )!;
      expect(pending).toBeDefined();
      await owner.e2ee.devices.approve(pending.id).wait();
      const approval = await owner.one(
        app.__e2ee_public_device_approvals.where({ deviceId: pending.id }),
        {
          tier: "global",
        },
      );
      expect(approval).toBeDefined();
      approvalSignature = Buffer.from(approval!.signature).toString("base64");
      expect(await owner.e2ee.devices.list()).toEqual(
        expect.arrayContaining([expect.objectContaining({ id: pending.id, state: "active" })]),
      );
      approvalVerifications = 0;
      for (let sample = 0; sample < 3; sample++) {
        expect(await owner.e2ee.devices.list()).toEqual(
          expect.arrayContaining([expect.objectContaining({ id: pending.id, state: "active" })]),
        );
      }
      expect(approvalVerifications).toBe(0);
      await owner.e2ee.devices.revoke(pending.id).wait();
      expect(await owner.e2ee.devices.list()).toEqual(
        expect.arrayContaining([expect.objectContaining({ id: pending.id, state: "revoked" })]),
      );
    } finally {
      await Promise.all(sessions.map((session) => session.close()));
      await server.stop();
    }
  },
  60_000,
);

import { expect, it } from "vitest";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestApp, deviceRequestPermissions } from "./device-requests.js";

it("retries device enrolment after disconnecting during local key loading", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let stored: string | null = null;
  let disconnectOnRead = true;
  let disconnected!: () => void;
  const interruption = new Promise<void>((resolve) => {
    disconnected = resolve;
  });
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: deviceRequestApp,
      permissions: deviceRequestPermissions,
    });
    db = await createDb({
      ...(await localAccountConfig(server.appId, server.url)),
      e2ee: {
        store: {
          async read() {
            if (disconnectOnRead && stored !== null) {
              disconnectOnRead = false;
              await db!.disconnect();
              disconnected();
            }
            return stored;
          },
          async update(transform: (current: string | null) => string) {
            stored = transform(stored);
          },
        },
      },
    });
    const listing = db.e2ee.devices.list().catch(() => undefined);
    // Online listing may remain pending while disconnected; reconnect after
    // the transport transition finishes, without waiting for that listing.
    await interruption;
    await db.reconnect();
    await listing;
    const devices = await db.e2ee.devices.list();
    expect(devices).toHaveLength(1);
    expect(devices[0]).toMatchObject({ state: "active" });
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 30_000);

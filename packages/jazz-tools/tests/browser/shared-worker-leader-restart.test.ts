import { describe, expect, it } from "vitest";
import { createSharedWorkerLeaderClient } from "../../src/runtime/shared-worker-leader/client.js";
import { schema as s } from "../../src/index.js";
import { leaderSupported } from "./fixtures/leader-support.js";

// A real schema so the leader bootstrap (runAsWorker) succeeds on WebKit — an
// empty "{}" schema can fail runtime init.
const schema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
};
type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);

describe.skipIf(!leaderSupported)("shared-worker-leader restart", () => {
  it("client receives a fresh PEER_PORT after forceReconnect()", async () => {
    const appId = `leader-restart-${Math.random().toString(36).slice(2, 6)}`;
    const dbName = `restart-${Date.now()}`;
    const leaderUrl = new URL(
      "../../src/runtime/shared-worker-leader/shared-worker-leader.ts",
      import.meta.url,
    ).toString();

    const client = createSharedWorkerLeaderClient({
      appId,
      dbName,
      jazzPackageVersion: "test",
      leaderUrl,
      tabId: "tab-A",
      bornAt: Date.now(),
    });

    expect(await client.checkCapability()).toBe(true);
    const first = await client.connect({ schemaJson: JSON.stringify(app.wasmSchema) });

    const secondPromise = new Promise<typeof first>((resolve) => {
      const off = client.onPortChanged((snap) => {
        off();
        resolve(snap);
      });
    });

    client.forceReconnect();
    const second = await secondPromise;
    expect(second.port).not.toBe(first.port);

    client.close();
  }, 30000);
});

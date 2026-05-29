import { describe, expect, it } from "vitest";
import { createSharedWorkerLeaderClient } from "../../src/runtime/shared-worker-leader/client.js";

const stubUrl = new URL("./fixtures/leader-stub.shared-worker.js", import.meta.url).toString();

function defaultOptions(suffix: string) {
  return {
    appId: `client-test-${suffix}`,
    dbName: `db-${suffix}`,
    jazzPackageVersion: "0.0.0",
    leaderUrl: stubUrl,
    tabId: `tab-${suffix}`,
    bornAt: Date.now(),
  };
}

describe("SharedWorkerLeaderClient", () => {
  it("checkCapability() resolves with supported=true from the stub", async () => {
    const client = createSharedWorkerLeaderClient(defaultOptions("cap"));
    const supported = await client.checkCapability();
    expect(supported).toBe(true);
    client.close();
  });

  it("connect() resolves with a PEER_PORT and generation 1", async () => {
    const client = createSharedWorkerLeaderClient(defaultOptions("a"));
    await client.checkCapability();
    const snap = await client.connect({ schemaJson: "{}" });
    expect(snap.generation).toBeGreaterThanOrEqual(1);
    expect(snap.port).toBeInstanceOf(MessagePort);
    client.close();
  });

  it("forceReconnect() emits a fresh PEER_PORT", async () => {
    const client = createSharedWorkerLeaderClient(defaultOptions("b"));
    await client.checkCapability();
    const first = await client.connect({ schemaJson: "{}" });
    const secondPromise = new Promise<{ port: MessagePort; generation: number }>((resolve) => {
      const off = client.onPortChanged((snap) => {
        off();
        resolve(snap);
      });
    });
    client.forceReconnect();
    const second = await secondPromise;
    expect(second.port).not.toBe(first.port);
    client.close();
  });
});

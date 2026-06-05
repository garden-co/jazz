import { afterEach, describe, expect, it } from "vitest";
import { BrowserBrokerClient } from "../../src/runtime/browser-broker-client.js";

function uniqueName(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function createOptions(
  dbName: string,
  tabId: string,
  fingerprint = "fingerprint-a",
): Parameters<typeof BrowserBrokerClient.connect>[0] {
  return {
    appId: "broker-test-app",
    dbName,
    tabId,
    fingerprint,
    visibility: "visible",
    onBecomeLeader: async (client, term) => {
      client.reportLeaderReady({
        term,
        tabLockName: `jazz-leader-tab:broker-test-app:${dbName}`,
        workerLockName: `jazz-leader-worker:broker-test-app:${dbName}`,
        compatibilityLockName: `jazz-leader-lock:broker-test-app:${dbName}`,
      });
    },
  };
}

describe("SharedWorker browser broker", () => {
  const clients: BrowserBrokerClient[] = [];

  afterEach(async () => {
    for (const client of clients.splice(0)) {
      await client.shutdown();
    }
  });

  it("shares one broker epoch and elects one leader for a namespace", async () => {
    const dbName = uniqueName("broker-election");
    const first = await BrowserBrokerClient.connect(createOptions(dbName, "tab-a"));
    clients.push(first);
    const second = await BrowserBrokerClient.connect(createOptions(dbName, "tab-b"));
    clients.push(second);

    await first.waitForRole("leader", 2000);
    await second.waitForRole("follower", 2000);

    expect(first.snapshot().brokerEpoch).toEqual(second.snapshot().brokerEpoch);
    expect(first.snapshot()).toMatchObject({
      role: "leader",
      tabId: "tab-a",
      leaderTabId: "tab-a",
      term: 1,
    });
    expect(second.snapshot()).toMatchObject({
      role: "follower",
      tabId: "tab-b",
      leaderTabId: "tab-a",
      term: 1,
    });
  });

  it("rejects tabs with a mismatched configuration fingerprint", async () => {
    const dbName = uniqueName("broker-fingerprint");
    const first = await BrowserBrokerClient.connect(createOptions(dbName, "tab-a"));
    clients.push(first);
    await first.waitForRole("leader", 2000);

    await expect(
      BrowserBrokerClient.connect(createOptions(dbName, "tab-b", "fingerprint-b")),
    ).rejects.toThrow("incompatible persistent browser configuration");
  });

  it("fails fast when required browser APIs are unavailable", async () => {
    await expect(
      BrowserBrokerClient.connect({
        ...createOptions(uniqueName("broker-unsupported"), "tab-a"),
        globalLike: {
          SharedWorker: undefined,
          MessageChannel,
          navigator,
        },
      }),
    ).rejects.toThrow(
      "Jazz persistent browser mode requires SharedWorker, MessageChannel, and Web Locks support. This environment is missing: SharedWorker.",
    );
  });
});

import { afterEach, describe, expect, it } from "vitest";
import { BrowserBrokerClient } from "../../src/runtime/browser-broker-client.js";
import { tryAcquireWebLock, type LeaderLockLease } from "../../src/runtime/leader-lock.js";

function uniqueName(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  message: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`Timed out: ${message}`);
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
  const lockLeases = new Map<string, LeaderLockLease>();

  afterEach(async () => {
    for (const client of clients.splice(0)) {
      await client.shutdown();
    }
    for (const lease of lockLeases.values()) {
      lease.release();
    }
    lockLeases.clear();
  });

  async function acquireLeaderLocks(
    dbName: string,
    options: { compatibility: boolean },
  ): Promise<{
    tabLockName: string;
    workerLockName: string;
    compatibilityLockName?: string;
  }> {
    const tabLockName = `jazz-leader-tab:broker-test-app:${dbName}`;
    const workerLockName = `jazz-leader-worker:broker-test-app:${dbName}`;
    const compatibilityLockName = `jazz-leader-lock:broker-test-app:${dbName}`;
    const tabLease = await tryAcquireWebLock(tabLockName);
    const workerLease = await tryAcquireWebLock(workerLockName);
    const compatibilityLease = options.compatibility
      ? await tryAcquireWebLock(compatibilityLockName)
      : null;
    if (!tabLease || !workerLease || !compatibilityLease) {
      if (!options.compatibility && tabLease && workerLease) {
        lockLeases.set(tabLockName, tabLease);
        lockLeases.set(workerLockName, workerLease);
        return { tabLockName, workerLockName };
      }
      tabLease?.release();
      workerLease?.release();
      compatibilityLease?.release();
      throw new Error("Unable to acquire broker test leader locks");
    }

    lockLeases.set(tabLockName, tabLease);
    lockLeases.set(workerLockName, workerLease);
    lockLeases.set(compatibilityLockName, compatibilityLease);
    return { tabLockName, workerLockName, compatibilityLockName };
  }

  function releaseHeldLock(lockName: string): void {
    const lease = lockLeases.get(lockName);
    lockLeases.delete(lockName);
    lease?.release();
  }

  function createLockingOptions(
    dbName: string,
    tabId: string,
    fingerprint = "fingerprint-a",
    options: {
      compatibility?: boolean;
      forceTakeoverTimeoutMs?: number;
      brokerPingIntervalMs?: number;
      brokerPongTimeoutMs?: number;
      onReady?: (term: number) => void;
      onDemote?: (term: number) => void;
      onFailure?: (term: number, reason: string) => void;
      reportFailures?: boolean;
    } = {},
  ): Parameters<typeof BrowserBrokerClient.connect>[0] {
    return {
      ...createOptions(dbName, tabId, fingerprint),
      forceTakeoverTimeoutMs: options.forceTakeoverTimeoutMs,
      brokerPingIntervalMs: options.brokerPingIntervalMs,
      brokerPongTimeoutMs: options.brokerPongTimeoutMs,
      onDemote: options.onDemote,
      onBecomeLeader: async (client, term) => {
        try {
          const locks = await acquireLeaderLocks(dbName, {
            compatibility: options.compatibility ?? true,
          });
          options.onReady?.(term);
          client.reportLeaderReady({
            term,
            ...locks,
          });
        } catch (error) {
          const reason = error instanceof Error ? error.message : String(error);
          options.onFailure?.(term, reason);
          if (options.reportFailures) {
            client.reportLeaderFailed(term, reason);
            return;
          }
          throw error;
        }
      },
    };
  }

  it("shares one broker epoch and elects one leader for a namespace", async () => {
    const dbName = uniqueName("broker-election");
    const first = await BrowserBrokerClient.connect(createLockingOptions(dbName, "tab-a"));
    clients.push(first);
    const second = await BrowserBrokerClient.connect(createLockingOptions(dbName, "tab-b"));
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
    const first = await BrowserBrokerClient.connect(createLockingOptions(dbName, "tab-a"));
    clients.push(first);
    await first.waitForRole("leader", 2000);

    await expect(
      BrowserBrokerClient.connect(createOptions(dbName, "tab-b", "fingerprint-b")),
    ).rejects.toThrow("incompatible persistent browser configuration");
  });

  it("rejects a tab that reports a mismatched schema fingerprint", async () => {
    const dbName = uniqueName("broker-schema-fingerprint");
    const first = await BrowserBrokerClient.connect(createLockingOptions(dbName, "tab-a"));
    clients.push(first);
    first.reportSchemaReady("schema-a");
    await first.waitForRole("leader", 2000);

    const second = await BrowserBrokerClient.connect(createOptions(dbName, "tab-b"));
    clients.push(second);
    second.reportSchemaReady("schema-b");

    await expect(second.waitForRole("leader", 250)).rejects.toThrow(
      "incompatible persistent browser schema",
    );
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

  it("promotes a follower when the current leader locks are released", async () => {
    const dbName = uniqueName("broker-lock-release");
    const first = await BrowserBrokerClient.connect(createLockingOptions(dbName, "tab-a"));
    clients.push(first);
    const second = await BrowserBrokerClient.connect(createLockingOptions(dbName, "tab-b"));
    clients.push(second);

    await first.waitForRole("leader", 2000);
    await second.waitForRole("follower", 2000);

    releaseHeldLock(`jazz-leader-tab:broker-test-app:${dbName}`);
    releaseHeldLock(`jazz-leader-worker:broker-test-app:${dbName}`);
    releaseHeldLock(`jazz-leader-lock:broker-test-app:${dbName}`);

    await second.waitForRole("leader", 4000);
    expect(second.snapshot()).toMatchObject({
      role: "leader",
      tabId: "tab-b",
      leaderTabId: "tab-b",
      term: 2,
    });
  });

  it("steals stuck leader locks before promoting a replacement", async () => {
    const dbName = uniqueName("broker-force-takeover");
    const first = await BrowserBrokerClient.connect(
      createLockingOptions(dbName, "tab-a", "fingerprint-a", {
        compatibility: false,
        forceTakeoverTimeoutMs: 50,
      }),
    );
    clients.push(first);

    const secondReadyTerms: number[] = [];
    const second = await BrowserBrokerClient.connect(
      createLockingOptions(dbName, "tab-b", "fingerprint-a", {
        compatibility: false,
        forceTakeoverTimeoutMs: 50,
        reportFailures: true,
        onReady: (term) => secondReadyTerms.push(term),
      }),
    );
    clients.push(second);

    await first.waitForRole("leader", 2000);
    await second.waitForRole("follower", 2000);

    await first.shutdown();

    await waitFor(
      () => secondReadyTerms.some((term) => term > 1),
      4000,
      "stuck tab and worker locks should be stolen so the follower can report leader-ready",
    );
    expect(second.snapshot().leaderTabId).toBe("tab-b");
  });

  it("promotes a replacement when the migration compatibility lock is released", async () => {
    const dbName = uniqueName("broker-compat-lock-release");
    const first = await BrowserBrokerClient.connect(
      createLockingOptions(dbName, "tab-a", "fingerprint-a", {
        onDemote: () => {
          releaseHeldLock(`jazz-leader-tab:broker-test-app:${dbName}`);
          releaseHeldLock(`jazz-leader-worker:broker-test-app:${dbName}`);
        },
      }),
    );
    clients.push(first);
    const second = await BrowserBrokerClient.connect(createLockingOptions(dbName, "tab-b"));
    clients.push(second);

    await first.waitForRole("leader", 2000);
    await second.waitForRole("follower", 2000);

    releaseHeldLock(`jazz-leader-lock:broker-test-app:${dbName}`);

    await second.waitForRole("leader", 4000);
    expect(second.snapshot()).toMatchObject({
      role: "leader",
      tabId: "tab-b",
      leaderTabId: "tab-b",
      term: 2,
    });
  });

  it("does not steal a stuck migration compatibility lock before promoting a replacement", async () => {
    const dbName = uniqueName("broker-compat-force-takeover");
    const first = await BrowserBrokerClient.connect(
      createLockingOptions(dbName, "tab-a", "fingerprint-a", {
        forceTakeoverTimeoutMs: 50,
      }),
    );
    clients.push(first);

    const secondReadyTerms: number[] = [];
    const second = await BrowserBrokerClient.connect(
      createLockingOptions(dbName, "tab-b", "fingerprint-a", {
        forceTakeoverTimeoutMs: 50,
        reportFailures: true,
        onReady: (term) => secondReadyTerms.push(term),
      }),
    );
    clients.push(second);

    await first.waitForRole("leader", 2000);
    await second.waitForRole("follower", 2000);

    await first.shutdown();

    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(secondReadyTerms.some((term) => term > 1)).toBe(false);
  });

  it("does not repeatedly promote a candidate blocked by the migration compatibility lock", async () => {
    const dbName = uniqueName("broker-compat-blocked");
    const compatibilityLockName = `jazz-leader-lock:broker-test-app:${dbName}`;
    const compatibilityLease = await tryAcquireWebLock(compatibilityLockName);
    if (!compatibilityLease) {
      throw new Error("Unable to acquire broker compatibility lock");
    }
    lockLeases.set(compatibilityLockName, compatibilityLease);

    let promotionAttempts = 0;
    let failureReason: string | null = null;
    const client = await BrowserBrokerClient.connect({
      ...createOptions(dbName, "tab-a"),
      onBecomeLeader: async (broker, term) => {
        promotionAttempts++;
        const tabLease = await tryAcquireWebLock(`jazz-leader-tab:broker-test-app:${dbName}`);
        if (!tabLease) {
          failureReason = "Unable to acquire tab lock";
          broker.reportLeaderFailed(term, failureReason);
          return;
        }

        const migrationLease = await tryAcquireWebLock(compatibilityLockName);
        tabLease.release();
        if (migrationLease) {
          migrationLease.release();
          return;
        }

        failureReason = `Unable to acquire ${compatibilityLockName}`;
        broker.reportLeaderFailed(term, failureReason);
      },
    });
    clients.push(client);

    await waitFor(() => failureReason !== null, 2000, "candidate should report lock failure");
    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(failureReason).toContain(compatibilityLockName);
    expect(promotionAttempts).toBe(1);
  });

  it("replaces a promoted tab that has not reported schema before leader-ready", async () => {
    const dbName = uniqueName("broker-schema-ready-promotes");
    const promotionAttempts: string[] = [];
    const demotedTerms: number[] = [];

    const silent = await BrowserBrokerClient.connect({
      ...createOptions(dbName, "tab-a"),
      onBecomeLeader: async () => {
        promotionAttempts.push("tab-a");
      },
      onDemote: (term) => {
        demotedTerms.push(term);
      },
    });
    clients.push(silent);

    await waitFor(
      () => promotionAttempts.includes("tab-a"),
      2000,
      "first tab should be promoted before it has a schema",
    );

    const schemaReady = await BrowserBrokerClient.connect(
      createLockingOptions(dbName, "tab-b", "fingerprint-a", {
        onReady: () => promotionAttempts.push("tab-b"),
      }),
    );
    clients.push(schemaReady);

    schemaReady.reportSchemaReady("schema-a");

    await schemaReady.waitForRole("leader", 4000);
    expect(schemaReady.snapshot()).toMatchObject({
      role: "leader",
      tabId: "tab-b",
      leaderTabId: "tab-b",
    });
    expect(demotedTerms).toContain(1);
  });

  it("evicts a leader tab that misses broker pongs", async () => {
    const dbName = uniqueName("broker-pong-timeout");
    let silentLeaderPings = 0;
    let silentLeaderReady = false;
    const silentLeader = await BrowserBrokerClient.connect({
      ...createLockingOptions(dbName, "tab-silent", "fingerprint-a", {
        compatibility: false,
        forceTakeoverTimeoutMs: 50,
        brokerPingIntervalMs: 50,
        brokerPongTimeoutMs: 1_000,
        onReady: () => {
          silentLeaderReady = true;
        },
      }),
      respondToBrokerPings: () => !silentLeaderReady,
      onBrokerPing: () => {
        silentLeaderPings++;
      },
    });
    clients.push(silentLeader);
    await silentLeader.waitForRole("leader", 2000);

    const secondReadyTerms: number[] = [];
    const secondFailures: string[] = [];
    const secondClosedTerms: number[] = [];
    const second = await BrowserBrokerClient.connect({
      ...createLockingOptions(dbName, "tab-b", "fingerprint-a", {
        compatibility: false,
        forceTakeoverTimeoutMs: 50,
        onReady: (term) => secondReadyTerms.push(term),
        onFailure: (term, reason) => secondFailures.push(`${term}:${reason}`),
        reportFailures: true,
      }),
      onCloseFollowerPort: (term) => secondClosedTerms.push(term),
    });
    clients.push(second);

    await new Promise((resolve) => setTimeout(resolve, 1_100));
    second.reportVisibility("visible");

    await waitFor(
      () => secondReadyTerms.some((term) => term > 1),
      4000,
      `leader that misses broker pongs should be evicted and replaced; pings: ${silentLeaderPings}; ready: ${secondReadyTerms.join(", ")}; closed: ${secondClosedTerms.join(", ")}; failures: ${secondFailures.join(", ")}`,
    );
    expect(second.snapshot().leaderTabId).toBe("tab-b");
  });

  it("coordinates storage reset and promotes a reset leader", async () => {
    const dbName = uniqueName("broker-storage-reset");
    const requestId = `reset-${dbName}`;
    const resetBegunByTab: string[] = [];
    const resetPromotions: Array<{ tabId: string; requestId: string; term: number }> = [];
    const clientByTabId = new Map<string, BrowserBrokerClient>();

    function createResetAwareOptions(
      tabId: string,
    ): Parameters<typeof BrowserBrokerClient.connect>[0] {
      return {
        ...createOptions(dbName, tabId),
        forceTakeoverTimeoutMs: 50,
        onStorageResetBegin: async (receivedRequestId) => {
          resetBegunByTab.push(`${tabId}:${receivedRequestId}`);
          releaseHeldLock(`jazz-leader-tab:broker-test-app:${dbName}`);
          releaseHeldLock(`jazz-leader-worker:broker-test-app:${dbName}`);
          releaseHeldLock(`jazz-leader-lock:broker-test-app:${dbName}`);
        },
        onBecomeLeader: async (client, term, resetRequestId) => {
          if (resetRequestId) {
            resetPromotions.push({ tabId, requestId: resetRequestId, term });
          }
          const locks = await acquireLeaderLocks(dbName, { compatibility: false });
          client.reportLeaderReady({
            term,
            ...locks,
          });
        },
        onAttachFollowerPort: (followerTabId, term, port) => {
          port.close();
          clientByTabId.get(tabId)?.reportFollowerPortAttached(followerTabId, term);
        },
        onUseFollowerPort: (_leaderTabId, _term, port) => {
          port.close();
        },
      };
    }

    const first = await BrowserBrokerClient.connect(createResetAwareOptions("tab-a"));
    clientByTabId.set("tab-a", first);
    clients.push(first);
    const second = await BrowserBrokerClient.connect(createResetAwareOptions("tab-b"));
    clientByTabId.set("tab-b", second);
    clients.push(second);
    const schemaless = await BrowserBrokerClient.connect(createResetAwareOptions("tab-c"));
    clientByTabId.set("tab-c", schemaless);
    clients.push(schemaless);

    await first.waitForRole("leader", 2000);
    first.reportSchemaReady("schema-a");
    second.reportSchemaReady("schema-a");
    await second.waitForRole("follower", 2000);

    const reset = second.requestStorageReset(requestId);
    reset.catch(() => undefined);

    await waitFor(
      () =>
        resetBegunByTab.includes(`tab-a:${requestId}`) &&
        resetBegunByTab.includes(`tab-b:${requestId}`) &&
        resetBegunByTab.includes(`tab-c:${requestId}`),
      2000,
      "broker should ask every connected tab to prepare storage reset",
    );

    await reset;

    expect(resetPromotions).toContainEqual(
      expect.objectContaining({
        requestId,
      }),
    );
    expect(second.snapshot().term).toBeGreaterThan(1);
  });

  it("continues storage reset when the promoted reset leader is evicted before ready", async () => {
    const dbName = uniqueName("broker-reset-leader-evicted");
    const requestId = `reset-${dbName}`;
    let silentResetLeader = false;
    const resetPromotions: string[] = [];

    function createResetEvictionOptions(
      tabId: string,
    ): Parameters<typeof BrowserBrokerClient.connect>[0] {
      return {
        ...createOptions(dbName, tabId),
        forceTakeoverTimeoutMs: 50,
        brokerPingIntervalMs: 20,
        brokerPongTimeoutMs: 60,
        respondToBrokerPings: () => tabId !== "tab-b" || !silentResetLeader,
        onStorageResetBegin: async () => {
          releaseHeldLock(`jazz-leader-tab:broker-test-app:${dbName}`);
          releaseHeldLock(`jazz-leader-worker:broker-test-app:${dbName}`);
          releaseHeldLock(`jazz-leader-lock:broker-test-app:${dbName}`);
        },
        onBecomeLeader: async (client, term, resetRequestId) => {
          if (resetRequestId) {
            resetPromotions.push(`${tabId}:${resetRequestId}`);
            if (tabId === "tab-b") {
              silentResetLeader = true;
              return;
            }
          }
          const locks = await acquireLeaderLocks(dbName, { compatibility: false });
          client.reportLeaderReady({
            term,
            ...locks,
          });
        },
        onAttachFollowerPort: (followerTabId, term, port) => {
          port.close();
          if (followerTabId === "tab-b") return;
          if (tabId === "tab-a") {
            first?.reportFollowerPortAttached(followerTabId, term);
          }
        },
        onUseFollowerPort: (_leaderTabId, _term, port) => {
          port.close();
        },
      };
    }

    let first: BrowserBrokerClient | null = null;
    first = await BrowserBrokerClient.connect(createResetEvictionOptions("tab-a"));
    clients.push(first);
    const second = await BrowserBrokerClient.connect(createResetEvictionOptions("tab-b"));
    clients.push(second);

    await first.waitForRole("leader", 2000);
    await second.waitForRole("follower", 2000);

    const reset = first.requestStorageReset(requestId);
    reset.catch(() => undefined);

    await reset;

    expect(resetPromotions).toContain(`tab-b:${requestId}`);
    expect(resetPromotions).toContain(`tab-a:${requestId}`);
    expect(first.snapshot().leaderTabId).toBe("tab-a");
  });
});

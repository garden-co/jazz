import { afterEach, describe, expect, it } from "vitest";
import { BrowserBrokerClient } from "../../src/runtime/browser-broker-client.js";
import { acquireWebLockWithRetry, type LeaderLockLease } from "../../src/runtime/leader-lock.js";

function uniqueName(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

const BROKER_TEST_APP_ID = "broker-test-app";

function tabLockName(dbName: string): string {
  return `jazz-leader-tab:${BROKER_TEST_APP_ID}:${dbName}`;
}

function workerLockName(dbName: string): string {
  return `jazz-leader-worker:${BROKER_TEST_APP_ID}:${dbName}`;
}

function leaderLockNames(dbName: string): { tabLockName: string; workerLockName: string } {
  return {
    tabLockName: tabLockName(dbName),
    workerLockName: workerLockName(dbName),
  };
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

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<never>((_resolve, reject) => {
    timeout = setTimeout(() => reject(new Error(message)), timeoutMs);
  });
  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

function createOptions(
  dbName: string,
  tabId: string,
  fingerprint = "fingerprint-a",
): Parameters<typeof BrowserBrokerClient.connect>[0] {
  return {
    appId: BROKER_TEST_APP_ID,
    dbName,
    tabId,
    fingerprint,
    visibility: "visible",
    onBecomeLeader: async (client, leadershipId) => {
      client.reportLeaderReady({
        leadershipId,
        ...leaderLockNames(dbName),
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

  async function acquireLeaderLocks(dbName: string): Promise<{
    tabLockName: string;
    workerLockName: string;
  }> {
    const { tabLockName, workerLockName } = leaderLockNames(dbName);
    const tabLease = await acquireWebLockWithRetry(tabLockName);
    const workerLease = await acquireWebLockWithRetry(workerLockName);
    if (!tabLease || !workerLease) {
      tabLease?.release();
      workerLease?.release();
      throw new Error("Unable to acquire broker test leader locks");
    }

    lockLeases.set(tabLockName, tabLease);
    lockLeases.set(workerLockName, workerLease);
    return { tabLockName, workerLockName };
  }

  function releaseLeaderLocks(dbName: string): void {
    const { tabLockName, workerLockName } = leaderLockNames(dbName);
    releaseHeldLock(tabLockName);
    releaseHeldLock(workerLockName);
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
      forceTakeoverTimeoutMs?: number;
      brokerPingIntervalMs?: number;
      brokerPongTimeoutMs?: number;
      onReady?: (leadershipId: number) => void;
      onDemote?: (leadershipId: number) => void;
      onFailure?: (leadershipId: number, reason: string) => void;
      reportFailures?: boolean;
    } = {},
  ): Parameters<typeof BrowserBrokerClient.connect>[0] {
    return {
      ...createOptions(dbName, tabId, fingerprint),
      forceTakeoverTimeoutMs: options.forceTakeoverTimeoutMs,
      brokerPingIntervalMs: options.brokerPingIntervalMs,
      brokerPongTimeoutMs: options.brokerPongTimeoutMs,
      onDemote: options.onDemote,
      onBecomeLeader: async (client, leadershipId) => {
        try {
          const locks = await acquireLeaderLocks(dbName);
          options.onReady?.(leadershipId);
          client.reportLeaderReady({
            leadershipId,
            ...locks,
          });
        } catch (error) {
          const reason = error instanceof Error ? error.message : String(error);
          options.onFailure?.(leadershipId, reason);
          if (options.reportFailures) {
            client.reportLeaderFailed(leadershipId, reason);
            return;
          }
          throw error;
        }
      },
    };
  }

  it("shares one broker instance and elects one leader for a namespace", async () => {
    const dbName = uniqueName("broker-election");
    const first = await BrowserBrokerClient.connect(createLockingOptions(dbName, "tab-a"));
    clients.push(first);
    const second = await BrowserBrokerClient.connect(createLockingOptions(dbName, "tab-b"));
    clients.push(second);

    await first.waitForRole("leader", 2000);
    await second.waitForRole("follower", 2000);

    expect(first.snapshot().brokerInstanceId).toEqual(second.snapshot().brokerInstanceId);
    expect(first.snapshot()).toMatchObject({
      role: "leader",
      tabId: "tab-a",
      leaderTabId: "tab-a",
      leadershipId: 1,
    });
    expect(second.snapshot()).toMatchObject({
      role: "follower",
      tabId: "tab-b",
      leaderTabId: "tab-a",
      leadershipId: 1,
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

  it("blocks a schema-mismatched tab and promotes it after the pinning tab departs", async () => {
    const dbName = uniqueName("broker-schema-blocked");
    const blockedReasons: string[] = [];

    const first = await BrowserBrokerClient.connect(
      createLockingOptions(dbName, "tab-a", "fingerprint-a", { forceTakeoverTimeoutMs: 50 }),
    );
    clients.push(first);
    first.reportSchemaReady("schema-a");
    await first.waitForRole("leader", 2000);

    const second = await BrowserBrokerClient.connect({
      ...createLockingOptions(dbName, "tab-b", "fingerprint-a", { forceTakeoverTimeoutMs: 50 }),
      onSchemaBlocked: (reason: string) => {
        blockedReasons.push(reason);
      },
    } as Parameters<typeof BrowserBrokerClient.connect>[0]);
    clients.push(second);
    second.reportSchemaReady("schema-b");

    await waitFor(
      () => blockedReasons.length > 0,
      2000,
      "mismatched tab should be told it is schema-blocked",
    );
    expect(blockedReasons[0]).toContain("incompatible persistent browser schema");

    await first.shutdown();
    clients.splice(clients.indexOf(first), 1);
    releaseLeaderLocks(dbName);

    await second.waitForRole("leader", 4000);
    expect(second.snapshot().role).toBe("leader");
  });

  it("keeps a replacement leader elected while a non-ready schema leader shuts down", async () => {
    const dbName = uniqueName("broker-shutdown-replacement");
    const replacementFailures: string[] = [];
    const replacementReadyLeadershipIds: number[] = [];

    const staleLeader = await BrowserBrokerClient.connect({
      ...createOptions(dbName, "tab-a"),
      onBecomeLeader: async () => {
        // Simulate a promoted tab that reported its schema but never finished
        // opening the persistent worker.
      },
    });
    clients.push(staleLeader);

    await waitFor(
      () => staleLeader.snapshot().leadershipId === 1,
      2000,
      "first tab should receive a leader promotion",
    );
    staleLeader.reportSchemaReady("schema-a");

    const replacement = await BrowserBrokerClient.connect(
      createLockingOptions(dbName, "tab-b", "fingerprint-a", {
        forceTakeoverTimeoutMs: 50,
        onReady: (leadershipId) => replacementReadyLeadershipIds.push(leadershipId),
        onFailure: (_leadershipId, reason) => replacementFailures.push(reason),
      }),
    );
    clients.push(replacement);
    replacement.reportSchemaReady("schema-b");

    await staleLeader.shutdown();
    clients.splice(clients.indexOf(staleLeader), 1);

    await replacement.waitForRole("leader", 4000);
    await new Promise((resolve) => setTimeout(resolve, 150));

    expect(replacement.snapshot()).toMatchObject({
      role: "leader",
      tabId: "tab-b",
      leaderTabId: "tab-b",
    });
    expect(replacementReadyLeadershipIds).toHaveLength(1);
    expect(replacementFailures).toEqual([]);
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

    releaseLeaderLocks(dbName);

    await second.waitForRole("leader", 4000);
    expect(second.snapshot()).toMatchObject({
      role: "leader",
      tabId: "tab-b",
      leaderTabId: "tab-b",
      leadershipId: 2,
    });
  });

  it("keeps leadership ids monotonic after the broker becomes idle", async () => {
    const dbName = uniqueName("broker-idle-leadership");
    const first = await BrowserBrokerClient.connect(createLockingOptions(dbName, "tab-a"));
    clients.push(first);

    await first.waitForRole("leader", 2000);
    const firstLeadershipId = first.snapshot().leadershipId;

    await first.shutdown();
    clients.splice(clients.indexOf(first), 1);
    releaseLeaderLocks(dbName);

    const second = await BrowserBrokerClient.connect(createLockingOptions(dbName, "tab-b"));
    clients.push(second);
    await second.waitForRole("leader", 2000);

    expect(second.snapshot().leadershipId).toBeGreaterThan(firstLeadershipId);
  });

  it("steals stuck leader locks before promoting a replacement", async () => {
    const dbName = uniqueName("broker-force-takeover");
    const first = await BrowserBrokerClient.connect(
      createLockingOptions(dbName, "tab-a", "fingerprint-a", {
        forceTakeoverTimeoutMs: 50,
      }),
    );
    clients.push(first);

    const secondReadyLeadershipIds: number[] = [];
    const second = await BrowserBrokerClient.connect(
      createLockingOptions(dbName, "tab-b", "fingerprint-a", {
        forceTakeoverTimeoutMs: 50,
        reportFailures: true,
        onReady: (leadershipId) => secondReadyLeadershipIds.push(leadershipId),
      }),
    );
    clients.push(second);

    await first.waitForRole("leader", 2000);
    await second.waitForRole("follower", 2000);

    await first.shutdown();

    await waitFor(
      () => secondReadyLeadershipIds.some((leadershipId) => leadershipId > 1),
      4000,
      "stuck tab and worker locks should be stolen so the follower can report leader-ready",
    );
    expect(second.snapshot().leaderTabId).toBe("tab-b");
  });

  it("does not let a stale replacement election steal a newly promoted leader's reused locks", async () => {
    const dbName = uniqueName("broker-reused-lock-takeover");
    const forceTakeoverTimeoutMs = 80;
    const first = await BrowserBrokerClient.connect(
      createLockingOptions(dbName, "tab-a", "fingerprint-a", {
        forceTakeoverTimeoutMs,
      }),
    );
    clients.push(first);
    const second = await BrowserBrokerClient.connect(
      createLockingOptions(dbName, "tab-b", "fingerprint-a", {
        forceTakeoverTimeoutMs,
      }),
    );
    clients.push(second);

    await first.waitForRole("leader", 2000);
    await second.waitForRole("follower", 2000);

    await first.shutdown();
    await new Promise((resolve) => setTimeout(resolve, 20));
    releaseLeaderLocks(dbName);

    const thirdDemotedLeadershipIds: number[] = [];
    const third = await BrowserBrokerClient.connect(
      createLockingOptions(dbName, "tab-c", "fingerprint-a", {
        forceTakeoverTimeoutMs,
        onDemote: (leadershipId) => thirdDemotedLeadershipIds.push(leadershipId),
      }),
    );
    clients.push(third);

    await third.waitForRole("leader", 2000);
    await new Promise((resolve) => setTimeout(resolve, forceTakeoverTimeoutMs + 100));

    expect(third.snapshot()).toMatchObject({
      role: "leader",
      tabId: "tab-c",
      leaderTabId: "tab-c",
    });
    expect(thirdDemotedLeadershipIds).toEqual([]);
  });

  it("replaces a promoted tab that has not reported schema before leader-ready", async () => {
    const dbName = uniqueName("broker-schema-ready-promotes");
    const promotionAttempts: string[] = [];
    const demotedLeadershipIds: number[] = [];

    const silent = await BrowserBrokerClient.connect({
      ...createOptions(dbName, "tab-a"),
      onBecomeLeader: async () => {
        promotionAttempts.push("tab-a");
      },
      onDemote: (leadershipId) => {
        demotedLeadershipIds.push(leadershipId);
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
    expect(demotedLeadershipIds).toContain(1);
  });

  it("demotes a failed leader candidate without closing the tab", async () => {
    const dbName = uniqueName("broker-leader-failed-demote");
    const promotionAttempts: number[] = [];
    const demotedLeadershipIds: number[] = [];
    const closedErrors: string[] = [];

    const failedCandidate = await BrowserBrokerClient.connect({
      ...createOptions(dbName, "tab-a"),
      brokerPingIntervalMs: 50,
      brokerPongTimeoutMs: 5_000,
      onBecomeLeader: async (client, leadershipId) => {
        promotionAttempts.push(leadershipId);
        client.reportLeaderFailed(leadershipId, "boom");
      },
      onDemote: (leadershipId) => {
        demotedLeadershipIds.push(leadershipId);
      },
      onClosed: (error) => {
        closedErrors.push(error.message);
      },
    });
    clients.push(failedCandidate);

    await waitFor(
      () => promotionAttempts.length === 1,
      2000,
      "candidate should receive its first leader promotion",
    );
    await waitFor(
      () => demotedLeadershipIds.includes(1),
      2000,
      "failed leader candidate should be demoted with the reported leadership id",
    );
    expect(closedErrors).toEqual([]);

    const replacement = await BrowserBrokerClient.connect(createLockingOptions(dbName, "tab-b"));
    clients.push(replacement);
    await replacement.waitForRole("leader", 2_000);

    expect(promotionAttempts).toEqual([1]);
    expect(failedCandidate.snapshot().role).toBe("follower");
  });

  it("steals locks from a failed candidate that never reported leader-ready", async () => {
    const dbName = uniqueName("broker-pre-ready-lock-takeover");
    const failedLeadershipIds: number[] = [];
    const replacementReadyLeadershipIds: number[] = [];

    const failedCandidate = await BrowserBrokerClient.connect({
      ...createOptions(dbName, "tab-a"),
      forceTakeoverTimeoutMs: 50,
      onBecomeLeader: async (client, leadershipId) => {
        await acquireLeaderLocks(dbName);
        failedLeadershipIds.push(leadershipId);
        client.reportLeaderFailed(leadershipId, "worker bootstrap hung");
      },
    });
    clients.push(failedCandidate);

    const replacement = await BrowserBrokerClient.connect(
      createLockingOptions(dbName, "tab-b", "fingerprint-a", {
        forceTakeoverTimeoutMs: 50,
        reportFailures: true,
        onReady: (leadershipId) => replacementReadyLeadershipIds.push(leadershipId),
      }),
    );
    clients.push(replacement);

    await waitFor(
      () => failedLeadershipIds.includes(1),
      2000,
      "first candidate should fail after taking the leader locks",
    );
    await replacement.waitForRole("leader", 4000);

    expect(replacementReadyLeadershipIds.some((leadershipId) => leadershipId > 1)).toBe(true);
  });

  it("demotes a stale candidate that reports leader-ready after being replaced", async () => {
    const dbName = uniqueName("broker-stale-leader-ready");
    const demotedLeadershipIds: number[] = [];
    let reportStaleLeaderReady: (() => void) | null = null;

    const staleCandidate = await BrowserBrokerClient.connect({
      ...createOptions(dbName, "tab-a"),
      onBecomeLeader: async (client, leadershipId) => {
        reportStaleLeaderReady = () => {
          client.reportLeaderReady({
            leadershipId,
            ...leaderLockNames(dbName),
          });
        };
      },
      onDemote: (leadershipId) => {
        demotedLeadershipIds.push(leadershipId);
      },
    });
    clients.push(staleCandidate);

    await waitFor(
      () => reportStaleLeaderReady !== null,
      2000,
      "first tab should receive a leader promotion",
    );

    const replacement = await BrowserBrokerClient.connect(createLockingOptions(dbName, "tab-b"));
    clients.push(replacement);
    replacement.reportSchemaReady("schema-a");

    await replacement.waitForRole("leader", 4000);
    await waitFor(
      () => demotedLeadershipIds.includes(1),
      2000,
      "broker should demote the replaced candidate",
    );

    const demotionsBeforeStaleReady = demotedLeadershipIds.length;
    reportStaleLeaderReady!();

    await waitFor(
      () => demotedLeadershipIds.length > demotionsBeforeStaleReady,
      2000,
      "broker should send a scoped demote for stale leader-ready",
    );
    expect(demotedLeadershipIds.slice(demotionsBeforeStaleReady)).toContain(1);
  });

  it("evicts a leader tab that misses broker pongs", async () => {
    const dbName = uniqueName("broker-pong-timeout");
    let silentLeaderPings = 0;
    let silentLeaderReady = false;
    const silentLeader = await BrowserBrokerClient.connect({
      ...createLockingOptions(dbName, "tab-silent", "fingerprint-a", {
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

    const secondReadyLeadershipIds: number[] = [];
    const secondFailures: string[] = [];
    const secondClosedLeadershipIds: number[] = [];
    const second = await BrowserBrokerClient.connect({
      ...createLockingOptions(dbName, "tab-b", "fingerprint-a", {
        forceTakeoverTimeoutMs: 50,
        onReady: (leadershipId) => secondReadyLeadershipIds.push(leadershipId),
        onFailure: (leadershipId, reason) => secondFailures.push(`${leadershipId}:${reason}`),
        reportFailures: true,
      }),
      onCloseFollowerPort: (leadershipId) => secondClosedLeadershipIds.push(leadershipId),
    });
    clients.push(second);

    await new Promise((resolve) => setTimeout(resolve, 1_100));
    second.reportVisibility("visible");

    await waitFor(
      () => secondReadyLeadershipIds.some((leadershipId) => leadershipId > 1),
      4000,
      `leader that misses broker pongs should be evicted and replaced; pings: ${silentLeaderPings}; ready: ${secondReadyLeadershipIds.join(", ")}; closed: ${secondClosedLeadershipIds.join(", ")}; failures: ${secondFailures.join(", ")}`,
    );
    expect(second.snapshot().leaderTabId).toBe("tab-b");
  });

  it("coordinates storage reset and promotes a reset leader", async () => {
    const dbName = uniqueName("broker-storage-reset");
    const requestId = `reset-${dbName}`;
    const resetBegunByTab: string[] = [];
    const resetPromotions: Array<{ tabId: string; requestId: string; leadershipId: number }> = [];
    const clientByTabId = new Map<string, BrowserBrokerClient>();

    function createResetAwareOptions(
      tabId: string,
    ): Parameters<typeof BrowserBrokerClient.connect>[0] {
      return {
        ...createOptions(dbName, tabId),
        forceTakeoverTimeoutMs: 50,
        onStorageResetBegin: async (receivedRequestId) => {
          resetBegunByTab.push(`${tabId}:${receivedRequestId}`);
          releaseLeaderLocks(dbName);
        },
        onBecomeLeader: async (client, leadershipId, resetRequestId) => {
          if (resetRequestId) {
            resetPromotions.push({ tabId, requestId: resetRequestId, leadershipId });
          }
          const locks = await acquireLeaderLocks(dbName);
          client.reportLeaderReady({
            leadershipId,
            ...locks,
          });
        },
        onAttachFollowerPort: (followerTabId, leadershipId, port) => {
          port.close();
          clientByTabId.get(tabId)?.reportFollowerPortAttached(followerTabId, leadershipId);
        },
        onUseFollowerPort: (_leaderTabId, _leadershipId, port) => {
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
    expect(second.snapshot().leadershipId).toBeGreaterThan(1);
  });

  it("ignores storage-reset-ready from a tab that already left the reset", async () => {
    const dbName = uniqueName("broker-storage-reset-late-ready");
    const requestId = `reset-${dbName}`;
    const resetBegun: string[] = [];
    const resetPromotions: string[] = [];
    const clientByTabId = new Map<string, BrowserBrokerClient>();
    let releaseDepartedTab!: () => void;
    let releaseRemainingTab!: () => void;
    const departedTabReady = new Promise<void>((resolve) => {
      releaseDepartedTab = resolve;
    });
    const remainingTabReady = new Promise<void>((resolve) => {
      releaseRemainingTab = resolve;
    });

    function createLateReadyOptions(
      tabId: string,
    ): Parameters<typeof BrowserBrokerClient.connect>[0] {
      return {
        ...createOptions(dbName, tabId),
        forceTakeoverTimeoutMs: 50,
        onStorageResetBegin: async () => {
          resetBegun.push(tabId);
          releaseLeaderLocks(dbName);
          await (tabId === "tab-a" ? departedTabReady : remainingTabReady);
        },
        onBecomeLeader: async (client, leadershipId, resetRequestId) => {
          if (resetRequestId) {
            resetPromotions.push(`${tabId}:${resetRequestId}`);
          }
          const locks = await acquireLeaderLocks(dbName);
          client.reportLeaderReady({
            leadershipId,
            ...locks,
          });
        },
        onAttachFollowerPort: (followerTabId, leadershipId, port) => {
          port.close();
          clientByTabId.get(tabId)?.reportFollowerPortAttached(followerTabId, leadershipId);
        },
        onUseFollowerPort: (_leaderTabId, _leadershipId, port) => {
          port.close();
        },
      };
    }

    const first = await BrowserBrokerClient.connect(createLateReadyOptions("tab-a"));
    clientByTabId.set("tab-a", first);
    clients.push(first);
    const second = await BrowserBrokerClient.connect(createLateReadyOptions("tab-b"));
    clientByTabId.set("tab-b", second);
    clients.push(second);

    await first.waitForRole("leader", 2000);
    first.reportSchemaReady("schema-a");
    second.reportSchemaReady("schema-a");
    await second.waitForRole("follower", 2000);

    const reset = second.requestStorageReset(requestId);
    reset.catch(() => undefined);

    await waitFor(
      () => resetBegun.includes("tab-a") && resetBegun.includes("tab-b"),
      2000,
      "broker should ask both tabs to prepare storage reset",
    );

    const brokerInstanceId = first.snapshot().brokerInstanceId;
    if (!brokerInstanceId) throw new Error("Expected first tab to be attached to a broker");
    (first as unknown as { port?: MessagePort | null }).port?.postMessage({
      type: "shutdown",
      brokerInstanceId,
    });
    releaseDepartedTab();

    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(resetPromotions).toEqual([]);

    releaseRemainingTab();
    await reset;
    expect(resetPromotions).toContain(`tab-b:${requestId}`);
  });

  it("settles every caller that joins an active storage reset", async () => {
    const dbName = uniqueName("broker-storage-reset-join");
    const firstRequestId = `reset-a-${dbName}`;
    const secondRequestId = `reset-b-${dbName}`;
    const resetBegun: string[] = [];
    const resetPromotions: string[] = [];
    const clientByTabId = new Map<string, BrowserBrokerClient>();
    let releaseResetPreparation!: () => void;
    const resetPreparation = new Promise<void>((resolve) => {
      releaseResetPreparation = resolve;
    });

    function createResetJoinOptions(
      tabId: string,
    ): Parameters<typeof BrowserBrokerClient.connect>[0] {
      return {
        ...createOptions(dbName, tabId),
        forceTakeoverTimeoutMs: 50,
        storageResetTimeoutMs: 50,
        onStorageResetBegin: async (requestId) => {
          resetBegun.push(`${tabId}:${requestId}`);
          releaseLeaderLocks(dbName);
          await resetPreparation;
        },
        onBecomeLeader: async (client, leadershipId, resetRequestId) => {
          if (resetRequestId) {
            resetPromotions.push(`${tabId}:${resetRequestId}`);
          }
          const locks = await acquireLeaderLocks(dbName);
          client.reportLeaderReady({
            leadershipId,
            ...locks,
          });
        },
        onAttachFollowerPort: (followerTabId, leadershipId, port) => {
          port.close();
          clientByTabId.get(tabId)?.reportFollowerPortAttached(followerTabId, leadershipId);
        },
        onUseFollowerPort: (_leaderTabId, _leadershipId, port) => {
          port.close();
        },
      };
    }

    const first = await BrowserBrokerClient.connect(createResetJoinOptions("tab-a"));
    clientByTabId.set("tab-a", first);
    clients.push(first);
    const second = await BrowserBrokerClient.connect(createResetJoinOptions("tab-b"));
    clientByTabId.set("tab-b", second);
    clients.push(second);

    await first.waitForRole("leader", 2000);
    first.reportSchemaReady("schema-a");
    second.reportSchemaReady("schema-a");
    await second.waitForRole("follower", 2000);

    const firstReset = first.requestStorageReset(firstRequestId);
    firstReset.catch(() => undefined);

    await waitFor(
      () =>
        resetBegun.includes(`tab-a:${firstRequestId}`) &&
        resetBegun.includes(`tab-b:${firstRequestId}`),
      2000,
      "first reset should enter prepare before another caller joins it",
    );

    let secondResetOutcome: "pending" | "resolved" | string = "pending";
    const secondReset = second.requestStorageReset(secondRequestId).then(
      () => {
        secondResetOutcome = "resolved";
      },
      (error) => {
        secondResetOutcome = error instanceof Error ? error.message : String(error);
      },
    );
    secondReset.catch(() => undefined);

    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(secondResetOutcome).toBe("pending");

    releaseResetPreparation();
    await withTimeout(firstReset, 4000, "first reset request should settle");
    await withTimeout(secondReset, 4000, "second reset request should settle");

    expect(resetBegun).toContain(`tab-a:${firstRequestId}`);
    expect(resetBegun).toContain(`tab-b:${firstRequestId}`);
    expect(resetPromotions.some((entry) => entry.endsWith(`:${firstRequestId}`))).toBe(true);
  });

  it("rejects a storage reset request after the requester reconnects mid-reset", async () => {
    const dbName = uniqueName("broker-storage-reset-redeliver");
    const requestId = `reset-${dbName}`;
    const resetBegun: string[] = [];
    const reconnects: string[] = [];
    const clientByTabId = new Map<string, BrowserBrokerClient>();
    let releaseLeaderPreparation!: () => void;
    const leaderPreparation = new Promise<void>((resolve) => {
      releaseLeaderPreparation = resolve;
    });

    function createResetRedeliveryOptions(
      tabId: string,
    ): Parameters<typeof BrowserBrokerClient.connect>[0] {
      return {
        ...createOptions(dbName, tabId),
        forceTakeoverTimeoutMs: 50,
        brokerPingIntervalMs: 50,
        brokerPongTimeoutMs: 100,
        storageResetTimeoutMs: 2_500,
        onStorageResetBegin: async (requestId) => {
          resetBegun.push(`${tabId}:${requestId}`);
          releaseLeaderLocks(dbName);
          if (tabId === "tab-a") {
            await leaderPreparation;
          }
        },
        onBecomeLeader: async (client, leadershipId) => {
          const locks = await acquireLeaderLocks(dbName);
          client.reportLeaderReady({
            leadershipId,
            ...locks,
          });
        },
        onAttachFollowerPort: (followerTabId, leadershipId, port) => {
          port.close();
          clientByTabId.get(tabId)?.reportFollowerPortAttached(followerTabId, leadershipId);
        },
        onUseFollowerPort: (_leaderTabId, _leadershipId, port) => {
          port.close();
        },
        onReconnected: (client) => {
          reconnects.push(tabId);
          client.reportSchemaReady("schema-a");
        },
      };
    }

    const first = await BrowserBrokerClient.connect(createResetRedeliveryOptions("tab-a"));
    clientByTabId.set("tab-a", first);
    clients.push(first);
    const second = await BrowserBrokerClient.connect(createResetRedeliveryOptions("tab-b"));
    clientByTabId.set("tab-b", second);
    clients.push(second);

    await first.waitForRole("leader", 2000);
    first.reportSchemaReady("schema-a");
    second.reportSchemaReady("schema-a");
    await second.waitForRole("follower", 2000);

    const reset = second.requestStorageReset(requestId);
    reset.catch(() => undefined);

    await waitFor(
      () => resetBegun.includes(`tab-a:${requestId}`) && resetBegun.includes(`tab-b:${requestId}`),
      2000,
      "broker should ask both tabs to prepare storage reset",
    );

    (second as unknown as { port?: MessagePort | null }).port?.close();
    await waitFor(
      () => reconnects.includes("tab-b"),
      5000,
      "reset requester should reconnect before the broker finishes the reset",
    );
    releaseLeaderPreparation();

    await expect(
      withTimeout(reset, 3000, "reset requester should reject after reconnecting mid-reset"),
    ).rejects.toThrow("Browser broker restarted during storage reset");
    expect(reconnects).toContain("tab-b");
  });

  it("reattaches a same-tab follower after shutdown and reconnect", async () => {
    const dbName = uniqueName("broker-follower-reattach");
    const attachedFollowers: string[] = [];
    const detachedFollowers: string[] = [];

    const leaderClientByTabId = new Map<string, BrowserBrokerClient>();
    function createFollowerAttachOptions(
      tabId: string,
    ): Parameters<typeof BrowserBrokerClient.connect>[0] {
      return {
        ...createOptions(dbName, tabId),
        onBecomeLeader: async (client, leadershipId) => {
          const locks = await acquireLeaderLocks(dbName);
          client.reportLeaderReady({
            leadershipId,
            ...locks,
          });
        },
        onAttachFollowerPort: (followerTabId, leadershipId, port) => {
          attachedFollowers.push(`${followerTabId}:${leadershipId}`);
          port.close();
          leaderClientByTabId.get(tabId)?.reportFollowerPortAttached(followerTabId, leadershipId);
        },
        onDetachFollowerPort: (followerTabId, leadershipId) => {
          detachedFollowers.push(`${followerTabId}:${leadershipId}`);
        },
        onUseFollowerPort: (_leaderTabId, _leadershipId, port) => {
          port.close();
        },
      } as Parameters<typeof BrowserBrokerClient.connect>[0];
    }

    const leaderTab = await BrowserBrokerClient.connect(createFollowerAttachOptions("tab-a"));
    leaderClientByTabId.set("tab-a", leaderTab);
    clients.push(leaderTab);
    await leaderTab.waitForRole("leader", 2000);
    leaderTab.reportSchemaReady("schema-a");

    const firstFollower = await BrowserBrokerClient.connect(createFollowerAttachOptions("tab-b"));
    clients.push(firstFollower);
    firstFollower.reportSchemaReady("schema-a");
    await firstFollower.waitForRole("follower", 2000);
    await waitFor(
      () => attachedFollowers.includes("tab-b:1"),
      2000,
      "initial follower attachment should complete",
    );

    await firstFollower.shutdown();
    clients.splice(clients.indexOf(firstFollower), 1);

    await waitFor(
      () => detachedFollowers.includes("tab-b:1"),
      2000,
      "leader should be asked to detach the closed follower",
    );

    const secondFollower = await BrowserBrokerClient.connect(createFollowerAttachOptions("tab-b"));
    clients.push(secondFollower);
    secondFollower.reportSchemaReady("schema-a");

    await waitFor(
      () => attachedFollowers.filter((entry) => entry === "tab-b:1").length === 2,
      2000,
      "same-tab follower should receive a fresh attachment after reconnect",
    );
  });

  it("reattaches a same-tab follower that reconnects without broker eviction", async () => {
    const dbName = uniqueName("broker-follower-rehello");
    const attachedFollowers: string[] = [];
    let leaderClient: BrowserBrokerClient | null = null;

    function createFollowerAttachOptions(
      tabId: string,
    ): Parameters<typeof BrowserBrokerClient.connect>[0] {
      return {
        ...createOptions(dbName, tabId),
        onBecomeLeader: async (client, leadershipId) => {
          const locks = await acquireLeaderLocks(dbName);
          client.reportLeaderReady({
            leadershipId,
            ...locks,
          });
        },
        onAttachFollowerPort: (followerTabId, leadershipId, port) => {
          attachedFollowers.push(`${followerTabId}:${leadershipId}`);
          port.close();
          leaderClient?.reportFollowerPortAttached(followerTabId, leadershipId);
        },
        onUseFollowerPort: (_leaderTabId, _leadershipId, port) => {
          port.close();
        },
      } as Parameters<typeof BrowserBrokerClient.connect>[0];
    }

    leaderClient = await BrowserBrokerClient.connect(createFollowerAttachOptions("tab-a"));
    clients.push(leaderClient);
    await leaderClient.waitForRole("leader", 2000);
    leaderClient.reportSchemaReady("schema-a");

    const firstFollower = await BrowserBrokerClient.connect(createFollowerAttachOptions("tab-b"));
    clients.push(firstFollower);
    firstFollower.reportSchemaReady("schema-a");
    await waitFor(
      () => attachedFollowers.includes("tab-b:1"),
      2000,
      "initial follower attachment should complete",
    );

    const secondFollower = await BrowserBrokerClient.connect(createFollowerAttachOptions("tab-b"));
    clients.push(secondFollower);
    secondFollower.reportSchemaReady("schema-a");

    await waitFor(
      () => attachedFollowers.filter((entry) => entry === "tab-b:1").length >= 2,
      2000,
      "same-tab re-hello should receive a fresh follower port",
    );
  });

  it("re-promotes a same-tab leader that reconnects to a live broker", async () => {
    const dbName = uniqueName("broker-leader-rehello");
    const rehelloPromotions: number[] = [];

    const firstLeader = await BrowserBrokerClient.connect(createLockingOptions(dbName, "tab-a"));
    clients.push(firstLeader);
    await firstLeader.waitForRole("leader", 2000);
    firstLeader.reportSchemaReady("schema-a");
    expect(firstLeader.snapshot().leadershipId).toBe(1);

    const secondLeader = await BrowserBrokerClient.connect({
      ...createOptions(dbName, "tab-a"),
      onBecomeLeader: async (client, leadershipId) => {
        rehelloPromotions.push(leadershipId);
        client.reportLeaderReady({
          leadershipId,
          ...leaderLockNames(dbName),
        });
      },
    });
    clients.push(secondLeader);
    secondLeader.reportSchemaReady("schema-a");

    await waitFor(
      () => rehelloPromotions.some((leadershipId) => leadershipId > 1),
      2000,
      "same-tab leader reconnect should receive a fresh promotion",
    );
    await secondLeader.waitForRole("leader", 2000);
    expect(secondLeader.snapshot()).toMatchObject({
      role: "leader",
      tabId: "tab-a",
      leaderTabId: "tab-a",
      leadershipId: rehelloPromotions.at(-1),
    });
  });

  it("re-promotes a same-tab non-ready leader that reconnects to a live broker", async () => {
    const dbName = uniqueName("broker-non-ready-leader-rehello");
    const initialPromotions: number[] = [];
    const rehelloPromotions: number[] = [];

    const firstCandidate = await BrowserBrokerClient.connect({
      ...createOptions(dbName, "tab-a"),
      onBecomeLeader: async (_client, leadershipId) => {
        initialPromotions.push(leadershipId);
      },
    });
    clients.push(firstCandidate);

    await waitFor(
      () => initialPromotions.includes(1),
      2000,
      "first same-tab candidate should receive the initial promotion",
    );

    const secondCandidate = await BrowserBrokerClient.connect({
      ...createOptions(dbName, "tab-a"),
      onBecomeLeader: async (_client, leadershipId) => {
        rehelloPromotions.push(leadershipId);
      },
    });
    clients.push(secondCandidate);
    secondCandidate.reportSchemaReady("schema-a");

    await waitFor(
      () => rehelloPromotions.some((leadershipId) => leadershipId > 1),
      2000,
      "same-tab non-ready leader reconnect should receive a fresh promotion",
    );
    expect(secondCandidate.snapshot().leadershipId).toBe(rehelloPromotions.at(-1));
  });

  it("reattaches a follower when the leader reports the data port closed", async () => {
    const dbName = uniqueName("broker-follower-port-closed");
    const attachedFollowers: string[] = [];
    let leaderClient: BrowserBrokerClient | null = null;

    function createFollowerAttachOptions(
      tabId: string,
    ): Parameters<typeof BrowserBrokerClient.connect>[0] {
      return {
        ...createOptions(dbName, tabId),
        onBecomeLeader: async (client, leadershipId) => {
          const locks = await acquireLeaderLocks(dbName);
          client.reportLeaderReady({
            leadershipId,
            ...locks,
          });
        },
        onAttachFollowerPort: (followerTabId, leadershipId, port) => {
          attachedFollowers.push(`${followerTabId}:${leadershipId}`);
          port.close();
          leaderClient?.reportFollowerPortAttached(followerTabId, leadershipId);
        },
        onUseFollowerPort: (_leaderTabId, _leadershipId, port) => {
          port.close();
        },
      } as Parameters<typeof BrowserBrokerClient.connect>[0];
    }

    leaderClient = await BrowserBrokerClient.connect(createFollowerAttachOptions("tab-a"));
    clients.push(leaderClient);
    await leaderClient.waitForRole("leader", 2000);
    leaderClient.reportSchemaReady("schema-a");

    const follower = await BrowserBrokerClient.connect(createFollowerAttachOptions("tab-b"));
    clients.push(follower);
    follower.reportSchemaReady("schema-a");
    await waitFor(
      () => attachedFollowers.includes("tab-b:1"),
      2000,
      "initial follower attachment should complete",
    );

    leaderClient.reportFollowerPortClosed("tab-b", 1);

    await waitFor(
      () => attachedFollowers.filter((entry) => entry === "tab-b:1").length >= 2,
      2000,
      "closed follower data port should be re-attached",
    );
  });

  it("backs off when retrying a follower attachment that is never acknowledged", async () => {
    const dbName = uniqueName("broker-follower-attach-timeout");
    const attachedFollowers: string[] = [];

    function createFollowerAttachOptions(
      tabId: string,
    ): Parameters<typeof BrowserBrokerClient.connect>[0] {
      return {
        ...createOptions(dbName, tabId),
        onBecomeLeader: async (client, leadershipId) => {
          const locks = await acquireLeaderLocks(dbName);
          client.reportLeaderReady({
            leadershipId,
            ...locks,
          });
        },
        onAttachFollowerPort: (followerTabId, leadershipId, port) => {
          attachedFollowers.push(`${followerTabId}:${leadershipId}`);
          port.close();
        },
        onUseFollowerPort: (_leaderTabId, _leadershipId, port) => {
          port.close();
        },
      } as Parameters<typeof BrowserBrokerClient.connect>[0];
    }

    const leaderClient = await BrowserBrokerClient.connect(createFollowerAttachOptions("tab-a"));
    clients.push(leaderClient);
    await leaderClient.waitForRole("leader", 2000);
    leaderClient.reportSchemaReady("schema-a");

    const follower = await BrowserBrokerClient.connect(createFollowerAttachOptions("tab-b"));
    clients.push(follower);
    follower.reportSchemaReady("schema-a");

    await waitFor(
      () => attachedFollowers.filter((entry) => entry === "tab-b:1").length >= 2,
      4000,
      "unacknowledged follower attachment should be retried",
    );

    await new Promise((resolve) => setTimeout(resolve, 1500));
    expect(attachedFollowers.filter((entry) => entry === "tab-b:1")).toHaveLength(2);
  });

  it("rejects a failed storage reset and still elects a leader afterwards", async () => {
    const dbName = uniqueName("broker-reset-failure");
    const requestId = `reset-${dbName}`;

    const promotions: number[] = [];

    function createFailingResetOptions(
      tabId: string,
    ): Parameters<typeof BrowserBrokerClient.connect>[0] {
      return {
        ...createOptions(dbName, tabId),
        forceTakeoverTimeoutMs: 50,
        onStorageResetBegin: async () => {
          releaseLeaderLocks(dbName);
          if (tabId === "tab-b") {
            throw new Error("prepare exploded");
          }
        },
        onBecomeLeader: async (client, leadershipId) => {
          promotions.push(leadershipId);
          const locks = await acquireLeaderLocks(dbName);
          client.reportLeaderReady({
            leadershipId,
            ...locks,
          });
        },
        onAttachFollowerPort: (_followerTabId, _leadershipId, port) => {
          port.close();
        },
        onUseFollowerPort: (_leaderTabId, _leadershipId, port) => {
          port.close();
        },
      };
    }

    const first = await BrowserBrokerClient.connect(createFailingResetOptions("tab-a"));
    clients.push(first);
    const second = await BrowserBrokerClient.connect(createFailingResetOptions("tab-b"));
    clients.push(second);

    await first.waitForRole("leader", 2000);
    first.reportSchemaReady("schema-a");
    second.reportSchemaReady("schema-a");
    await second.waitForRole("follower", 2000);

    await expect(
      withTimeout(
        first.requestStorageReset(requestId),
        4000,
        "failed reset request should settle with the prepare error",
      ),
    ).rejects.toThrow("prepare exploded");

    // The pre-reset role snapshot is stale (the reset cleared the leader
    // without demoting), so recovery means a NEW leadership is established.
    await waitFor(
      () => promotions.some((leadershipId) => leadershipId > 1),
      4000,
      "a new leader should be promoted after the failed reset",
    );
    await waitFor(
      () =>
        [first, second].some(
          (client) => client.snapshot().role === "leader" && client.snapshot().leadershipId > 1,
        ),
      4000,
      "the post-failure leadership should reach ready",
    );
  });

  it("reconnects and reattaches a follower after it is evicted for missed pongs", async () => {
    const dbName = uniqueName("broker-evict-reconnect");
    let followerSilent = false;
    const attachedFollowers: string[] = [];
    const detachedFollowers: string[] = [];
    const reconnects: string[] = [];
    let leaderClient: BrowserBrokerClient | null = null;

    function createEvictionOptions(
      tabId: string,
    ): Parameters<typeof BrowserBrokerClient.connect>[0] {
      return {
        ...createOptions(dbName, tabId),
        forceTakeoverTimeoutMs: 50,
        brokerPingIntervalMs: 50,
        brokerPongTimeoutMs: 150,
        respondToBrokerPings: () => tabId !== "tab-b" || !followerSilent,
        onBecomeLeader: async (client, leadershipId) => {
          const locks = await acquireLeaderLocks(dbName);
          client.reportLeaderReady({
            leadershipId,
            ...locks,
          });
        },
        onAttachFollowerPort: (followerTabId, leadershipId, port) => {
          attachedFollowers.push(`${followerTabId}:${leadershipId}`);
          port.close();
          leaderClient?.reportFollowerPortAttached(followerTabId, leadershipId);
        },
        onDetachFollowerPort: (followerTabId, leadershipId) => {
          detachedFollowers.push(`${followerTabId}:${leadershipId}`);
        },
        onUseFollowerPort: (_leaderTabId, _leadershipId, port) => {
          port.close();
        },
        onReconnected: (client) => {
          reconnects.push(tabId);
          // Mirrors Db.handleBrokerReconnected: re-report the cached schema
          // so the fresh TabState becomes attach-eligible again.
          client.reportSchemaReady("schema-a");
        },
      } as Parameters<typeof BrowserBrokerClient.connect>[0];
    }

    leaderClient = await BrowserBrokerClient.connect(createEvictionOptions("tab-a"));
    clients.push(leaderClient);
    await leaderClient.waitForRole("leader", 2000);
    leaderClient.reportSchemaReady("schema-a");

    const follower = await BrowserBrokerClient.connect(createEvictionOptions("tab-b"));
    clients.push(follower);
    follower.reportSchemaReady("schema-a");
    await follower.waitForRole("follower", 2000);
    await waitFor(
      () => attachedFollowers.includes("tab-b:1"),
      2000,
      "initial follower attachment should complete",
    );

    followerSilent = true;
    await waitFor(
      () => detachedFollowers.includes("tab-b:1"),
      3000,
      "broker should evict the silent follower and detach it from the leader",
    );
    followerSilent = false;

    await waitFor(
      () => reconnects.includes("tab-b"),
      5000,
      "evicted follower should notice broker silence and reconnect",
    );
    await waitFor(
      () => attachedFollowers.filter((entry) => entry === "tab-b:1").length >= 2,
      5000,
      "reconnected follower should be re-attached to the leader",
    );
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
          releaseLeaderLocks(dbName);
        },
        onBecomeLeader: async (client, leadershipId, resetRequestId) => {
          if (resetRequestId) {
            resetPromotions.push(`${tabId}:${resetRequestId}`);
            if (tabId === "tab-b") {
              silentResetLeader = true;
              return;
            }
          }
          const locks = await acquireLeaderLocks(dbName);
          client.reportLeaderReady({
            leadershipId,
            ...locks,
          });
        },
        onAttachFollowerPort: (followerTabId, leadershipId, port) => {
          port.close();
          if (followerTabId === "tab-b") return;
          if (tabId === "tab-a") {
            first?.reportFollowerPortAttached(followerTabId, leadershipId);
          }
        },
        onUseFollowerPort: (_leaderTabId, _leadershipId, port) => {
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

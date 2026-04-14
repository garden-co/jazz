import { afterEach, describe, expect, test, vi } from "vitest";
import { logger } from "../logger.js";
import {
  loadCoValueOrFail,
  setupTestAccount,
  setupTestNode,
  waitFor,
} from "./testUtils.js";

const CLIENT_SKEW_MS = 20_000;

afterEach(() => {
  vi.restoreAllMocks();
});

describe("clock drift across peers with clock sync enabled", () => {
  test("worker can publish and self-remove after a skewed client with clock sync creates group, grant and chat", async () => {
    const errorSpy = vi.spyOn(logger, "error");

    const realNow = Math.floor(performance.timeOrigin + performance.now());

    vi.spyOn(Date, "now").mockImplementation(() => {
      const real = performance.timeOrigin + performance.now();
      return Math.floor(real) + CLIENT_SKEW_MS;
    });

    const worker = await setupTestAccount({
      isSyncServer: true,
    });

    const client = await setupTestAccount({
      connected: true,
      experimental_clockSyncFromServerPings: true,
    });

    const skewedNow = Date.now();
    client.node.clockOffset.addSample({
      serverTime: skewedNow - CLIENT_SKEW_MS,
      localReceiveTime: skewedNow,
    });

    const group = client.node.createGroup();

    const workerAccountOnClient = await loadCoValueOrFail(
      client.node,
      worker.accountID,
    );
    group.addMember(workerAccountOnClient, "admin");

    const oneToOneChat = group.createMap();
    oneToOneChat.set("kind", "OneToOneChat", "trusting");
    oneToOneChat.set("published", false, "trusting");

    await oneToOneChat.core.waitForSync();
    await group.core.waitForSync();

    const clientGroupTxs = group.core.getValidSortedTransactions();
    const maxClientGroupMadeAt = Math.max(
      ...clientGroupTxs.map((tx) => tx.madeAt),
    );
    expect(maxClientGroupMadeAt).toBeLessThan(realNow + CLIENT_SKEW_MS - 5_000);

    vi.restoreAllMocks();
    const errorSpyAfter = vi.spyOn(logger, "error");

    const chatOnWorker = await loadCoValueOrFail(worker.node, oneToOneChat.id);
    const groupOnWorker = await loadCoValueOrFail(worker.node, group.id);
    const workerAccountOnWorker = await loadCoValueOrFail(
      worker.node,
      worker.accountID,
    );

    await waitFor(() => {
      expect(chatOnWorker.get("kind")).toBe("OneToOneChat");
      expect(groupOnWorker.roleOf(worker.accountID)).toBe("admin");
    });

    chatOnWorker.set("published", true, "trusting");
    groupOnWorker.removeMember(workerAccountOnWorker);

    await chatOnWorker.core.waitForSync();
    await groupOnWorker.core.waitForSync();

    await waitFor(() => {
      expect(oneToOneChat.get("published")).toBe(true);
      expect(group.roleOf(worker.accountID)).not.toBe("admin");
      expect(chatOnWorker.get("published")).toBe(true);
      expect(groupOnWorker.roleOf(worker.accountID)).not.toBe("admin");
    });

    expect(oneToOneChat.get("published")).toBe(true);
    expect(chatOnWorker.get("published")).toBe(true);
    expect(group.roleOf(worker.accountID)).not.toBe("admin");
    expect(groupOnWorker.roleOf(worker.accountID)).not.toBe("admin");

    const forbiddenFragments = [
      "invalid transaction",
      "permission",
      "rejected",
      "not authorized",
      "not authorised",
    ];
    const offendingCalls = [...errorSpy.mock.calls, ...errorSpyAfter.mock.calls]
      .map((call) => call.map((arg) => String(arg)).join(" "))
      .filter((line) =>
        forbiddenFragments.some((f) => line.toLowerCase().includes(f)),
      );
    expect(offendingCalls).toEqual([]);
  });
});

describe("experimental_clockSyncFromServerPings flag wiring", () => {
  function seedOffset(
    clockOffset: {
      addSample: (s: { serverTime: number; localReceiveTime: number }) => void;
    },
    offsetMs: number,
  ) {
    const localReceiveTime = Date.now();
    clockOffset.addSample({
      serverTime: localReceiveTime + offsetMs,
      localReceiveTime,
    });
  }

  test("with the flag on, a seeded +10_000 ms offset pulls locally-stamped madeAt forward", () => {
    const { node } = setupTestNode({
      experimental_clockSyncFromServerPings: true,
    });

    seedOffset(node.clockOffset, 10_000);

    const group = node.createGroup();
    const map = group.createMap();

    const before = Date.now();
    map.set("k", "v", "trusting");
    const after = Date.now();

    const txs = map.core.getValidSortedTransactions();
    const lastTx = txs.at(-1);
    expect(lastTx).toBeDefined();
    expect(lastTx!.madeAt).toBeGreaterThanOrEqual(before + 9_900);
    expect(lastTx!.madeAt).toBeLessThanOrEqual(after + 10_100);
  });

  test("without the flag, a seeded +10_000 ms offset does NOT shift locally-stamped madeAt", () => {
    const { node } = setupTestNode({
      experimental_clockSyncFromServerPings: false,
    });

    seedOffset(node.clockOffset, 10_000);

    const group = node.createGroup();
    const map = group.createMap();

    const before = Date.now();
    map.set("k", "v", "trusting");
    const after = Date.now();

    const txs = map.core.getValidSortedTransactions();
    const lastTx = txs.at(-1);
    expect(lastTx).toBeDefined();
    expect(lastTx!.madeAt).toBeGreaterThanOrEqual(before);
    expect(lastTx!.madeAt).toBeLessThanOrEqual(after + 100);
  });
});

describe("clock sync pulls skewed client stamps toward server time", () => {
  test("with both nodes flag-on, a client whose wall clock is 20s ahead authors transactions that land near real time on the worker", async () => {
    const errorSpy = vi.spyOn(logger, "error");

    const worker = await setupTestAccount({
      isSyncServer: true,
      experimental_clockSyncFromServerPings: true,
    });

    const realNowBeforeClient = Date.now();

    const SKEW_MS = 20_000;
    vi.spyOn(Date, "now").mockImplementation(() => {
      const real = Math.floor(performance.timeOrigin + performance.now());
      return real + SKEW_MS;
    });

    const client = await setupTestAccount({
      connected: true,
      experimental_clockSyncFromServerPings: true,
    });

    const localReceiveTime = Date.now();
    client.node.clockOffset.addSample({
      serverTime: localReceiveTime - SKEW_MS,
      localReceiveTime,
    });

    expect(client.node.clockOffset.currentOffset()).toBeLessThanOrEqual(
      -SKEW_MS + 100,
    );
    expect(client.node.clockOffset.currentOffset()).toBeGreaterThanOrEqual(
      -SKEW_MS - 100,
    );

    const group = client.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap();
    map.set("from", "skewed-client", "trusting");

    await map.core.waitForSync();
    await group.core.waitForSync();

    vi.restoreAllMocks();
    const errorSpyAfter = vi.spyOn(logger, "error");

    const realNowAfter = Date.now();

    const mapOnWorker = await loadCoValueOrFail(worker.node, map.id);

    await waitFor(() => {
      expect(mapOnWorker.get("from")).toBe("skewed-client");
    });

    const clientTxsOnWorker = mapOnWorker.core.getValidSortedTransactions();
    const setTx = clientTxsOnWorker.find((tx) => {
      const changes = tx.changes as ReadonlyArray<{
        op?: string;
        key?: string;
        value?: unknown;
      }>;
      return (
        changes?.[0]?.key === "from" && changes[0]?.value === "skewed-client"
      );
    });

    expect(setTx).toBeDefined();

    const TOLERANCE_MS = 2_000;
    expect(setTx!.madeAt).toBeGreaterThanOrEqual(
      realNowBeforeClient - TOLERANCE_MS,
    );
    expect(setTx!.madeAt).toBeLessThanOrEqual(realNowAfter + TOLERANCE_MS);

    expect(setTx!.madeAt).toBeLessThan(realNowAfter + SKEW_MS - 5_000);

    const forbiddenFragments = [
      "invalid transaction",
      "permission",
      "rejected",
      "not authorized",
      "not authorised",
      "out of order",
    ];
    const offendingCalls = [...errorSpy.mock.calls, ...errorSpyAfter.mock.calls]
      .map((call) => call.map((arg) => String(arg)).join(" "))
      .filter((line) =>
        forbiddenFragments.some((f) => line.toLowerCase().includes(f)),
      );
    expect(offendingCalls).toEqual([]);
  });
});

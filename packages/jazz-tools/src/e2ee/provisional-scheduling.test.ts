import { afterEach, expect, it, vi } from "vitest";
import type { Db } from "../runtime/db.js";
import {
  createInitializationSeal,
  type InitializationTransactionStatus,
} from "../runtime/provisional-initialization.js";
import { InitializationJournal } from "./provisional-initialization.js";

// Control the admitted owner's fallible status port, not journal scheduling or
// persistence. These tests pin background I/O bounds without real wall-clock waits.
const status = vi.hoisted(() => vi.fn());
vi.mock("../runtime/db.js", () => ({ initializationStatus: status }));
afterEach(() => {
  vi.useRealTimers();
  status.mockReset();
});

function fixture(promote: () => Promise<void> = async () => {}) {
  vi.useFakeTimers();
  let offline = false;
  let shutdown!: () => void;
  const db = {
    onShutdown(callback: () => void) {
      shutdown = callback;
    },
    async e2eeIsExplicitlyOffline() {
      return offline;
    },
  } as unknown as Db;
  let value = JSON.stringify({
    format: "jazz-e2ee-local-devices-v2",
    devices: [],
    initializationJournalV1: [
      {
        scope: "scope",
        reservation: "reserved",
        local: false,
        outcome: "pending",
        proposal: JSON.stringify({
          kind: "founder",
          id: "account",
          deviceId: "device",
          epochId: "epoch",
          publicKeyId: "keys",
          rootId: "root",
          envelope: { e2eeBytesV1: [1] },
          verification: { e2eeBytesV1: [2] },
        }),
      },
    ],
  });
  const read = vi.fn(async () => value);
  const journal = new InitializationJournal(
    db,
    {
      read,
      async update(transform) {
        value = transform(value);
      },
    },
    "scope",
    () => {},
    promote,
    async () => {
      throw new Error("Unexpected space initialization in founder fixture");
    },
  );
  const pending: readonly InitializationTransactionStatus[] = [
    {
      kind: "complete",
      reservedTxId: createInitializationSeal("reserved").reservedTxId,
      fate: { kind: "pending" },
      durability: "none",
    },
  ];
  status.mockResolvedValue(pending);
  return {
    journal,
    read,
    pending,
    shutdown: () => shutdown(),
    setOffline(value: boolean) {
      offline = value;
    },
  };
}

it("does no background store/status work while explicitly offline and promptly resumes on reconnect", async () => {
  const f = fixture();
  f.setOffline(true);
  f.journal.wake(true);
  await vi.advanceTimersByTimeAsync(120_000);
  expect(f.read).not.toHaveBeenCalled();
  expect(status).not.toHaveBeenCalled();
  expect(vi.getTimerCount()).toBe(0);
  // Startup/user probes remain immediate, even while background polling is paused.
  await f.journal.reconcile();
  expect(status).toHaveBeenCalledTimes(1);
  f.setOffline(false);
  f.journal.wake(true);
  await vi.advanceTimersByTimeAsync(249);
  expect(status).toHaveBeenCalledTimes(1);
  await vi.advanceTimersByTimeAsync(1);
  expect(status).toHaveBeenCalledTimes(2);
  f.shutdown();
  expect(vi.getTimerCount()).toBe(0);
});

it("backs unchanged and failed probes off to a bounded recovery interval without read-induced resets", async () => {
  const f = fixture();
  f.journal.wake();
  let calls = 0;
  for (const delay of [250, 500, 1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000]) {
    await vi.advanceTimersByTimeAsync(delay - 1);
    expect(status).toHaveBeenCalledTimes(calls);
    await vi.advanceTimersByTimeAsync(1);
    expect(status).toHaveBeenCalledTimes(++calls);
    f.journal.wake();
  }
  // A fallible unavailable-owner probe stays bounded; automatic recovery still
  // discovers progress without an explicit reconnect notification.
  status.mockRejectedValueOnce(new Error("Owner temporarily unavailable"));
  await vi.advanceTimersByTimeAsync(30_000);
  expect(status).toHaveBeenCalledTimes(++calls);
  status.mockResolvedValue([{ ...f.pending[0], durability: "local" }]);
  await vi.advanceTimersByTimeAsync(30_000);
  expect(status).toHaveBeenCalledTimes(++calls);
  await vi.advanceTimersByTimeAsync(249);
  expect(status).toHaveBeenCalledTimes(calls);
  await vi.advanceTimersByTimeAsync(1);
  expect(status).toHaveBeenCalledTimes(++calls);
  f.shutdown();
  await vi.advanceTimersByTimeAsync(120_000);
  expect(status).toHaveBeenCalledTimes(calls);
});

it("coalesces concurrent status probes and retains reconnect wakes during an in-flight pass", async () => {
  const f = fixture();
  let finish!: (value: readonly InitializationTransactionStatus[]) => void;
  status.mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  f.journal.wake();
  await vi.advanceTimersByTimeAsync(250);
  const left = f.journal.reconcile();
  const right = f.journal.reconcile();
  expect(status).toHaveBeenCalledTimes(1);
  f.journal.wake(true);
  finish(f.pending);
  await Promise.all([left, right]);
  await vi.advanceTimersByTimeAsync(0);
  await vi.advanceTimersByTimeAsync(249);
  expect(status).toHaveBeenCalledTimes(1);
  await vi.advanceTimersByTimeAsync(1);
  expect(status).toHaveBeenCalledTimes(2);
  f.shutdown();
});

it("waits for durable promotion when the founder was accepted before the readiness check", async () => {
  let finish!: () => void;
  const promotion = vi.fn(
    () =>
      new Promise<void>((resolve) => {
        finish = resolve;
      }),
  );
  const f = fixture(promotion);
  try {
    status.mockResolvedValue([
      { ...f.pending[0], fate: { kind: "accepted" }, durability: "global" },
    ]);
    await f.journal.reconcile();
    expect((await f.journal.entries())[0]).toMatchObject({ outcome: "accepted" });
    let ready = false;
    const pending = f.journal.waitForFounderAcceptance().then(() => {
      ready = true;
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(promotion).toHaveBeenCalledTimes(1);
    expect(ready).toBe(false);
    expect((await f.journal.entries())[0]?.promoted).not.toBe(true);
    finish();
    await pending;
    expect((await f.journal.entries())[0]).toMatchObject({ outcome: "accepted", promoted: true });
    await f.journal.waitForFounderAcceptance();
    expect(promotion).toHaveBeenCalledTimes(1);
  } finally {
    f.shutdown();
  }
});

it("surfaces failed founder promotion and retries it without replacing the accepted proposal", async () => {
  const promotion = vi
    .fn()
    .mockRejectedValueOnce(new Error("Promotion storage unavailable"))
    .mockResolvedValue(undefined);
  const f = fixture(promotion);
  try {
    status.mockResolvedValue([
      { ...f.pending[0], fate: { kind: "accepted" }, durability: "global" },
    ]);
    await f.journal.reconcile();
    const proposal = (await f.journal.entries())[0]!.proposal;
    await expect(f.journal.waitForFounderAcceptance()).rejects.toThrow(
      "Promotion storage unavailable",
    );
    expect((await f.journal.entries())[0]).toMatchObject({ proposal, outcome: "accepted" });
    expect((await f.journal.entries())[0]?.promoted).not.toBe(true);
    await f.journal.waitForFounderAcceptance();
    expect((await f.journal.entries())[0]).toMatchObject({
      proposal,
      outcome: "accepted",
      promoted: true,
    });
    expect(promotion).toHaveBeenCalledTimes(2);
  } finally {
    f.shutdown();
  }
});

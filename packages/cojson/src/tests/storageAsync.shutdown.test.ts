import { afterEach, describe, expect, test, vi } from "vitest";
import type { RawCoID, SessionID } from "../exports.js";
import { logger } from "../exports.js";
import { StorageApiAsync } from "../storage/storageAsync.js";
import type {
  DBClientInterfaceAsync,
  StorageReconciliationAcquireResult,
} from "../storage/types.js";

function createClosingError() {
  return new DOMException(
    "Failed to execute 'transaction' on 'IDBDatabase': The database connection is closing.",
    "InvalidStateError",
  );
}

function createMockDbClient(
  overrides: Partial<DBClientInterfaceAsync> = {},
): DBClientInterfaceAsync {
  return {
    getCoValue: vi.fn().mockResolvedValue(undefined),
    upsertCoValue: vi.fn().mockResolvedValue(undefined),
    getAllCoValuesWaitingForDelete: vi.fn().mockResolvedValue([]),
    getCoValueSessions: vi.fn().mockResolvedValue([]),
    getNewTransactionInSession: vi.fn().mockResolvedValue([]),
    getSignatures: vi.fn().mockResolvedValue([]),
    transaction: vi.fn().mockResolvedValue(undefined),
    trackCoValuesSyncState: vi.fn().mockResolvedValue(undefined),
    getUnsyncedCoValueIDs: vi.fn().mockResolvedValue([]),
    stopTrackingSyncState: vi.fn().mockResolvedValue(undefined),
    eraseCoValueButKeepTombstone: vi.fn().mockResolvedValue(undefined),
    getCoValueKnownState: vi.fn().mockResolvedValue(undefined),
    getCoValueIDs: vi.fn().mockResolvedValue([]),
    getCoValueCount: vi.fn().mockResolvedValue(0),
    tryAcquireStorageReconciliationLock: vi.fn().mockResolvedValue({
      acquired: false,
      reason: "not_due",
    } satisfies StorageReconciliationAcquireResult),
    renewStorageReconciliationLock: vi.fn().mockResolvedValue(undefined),
    releaseStorageReconciliationLock: vi.fn().mockResolvedValue(undefined),
    close: vi.fn().mockResolvedValue(undefined),
    isClosed: vi.fn().mockReturnValue(false),
    ...overrides,
  };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("StorageApiAsync shutdown handling", () => {
  test("uses safe fallbacks for callback-based async storage methods", async () => {
    const closingError = createClosingError();
    let closed = true;
    const dbClient = createMockDbClient({
      trackCoValuesSyncState: vi.fn().mockRejectedValue(closingError),
      getCoValueIDs: vi.fn().mockRejectedValue(closingError),
      getCoValueCount: vi.fn().mockRejectedValue(closingError),
      tryAcquireStorageReconciliationLock: vi
        .fn()
        .mockRejectedValue(closingError),
      getUnsyncedCoValueIDs: vi.fn().mockRejectedValue(closingError),
      stopTrackingSyncState: vi.fn().mockRejectedValue(closingError),
      renewStorageReconciliationLock: vi.fn().mockRejectedValue(closingError),
      releaseStorageReconciliationLock: vi.fn().mockRejectedValue(closingError),
      isClosed: vi.fn(() => closed),
    });
    const storage = new StorageApiAsync(dbClient);
    const warnSpy = vi.spyOn(logger, "warn");

    await new Promise<void>((resolve) => {
      storage.trackCoValuesSyncState(
        [{ id: "co_zclosing" as RawCoID, peerId: "peer", synced: false }],
        resolve,
      );
    });

    const ids = await new Promise<{ id: RawCoID }[]>((resolve) => {
      storage.getCoValueIDs(10, 0, resolve);
    });
    expect(ids).toEqual([]);

    const count = await new Promise<number>((resolve) => {
      storage.getCoValueCount(resolve);
    });
    expect(count).toBe(0);

    const lock = await new Promise<StorageReconciliationAcquireResult>(
      (resolve) => {
        storage.tryAcquireStorageReconciliationLock(
          "session" as SessionID,
          "peer",
          resolve,
        );
      },
    );
    expect(lock).toEqual({
      acquired: false,
      reason: "not_due",
    });

    const unsyncedIds = await new Promise<RawCoID[]>((resolve) => {
      storage.getUnsyncedCoValueIDs(resolve);
    });
    expect(unsyncedIds).toEqual([]);

    storage.stopTrackingSyncState("co_zclosing" as RawCoID);
    storage.renewStorageReconciliationLock("session" as SessionID, "peer", 5);
    storage.releaseStorageReconciliationLock("session" as SessionID, "peer");

    await new Promise<void>(queueMicrotask);

    expect(warnSpy).not.toHaveBeenCalled();
  });
});

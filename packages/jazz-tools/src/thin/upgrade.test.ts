import { afterEach, describe, expect, it, vi } from "vitest";
import { Db } from "../runtime/db.js";
import { GracefulShutdownSyncError } from "../runtime/graceful-shutdown-error.js";
import { markThinDb } from "../runtime/local-first-upgrade.js";
import { upgradeToLocalFirst } from "./index.js";
import { ThinRuntime } from "./thin-runtime.js";
import { ThinRuntimeSource } from "./thin-runtime-source.js";

const opened: Db[] = [];

afterEach(async () => {
  await Promise.allSettled(opened.splice(0).map((db) => db.shutdown()));
  vi.restoreAllMocks();
});

function setup() {
  // Exercise real Db shutdown ownership without loading a native engine or
  // opening a socket. Prefetch and pending-write completion are fault boundaries.
  const oldSource = new ThinRuntimeSource();
  oldSource.waitForPendingWrites = vi.fn(async () => {});
  const thin = Db.create({ appId: "upgrade-test", driver: { type: "memory" } }, oldSource);
  markThinDb(thin);
  vi.spyOn(thin, "currentRuntime").mockReturnValue(new ThinRuntime({}));
  const nextSource = new ThinRuntimeSource();
  const next = Db.create({ appId: "upgrade-test", driver: { type: "memory" } }, nextSource);
  const prefetch = vi.spyOn(next, "prefetchRuntimeQueries").mockResolvedValue();
  const disposeNext = vi.spyOn(nextSource, "shutdown");
  const disposeOld = vi.spyOn(oldSource, "shutdown");
  opened.push(thin, next);
  return { thin, next, oldSource, prefetch, disposeNext, disposeOld };
}

describe("imperative local-first upgrade ownership", () => {
  it("disposes the replacement when synchronization fails and keeps the old Db usable", async () => {
    const { thin, next, oldSource, disposeNext, disposeOld } = setup();
    const syncError = new Error("sync failed");
    vi.mocked(oldSource.waitForPendingWrites!).mockRejectedValueOnce(syncError);
    const oldClosed = vi.fn();
    thin.onShutdown(oldClosed);

    await expect(upgradeToLocalFirst(thin, async () => next)).rejects.toMatchObject({
      name: "GracefulShutdownSyncError",
      cause: syncError,
    });
    expect(disposeNext).toHaveBeenCalledOnce();
    expect(disposeOld).not.toHaveBeenCalled();
    expect(oldClosed).not.toHaveBeenCalled();

    // A failed sync must leave the original context available for retry.
    const onRetry = vi.fn();
    thin.onShutdown(onRetry);
    expect(onRetry).not.toHaveBeenCalled();
    await thin.shutdown({ waitForSync: true });
    expect(oldSource.waitForPendingWrites).toHaveBeenCalledTimes(2);
    expect(disposeOld).toHaveBeenCalledOnce();
    expect(onRetry).toHaveBeenCalledOnce();
  });

  it("disposes the replacement after prefetch failure without shutting down the old Db", async () => {
    const { thin, next, prefetch, disposeNext, disposeOld } = setup();
    const error = new Error("prefetch failed");
    prefetch.mockRejectedValueOnce(error);

    await expect(upgradeToLocalFirst(thin, async () => next)).rejects.toBe(error);
    expect(disposeNext).toHaveBeenCalledOnce();
    expect(disposeOld).not.toHaveBeenCalled();
  });

  it.each(["prefetch", "shutdown"] as const)(
    "preserves the %s error when replacement cleanup also fails",
    async (phase) => {
      const { thin, next, prefetch, disposeNext } = setup();
      const error =
        phase === "shutdown"
          ? new GracefulShutdownSyncError(new Error("sync failed"))
          : new Error("prefetch failed");
      if (phase === "prefetch") prefetch.mockRejectedValueOnce(error);
      else vi.spyOn(thin, "shutdown").mockRejectedValueOnce(error);
      disposeNext.mockRejectedValueOnce(new Error("cleanup failed"));

      await expect(upgradeToLocalFirst(thin, async () => next)).rejects.toBe(error);
      expect(disposeNext).toHaveBeenCalledOnce();
    },
  );

  it("hands off the open replacement only after the old Db finishes shutting down", async () => {
    const { thin, next, oldSource, prefetch, disposeNext, disposeOld } = setup();
    let finishSync!: () => void;
    const pending = new Promise<void>((resolve) => {
      finishSync = resolve;
    });
    vi.mocked(oldSource.waitForPendingWrites!).mockReturnValueOnce(pending);
    let handedOff = false;
    const upgrade = upgradeToLocalFirst(thin, async () => next).then((db) => {
      handedOff = true;
      return db;
    });
    await vi.waitFor(() => expect(oldSource.waitForPendingWrites).toHaveBeenCalledOnce());
    expect(prefetch).toHaveBeenCalledOnce();
    expect(handedOff).toBe(false);
    expect(disposeNext).not.toHaveBeenCalled();
    finishSync();

    await expect(upgrade).resolves.toBe(next);
    expect(disposeOld).toHaveBeenCalledOnce();
    expect(disposeNext).not.toHaveBeenCalled();
  });
});

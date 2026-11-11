import { expect, it, vi } from "vitest";
import {
  DurableInitializationOwner,
  type DurableInitializationAdmission,
} from "./durable-initialization-owner.js";
import type { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import type { InitializationSeal, ReservedTxId } from "../runtime/provisional-initialization.js";
import type { OpenTransactionId, TxId } from "../runtime/client.js";
function deferred<T = void>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
function fixture() {
  const host = new DurableInitializationOwner();
  const peer = {};
  const events: string[] = [];
  let current = true;
  const seal = { reservedTxId: "reservation" as ReservedTxId } as InitializationSeal;
  const runtime = {
    openInitializationTransaction: vi.fn(
      async (_id: unknown, _ctx: unknown, _cap: unknown, _claims: unknown, guard: () => void) => {
        guard();
        events.push("native begin");
      },
    ),
    assertInitializationOwnerContext: vi.fn(),
    commitTransaction: vi.fn((_id: unknown, onPublished?: (tx: TxId) => void) => {
      onPublished?.("normal commit" as TxId);
      return "normal commit" as TxId;
    }),
    waitForTransaction: vi.fn(
      async (_tx: unknown, _tier: unknown, _options?: { signal?: AbortSignal }) => {},
    ),
    rollbackTransaction: vi.fn(async () => {
      events.push("rollback");
      return true;
    }),
    sealInitializationTransaction: vi.fn(async () => {
      events.push("seal");
      return seal;
    }),
    cancelInitializationTransaction: vi.fn(async () => {
      events.push("cancel seal");
    }),
    publishInitializationTransaction: vi.fn(
      async (_seal: unknown, onPublished?: (tx: TxId) => void) => {
        events.push("publish");
        onPublished?.("published" as TxId);
        return "published" as TxId;
      },
    ),
  };
  const admission: DurableInitializationAdmission = {
    runtime: runtime as unknown as NativeRuntimeAdapter,
    context: undefined,
    readCapability: "client-accepted-evidence",
    claims: { role: "reader" },
    assertCurrent() {
      if (!current) throw new Error("admitted scope changed");
    },
  };
  const open = () => host.begin(peer, admission, crypto.randomUUID() as OpenTransactionId);
  return {
    host,
    peer,
    runtime,
    admission,
    events,
    seal,
    open,
    retireScope: () => {
      current = false;
    },
  };
}

it("opens on the actual supplied owner before operations and never returns its native seal", async () => {
  const f = fixture();
  const handle = await f.open();
  await f.host.run(f.peer, handle, (runtime, _id, context) => {
    expect(runtime).toBe(f.admission.runtime);
    expect(context).toBeUndefined();
    f.events.push("read/write");
  });
  expect(await f.host.seal(f.peer, handle)).toBe(f.seal.reservedTxId);
  expect(await f.host.publish(f.peer, handle)).toBe("published");
  expect(f.events).toEqual(["native begin", "read/write", "seal", "publish"]);
  expect(() => f.host.run(f.peer, handle, () => {})).toThrow(/foreign|retired/);
});

it("cannot use another peer's handle even with the same runtime", async () => {
  const f = fixture();
  const handle = await f.open();
  expect(() =>
    f.host.run({}, handle, () => {
      throw new Error("must never execute");
    }),
  ).toThrow(/foreign/);
  await f.host.cancel(f.peer, handle);
  expect(f.runtime.rollbackTransaction).toHaveBeenCalledTimes(1);
});

it("rolls back an actually opened batch when the peer retires before the begin reply", async () => {
  const f = fixture();
  const opening = f.open().catch((error) => error);
  const retired = f.host.revokeOwner(f.peer);
  expect(await opening).toBeInstanceOf(Error);
  await retired;
  expect(f.runtime.rollbackTransaction).toHaveBeenCalledTimes(1);
});

it("aborts a pending read and rolls back without waiting for that read first", async () => {
  const f = fixture();
  const handle = await f.open();
  const started = deferred();
  const reading = f.host
    .run(
      f.peer,
      handle,
      (_runtime, _id, _ctx, signal) =>
        new Promise<void>((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), { once: true });
          started.resolve();
        }),
    )
    .catch((error) => error);
  await started.promise;
  await f.host.cancel(f.peer, handle);
  expect(await reading).toBeInstanceOf(Error);
  expect(f.runtime.rollbackTransaction).toHaveBeenCalledTimes(1);
});

it("cancels a seal that finishes after owner retirement without allowing publication", async () => {
  const f = fixture();
  const handle = await f.open();
  const started = deferred();
  const release = deferred<InitializationSeal>();
  f.runtime.sealInitializationTransaction.mockImplementation(async () => {
    started.resolve();
    return release.promise;
  });
  const sealing = f.host.seal(f.peer, handle).catch((error) => error);
  await started.promise;
  const cancelled = f.host.cancel(f.peer, handle);
  release.resolve(f.seal);
  expect(await sealing).toBeInstanceOf(Error);
  await cancelled;
  expect(f.runtime.cancelInitializationTransaction).toHaveBeenCalledExactlyOnceWith(f.seal);
  expect(f.runtime.publishInitializationTransaction).not.toHaveBeenCalled();
});

it("refuses a failed queued prefix even when JavaScript throws undefined", async () => {
  const f = fixture();
  const handle = await f.open();
  const failed = f.host
    .run(f.peer, handle, () => {
      throw undefined;
    })
    .catch((error) => error);
  const sealing = f.host.seal(f.peer, handle).catch((error) => error);
  await failed;
  await sealing;
  expect(f.runtime.sealInitializationTransaction).not.toHaveBeenCalled();
  await f.host.cancel(f.peer, handle);
  expect(f.runtime.rollbackTransaction).toHaveBeenCalledTimes(1);
});

it("checks admitted scope again after every awaited result", async () => {
  const f = fixture();
  const handle = await f.open();
  const ready = deferred();
  const started = deferred();
  const reading = f.host
    .run(f.peer, handle, async () => {
      started.resolve();
      await ready.promise;
      return "old scope rows";
    })
    .catch((error) => error);
  await started.promise;
  f.retireScope();
  ready.resolve();
  expect(await reading).toMatchObject({ message: "admitted scope changed" });
  await f.host.cancel(f.peer, handle);
});

it("does not claim to cancel publication that has already started", async () => {
  const f = fixture();
  const handle = await f.open();
  await f.host.seal(f.peer, handle);
  const release = deferred<TxId>();
  f.runtime.publishInitializationTransaction.mockImplementation(() => release.promise);
  const publishing = f.host.publish(f.peer, handle);
  await expect(f.host.cancel(f.peer, handle)).rejects.toThrow(/already started/);
  release.resolve("actual write" as TxId);
  expect(await publishing).toBe("actual write");
  expect(f.runtime.rollbackTransaction).not.toHaveBeenCalled();
});

it("closes sealed handles and refuses every later begin", async () => {
  const f = fixture();
  const handle = await f.open();
  await f.host.seal(f.peer, handle);
  await f.host.close();
  expect(f.runtime.cancelInitializationTransaction).toHaveBeenCalledTimes(1);
  await expect(f.open()).rejects.toThrow(/closed/);
});

it("commits a no-journal transaction on the same owner after its queued operations", async () => {
  const f = fixture();
  const handle = await f.open();
  const release = deferred();
  const writing = f.host.run(f.peer, handle, async () => {
    await release.promise;
    f.events.push("write");
  });
  const committed = f.host.commit(f.peer, handle);
  expect(f.runtime.commitTransaction).not.toHaveBeenCalled();
  release.resolve();
  await writing;
  expect(await committed).toBe("normal commit");
  expect(f.runtime.commitTransaction).toHaveBeenCalledExactlyOnceWith(
    expect.any(String),
    expect.any(Function),
  );
  expect(f.runtime.sealInitializationTransaction).not.toHaveBeenCalled();
});

it("rolls back a refused normal commit and retires its handle", async () => {
  const f = fixture();
  const handle = await f.open();
  f.runtime.commitTransaction.mockImplementation(() => {
    throw new Error("commit refused");
  });
  await expect(f.host.commit(f.peer, handle)).rejects.toThrow("commit refused");
  expect(f.runtime.rollbackTransaction).toHaveBeenCalledTimes(1);
  expect(() => f.host.run(f.peer, handle, () => {})).toThrow(/retired/);
});

it("registers real wait intent at publication and refuses another peer's wait", async () => {
  const f = fixture();
  const handle = await f.open();
  const waitId = crypto.randomUUID();
  expect(await f.host.commit(f.peer, handle, [{ waitId, tier: "global" }])).toBe("normal commit");
  expect(f.runtime.waitForTransaction).toHaveBeenCalledExactlyOnceWith("normal commit", "global", {
    signal: expect.any(AbortSignal),
  });
  await f.host.wait(f.peer, "normal commit" as TxId, "global", waitId);
  expect(() => f.host.wait({}, "normal commit" as TxId, "global", crypto.randomUUID())).toThrow(
    /live peer/,
  );
});

it("aborts an actual durability wait without cancelling or changing the write", async () => {
  const f = fixture();
  const handle = await f.open();
  f.runtime.waitForTransaction.mockImplementation(
    async (_tx, _tier, options) =>
      new Promise<void>((resolve, reject) => {
        if (options?.signal?.aborted) reject(options.signal.reason);
        else
          options?.signal?.addEventListener("abort", () => reject(options.signal!.reason), {
            once: true,
          });
      }),
  );
  await f.host.commit(f.peer, handle);
  const waitId = crypto.randomUUID();
  const waiting = f.host.wait(f.peer, "normal commit" as TxId, "global", waitId);
  const refused = expect(waiting).rejects.toThrow(/wait ended/);
  f.host.cancelWait(f.peer, waitId);
  await refused;
  expect(f.runtime.rollbackTransaction).not.toHaveBeenCalled();
});

it("retains a cancellation which arrives while publication is queued", async () => {
  const f = fixture();
  const handle = await f.open();
  const waitId = crypto.randomUUID();
  f.host.cancelWait(f.peer, waitId);
  f.runtime.waitForTransaction.mockImplementation(async (_tx, _tier, options) => {
    options?.signal?.throwIfAborted();
  });
  await f.host.commit(f.peer, handle, [{ waitId, tier: "global" }]);
  await expect(f.host.wait(f.peer, "normal commit" as TxId, "global", waitId)).rejects.toThrow(
    /wait ended/,
  );
});

it.each(["before publication", "after publication"])(
  "reuses cancelled prepublication wait capacity %s without a later wait RPC",
  async (phase) => {
    const f = fixture();
    let count = 0;
    f.runtime.commitTransaction.mockImplementation((_id, onPublished) => {
      const tx = `actual-${++count}` as TxId;
      onPublished?.(tx);
      return tx;
    });
    for (let i = 0; i < 4097; i++) {
      const handle = await f.open();
      const waitId = crypto.randomUUID();
      const publication = f.host.commit(f.peer, handle, [{ waitId, tier: "global" }]);
      if (phase === "before publication") f.host.cancelWait(f.peer, waitId);
      const tx = await publication;
      if (phase === "after publication") f.host.cancelWait(f.peer, waitId);
      // A timed-out page deliberately sends no later wait RPC. The next
      // independent commit must still be able to register its genuine wait.
      expect(tx).toBe(`actual-${i + 1}`);
    }
  },
);

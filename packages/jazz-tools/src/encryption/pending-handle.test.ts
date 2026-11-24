import { describe, expect, it, vi } from "vitest";
import { Encryption } from "./index.js";
import { pendingEncryption, type PendingEncryptionHost } from "./pending-handle.js";

function setup() {
  const real = {
    recovery: { create: vi.fn(() => "phrase") },
    deviceId: vi.fn(async () => "device"),
    start: vi.fn(),
    stop: vi.fn(),
  };
  let live: Encryption | undefined;
  let closed: Error | undefined;
  const refuseSync = vi.fn((op: string) => new Error(`not-loaded ${op}`));
  const host: PendingEncryptionHost = {
    live: () => {
      if (closed) throw closed;
      return live;
    },
    resolve: async () => real as unknown as Encryption,
    refuseSync,
    whenLocalFirst: () => () => {},
  };
  return {
    real,
    refuseSync,
    handle: pendingEncryption(host),
    upgrade: () => (live = real as unknown as Encryption),
    close: (error: Error) => (closed = error),
  };
}

describe("a handle taken while thin", () => {
  it("counts a slow handover against the ready timeout", async () => {
    vi.useFakeTimers();
    try {
      const real = new Encryption({
        encryptionState: () => JSON.stringify({ status: "active" }),
        encryptionCall: () =>
          new Promise((resolve) => setTimeout(() => resolve(JSON.stringify("active")), 40)),
        takeEncryptionReports: () => [],
      });
      const resolve = vi.fn(
        () => new Promise<Encryption>((done) => setTimeout(() => done(real), 80)),
      );
      const handle = pendingEncryption({
        live: () => undefined,
        resolve,
        refuseSync: (op) => new Error(op),
        whenLocalFirst: () => () => {},
      });
      const failed = vi.fn();
      const ready = handle.ready({ timeoutMs: 100 }).catch(failed);
      expect(resolve).toHaveBeenCalledWith(100);
      await vi.advanceTimersByTimeAsync(99);
      expect(failed).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      expect(failed).toHaveBeenCalledWith(expect.objectContaining({ code: "database" }));
      await vi.advanceTimersByTimeAsync(20);
      await ready;
    } finally {
      vi.useRealTimers();
    }
  });

  it("refuses recovery.create() and ignores stop() before the upgrade", () => {
    const { handle, real, refuseSync } = setup();
    expect(() => handle.recovery.create()).toThrow("not-loaded");
    expect(refuseSync).toHaveBeenCalledTimes(1);
    handle.stop();
    expect(real.stop).not.toHaveBeenCalled();
  });

  it("forwards synchronous and lifecycle calls to the real handle after the upgrade", async () => {
    const { handle, real, refuseSync, upgrade } = setup();
    upgrade();
    expect(handle.recovery.create()).toBe("phrase");
    handle.stop();
    expect(real.stop).toHaveBeenCalledTimes(1);
    await expect(handle.deviceId()).resolves.toBe("device");
    expect(refuseSync).not.toHaveBeenCalled();
  });

  it("gets the Db's own error after it closed without upgrading", async () => {
    const { handle, real, close } = setup();
    const closedError = new Error("db closed");
    close(closedError);
    expect(() => handle.recovery.create()).toThrow(closedError);
    await expect(handle.deviceId()).rejects.toBe(closedError);
    expect(() => handle.stop()).not.toThrow();
    expect(real.stop).not.toHaveBeenCalled();
  });
});

describe("the stand-in's members", () => {
  it("exposes only what the real handle has", () => {
    const { handle } = setup();
    const any = handle as unknown as Record<string, unknown>;
    for (const name of ["toJSON", "then", "inspect", "nope", "constructor"]) {
      expect(any[name]).toBeUndefined();
    }
    expect((handle as unknown as Record<symbol, unknown>)[Symbol.toPrimitive]).toBeUndefined();
    expect((handle.recovery as unknown as Record<string, unknown>).nope).toBeUndefined();
    expect(typeof handle.recovery.create).toBe("function");
  });

  it("does not start a load or leave a rejection when serialised", () => {
    const resolve = vi.fn(() => Promise.reject(new Error("load failed")));
    const handle = pendingEncryption({
      live: () => undefined,
      resolve,
      refuseSync: (op) => new Error(op),
      whenLocalFirst: () => () => {},
    });
    expect(JSON.stringify(handle)).toBeUndefined();
    expect(JSON.stringify({ handle })).toBe("{}");
    expect(resolve).not.toHaveBeenCalled();
  });

  it("lists exactly the members of the real class", async () => {
    const { PENDING_HANDLE_MEMBERS } = await import("./pending-handle.js");
    const real = new Encryption({} as never);
    const own = (o: object) =>
      Object.getOwnPropertyNames(o)
        .filter((n) => !n.startsWith("#") && n !== "constructor")
        .sort();
    const publicTop = [
      ...own(Object.getPrototypeOf(real)).filter((n) => typeof (real as never)[n] === "function"),
      "devices",
      "recovery",
      "keys",
      "members",
    ].sort();
    expect(Object.keys(PENDING_HANDLE_MEMBERS).sort()).toEqual(
      publicTop.filter(
        (n) => !["call", "verified", "verify", "verifyOnce", "deliver", "emit"].includes(n),
      ),
    );
    for (const group of ["devices", "recovery", "keys", "members"]) {
      const methods = own(Object.getPrototypeOf((real as never)[group])).filter(
        (n) => !["phraseSecret", "refuseForDeviceAccount"].includes(n),
      );
      expect([...(PENDING_HANDLE_MEMBERS[group] as string[])].sort()).toEqual(methods);
    }
  });
});

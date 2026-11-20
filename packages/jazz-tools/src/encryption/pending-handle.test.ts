import { describe, expect, it, vi } from "vitest";
import type { Encryption } from "./index.js";
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

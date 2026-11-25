import { expect, it, vi } from "vitest";
import { Encryption } from "./index.js";
import { memoryPrivateStore, servePrivateStore } from "./private-store.js";

it("wakes all readability waiters when the host signals, without polling while held", async () => {
  vi.useFakeTimers();
  let wake: ((event: string) => void) | null = null;
  let readable = false;
  const native = {
    encryptionCall: async () => "null",
    takeEncryptionReports: () => [],
    encryptionState: vi.fn(() => JSON.stringify({ readable })),
    takeStoreRequests: () => [],
    replyStore: () => true,
    setStoreWake(callback: typeof wake) {
      wake = callback;
    },
  };
  const serving = servePrivateStore(native, memoryPrivateStore());
  const encryption = new Encryption(native, { stateChanges: serving.onStateChange });
  try {
    const first = encryption.readable();
    const second = encryption.readable();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(native.encryptionState).toHaveBeenCalledTimes(2);
    readable = true;
    (wake as ((event: string) => void) | null)?.("encryption-state");
    await Promise.all([first, second]);
    expect(native.encryptionState).toHaveBeenCalledTimes(4);
  } finally {
    encryption.stop();
    serving.stop();
    vi.useRealTimers();
  }
});

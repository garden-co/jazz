import { afterEach, describe, expect, it, vi } from "vitest";
import type { AccountStore } from "../accounts/persistence.js";
import { browserBackendWatchEvents } from "./backend-watch.js";
import { openBrowserSessionCore as createJazzClient } from "./create-jazz-session.js";

function memoryStore(): AccountStore {
  let stored: string | null = null;
  return {
    async read() {
      return stored;
    },
    async update(transform) {
      stored = transform(stored);
    },
  };
}

/** A BroadcastChannel that counts the channels left open. */
const open = new Set<FakeChannel>();
class FakeChannel {
  onmessage: ((event: unknown) => void) | null = null;
  constructor(readonly name: string) {
    open.add(this);
  }
  posted = 0;
  postMessage() {
    if (!open.has(this)) throw new Error("InvalidStateError: posted on a closed channel");
    this.posted++;
  }
  close() {
    open.delete(this);
  }
}

afterEach(() => {
  vi.unstubAllGlobals();
  open.clear();
});

describe("a browser createJazzClient whose startup fails (#4785)", () => {
  it("leaves no cross-tab channel open, however often a provider retries", async () => {
    vi.stubGlobal("BroadcastChannel", FakeChannel);
    for (let attempt = 0; attempt < 3; attempt++) {
      await expect(
        createJazzClient({
          serverUrl: "http://127.0.0.1:9/jazz",
          store: memoryStore(),
          driver: { type: "memory" },
          initial: "device",
          localFirst: "now",
          client: async () => {
            throw new Error("synthetic startup failure");
          },
        }),
      ).rejects.toThrow("synthetic startup failure");
      // Before: one more open channel (and its listener) per failed attempt.
      expect(open.size).toBe(0);
    }
  });
});

describe("browserBackendWatchEvents' cross-tab channel", () => {
  it("is open only while something listens, and opens again for a later listener", () => {
    vi.stubGlobal("BroadcastChannel", FakeChannel);
    const { crossTab } = browserBackendWatchEvents("jazz-account:test");
    // Made, never listened to: nothing open.
    expect(open.size).toBe(0);
    // A post with no listener here still reaches other tabs, then closes.
    crossTab!.post();
    expect(open.size).toBe(0);
    const stop = crossTab!.listen(() => {});
    const stopToo = crossTab!.listen(() => {});
    expect(open.size).toBe(1);
    stop();
    expect(open.size).toBe(1);
    stopToo();
    expect(open.size).toBe(0);
    // A watch started again (a new session on the same events) listens and
    // posts on a fresh channel, not the closed one.
    const again = crossTab!.listen(() => {});
    crossTab!.post();
    expect([...open].map((channel) => channel.posted)).toEqual([1]);
    again();
    expect(open.size).toBe(0);
  });
});

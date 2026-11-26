import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { memoryPrivateStore, servePrivateStore } from "./private-store.js";

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

function target() {
  let wake: ((urgency: string) => void) | null = null;
  const requests: string[] = [];
  return {
    request(id: string) {
      requests.push(JSON.stringify({ id, op: "openScope", scope: "alice" }));
      wake?.("immediate");
    },
    signal(urgency: string) {
      wake?.(urgency);
    },
    takeStoreRequests: vi.fn(() => requests.splice(0)),
    replyStore: vi.fn(() => true),
    setStoreWake(callback: typeof wake) {
      wake = callback;
    },
  };
}

it("takes a standalone startup request on its wake, then stays idle", async () => {
  const runtime = target();
  runtime.request("1");
  const store = servePrivateStore(runtime, memoryPrivateStore());
  await vi.advanceTimersByTimeAsync(0);
  expect(runtime.replyStore).toHaveBeenCalledExactlyOnceWith("1", true, '"1"');
  expect(vi.getTimerCount()).toBe(0);
  runtime.takeStoreRequests.mockClear();
  await vi.advanceTimersByTimeAsync(10_000);
  expect(runtime.takeStoreRequests).not.toHaveBeenCalled();
  store.stop();
});

it("cancels a running request and its deadline", async () => {
  const runtime = target();
  const openScope = vi.fn(() => new Promise<bigint>(() => {}));
  const store = servePrivateStore(runtime, { ...memoryPrivateStore(), openScope });
  runtime.request("1");
  await vi.advanceTimersByTimeAsync(0);
  expect(openScope).toHaveBeenCalledOnce();
  store.stop({ cancelPending: true });
  await vi.advanceTimersByTimeAsync(30_000);
  expect(runtime.replyStore).not.toHaveBeenCalled();
  expect(vi.getTimerCount()).toBe(0);
});

it("refuses a hung request at its deadline with periodic ticks disabled", async () => {
  const runtime = target();
  const openScope = vi.fn(() => new Promise<bigint>(() => {}));
  const store = servePrivateStore(runtime, { ...memoryPrivateStore(), openScope });
  runtime.request("1");
  await vi.advanceTimersByTimeAsync(29_999);
  expect(runtime.replyStore).not.toHaveBeenCalled();
  expect(runtime.takeStoreRequests).toHaveBeenCalledOnce();
  await vi.advanceTimersByTimeAsync(1);
  expect(runtime.replyStore).toHaveBeenCalledExactlyOnceWith(
    "1",
    false,
    "the private store did not answer `openScope` in time",
  );
  expect(vi.getTimerCount()).toBe(0);
  store.stop();
});

it("drives a pending Rust deadline once and cancels a superseded timer", async () => {
  const runtime = target();
  const store = servePrivateStore(runtime, memoryPrivateStore());
  await vi.advanceTimersByTimeAsync(0);
  runtime.takeStoreRequests.mockClear();
  runtime.signal("after:100");
  runtime.signal("cancel");
  runtime.signal("after:2000");
  await vi.advanceTimersByTimeAsync(1999);
  expect(runtime.takeStoreRequests).not.toHaveBeenCalled();
  await vi.advanceTimersByTimeAsync(1);
  expect(runtime.takeStoreRequests).toHaveBeenCalledOnce();
  expect(vi.getTimerCount()).toBe(0);
  store.stop();
});

it("serves store wakes without advertising encryption state wakes on a store-only host", async () => {
  const runtime = { ...target(), supportsEncryptionStateWake: false };
  const serving = servePrivateStore(runtime, memoryPrivateStore());
  try {
    expect(serving.onStateChange).toBeUndefined();
    runtime.request("1");
    await vi.advanceTimersByTimeAsync(0);
    expect(runtime.replyStore).toHaveBeenCalledExactlyOnceWith("1", true, '"1"');
    expect(vi.getTimerCount()).toBe(0);
  } finally {
    serving.stop();
  }
});

it("clears the host's store wake when serving stops", () => {
  const runtime = target();
  const setWake = vi.spyOn(runtime, "setStoreWake");
  const serving = servePrivateStore(runtime, memoryPrivateStore());
  serving.stop();
  expect(setWake.mock.calls).toEqual([[expect.any(Function)], [null]]);
});

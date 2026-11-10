import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { WsLikeSocket } from "./create-jazz.js";
import {
  DEADLINE_CLOSE_MARGIN_MS,
  DEADLINE_CLOSE_WINDOW_MS,
  closingBeforeDeadline,
} from "./socket-deadline.js";

/** A `ws`-like socket that records closes and lets the test deliver frames. */
function fakeSocket() {
  const listeners: Record<string, Array<(...args: unknown[]) => void>> = {};
  const closes: Array<[number | undefined, string | undefined]> = [];
  const sent: Uint8Array[] = [];
  const ws = {
    send: (data: Uint8Array) => void sent.push(data),
    close(code?: number, reason?: string) {
      closes.push([code, reason]);
      for (const listener of listeners.close ?? []) listener();
    },
    on(event: string, listener: (...args: unknown[]) => void) {
      (listeners[event] ??= []).push(listener);
      return ws;
    },
  } as unknown as WsLikeSocket;
  const receive = (data: unknown) => {
    for (const listener of listeners.message ?? []) listener(data, true);
  };
  return { ws, closes, sent, receive };
}

describe("closingBeforeDeadline", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  const lifetime = 300_000;

  it("closes a quiet socket once the window before the deadline opens", () => {
    const { ws, closes } = fakeSocket();
    closingBeforeDeadline(ws, Date.now() + lifetime);
    vi.advanceTimersByTime(lifetime - DEADLINE_CLOSE_WINDOW_MS - 1_000);
    expect(closes).toEqual([]);
    vi.advanceTimersByTime(2_000);
    expect(closes).toEqual([[1012, "reconnect: the function reaches its deadline"]]);
  });

  it("waits for a quiet moment, and closes a busy socket at the margin anyway", () => {
    const { ws, closes, receive } = fakeSocket();
    const socket = closingBeforeDeadline(ws, Date.now() + lifetime);
    const messages: unknown[] = [];
    socket.on("message", (data) => void messages.push(data));
    // Frames every 200 ms, both ways, from just before the window on.
    const busy = setInterval(() => {
      receive("in");
      socket.send(new Uint8Array([1]));
    }, 200);
    vi.advanceTimersByTime(lifetime - DEADLINE_CLOSE_MARGIN_MS - 100);
    expect(closes).toEqual([]);
    vi.advanceTimersByTime(200);
    expect(closes.map(([code]) => code)).toEqual([1012]);
    clearInterval(busy);
    // Messages still reach the session's listener.
    expect(messages.length).toBeGreaterThan(0);
  });

  it("scales the window to a short maxDuration", () => {
    const { ws, closes } = fakeSocket();
    closingBeforeDeadline(ws, Date.now() + 10_000);
    vi.advanceTimersByTime(6_900);
    expect(closes).toEqual([]);
    // 30% of the lifetime before the deadline, plus the quiet check.
    vi.advanceTimersByTime(200);
    expect(closes.map(([code]) => code)).toEqual([1012]);
  });

  it("stops its timers when the socket closes first", () => {
    const { ws, closes } = fakeSocket();
    const socket = closingBeforeDeadline(ws, Date.now() + lifetime);
    socket.close(1000, "done");
    vi.advanceTimersByTime(lifetime);
    expect(closes).toEqual([[1000, "done"]]);
    expect(vi.getTimerCount()).toBe(0);
  });
});

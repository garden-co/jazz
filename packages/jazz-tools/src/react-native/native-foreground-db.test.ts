import { expect, it, vi } from "vitest";
import { NativeForegroundDb } from "./native-foreground-db.js";
import { UnsupportedSnapshotError } from "../runtime/snapshot-ref.js";

it.each([undefined, false])(
  "keeps existing reads usable when native snapshot support is %s",
  (supportsSnapshotCapture) => {
    const rows = Uint8Array.of(9);
    const execute = vi.fn(() => rows);
    const db = new NativeForegroundDb(
      { supportsSnapshotCapture, execute, tick: vi.fn(), close: () => true },
      {
        encodeNativeForegroundCommand: vi.fn(() => Uint8Array.of(2)),
        decodeNativeForegroundResponse: () => ({ type: "rows", rows }),
      } as never,
    );
    expect(db.all(Uint8Array.of(1), {})).toEqual(rows);
    expect(() => db.captureSnapshot()).toThrow(UnsupportedSnapshotError);
    expect(execute).toHaveBeenCalledTimes(1);
    expect(db.all(Uint8Array.of(1), {})).toEqual(rows);
  },
);

it("uses snapshot capture only when the native handle advertises support", () => {
  const reference = Uint8Array.of(7);
  const execute = vi.fn(() => reference);
  const encode = vi.fn(() => Uint8Array.of(34));
  const db = new NativeForegroundDb(
    { supportsSnapshotCapture: true, execute, tick: vi.fn(), close: () => true },
    {
      encodeNativeForegroundCommand: encode,
      decodeNativeForegroundResponse: () => ({ type: "rows", rows: reference }),
    } as never,
  );
  expect(db.captureSnapshot()).toEqual(reference);
  expect(encode).toHaveBeenCalledWith({ type: "captureSnapshot" });
  expect(execute).toHaveBeenCalledWith(Uint8Array.of(34));
});

/**
 * The native callback can be queued by CallInvoker before close/revocation.
 * These adapter receipts deliberately use opaque byte tags: they test the
 * JS-side lifecycle and pending-operation protocol, not a duplicate row codec.
 */
it("drains pending reads and subscriptions, then drops a delayed native wake after close", () => {
  let nativeWake: ((urgency: string) => void) | undefined;
  const ticks = vi.fn();
  const execute = vi.fn((command: Uint8Array) => command);
  let pollCount = 0;
  const db = new NativeForegroundDb(
    {
      execute,
      tick: ticks,
      close: vi.fn(() => true),
      setTickScheduler: (callback) => {
        nativeWake = callback;
      },
    },
    {
      encodeNativeForegroundCommand(command: unknown) {
        const type =
          typeof command === "object" && command !== null
            ? (command as { type?: unknown }).type
            : command;
        if (type === "all") return Uint8Array.of(1);
        if (type === "subscribe") return Uint8Array.of(2);
        if (type === "drainSubscription") return Uint8Array.of(3);
        if (type === "poll") return Uint8Array.of(4);
        if (type === "close") return Uint8Array.of(5);
        throw new Error("unexpected foreground fixture command");
      },
      decodeNativeForegroundResponse(bytes: Uint8Array) {
        switch (bytes[0]) {
          case 1:
            return { type: "pending", operation: 11 };
          case 2:
            return { type: "subscribed", subscription: 12 };
          case 3:
            return { type: "pending", operation: 13 };
          case 4:
            pollCount += 1;
            return pollCount === 1
              ? { type: "rows", rows: Uint8Array.of(9) }
              : {
                  type: "subscriptionEvents",
                  events: [
                    {
                      type: "delta",
                      reset: false,
                      settled: true,
                      tier: "local",
                      delta: Uint8Array.of(7),
                    },
                  ],
                };
          case 5:
            return { type: "closed", closed: true };
          default:
            throw new Error("unexpected foreground fixture response");
        }
      },
      installNativeForegroundRuntime: () => {
        throw new Error("not used");
      },
    } as never,
  );

  const wakes: string[] = [];
  db.setTickScheduler((urgency) => wakes.push(String(urgency)));
  const query = Uint8Array.of(1);
  const pendingRows = db.all(query, { tier: "local-first" });
  expect(typeof pendingRows).toBe("object");
  expect("poll" in pendingRows && pendingRows.poll()).toEqual(Uint8Array.of(9));

  const subscription = db.subscribe(query, { tier: "local-first" });
  const ticksBeforeSubscriptionRead = ticks.mock.calls.length;
  const pendingSubscription = subscription.readAll();
  expect(Array.isArray(pendingSubscription)).toBe(false);
  expect("retryAfterMs" in pendingSubscription && pendingSubscription.retryAfterMs()).toBe(0);
  expect(ticks).toHaveBeenCalledTimes(ticksBeforeSubscriptionRead);
  expect(subscription.readAll()).toEqual([
    {
      type: "delta",
      reset: false,
      settled: true,
      tier: "local",
      delta: Uint8Array.of(7),
    },
  ]);
  expect(ticks).toHaveBeenCalledTimes(ticksBeforeSubscriptionRead);

  nativeWake?.("deferred");
  expect(wakes).toEqual(["deferred"]);
  expect(db.close()).toBe(true);
  nativeWake?.("immediate");
  expect(wakes).toEqual(["deferred"]);
  expect(() => db.all(query, { tier: "local-first" })).toThrow("runtime is closed");
});

it("keeps an empty native drain ready for the next native wake", () => {
  const ticks = vi.fn();
  const execute = vi.fn((command: Uint8Array) => command);
  const db = new NativeForegroundDb(
    {
      execute,
      tick: ticks,
      close: vi.fn(() => true),
      setTickScheduler: vi.fn(),
    },
    {
      encodeNativeForegroundCommand(command: unknown) {
        const type =
          typeof command === "object" && command !== null
            ? (command as { type?: unknown }).type
            : command;
        if (type === "subscribe") return Uint8Array.of(2);
        if (type === "drainSubscription") return Uint8Array.of(3);
        if (type === "close") return Uint8Array.of(5);
        throw new Error("unexpected foreground fixture command");
      },
      decodeNativeForegroundResponse(bytes: Uint8Array) {
        if (bytes[0] === 2) return { type: "subscribed", subscription: 12 };
        if (bytes[0] === 3) return { type: "subscriptionEvents", events: [] };
        if (bytes[0] === 5) return { type: "closed", closed: true };
        throw new Error("unexpected foreground fixture response");
      },
      installNativeForegroundRuntime: () => {
        throw new Error("not used");
      },
    } as never,
  );

  // #3273: opening a subscription no longer ticks (pumps) the native relay;
  // the native owner fences and drives it off the JS thread.
  const subscription = db.subscribe(Uint8Array.of(1), { tier: "local-first" });
  expect(ticks).not.toHaveBeenCalled();
  expect(subscription.readAll()).toEqual([]);
  expect(ticks).not.toHaveBeenCalled();
});

// Unknown transaction handles must fail before any ordinary read is issued.
it("rejects unknown transaction reads before invoking native commands", () => {
  const execute = vi.fn();
  const tick = vi.fn();
  const db = new NativeForegroundDb({ execute, tick, close: () => true }, {} as never);
  expect(() => db.all(Uint8Array.of(2), { tier: "local-first" }, "missing")).toThrow(
    "cannot read unknown transaction missing",
  );
  expect(execute).not.toHaveBeenCalled();
  expect(tick).not.toHaveBeenCalled();
});

// #4296: a refused credential is auth state, not a failed tick. Each new
// refusal is reported once; a renewed credential that is refused again is a
// new refusal.
it("reports each new upstream credential refusal once while ticks keep running", async () => {
  const statuses: (string | null)[] = [
    null,
    "token expired",
    "token expired",
    null,
    "token expired",
  ];
  let authRejected: string | null = null;
  const tick = vi.fn(() => {
    authRejected = statuses.shift() ?? null;
  });
  const db = new NativeForegroundDb(
    { execute: (command: Uint8Array) => command, tick, close: () => true },
    {
      encodeNativeForegroundCommand: () => Uint8Array.of(1),
      decodeNativeForegroundResponse: () => ({
        type: "nativeConnectionStatus",
        configured: true,
        explicitlyOffline: false,
        connected: false,
        authRejected,
      }),
    } as never,
  );
  const reported: string[] = [];
  db.onAuthRejected((reason) => reported.push(reason));

  for (let turn = 0; turn < 5; turn += 1) db.tick();
  await Promise.resolve();

  expect(tick).toHaveBeenCalledTimes(5);
  expect(reported).toEqual(["token expired", "token expired"]);
});

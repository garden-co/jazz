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

// #4296: a refused credential is auth state, not a failed tick. Each tick
// drains its events in one native call, and each refusal is reported once:
// a renewed credential refused again, even for the same reason, is reported
// again.
it("reports each new upstream credential refusal once while ticks keep running", async () => {
  const refusal = (sequence: number) => ({ sequence, reason: "token expired" });
  const drains = [null, refusal(1), refusal(1), null, refusal(2), refusal(2)];
  let authRejection: { sequence: number; reason: string } | null = null;
  const tick = vi.fn(() => {
    authRejection = drains.shift() ?? null;
  });
  const execute = vi.fn((command: Uint8Array) => command);
  const db = new NativeForegroundDb({ execute, tick, close: () => true }, {
    encodeNativeForegroundCommand: (command: { type: string }) => {
      expect(command).toEqual({ type: "drainTickEvents" });
      return Uint8Array.of(35);
    },
    decodeNativeForegroundResponse: () => ({
      type: "tickEvents",
      eventsJson: "[]",
      authRejection,
    }),
  } as never);
  const reported: string[] = [];
  db.onAuthRejected((reason) => reported.push(reason));

  for (let turn = 0; turn < 6; turn += 1) db.tick();
  await Promise.resolve();

  expect(tick).toHaveBeenCalledTimes(6);
  expect(execute).toHaveBeenCalledTimes(6);
  expect(reported).toEqual(["token expired", "token expired"]);
});

// #4337: another upstream connection failure is connection state too. Each
// change is reported once: a new failure, the socket giving up retrying it,
// and its recovery.
it("reports each upstream connection failure change once while ticks keep running", async () => {
  const failure = (sequence: number, retrying: boolean) => ({ sequence, reason: "tls", retrying });
  const drains = [
    null,
    failure(1, true),
    failure(1, true),
    failure(1, false),
    failure(1, false),
    null,
    null,
    failure(2, true),
  ];
  let upstreamFailure: { sequence: number; reason: string; retrying: boolean } | null = null;
  const tick = vi.fn(() => {
    upstreamFailure = drains.shift() ?? null;
  });
  const db = new NativeForegroundDb(
    { execute: (command: Uint8Array) => command, tick, close: () => true },
    {
      encodeNativeForegroundCommand: () => Uint8Array.of(35),
      decodeNativeForegroundResponse: () => ({
        type: "tickEvents",
        eventsJson: "[]",
        authRejection: null,
        upstreamFailure,
      }),
    } as never,
  );
  const reported: unknown[] = [];
  db.onUpstreamFailure((change) => reported.push(change));

  for (let turn = 0; turn < 8; turn += 1) db.tick();
  await Promise.resolve();

  expect(tick).toHaveBeenCalledTimes(8);
  expect(reported).toEqual([
    { reason: "tls", retrying: true },
    { reason: "tls", retrying: false },
    null,
    { reason: "tls", retrying: true },
  ]);
});

// The native host sets the relay status as `error.code`. Only an unknown
// command (`invalid_command`) means an older build without it; a lifecycle
// or encode failure, under the same message, must not turn the count off.
it("turns the pending-upload count off only for a command the host does not know", () => {
  const codedFailure = (code: string) =>
    Object.assign(new Error("Jazz native foreground runtime failed during execute"), { code });
  const counting = (failure: Error) => {
    let fail = true;
    const db = new NativeForegroundDb(
      {
        execute: () => {
          if (fail) {
            fail = false;
            throw failure;
          }
          return Uint8Array.of(1);
        },
        tick: vi.fn(),
        close: () => true,
      },
      {
        encodeNativeForegroundCommand: () => Uint8Array.of(1),
        decodeNativeForegroundResponse: () => ({ type: "pendingUploadCount", count: 3 }),
      } as never,
    );
    return [db.pendingUploadCount(), db.pendingUploadCount()];
  };

  expect(counting(codedFailure("lifecycle_failure"))).toEqual([0, 3]);
  expect(counting(codedFailure("encode_failure"))).toEqual([0, 3]);
  expect(counting(codedFailure("invalid_command"))).toEqual([0, 0]);
  // A native build older than the code: the message is all there is.
  expect(counting(new Error("Jazz native foreground runtime failed during execute"))).toEqual([
    0, 0,
  ]);
});

it("names pending upload rows, keeps a core error to its call, and goes unknown only on an older native build", () => {
  const id = new Uint8Array(16).fill(0xab);
  const answers: unknown[] = [
    { type: "pendingUploadRows", rows: [id] },
    { type: "operationError", code: "invalid_argument", reason: "unknown table nope" },
    { type: "pendingUploadRows", rows: [] },
  ];
  const execute = vi.fn((command: Uint8Array) => {
    if (answers.length === 0) throw new Error("JazzNativeForeground runtime failed during execute");
    return command;
  });
  const db = new NativeForegroundDb({ execute, tick: vi.fn(), close: () => true }, {
    encodeNativeForegroundCommand: () => Uint8Array.of(41),
    decodeNativeForegroundResponse: () => answers.shift(),
  } as never);

  expect(db.pendingUploadRows("todos")).toEqual(["abababab-abab-abab-abab-abababababab"]);
  // A core error is this call's: it throws, and the next call still asks.
  expect(() => db.pendingUploadRows("nope")).toThrow(/unknown table nope/);
  expect(db.pendingUploadRows("todos")).toEqual([]);
  // A native build without the command can't tell: unknown, never "none".
  expect(db.pendingUploadRows("todos")).toBeNull();
  expect(db.pendingUploadRows("todos")).toBeNull();
  expect(execute).toHaveBeenCalledTimes(4);
});

// A subscribe answers at once and opens on the owner's next turn, so its
// failure arrives as an event; it keeps the code a failed command carries.
it("drains a subscription's open failure as a rejection that keeps its core code", () => {
  const db = new NativeForegroundDb(
    { execute: (command: Uint8Array) => command, tick: vi.fn(), close: () => true },
    {
      encodeNativeForegroundCommand: () => Uint8Array.of(3),
      decodeNativeForegroundResponse: () => ({
        type: "subscriptionEvents",
        events: [
          { type: "openFailed", reason: "no table", code: "schema_error", reasonName: "unknown" },
          { type: "openFailed", reason: "bad bytes" },
        ],
      }),
    } as never,
  );
  expect(db.drain(1)).toEqual([
    {
      type: "rejected",
      reason: {
        type: "ServerFailure",
        code: "no table",
        errorCode: "schema_error",
        reasonName: "unknown",
      },
    },
    { type: "rejected", reason: { type: "ServerFailure", code: "bad bytes" } },
  ]);
});

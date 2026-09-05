import { col } from "../dsl.js";
import { defineApp } from "../typed-app.js";
import { expect, it, vi } from "vitest";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import type { OpenTransactionId } from "../runtime/client.js";
import { NativeForegroundDb } from "./native-foreground-db.js";

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
        if (type === "prepareQuery") return Uint8Array.of(0);
        if (type === "all") return Uint8Array.of(1);
        if (type === "subscribe") return Uint8Array.of(2);
        if (type === "drainSubscription") return Uint8Array.of(3);
        if (type === "poll") return Uint8Array.of(4);
        if (type === "close") return Uint8Array.of(5);
        throw new Error("unexpected foreground fixture command");
      },
      decodeNativeForegroundResponse(bytes: Uint8Array) {
        switch (bytes[0]) {
          case 0:
            return { type: "preparedQuery", query: 10 };
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
  const query = db.prepareQuery(Uint8Array.of(1));
  const pendingRows = db.all(query, { tier: "local" });
  expect(typeof pendingRows).toBe("object");
  expect("poll" in pendingRows && pendingRows.poll()).toEqual(Uint8Array.of(9));

  const subscription = db.subscribe(query, { tier: "local" });
  const pendingSubscription = subscription.readAll();
  expect(Array.isArray(pendingSubscription)).toBe(false);
  expect("retryAfterMs" in pendingSubscription && pendingSubscription.retryAfterMs()).toBe(0);
  expect(subscription.readAll()).toEqual([
    {
      type: "delta",
      reset: false,
      settled: true,
      tier: "local",
      delta: Uint8Array.of(7),
    },
  ]);
  expect(ticks).toHaveBeenCalledTimes(5);

  nativeWake?.("deferred");
  expect(wakes).toEqual(["deferred"]);
  expect(db.close()).toBe(true);
  nativeWake?.("immediate");
  expect(wakes).toEqual(["deferred"]);
  expect(() => db.all(query, { tier: "local" })).toThrow("runtime is closed");
});

it("rejects transaction reads through the shared adapter without issuing an ordinary native read", async () => {
  const commands: string[] = [];
  let current = "";
  const tick = vi.fn();
  const db = new NativeForegroundDb(
    { execute: (bytes) => bytes, tick, close: () => true, setTickScheduler() {} },
    {
      installNativeForegroundRuntime() {
        throw new Error("unused");
      },
      encodeNativeForegroundCommand(command: { type: string }) {
        current = command.type;
        commands.push(current);
        return new Uint8Array();
      },
      decodeNativeForegroundResponse() {
        if (current === "beginTransaction") return { type: "transactionOpened", transaction: 1 };
        if (current === "prepareQuery") return { type: "preparedQuery", query: 2 };
        if (current === "all") return { type: "rows", rows: Uint8Array.of(0) };
        throw new Error(`unexpected ${current}`);
      },
    } as never,
  );
  const app = defineApp({ todos: { title: col.string() } });
  const runtime = NativeRuntimeAdapter.fromDb(
    db,
    app.wasmSchema,
    new Uint8Array(16),
    new TextEncoder().encode('["urn:jazz:test","runtime"]'),
    1,
    true,
  );
  const id = "otx_00000000000000000000000000" as OpenTransactionId;
  runtime.beginTransaction("exclusive", id);
  await expect(runtime.query(JSON.stringify(app.todos), undefined, "local")).resolves.toBeDefined();
  const readsBefore = commands.filter((type) => type === "all").length;
  const ticksBefore = tick.mock.calls.length;
  await expect(
    runtime.query(
      JSON.stringify(app.todos),
      undefined,
      "local",
      JSON.stringify({ transaction_id: id }),
    ),
  ).rejects.toThrow("transaction reads is unavailable");
  expect(commands.filter((type) => type === "all")).toHaveLength(readsBefore);
  expect(tick).toHaveBeenCalledTimes(ticksBefore);
  expect(() => db.all({ nativeForegroundQuery: 2 }, { tier: "local" }, id)).toThrow(
    "transaction reads is unavailable",
  );
  await expect(runtime.query(JSON.stringify(app.todos), undefined, "local")).resolves.toBeDefined();
  expect(commands.filter((type) => type === "all")).toHaveLength(readsBefore + 1);
});

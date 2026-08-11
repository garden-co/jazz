import { describe, expect, it, vi } from "vitest";
import { RnDbShim, type JazzRnModule } from "./native-db.js";

function buffer(...bytes: number[]): ArrayBuffer {
  return new Uint8Array(bytes).buffer;
}

function shim(generated: Record<string, unknown>): RnDbShim {
  return new RnDbShim(generated as never);
}

describe("RnDbShim", () => {
  it("copies Uint8Array views into exact ArrayBuffers at open", () => {
    const openMemory = vi.fn((_node: ArrayBuffer, _author: ArrayBuffer) => ({}));
    const module = {
      RnDb: { openMemory, openPersistent: vi.fn() },
      mintAnonymousToken: vi.fn(),
      mintLocalFirstToken: vi.fn(),
    } as unknown as JazzRnModule;
    const bytes = new Uint8Array([99, 1, 2, 88]);

    RnDbShim.forModule(module).openMemory(bytes.subarray(1, 3), new Uint8Array([3]));

    expect(new Uint8Array(openMemory.mock.calls[0]![0])).toEqual(new Uint8Array([1, 2]));
    expect(new Uint8Array(openMemory.mock.calls[0]![1])).toEqual(new Uint8Array([3]));
  });

  it("serializes read options and decodes byte results", () => {
    const all = vi.fn(() => buffer(7, 8));
    const db = shim({ all });
    const query = {};

    expect(db.all(query, { tier: "local", include_deleted: true })).toEqual(new Uint8Array([7, 8]));
    expect(all).toHaveBeenCalledWith(
      query,
      JSON.stringify({ tier: "local", include_deleted: true }),
    );
  });

  it("adapts the tick callback interface", () => {
    let generatedCallback: { onTickNeeded(urgency: string): void } | undefined;
    const db = shim({
      setTickScheduler(callback: { onTickNeeded(urgency: string): void }) {
        generatedCallback = callback;
      },
    });
    const callback = vi.fn();

    db.setTickScheduler(callback);
    generatedCallback?.onTickNeeded("immediate");

    expect(callback).toHaveBeenCalledWith("immediate");
  });

  it("registers the write waiter synchronously before awaiting it", async () => {
    const order: string[] = [];
    const pending = Promise.resolve().then(() => {
      order.push("settled");
    });
    const write = {
      payload: () => buffer(1),
      wait: vi.fn(),
      writeState: () => JSON.stringify({ kind: "Pending" }),
      close: () => true,
      registerWriteStateWaiter: () => {
        order.push("registered");
        return {
          wait: () => {
            order.push("waited");
            return pending;
          },
        };
      },
    };
    const db = shim({
      insertWithIdEncoded: () => write,
    });

    const adapted = db.insertWithIdEncoded("todos", new Uint8Array(16), new Uint8Array());
    const change = adapted.nextWriteStateChange();

    expect(order).toEqual(["registered", "waited"]);
    await change;
    expect(order).toEqual(["registered", "waited", "settled"]);
    expect(adapted.writeState()).toEqual({ kind: "Pending" });
    expect(adapted.payload).toEqual(new Uint8Array([1]));
  });

  it("parses subscription events and restores native error markers", () => {
    const generatedError = Object.assign(new Error("JazzRnError.Runtime"), {
      tag: "Runtime",
      inner: { message: "NotObserved: write has not reached edge" },
    });
    const db = shim({
      subscribe: () => ({
        close: () => true,
        drain: () => [
          {
            eventType: "closed",
            reset: undefined,
            delta: undefined,
            relationDelta: undefined,
            settled: undefined,
            tier: undefined,
            reasonJson: undefined,
          },
        ],
        readAll: () => [
          {
            eventType: "delta",
            reset: true,
            delta: buffer(1, 2),
            relationDelta: buffer(3, 4),
            settled: true,
            tier: "Local",
            reasonJson: undefined,
          },
          {
            eventType: "rejected",
            reset: undefined,
            delta: undefined,
            relationDelta: undefined,
            settled: undefined,
            tier: undefined,
            reasonJson: JSON.stringify({
              type: "ServerFailure",
              code: "Internal",
            }),
          },
        ],
      }),
      insertWithIdEncoded: () => ({
        close: () => true,
        payload: () => buffer(),
        registerWriteStateWaiter: () => ({ wait: async () => {} }),
        wait: () => {
          throw generatedError;
        },
        writeState: () => JSON.stringify({}),
      }),
    });

    const subscription = db.subscribe({}, null);
    expect(subscription.readAll()).toEqual([
      {
        type: "delta",
        reset: true,
        delta: new Uint8Array([1, 2]),
        relation_delta: new Uint8Array([3, 4]),
        settled: true,
        tier: "Local",
      },
      {
        type: "rejected",
        reason: { type: "ServerFailure", code: "Internal" },
      },
    ]);
    expect(subscription.drain?.()).toEqual([{ type: "closed" }]);
    const write = db.insertWithIdEncoded("todos", new Uint8Array(16), new Uint8Array());
    try {
      write.wait("edge");
      expect.unreachable("wait should throw");
    } catch (error) {
      expect(error).toMatchObject({
        message: "NotObserved: write has not reached edge",
        cause: generatedError,
        tag: "Runtime",
      });
    }
  });
});

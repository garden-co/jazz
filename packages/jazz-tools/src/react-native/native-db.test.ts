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

  it("copies large-value handles and hydrated bytes across the generated boundary", () => {
    const hydrateLargeValue = vi.fn((_handle: ArrayBuffer) => buffer(4, 5, 6));
    const db = shim({ hydrateLargeValue });
    const backing = new Uint8Array([0, 1, 2, 3, 0]);

    expect(db.hydrateLargeValue(backing.subarray(1, 4))).toEqual(new Uint8Array([4, 5, 6]));
    expect(new Uint8Array(hydrateLargeValue.mock.calls[0]![0])).toEqual(new Uint8Array([1, 2, 3]));
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

  it("parses mutation errors delivered by the detached native callback", () => {
    let generatedCallback: { onMutationError(eventJson: string): void } | undefined;
    const db = shim({
      onMutationError(callback: { onMutationError(eventJson: string): void }) {
        generatedCallback = callback;
      },
    });
    const callback = vi.fn();
    const event = {
      code: "permission_denied",
      reason: "Write rejected by server authorization",
      transaction: {
        batchId: "batch-1",
        kind: "mergeable",
        sealed: true,
        latestSettlement: {
          kind: "rejected",
          batchId: "batch-1",
          code: "permission_denied",
          reason: "Write rejected by server authorization",
        },
      },
    };

    db.onMutationError(callback);
    generatedCallback?.onMutationError(JSON.stringify(event));

    expect(callback).toHaveBeenCalledWith(event);
  });

  it("forwards asynchronous write settlement without blocking the shim", async () => {
    const order: string[] = [];
    const pending = Promise.resolve().then(() => {
      order.push("settled");
    });
    const write = {
      batchId: () => "committed-batch",
      payload: () => buffer(1),
      wait: vi.fn(async (tier: string) => {
        order.push(`wait:${tier}`);
        await pending;
      }),
      writeState: () => JSON.stringify({ kind: "Pending" }),
      close: () => true,
    };
    const db = shim({
      insertWithIdEncoded: () => write,
    });

    const adapted = db.insertWithIdEncoded("todos", new Uint8Array(16), new Uint8Array());
    const settlement = adapted.wait("edge");

    expect(order).toEqual(["wait:edge"]);
    await settlement;
    expect(order).toEqual(["wait:edge", "settled"]);
    expect(adapted.writeState()).toEqual({ kind: "Pending" });
    expect(adapted.batchId).toBe("committed-batch");
    expect(adapted.payload).toEqual(new Uint8Array([1]));
  });

  it("adapts schema views, caller-minted batches, and session transports", () => {
    const view = { free: vi.fn() };
    const registerSchema = vi.fn((_schema: ArrayBuffer) => view);
    const beginTransaction = vi.fn(
      (_openBatchId: string, _kind: string, _author: ArrayBuffer | undefined) => {},
    );
    const rollbackTransaction = vi.fn();
    const attachMergeableTx = vi.fn(() => ({}));
    const attachExclusiveTx = vi.fn(() => ({}));
    const commitTransaction = vi.fn(() => ({
      batchId: () => "batch-1",
      close: () => true,
      payload: () => buffer(),
      wait: vi.fn(async () => {}),
      writeState: () => JSON.stringify({}),
    }));
    const transport = {
      close: () => true,
      recvWireFrames: () => [],
      sendWireFrame: vi.fn(),
      sendWireFrames: vi.fn(),
      tick: () => 0,
    };
    const connectUpstreamWithSession = vi.fn(() => transport);
    const free = vi.fn();
    const db = shim({
      registerSchema,
      beginTransaction,
      commitTransaction,
      rollbackTransaction,
      attachMergeableTx,
      attachExclusiveTx,
      connectUpstreamWithSession,
      free,
    });
    const schema = new Uint8Array([9, 8]);
    const authorBacking = new Uint8Array([0, 7, 6, 0]);
    const author = authorBacking.subarray(1, 3);

    const adaptedView = db.registerSchema(schema);
    db.beginTransaction("open-1", "mergeable", author);
    expect(db.commitTransaction("open-1", "mergeable").batchId).toBe("batch-1");
    db.rollbackTransaction("open-2");
    db.attachMergeableTx("open-3");
    db.attachExclusiveTx("open-4");
    db.connectUpstreamWithSession(1, 3, new Uint8Array([1]), 5n, new Uint8Array([2]), 7n);
    db.free();

    expect(adaptedView).toBeInstanceOf(RnDbShim);
    expect(new Uint8Array(registerSchema.mock.calls[0]![0])).toEqual(schema);
    expect(beginTransaction.mock.calls[0]!.slice(0, 2)).toEqual(["open-1", "mergeable"]);
    expect(new Uint8Array(beginTransaction.mock.calls[0]![2]!)).toEqual(new Uint8Array([7, 6]));
    expect(commitTransaction).toHaveBeenCalledWith("open-1", "mergeable");
    expect(rollbackTransaction).toHaveBeenCalledWith("open-2");
    expect(attachMergeableTx).toHaveBeenCalledWith("open-3");
    expect(attachExclusiveTx).toHaveBeenCalledWith("open-4");
    expect(connectUpstreamWithSession).toHaveBeenCalledWith(
      1,
      3,
      expect.any(ArrayBuffer),
      5n,
      expect.any(ArrayBuffer),
      7n,
    );
    expect(free).toHaveBeenCalledOnce();
  });

  it("parses subscription events and restores native error markers", async () => {
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
            terminalOperationsJson: undefined,
            terminalLayoutsJson: undefined,
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
            terminalOperationsJson: JSON.stringify([
              {
                rootLayoutId: "todos-v1",
                root_key: [1],
                path: [],
                edit: { Remove: { key: [2] } },
              },
            ]),
            terminalLayoutsJson: JSON.stringify([
              {
                id: "todos-v1",
                rootDescriptor: [3],
                rootKeySlot: 0,
                rootKeyFieldName: "id",
                publicFields: [],
                carrier: "Logical",
              },
            ]),
            settled: true,
            tier: "Local",
            reasonJson: undefined,
          },
          {
            eventType: "rejected",
            reset: undefined,
            delta: undefined,
            terminalOperationsJson: undefined,
            terminalLayoutsJson: undefined,
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
        batchId: () => "error-batch",
        close: () => true,
        payload: () => buffer(),
        wait: async () => {
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
        terminalOperations: [
          {
            rootLayoutId: "todos-v1",
            root_key: [1],
            path: [],
            edit: { Remove: { key: [2] } },
          },
        ],
        terminalLayouts: [
          {
            id: "todos-v1",
            rootDescriptor: [3],
            rootKeySlot: 0,
            rootKeyFieldName: "id",
            publicFields: [],
            carrier: "Logical",
          },
        ],
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
    await expect(write.wait("edge")).rejects.toMatchObject({
      message: "NotObserved: write has not reached edge",
      cause: generatedError,
      tag: "Runtime",
    });
  });
});

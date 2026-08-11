import { afterEach, describe, expect, it, vi } from "vitest";
import type { NativeRowDelta, WasmSchema } from "../../drivers/types.js";
import { createOpenBatchId, type BatchId } from "../client.js";
import type { InsertResult, MutationResult } from "../client.js";
import type { PersistentBrowserSubscriptionMessage } from "./persistent-browser-protocol.js";
import {
  PersistentBrowserOpfsRuntime,
  type PersistentBrowserOpfsOwnerRequest,
} from "./persistent-browser-runtime.js";

const schema = {
  todos: {
    columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
  },
} satisfies WasmSchema;

function committed<T extends InsertResult | MutationResult>(
  result: T,
): Extract<T, { kind: "committed" }> {
  if (result.kind !== "committed") throw new Error("expected committed write");
  return result as Extract<T, { kind: "committed" }>;
}

class FakeWorker {
  static instances: FakeWorker[] = [];

  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: ((event: ErrorEvent) => void) | null = null;
  messages: PersistentBrowserOpfsOwnerRequest[] = [];
  terminated = false;

  constructor() {
    FakeWorker.instances.push(this);
  }

  postMessage(message: PersistentBrowserOpfsOwnerRequest): void {
    this.messages.push(message);

    if (message.method === "open" || message.method === "close") {
      this.respond(message.id, undefined);
    }
  }

  terminate(): void {
    this.terminated = true;
  }

  respond(id: number, result: unknown): void {
    queueMicrotask(() => {
      this.onmessage?.({ data: { id, ok: true, result } } as MessageEvent);
    });
  }

  reject(id: number, message: string): void {
    queueMicrotask(() => {
      this.onmessage?.({ data: { id, ok: false, error: { message } } } as MessageEvent);
    });
  }

  emitSubscription(subscription: number, delta: NativeRowDelta): void {
    queueMicrotask(() => {
      this.onmessage?.({
        data: structuredClone({
          subscription,
          frame: {
            kind: "native-row-delta",
            reset: delta.reset,
            added: delta.added.buffer.slice(
              delta.added.byteOffset,
              delta.added.byteOffset + delta.added.byteLength,
            ),
            removed: delta.removed.buffer.slice(
              delta.removed.byteOffset,
              delta.removed.byteOffset + delta.removed.byteLength,
            ),
            updated: delta.updated.buffer.slice(
              delta.updated.byteOffset,
              delta.updated.byteOffset + delta.updated.byteLength,
            ),
            addedCount: delta.addedCount,
            removedCount: delta.removedCount,
            updatedCount: delta.updatedCount,
            terminalOperations: delta.terminalOperations,
          },
        }),
      } as MessageEvent);
    });
  }

  emitSubscriptionError(subscription: number, message: string, stack?: string): void {
    const data = {
      subscription,
      error: { name: "Error", message, stack },
    } satisfies PersistentBrowserSubscriptionMessage;
    queueMicrotask(() => {
      this.onmessage?.({
        data,
      } as MessageEvent);
    });
  }
}

function uuidBytes(id: string): Uint8Array {
  return Uint8Array.from(
    id
      .replaceAll("-", "")
      .match(/../g)!
      .map((hex) => Number.parseInt(hex, 16)),
  );
}

function pushU32(target: number[], value: number): void {
  target.push(value & 0xff, (value >>> 8) & 0xff, (value >>> 16) & 0xff, (value >>> 24) & 0xff);
}

function nativeAddedRecord(id: string, index: number, text: string): Uint8Array {
  const raw = new TextEncoder().encode(text);
  const bytes: number[] = [...uuidBytes(id)];
  pushU32(bytes, index);
  pushU32(bytes, raw.byteLength);
  bytes.push(...raw);
  return Uint8Array.from(bytes);
}

describe("PersistentBrowserOpfsRuntime", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    FakeWorker.instances = [];
  });

  it("does not require connect before server-tier work in a local-only runtime", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-local-only-ready-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    const wait = runtime.waitForTransaction("local-only-transaction" as BatchId, "edge");
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "waitForTransaction")).toBe(true);
    });
    const waitMessage = worker.messages.find((message) => message.method === "waitForTransaction");
    worker.respond(waitMessage!.id, undefined);

    await expect(wait).resolves.toBeUndefined();
    await runtime.close();
  });

  it("handles rejected connection gates that have no server-tier waiters", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-unused-gate-rejection-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    runtime.connect("ws://127.0.0.1:4200/apps/app/ws", "{}");
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "connect")).toBe(true);
    });
    worker.reject(
      worker.messages.find((message) => message.method === "connect")!.id,
      "Persistent browser native runtime connect failed",
    );
    await new Promise((resolve) => setTimeout(resolve, 0));

    runtime.updateAuth("{}");
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "updateAuth")).toBe(true);
    });
    worker.reject(
      worker.messages.find((message) => message.method === "updateAuth")!.id,
      "Persistent browser native runtime auth update failed",
    );
    await new Promise((resolve) => setTimeout(resolve, 0));

    await runtime.close();
  });

  it("surfaces an unexpected connection failure exactly once through the RPC", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-connect-failure-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    runtime.connect("ws://127.0.0.1:4200/apps/app/ws", "{}");
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "connect")).toBe(true);
    });

    const surfaced: Array<() => void> = [];
    const setTimeoutSpy = vi.spyOn(globalThis, "setTimeout").mockImplementation(((
      callback: () => void,
    ) => {
      surfaced.push(callback);
      return 0 as unknown as ReturnType<typeof setTimeout>;
    }) as typeof setTimeout);

    worker.reject(
      worker.messages.find((message) => message.method === "connect")!.id,
      "arbitrary websocket failure",
    );
    for (let attempt = 0; surfaced.length === 0 && attempt < 10; attempt += 1) {
      await Promise.resolve();
    }

    expect(surfaced).toHaveLength(1);
    expect(() => surfaced[0]!()).toThrow("arbitrary websocket failure");

    setTimeoutSpy.mockRestore();
    await runtime.close();
  });

  it("rejects server-tier work parked behind a reconnect when closed", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-close-reconnect-waiters-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    const disconnect = runtime.disconnect({ rejectWaiters: false });
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "disconnect")).toBe(true);
    });
    const disconnectMessage = worker.messages.find((message) => message.method === "disconnect");
    worker.respond(disconnectMessage!.id, undefined);
    await disconnect;

    const query = runtime.query(JSON.stringify({ table: "todos" }), null, "edge", null);
    const wait = runtime.waitForTransaction("parked-transaction" as BatchId, "global");
    const queryRejection = expect(query).rejects.toThrow(
      "Persistent browser native runtime is closed",
    );
    const waitRejection = expect(wait).rejects.toThrow(
      "Persistent browser native runtime is closed",
    );
    await runtime.close();

    await queryRejection;
    await waitRejection;
  });

  it.each(["reconnect", "close"] as const)(
    "preserves parked server-tier work across repeated disconnects until %s",
    async (outcome) => {
      vi.stubGlobal("Worker", FakeWorker);

      const runtime = new PersistentBrowserOpfsRuntime(
        undefined,
        schema,
        `persistent-browser-runtime-repeat-disconnect-${outcome}-test`,
        new Uint8Array(16),
        new Uint8Array(16),
      );
      const worker = FakeWorker.instances[0];

      const firstDisconnect = runtime.disconnect({ rejectWaiters: false });
      await vi.waitFor(() => {
        expect(worker.messages.filter((message) => message.method === "disconnect")).toHaveLength(
          1,
        );
      });
      worker.respond(
        worker.messages.find((message) => message.method === "disconnect")!.id,
        undefined,
      );
      await firstDisconnect;

      const query = runtime.query(JSON.stringify({ table: "todos" }), null, "edge", null);
      const secondDisconnect = runtime.disconnect({ rejectWaiters: false });
      await vi.waitFor(() => {
        expect(worker.messages.filter((message) => message.method === "disconnect")).toHaveLength(
          2,
        );
      });
      worker.respond(
        worker.messages.filter((message) => message.method === "disconnect")[1]!.id,
        undefined,
      );
      await secondDisconnect;

      if (outcome === "reconnect") {
        runtime.connect("ws://127.0.0.1:4200/apps/app/ws", "{}");
        await vi.waitFor(() => {
          expect(worker.messages.some((message) => message.method === "connect")).toBe(true);
        });
        worker.respond(
          worker.messages.find((message) => message.method === "connect")!.id,
          undefined,
        );
        await vi.waitFor(() => {
          expect(worker.messages.some((message) => message.method === "query")).toBe(true);
        });
        worker.respond(worker.messages.find((message) => message.method === "query")!.id, []);
        await expect(query).resolves.toEqual([]);
        await runtime.close();
      } else {
        const rejection = expect(query).rejects.toThrow(
          "Persistent browser native runtime is closed",
        );
        await runtime.close();
        await rejection;
      }
    },
  );

  it.each(["during close", "after close"] as const)(
    "does not install a new reconnect gate when disconnecting %s",
    async (timing) => {
      vi.stubGlobal("Worker", FakeWorker);

      const runtime = new PersistentBrowserOpfsRuntime(
        undefined,
        schema,
        `persistent-browser-runtime-disconnect-${timing.replace(" ", "-")}-test`,
        new Uint8Array(16),
        new Uint8Array(16),
      );

      const close = runtime.close();
      if (timing === "after close") await close;
      await runtime.disconnect({ rejectWaiters: false });

      await expect(
        runtime.query(JSON.stringify({ table: "todos" }), null, "edge", null),
      ).rejects.toThrow("Persistent browser native runtime is closed");
      await close;
    },
  );

  it.each([
    ["connect", "during close"],
    ["connect", "after close"],
    ["updateAuth", "during close"],
    ["updateAuth", "after close"],
  ] as const)("does not replace the terminal gate via %s %s", async (operation, timing) => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      `persistent-browser-runtime-${operation}-${timing.replace(" ", "-")}-test`,
      new Uint8Array(16),
      new Uint8Array(16),
    );

    const close = runtime.close();
    if (timing === "after close") await close;
    if (operation === "connect") {
      runtime.connect("ws://127.0.0.1:4200/apps/app/ws", "{}");
    } else {
      runtime.updateAuth("{}");
    }

    await expect(
      runtime.query(JSON.stringify({ table: "todos" }), null, "edge", null),
    ).rejects.toThrow("Persistent browser native runtime is closed");
    await close;
  });

  it("posts createSubscription before a subsequently called write", async () => {
    vi.stubGlobal("Worker", FakeWorker);
    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-subscription-write-fifo-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0]!;

    runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "local", null);
    runtime.insert("todos", { title: { type: "Text", value: "after" } });

    await vi.waitFor(() => expect(worker.messages).toHaveLength(2));
    expect(worker.messages.map(({ method }) => method)).toEqual([
      "open",
      "createExecutedSubscription",
    ]);
    worker.respond(worker.messages[1]!.id, 1);
    await vi.waitFor(() => expect(worker.messages).toHaveLength(3));
    expect(worker.messages[2]!.method).toBe("insert");
    worker.respond(worker.messages[2]!.id, { kind: "committed", batchId: "batch-1" });
    await runtime.close();
  });

  it("posts a write before a subsequently called createSubscription", async () => {
    vi.stubGlobal("Worker", FakeWorker);
    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-write-subscription-fifo-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0]!;

    runtime.insert("todos", { title: { type: "Text", value: "before" } });
    runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "local", null);

    await vi.waitFor(() =>
      expect(worker.messages.some(({ method }) => method === "insert")).toBe(true),
    );
    expect(worker.messages.map(({ method }) => method)).toEqual(["open", "insert"]);
    const write = worker.messages[1]!;
    worker.respond(write.id, { kind: "committed", batchId: "batch-1" });
    await vi.waitFor(() => expect(worker.messages).toHaveLength(3));
    expect(worker.messages[2]!.method).toBe("createExecutedSubscription");
    worker.respond(worker.messages[2]!.id, 1);
    await runtime.close();
  });

  it("returns a pending write handle and waits on the worker transaction id", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    const insert = runtime.insert(
      "todos",
      { title: { type: "Text", value: "write through worker" } },
      undefined,
      "00000000-0000-0000-0000-000000000001",
    );
    expect(insert.kind).toBe("committed");

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "insert")).toBe(true);
    });
    const insertMessage = worker.messages.find((message) => message.method === "insert");
    expect(insertMessage).toBeDefined();
    worker.respond(insertMessage!.id, {
      kind: "committed",
      batchId: "native-runtime-transaction",
    });

    const waitPromise = runtime.waitForTransaction(await committed(insert).batchId, "local");

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "waitForTransaction")).toBe(true);
    });
    const waitMessage = worker.messages.find((message) => message.method === "waitForTransaction");
    expect(waitMessage?.args).toEqual(["native-runtime-transaction", "local"]);
    worker.respond(waitMessage!.id, undefined);

    await expect(waitPromise).resolves.toBeUndefined();
    await runtime.close();
  });

  it("registers a subscription before a subsequent fire-and-forget write", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-subscribe-before-write-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    const subscription = runtime.createSubscription(JSON.stringify({ table: "todos" }));
    runtime.executeSubscription(subscription, () => undefined);
    runtime.insert(
      "todos",
      { title: { type: "Text", value: "must follow subscription registration" } },
      undefined,
      "00000000-0000-0000-0000-000000000001",
    );

    await vi.waitFor(() => {
      expect(
        worker.messages.some((message) => message.method === "createExecutedSubscription"),
      ).toBe(true);
    });
    expect(worker.messages.some((message) => message.method === "insert")).toBe(false);

    const create = worker.messages.find(
      (message) => message.method === "createExecutedSubscription",
    );
    worker.respond(create!.id, 7);
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "insert")).toBe(true);
    });
    const insert = worker.messages.find((message) => message.method === "insert");
    worker.respond(insert!.id, { kind: "committed", batchId: "native-runtime-transaction" });

    await runtime.close();
  });

  it("does not delay a write behind a subscription created later", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-write-before-subscribe-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    runtime.insert(
      "todos",
      { title: { type: "Text", value: "must be in the subscription snapshot" } },
      undefined,
      "00000000-0000-0000-0000-000000000001",
    );
    const subscription = runtime.createSubscription(JSON.stringify({ table: "todos" }));
    runtime.executeSubscription(subscription, () => undefined);

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "insert")).toBe(true);
    });
    expect(worker.messages.some((message) => message.method === "createExecutedSubscription")).toBe(
      false,
    );

    const insert = worker.messages.find((message) => message.method === "insert")!;
    worker.respond(insert.id, { kind: "committed", batchId: "native-runtime-transaction" });
    await vi.waitFor(() => {
      expect(
        worker.messages.some((message) => message.method === "createExecutedSubscription"),
      ).toBe(true);
    });
    const create = worker.messages.find(
      (message) => message.method === "createExecutedSubscription",
    )!;
    expect(worker.messages.indexOf(insert)).toBeLessThan(worker.messages.indexOf(create));
    worker.respond(create.id, 7);

    await runtime.close();
  });

  it("rejects waits when the worker write fails before core returns a transaction id", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-write-failure-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    const update = runtime.update(
      "todos",
      "00000000-0000-0000-0000-000000000001",
      { title: { type: "Text", value: "rejected by worker" } },
      undefined,
    );

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "update")).toBe(true);
    });
    const updateMessage = worker.messages.find((message) => message.method === "update");
    expect(updateMessage).toBeDefined();
    worker.reject(updateMessage!.id, "native runtime rejected write");

    await expect(update.kind === "committed" ? update.batchId : Promise.resolve()).rejects.toThrow(
      "native runtime rejected write",
    );
    expect(worker.messages.some((message) => message.method === "waitForTransaction")).toBe(false);

    await runtime.close();
  });

  it("waits for the worker connect command before edge durability waits", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-connect-before-edge-wait-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    runtime.connect("ws://127.0.0.1:4200/apps/app/ws", "{}");
    const insert = runtime.insert(
      "todos",
      { title: { type: "Text", value: "connect before wait" } },
      undefined,
      "00000000-0000-0000-0000-000000000001",
    );

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "connect")).toBe(true);
    });

    const connectMessage = worker.messages.find((message) => message.method === "connect");
    expect(worker.messages.some((message) => message.method === "insert")).toBe(false);
    worker.respond(connectMessage!.id, undefined);
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "insert")).toBe(true);
    });
    const insertMessage = worker.messages.find((message) => message.method === "insert");
    expect(connectMessage).toBeDefined();
    expect(insertMessage).toBeDefined();
    worker.respond(insertMessage!.id, {
      kind: "committed",
      batchId: "native-runtime-transaction",
    });

    const waitPromise = runtime.waitForTransaction(await committed(insert).batchId, "edge");
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "waitForTransaction")).toBe(true);
    });
    const waitMessage = worker.messages.find((message) => message.method === "waitForTransaction");
    expect(waitMessage?.args).toEqual(["native-runtime-transaction", "edge"]);
    worker.respond(waitMessage!.id, undefined);

    await expect(waitPromise).resolves.toBeUndefined();
    await runtime.close();
  });

  it("keeps server-tier waits alive across a disconnect and reconnect", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-reconnect-wait-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    runtime.connect("ws://127.0.0.1:4200/apps/app/ws", "{}");
    const insert = runtime.insert(
      "todos",
      { title: { type: "Text", value: "reconnect wait" } },
      undefined,
      "00000000-0000-0000-0000-000000000001",
    );
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "connect")).toBe(true);
    });
    const connectMessage = worker.messages.find((message) => message.method === "connect");
    worker.respond(connectMessage!.id, undefined);
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "insert")).toBe(true);
    });
    const insertMessage = worker.messages.find((message) => message.method === "insert");
    worker.respond(insertMessage!.id, {
      kind: "committed",
      batchId: "native-runtime-transaction",
    });

    const waitPromise = runtime.waitForTransaction(await committed(insert).batchId, "edge");
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "waitForTransaction")).toBe(true);
    });
    const waitMessage = worker.messages.find((message) => message.method === "waitForTransaction");

    const disconnectPromise = runtime.disconnect({ rejectWaiters: false });
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "disconnect")).toBe(true);
    });
    const disconnectMessage = worker.messages.find((message) => message.method === "disconnect");
    expect(disconnectMessage?.args).toEqual([{ rejectWaiters: false }]);
    worker.respond(disconnectMessage!.id, undefined);
    await disconnectPromise;

    runtime.connect("ws://127.0.0.1:4200/apps/app/ws", "{}");
    await vi.waitFor(() => {
      expect(worker.messages.filter((message) => message.method === "connect")).toHaveLength(2);
    });
    const reconnectMessage = worker.messages.filter((message) => message.method === "connect")[1];
    worker.respond(reconnectMessage!.id, undefined);
    worker.respond(waitMessage!.id, undefined);

    await expect(waitPromise).resolves.toBeUndefined();
    await runtime.close();
  });

  it("runs local reads and defers edge reads until reconnect", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-disconnected-read-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    const disconnectPromise = runtime.disconnect({ rejectWaiters: false });
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "disconnect")).toBe(true);
    });
    const disconnectMessage = worker.messages.find((message) => message.method === "disconnect");
    worker.respond(disconnectMessage!.id, undefined);
    await disconnectPromise;

    const localRead = runtime.query(
      JSON.stringify({ table: "todos" }),
      null,
      "local",
      JSON.stringify({ propagation: "full" }),
    );
    const edgeRead = runtime.query(JSON.stringify({ table: "todos" }), null, "edge", null);
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "query")).toBe(true);
    });
    expect(worker.messages.filter((message) => message.method === "query")).toHaveLength(1);
    const localQueryMessage = worker.messages.find((message) => message.method === "query");
    expect(localQueryMessage?.args[2]).toBe("local");
    worker.respond(localQueryMessage!.id, ["local"]);
    await expect(localRead).resolves.toEqual(["local"]);

    runtime.connect("ws://127.0.0.1:4200/apps/app/ws", "{}");
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "connect")).toBe(true);
    });
    const connectMessage = worker.messages.find((message) => message.method === "connect");
    worker.respond(connectMessage!.id, undefined);

    await vi.waitFor(() => {
      expect(worker.messages.filter((message) => message.method === "query")).toHaveLength(2);
    });
    const edgeQueryMessage = worker.messages.filter((message) => message.method === "query")[1];
    expect(edgeQueryMessage?.args[2]).toBe("edge");
    worker.respond(edgeQueryMessage!.id, ["edge"]);
    await expect(edgeRead).resolves.toEqual(["edge"]);
    await runtime.close();
  });

  it("does not release disconnected server-tier work when auth changes", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-disconnected-auth-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    const disconnect = runtime.disconnect({ rejectWaiters: false });
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "disconnect")).toBe(true);
    });
    worker.respond(
      worker.messages.find((message) => message.method === "disconnect")!.id,
      undefined,
    );
    await disconnect;

    const query = runtime.query(JSON.stringify({ table: "todos" }), null, "edge", null);
    const wait = runtime.waitForTransaction("disconnected-auth-transaction" as BatchId, "global");
    runtime.updateAuth('{"token":"replacement"}');
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "updateAuth")).toBe(true);
    });
    worker.respond(
      worker.messages.find((message) => message.method === "updateAuth")!.id,
      undefined,
    );
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(worker.messages.some((message) => message.method === "query")).toBe(false);
    expect(worker.messages.some((message) => message.method === "waitForTransaction")).toBe(false);

    runtime.connect("ws://127.0.0.1:4200/apps/app/ws", "{}");
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "connect")).toBe(true);
    });
    worker.respond(worker.messages.find((message) => message.method === "connect")!.id, undefined);
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "query")).toBe(true);
    });
    worker.respond(worker.messages.find((message) => message.method === "query")!.id, []);
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "waitForTransaction")).toBe(true);
    });
    worker.respond(
      worker.messages.find((message) => message.method === "waitForTransaction")!.id,
      undefined,
    );

    await expect(query).resolves.toEqual([]);
    await expect(wait).resolves.toBeUndefined();
    await runtime.close();
  });

  it("orders edge reads after prior edge durability waits", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-edge-read-after-wait-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    runtime.connect("ws://127.0.0.1:4200/apps/app/ws", "{}");
    const insert = runtime.insert(
      "todos",
      { title: { type: "Text", value: "edge read after wait" } },
      undefined,
      "00000000-0000-0000-0000-000000000001",
    );

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "connect")).toBe(true);
    });

    const connectMessage = worker.messages.find((message) => message.method === "connect");
    worker.respond(connectMessage!.id, undefined);
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "insert")).toBe(true);
    });
    const insertMessage = worker.messages.find((message) => message.method === "insert");
    worker.respond(insertMessage!.id, {
      kind: "committed",
      batchId: "native-runtime-transaction",
    });

    const waitPromise = runtime.waitForTransaction(await committed(insert).batchId, "edge");
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "waitForTransaction")).toBe(true);
    });
    const waitMessage = worker.messages.find((message) => message.method === "waitForTransaction");

    const queryPromise = runtime.query(JSON.stringify({ table: "todos" }), null, "edge", null);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(worker.messages.some((message) => message.method === "query")).toBe(false);

    worker.respond(waitMessage!.id, undefined);
    await waitPromise;

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "query")).toBe(true);
    });
    const queryMessage = worker.messages.find((message) => message.method === "query");
    expect(queryMessage?.args).toEqual([JSON.stringify({ table: "todos" }), null, "edge", null]);
    worker.respond(queryMessage!.id, []);

    await expect(queryPromise).resolves.toEqual([]);
    await runtime.close();
  });

  it("waits for the worker connect command before server-backed reads subscribe", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-connect-before-edge-read-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    runtime.connect("ws://127.0.0.1:4200/apps/app/ws", "{}");
    const queryPromise = runtime.query(JSON.stringify({ table: "todos" }), null, "edge", null);
    const subscriptionQuery = JSON.stringify({ table: "todos", debugName: "active todos" });
    const subscriptionHandle = runtime.createSubscription(subscriptionQuery, null, "edge", null);
    runtime.executeSubscription(subscriptionHandle, () => undefined);

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "connect")).toBe(true);
    });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(worker.messages.some((message) => message.method === "query")).toBe(false);
    expect(worker.messages.some((message) => message.method === "createExecutedSubscription")).toBe(
      false,
    );

    const connectMessage = worker.messages.find((message) => message.method === "connect");
    worker.respond(connectMessage!.id, undefined);

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "query")).toBe(true);
    });

    const queryMessage = worker.messages.find((message) => message.method === "query");
    worker.respond(queryMessage!.id, []);
    await vi.waitFor(() => {
      expect(
        worker.messages.some((message) => message.method === "createExecutedSubscription"),
      ).toBe(true);
    });
    const createSubscriptionMessage = worker.messages.find(
      (message) => message.method === "createExecutedSubscription",
    );
    worker.respond(createSubscriptionMessage!.id, 7);

    await expect(queryPromise).resolves.toEqual([]);
    expect(createSubscriptionMessage?.args[0]).toBe(subscriptionHandle);
    expect(createSubscriptionMessage?.query).toBe(subscriptionQuery);
    expect(createSubscriptionMessage?.debugName).toBe("active todos");

    await runtime.close();
  });

  it("decodes transferable encoded subscription frames from the worker", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-encoded-subscription-frame-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    const subscriptionHandle = runtime.createSubscription(
      JSON.stringify({ table: "todos" }),
      null,
      "local",
      null,
    );
    const updates: NativeRowDelta[] = [];
    runtime.executeSubscription(subscriptionHandle, (delta: NativeRowDelta) => {
      updates.push(delta);
    });

    await vi.waitFor(() => {
      expect(
        worker.messages.some((message) => message.method === "createExecutedSubscription"),
      ).toBe(true);
    });
    const createSubscriptionMessage = worker.messages.find(
      (message) => message.method === "createExecutedSubscription",
    );
    expect(createSubscriptionMessage?.args[0]).toBe(subscriptionHandle);
    worker.respond(createSubscriptionMessage!.id, 7);

    const added = nativeAddedRecord("00000000-0000-4000-8000-000000000001", 0, "encoded");
    worker.emitSubscription(subscriptionHandle, {
      __jazzNativeRowDelta: true,
      added,
      removed: new Uint8Array(),
      updated: new Uint8Array(),
      addedCount: 1,
      removedCount: 0,
      updatedCount: 0,
    });

    await vi.waitFor(() => {
      expect(updates).toHaveLength(1);
    });
    expect(updates[0]).toMatchObject({
      __jazzNativeRowDelta: true,
      addedCount: 1,
      removedCount: 0,
      updatedCount: 0,
    });
    expect([...updates[0]!.added]).toEqual([...added]);

    await runtime.close();
  });

  it("round-trips canonical terminal operations through the worker subscription frame", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-terminal-operations-frame-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];
    const subscriptionHandle = runtime.createSubscription(
      JSON.stringify({ table: "todos" }),
      null,
      "local",
      null,
    );
    const updates: NativeRowDelta[] = [];
    runtime.executeSubscription(subscriptionHandle, (delta: NativeRowDelta) => updates.push(delta));

    await vi.waitFor(() => {
      expect(
        worker.messages.some((message) => message.method === "createExecutedSubscription"),
      ).toBe(true);
    });
    worker.respond(
      worker.messages.find((message) => message.method === "createExecutedSubscription")!.id,
      7,
    );

    const rootId = "00000000-0000-0000-0000-000000000123";
    const childId = "00000000-0000-0000-0000-000000000124";
    const rootKey = [10, ...uuidBytes(rootId)];
    const childKey = [10, ...uuidBytes(childId)];
    // CurrentRow root: key followed by the nullable-carried Text field.
    const rootPayload = [...uuidBytes(rootId), 1, ...new TextEncoder().encode("root")];
    const terminalOperations = [
      {
        root_key: rootKey,
        path: [],
        edit: { Insert: { index: 0, key: rootKey, value: rootPayload } },
      },
      {
        root_key: rootKey,
        path: [{ Collection: "children" }],
        edit: { Insert: { index: 0, key: childKey, value: [...uuidBytes(childId), 99] } },
      },
    ];
    worker.emitSubscription(subscriptionHandle, {
      __jazzNativeRowDelta: true,
      added: new Uint8Array(),
      removed: new Uint8Array(),
      updated: new Uint8Array(),
      addedCount: 0,
      removedCount: 0,
      updatedCount: 0,
      terminalOperations,
    });

    await vi.waitFor(() => expect(updates).toHaveLength(1));
    expect(updates[0]!.terminalOperations).toEqual(terminalOperations);
    await runtime.close();
  });

  it("surfaces worker-owned subscription errors to the subscription callback", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-subscription-error-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    const subscriptionHandle = runtime.createSubscription(
      JSON.stringify({ table: "todos" }),
      null,
      "local",
      null,
    );
    const updates: unknown[][] = [];
    runtime.executeSubscription(subscriptionHandle, (...args: unknown[]) => {
      updates.push(args);
    });

    await vi.waitFor(() => {
      expect(
        worker.messages.some((message) => message.method === "createExecutedSubscription"),
      ).toBe(true);
    });
    const createSubscriptionMessage = worker.messages.find(
      (message) => message.method === "createExecutedSubscription",
    );
    worker.respond(createSubscriptionMessage!.id, 7);

    worker.emitSubscriptionError(subscriptionHandle, "server transport died", "remote stack");

    await vi.waitFor(() => {
      expect(updates).toHaveLength(1);
    });
    expect(updates[0]![0]).toBeInstanceOf(Error);
    expect((updates[0]![0] as Error).message).toBe("server transport died");
    expect((updates[0]![0] as Error).stack).toBe("remote stack");
    expect(updates[0]![1]).toBeNull();

    await runtime.close();
  });

  it("surfaces worker-owned edge read failures to the caller", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-edge-read-error-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    runtime.connect("ws://127.0.0.1:4200/apps/app/ws", "{}");
    const queryPromise = runtime.query(JSON.stringify({ table: "todos" }), null, "edge", null);

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "connect")).toBe(true);
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(worker.messages.some((message) => message.method === "query")).toBe(false);

    const connectMessage = worker.messages.find((message) => message.method === "connect");
    worker.respond(connectMessage!.id, undefined);

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "query")).toBe(true);
    });
    const queryMessage = worker.messages.find((message) => message.method === "query");
    worker.reject(queryMessage!.id, "edge coverage failed");

    await expect(queryPromise).rejects.toThrow("edge coverage failed");
    await runtime.close();
  });

  it("does not send local reads to the worker before queued writes are visible", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-read-after-write-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    runtime.insert(
      "todos",
      { title: { type: "Text", value: "read after write" } },
      undefined,
      "00000000-0000-0000-0000-000000000001",
    );

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "insert")).toBe(true);
    });

    const queryPromise = runtime.query(JSON.stringify({ table: "todos" }), null, "local", null);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(worker.messages.some((message) => message.method === "query")).toBe(false);

    const insertMessage = worker.messages.find((message) => message.method === "insert");
    worker.respond(insertMessage!.id, {
      kind: "committed",
      batchId: "native-runtime-transaction",
    });

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "query")).toBe(true);
    });
    const queryMessage = worker.messages.find((message) => message.method === "query");
    worker.respond(queryMessage!.id, []);

    await expect(queryPromise).resolves.toEqual([]);
    await runtime.close();
  });

  it("translates transaction read ids after staged transaction writes settle", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-transaction-read-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    const localTxId = createOpenBatchId();
    runtime.beginTransaction("mergeable", localTxId);
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "beginTransaction")).toBe(true);
    });
    const beginMessage = worker.messages.find((message) => message.method === "beginTransaction");
    expect(beginMessage?.args).toEqual(["mergeable", localTxId]);
    worker.respond(beginMessage!.id, localTxId);

    runtime.insert(
      "todos",
      { title: { type: "Text", value: "inside tx" } },
      JSON.stringify({ batch_id: localTxId }),
      "00000000-0000-0000-0000-000000000001",
    );

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "insert")).toBe(true);
    });
    const insertMessage = worker.messages.find((message) => message.method === "insert");
    expect(insertMessage?.args[2]).toBe(JSON.stringify({ batch_id: localTxId }));

    const queryPromise = runtime.query(
      JSON.stringify({ table: "todos" }),
      null,
      "local",
      JSON.stringify({ transaction_batch_id: localTxId }),
    );
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(worker.messages.some((message) => message.method === "query")).toBe(false);

    worker.respond(insertMessage!.id, { kind: "staged", openBatchId: localTxId });

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "query")).toBe(true);
    });
    const queryMessage = worker.messages.find((message) => message.method === "query");
    expect(queryMessage?.args[3]).toBe(JSON.stringify({ transaction_batch_id: localTxId }));
    worker.respond(queryMessage!.id, []);

    await expect(queryPromise).resolves.toEqual([]);
    await runtime.close();
  });

  it("rejects repeated transaction completion and writes after completion synchronously", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-completed-transaction-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    const tx = createOpenBatchId();
    runtime.beginTransaction("mergeable", tx);
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "beginTransaction")).toBe(true);
    });
    const beginMessage = worker.messages.find((message) => message.method === "beginTransaction");
    worker.respond(beginMessage!.id, tx);

    const committed = runtime.commitTransaction(tx);

    expect(() => runtime.commitTransaction(tx)).toThrow(
      `Commit transaction failed: batch ${tx} is already committing`,
    );
    expect(() => runtime.rollbackTransaction(tx)).toThrow(
      `Rollback transaction failed: batch ${tx} is already committing`,
    );
    expect(() =>
      runtime.insert(
        "todos",
        { title: { type: "Text", value: "too late" } },
        JSON.stringify({ batch_id: tx }),
        "00000000-0000-0000-0000-000000000001",
      ),
    ).toThrow(`Insert failed: batch ${tx} is completing`);

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "commitTransaction")).toBe(true);
    });
    const commitMessage = worker.messages.find((message) => message.method === "commitTransaction");
    worker.respond(commitMessage!.id, "00000000-0000-7000-8000-000000000001");
    await expect(committed).resolves.toBe("00000000-0000-7000-8000-000000000001");

    await runtime.close();
  });

  it("rejects a duplicate live OpenBatchId before it can overwrite staged worker state", async () => {
    vi.stubGlobal("Worker", FakeWorker);
    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-duplicate-open-batch-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];
    const id = createOpenBatchId();
    runtime.beginTransaction("mergeable", id);
    await vi.waitFor(() =>
      expect(worker.messages.some((message) => message.method === "beginTransaction")).toBe(true),
    );
    const begin = worker.messages.find((message) => message.method === "beginTransaction")!;
    worker.respond(begin.id, id);

    runtime.insert(
      "todos",
      { title: { type: "Text", value: "first" } },
      JSON.stringify({ batch_id: id }),
    );
    await vi.waitFor(() =>
      expect(worker.messages.filter((message) => message.method === "insert")).toHaveLength(1),
    );
    const first = worker.messages.find((message) => message.method === "insert")!;
    worker.respond(first.id, { kind: "staged", openBatchId: id });

    expect(() => runtime.beginTransaction("mergeable", id)).toThrow(
      `Begin transaction failed: batch ${id} has already been opened`,
    );
    runtime.insert(
      "todos",
      { title: { type: "Text", value: "second" } },
      JSON.stringify({ batch_id: id }),
    );
    await vi.waitFor(() =>
      expect(worker.messages.filter((message) => message.method === "insert")).toHaveLength(2),
    );
    expect(worker.messages.filter((message) => message.method === "beginTransaction")).toHaveLength(
      1,
    );
    const second = worker.messages.filter((message) => message.method === "insert")[1]!;
    worker.respond(second.id, { kind: "staged", openBatchId: id });
    await runtime.close();
  });

  it("fences commands for a failed begin without poisoning unrelated FIFO work", async () => {
    vi.stubGlobal("Worker", FakeWorker);
    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-failed-begin-fence-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];
    const id = createOpenBatchId();
    runtime.beginTransaction("mergeable", id);
    runtime.insert(
      "todos",
      { title: { type: "Text", value: "must not send" } },
      JSON.stringify({ batch_id: id }),
    );
    const commit = runtime.commitTransaction(id);
    const unrelated = committed(
      runtime.insert("todos", { title: { type: "Text", value: "unrelated" } }),
    );
    await vi.waitFor(() =>
      expect(worker.messages.some((message) => message.method === "beginTransaction")).toBe(true),
    );
    const begin = worker.messages.find((message) => message.method === "beginTransaction")!;
    worker.reject(begin.id, "forced OpenBatchId collision");

    await expect(commit).rejects.toThrow("forced OpenBatchId collision");
    await vi.waitFor(() =>
      expect(worker.messages.filter((message) => message.method === "insert")).toHaveLength(1),
    );
    const sent = worker.messages.find((message) => message.method === "insert")!;
    worker.respond(sent.id, { kind: "committed", batchId: "00000000000070008000000000000009" });
    await expect(unrelated.batchId).resolves.toBe("00000000000070008000000000000009");
    expect(() => runtime.beginTransaction("mergeable", id)).toThrow(
      `Begin transaction failed: batch ${id} has already been opened`,
    );
    await runtime.close();
  });

  it("closes the OPFS owner before terminating the worker", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-close-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "open")).toBe(true);
    });

    await runtime.close();

    expect(worker.terminated).toBe(true);
    expect(worker.messages.some((message) => message.method === "close")).toBe(true);
  });

  it("lets close preempt a durability wait without interleaving ordinary FIFO commands", async () => {
    vi.stubGlobal("Worker", FakeWorker);
    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-close-behind-wait-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];
    const wait = runtime.waitForTransaction("00000000000070008000000000000003" as never, "local");
    await vi.waitFor(() =>
      expect(worker.messages.some((message) => message.method === "waitForTransaction")).toBe(true),
    );
    const laterWrite = committed(
      runtime.insert("todos", { title: { type: "Text", value: "queued behind wait" } }),
    );

    await expect(runtime.close()).resolves.toBeUndefined();
    await expect(wait).rejects.toThrow("closed");
    await expect(laterWrite.batchId).rejects.toThrow("closed");
    expect(worker.messages.map((message) => message.method)).toContain("close");
    expect(worker.terminated).toBe(true);
  });

  it("forwards the configured initial-sync flush cadence to the OPFS owner", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-flush-cadence-test",
      new Uint8Array(16),
      new Uint8Array(16),
      17,
    );
    const worker = FakeWorker.instances[0];

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "open")).toBe(true);
    });
    const openMessage = worker.messages.find((message) => message.method === "open");
    expect(openMessage?.args[5]).toBe(17);

    await runtime.close();
  });

  it("defaults the OPFS owner initial-sync flush cadence to 512 writes", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserOpfsRuntime(
      undefined,
      schema,
      "persistent-browser-runtime-default-flush-cadence-test",
      new Uint8Array(16),
      new Uint8Array(16),
    );
    const worker = FakeWorker.instances[0];

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "open")).toBe(true);
    });
    const openMessage = worker.messages.find((message) => message.method === "open");
    expect(openMessage?.args[5]).toBe(512);

    await runtime.close();
  });
});

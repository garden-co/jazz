import { afterEach, describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../../drivers/types.js";
import {
  PersistentBrowserOpfsRuntime,
  type PersistentBrowserOpfsOwnerRequest,
} from "./persistent-browser-runtime.js";

const schema = {
  todos: {
    columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
  },
} satisfies WasmSchema;

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

    if (message.method === "open") {
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
}

describe("PersistentBrowserOpfsRuntime", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    FakeWorker.instances = [];
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
    expect(insert.transactionId).toMatch(/^pending-worker-write-/);

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "insert")).toBe(true);
    });
    const insertMessage = worker.messages.find((message) => message.method === "insert");
    expect(insertMessage).toBeDefined();
    worker.respond(insertMessage!.id, { transactionId: "native-runtime-transaction" });

    const waitPromise = runtime.waitForTransaction(insert.transactionId, "local");

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "waitForTransaction")).toBe(true);
    });
    const waitMessage = worker.messages.find((message) => message.method === "waitForTransaction");
    expect(waitMessage?.args).toEqual(["native-runtime-transaction", "local"]);
    worker.respond(waitMessage!.id, undefined);

    await expect(waitPromise).resolves.toBeUndefined();
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

    await expect(runtime.waitForTransaction(update.transactionId, "local")).rejects.toThrow(
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
      expect(worker.messages.some((message) => message.method === "insert")).toBe(true);
    });

    const connectMessage = worker.messages.find((message) => message.method === "connect");
    const insertMessage = worker.messages.find((message) => message.method === "insert");
    expect(connectMessage).toBeDefined();
    expect(insertMessage).toBeDefined();
    worker.respond(insertMessage!.id, { transactionId: "native-runtime-transaction" });

    const waitPromise = runtime.waitForTransaction(insert.transactionId, "edge");
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(worker.messages.some((message) => message.method === "waitForTransaction")).toBe(false);

    worker.respond(connectMessage!.id, undefined);

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "waitForTransaction")).toBe(true);
    });
    const waitMessage = worker.messages.find((message) => message.method === "waitForTransaction");
    expect(waitMessage?.args).toEqual(["native-runtime-transaction", "edge"]);
    worker.respond(waitMessage!.id, undefined);

    await expect(waitPromise).resolves.toBeUndefined();
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
      expect(worker.messages.some((message) => message.method === "insert")).toBe(true);
    });

    const connectMessage = worker.messages.find((message) => message.method === "connect");
    const insertMessage = worker.messages.find((message) => message.method === "insert");
    worker.respond(connectMessage!.id, undefined);
    worker.respond(insertMessage!.id, { transactionId: "native-runtime-transaction" });

    const waitPromise = runtime.waitForTransaction(insert.transactionId, "edge");
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
    const subscriptionHandle = runtime.createSubscription(
      JSON.stringify({ table: "todos" }),
      null,
      "edge",
      null,
    );
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
      expect(
        worker.messages.some((message) => message.method === "createExecutedSubscription"),
      ).toBe(true);
    });

    const queryMessage = worker.messages.find((message) => message.method === "query");
    const createSubscriptionMessage = worker.messages.find(
      (message) => message.method === "createExecutedSubscription",
    );
    worker.respond(queryMessage!.id, []);
    worker.respond(createSubscriptionMessage!.id, 7);

    await expect(queryPromise).resolves.toEqual([]);
    expect(createSubscriptionMessage?.args[0]).toBe(subscriptionHandle);

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
    worker.respond(insertMessage!.id, { transactionId: "native-runtime-transaction" });

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

    const localTxId = runtime.beginTransaction("mergeable");
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "beginTransaction")).toBe(true);
    });
    const beginMessage = worker.messages.find((message) => message.method === "beginTransaction");
    worker.respond(beginMessage!.id, "worker-tx-1");

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
    expect(insertMessage?.args[2]).toBe(JSON.stringify({ batch_id: "worker-tx-1" }));

    const queryPromise = runtime.query(
      JSON.stringify({ table: "todos" }),
      null,
      "local",
      JSON.stringify({ transaction_batch_id: localTxId }),
    );
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(worker.messages.some((message) => message.method === "query")).toBe(false);

    worker.respond(insertMessage!.id, { transactionId: "worker-tx-1" });

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "query")).toBe(true);
    });
    const queryMessage = worker.messages.find((message) => message.method === "query");
    expect(queryMessage?.args[3]).toBe(JSON.stringify({ transaction_batch_id: "worker-tx-1" }));
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

    const tx = runtime.beginTransaction("mergeable");
    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "beginTransaction")).toBe(true);
    });
    const beginMessage = worker.messages.find((message) => message.method === "beginTransaction");
    worker.respond(beginMessage!.id, "worker-tx-1");

    runtime.commitTransaction(tx);

    expect(() => runtime.commitTransaction(tx)).toThrow(
      `Write error: transaction ${tx} is already committed`,
    );
    expect(() => runtime.rollbackTransaction(tx)).toThrow(
      `Write error: transaction ${tx} is already committed`,
    );
    expect(() =>
      runtime.insert(
        "todos",
        { title: { type: "Text", value: "too late" } },
        JSON.stringify({ batch_id: tx }),
        "00000000-0000-0000-0000-000000000001",
      ),
    ).toThrow(`Insert failed: WriteError("transaction ${tx} is already committed")`);

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "commitTransaction")).toBe(true);
    });
    const commitMessage = worker.messages.find((message) => message.method === "commitTransaction");
    worker.respond(commitMessage!.id, undefined);

    await runtime.close();
  });

  it("terminates locally on close without sending an OPFS owner close command", async () => {
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
    expect(
      worker.messages.some((message) => (message as { method: string }).method === "close"),
    ).toBe(false);
  });
});

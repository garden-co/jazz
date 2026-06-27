import { afterEach, describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../../drivers/types.js";
import {
  PersistentBrowserRuntime,
  type PersistentBrowserWorkerRequest,
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
  messages: PersistentBrowserWorkerRequest[] = [];

  constructor() {
    FakeWorker.instances.push(this);
  }

  postMessage(message: PersistentBrowserWorkerRequest): void {
    this.messages.push(message);

    if (message.method === "open") {
      this.respond(message.id, undefined);
    }
  }

  terminate(): void {}

  respond(id: number, result: unknown): void {
    queueMicrotask(() => {
      this.onmessage?.({ data: { id, ok: true, result } } as MessageEvent);
    });
  }
}

describe("PersistentBrowserRuntime", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    FakeWorker.instances = [];
  });

  it("waits on the worker transaction id for proxied writes", async () => {
    vi.stubGlobal("Worker", FakeWorker);

    const runtime = new PersistentBrowserRuntime(
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
    expect(insert.transactionId).toMatch(/^worker-write-/);

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "insert")).toBe(true);
    });
    const insertMessage = worker.messages.find((message) => message.method === "insert");
    expect(insertMessage).toBeDefined();
    worker.respond(insertMessage!.id, { transactionId: "core-runtime-transaction" });

    const waitPromise = runtime.waitForTransaction(insert.transactionId, "local");

    await vi.waitFor(() => {
      expect(worker.messages.some((message) => message.method === "waitForTransaction")).toBe(true);
    });
    const waitMessage = worker.messages.find((message) => message.method === "waitForTransaction");
    expect(waitMessage?.args).toEqual(["core-runtime-transaction", "local"]);
    worker.respond(waitMessage!.id, undefined);

    await expect(waitPromise).resolves.toBeUndefined();
    await runtime.close();
  });
});

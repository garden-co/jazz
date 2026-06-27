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
}

describe("PersistentBrowserOpfsRuntime", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    FakeWorker.instances = [];
  });

  it("waits on the worker transaction id for proxied writes", async () => {
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

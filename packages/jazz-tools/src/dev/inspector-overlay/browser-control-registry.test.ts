import { afterEach, describe, expect, it, vi } from "vitest";
import type {
  BrowserInspectorControlEvent,
  BrowserInspectorControlRequest,
} from "../../runtime/native-runtime/browser-worker-protocol.js";
import {
  openAggregatedBrowserInspectorControlPort,
  registerBrowserInspectorControl,
} from "./browser-control-registry.js";

class TestPort {
  readonly close = vi.fn();
  readonly start = vi.fn();
  peer!: TestPort;
  private readonly listeners = new Set<(event: MessageEvent) => void>();

  addEventListener(type: string, listener: (event: MessageEvent) => void): void {
    if (type === "message") this.listeners.add(listener);
  }

  removeEventListener(type: string, listener: (event: MessageEvent) => void): void {
    if (type === "message") this.listeners.delete(listener);
  }

  postMessage(message: unknown): void {
    queueMicrotask(() => {
      for (const listener of this.peer.listeners) listener({ data: message } as MessageEvent);
    });
  }
}

class TestMessageChannel {
  readonly port1 = new TestPort();
  readonly port2 = new TestPort();

  constructor() {
    this.port1.peer = this.port2;
    this.port2.peer = this.port1;
  }
}

function nextMessage<T>(port: TestPort): Promise<T> {
  return new Promise((resolve) => {
    const listener = (event: MessageEvent<T>) => {
      port.removeEventListener("message", listener as (event: MessageEvent) => void);
      resolve(event.data);
    };
    port.addEventListener("message", listener as (event: MessageEvent) => void);
  });
}

const previousMessageChannel = globalThis.MessageChannel;
afterEach(() => {
  Object.defineProperty(globalThis, "MessageChannel", {
    configurable: true,
    value: previousMessageChannel,
  });
});

describe("openAggregatedBrowserInspectorControlPort", () => {
  it("closes the transferred attachment port when the routed host rejects it", async () => {
    Object.defineProperty(globalThis, "MessageChannel", {
      configurable: true,
      value: TestMessageChannel,
    });
    const downstream = new TestMessageChannel();
    downstream.port2.addEventListener("message", (event) => {
      const message = event.data as BrowserInspectorControlRequest;
      if (message.type === "close") return;
      const response: BrowserInspectorControlEvent =
        message.type === "list-contexts"
          ? {
              type: "contexts",
              id: message.id,
              contexts: [
                { key: "context", appId: "app", dbName: "db", schema: {}, workerRealmId: "worker" },
              ],
            }
          : { type: "result", id: message.id, error: "attach failed" };
      downstream.port2.postMessage(response);
    });
    const unregister = registerBrowserInspectorControl(
      async () => downstream.port1 as unknown as MessagePort,
    );
    try {
      const client = (await openAggregatedBrowserInspectorControlPort(async () => {
        throw new Error("fallback should not be used");
      })) as unknown as TestPort;
      const listed = nextMessage<BrowserInspectorControlEvent>(client);
      client.postMessage({ type: "list-contexts", id: 1 });
      await expect(listed).resolves.toMatchObject({ type: "contexts", id: 1 });

      const attachment = new TestMessageChannel().port1;
      const attached = nextMessage<BrowserInspectorControlEvent>(client);
      client.postMessage({
        type: "attach-context",
        id: 2,
        contextKey: "1:context",
        tabId: "tab",
        port: attachment,
      });
      await expect(attached).resolves.toMatchObject({
        type: "result",
        id: 2,
        error: "attach failed",
      });
      expect(attachment.close).toHaveBeenCalledOnce();
    } finally {
      unregister();
    }
  });
});

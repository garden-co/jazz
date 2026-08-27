import { describe, expect, it, vi } from "vitest";
import { MessagePortBrowserFollowerConnection } from "./browser-follower-connection.js";
import type {
  BrowserFollowerPortEvent,
  BrowserFollowerPortRequest,
} from "./browser-worker-protocol.js";

class TestPort {
  readonly close = vi.fn();
  readonly start = vi.fn();
  readonly sent: BrowserFollowerPortRequest[] = [];
  private readonly listeners = new Map<string, Set<(event: MessageEvent) => void>>();

  addEventListener(type: string, listener: (event: MessageEvent) => void): void {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: (event: MessageEvent) => void): void {
    this.listeners.get(type)?.delete(listener);
  }

  postMessage(message: BrowserFollowerPortRequest): void {
    this.sent.push(message);
  }

  emit(event: BrowserFollowerPortEvent): void {
    for (const listener of this.listeners.get("message") ?? []) {
      listener({ data: event } as MessageEvent);
    }
  }
}

describe("MessagePortBrowserFollowerConnection", () => {
  it("records a relayed transport error before follower disposal without treating it as a mutation rejection", async () => {
    const port = new TestPort();
    const transport = {
      recvWireFrames: () => [],
      sendWireFrame: () => undefined,
      tick: () => 0,
    };
    const runtime = {
      connectUpstreamPeer: vi.fn(() => transport),
      onPeerTransportWork: vi.fn(() => () => undefined),
      progressPeerTransport: vi.fn(async () => undefined),
      retirePeerTransport: vi.fn(async () => undefined),
      reportRemoteServerTransportError: vi.fn(),
      reportRemoteMutationError: vi.fn(),
    };
    const onFailure = vi.fn();
    const connection = new MessagePortBrowserFollowerConnection(
      runtime as never,
      port as unknown as MessagePort,
      {},
      null,
      {
        onAuthFailure: vi.fn(),
        onAuthRestored: vi.fn(),
        onFailure,
      },
    );

    const init = port.sent[0];
    if (!init || init.type !== "init") throw new Error("follower did not initialize");
    port.emit({ type: "result", id: init.id });
    await connection.ready();

    port.emit({ type: "transport-error", message: "Protocol: terminal upstream failure" });

    expect(runtime.reportRemoteServerTransportError).toHaveBeenCalledWith(
      expect.objectContaining({ message: "Protocol: terminal upstream failure" }),
    );
    expect(runtime.reportRemoteMutationError).not.toHaveBeenCalled();
    expect(onFailure).not.toHaveBeenCalled();
    expect(port.close).not.toHaveBeenCalled();

    connection.detachForReconnect();
  });
});

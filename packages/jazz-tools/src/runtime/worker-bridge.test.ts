import { describe, expect, it, vi } from "vitest";
import { MessagePortRuntimeBridge } from "./worker-bridge.js";
import type { Runtime } from "./client.js";

function testPort(): MessagePort & { sent: unknown[]; emit(message: unknown): void } {
  let listener: ((event: MessageEvent) => void) | null = null;
  const sent: unknown[] = [];
  return {
    sent,
    postMessage: vi.fn((message: unknown) => {
      sent.push(message);
    }),
    addEventListener: vi.fn((_type: string, next: (event: MessageEvent) => void) => {
      listener = next;
    }),
    start: vi.fn(),
    emit(message: unknown) {
      listener?.({ data: message } as MessageEvent);
    },
  } as unknown as MessagePort & { sent: unknown[]; emit(message: unknown): void };
}

function testRuntime() {
  const transport = {
    close: vi.fn(() => true),
    recvWireFrames: vi.fn(() => []),
    sendWireFrame: vi.fn(),
    tick: vi.fn(() => 0),
  };
  const runtime = {
    connectUpstreamPeer: vi.fn(() => transport),
  } as unknown as Runtime;
  return { runtime, transport };
}

describe("MessagePortRuntimeBridge", () => {
  it("forwards auth updates over the follower data port bridge", () => {
    const { runtime } = testRuntime();
    const port = testPort();

    const bridge = new MessagePortRuntimeBridge(port, runtime);
    bridge.init();
    bridge.updateAuth({ jwtToken: "jwt-refresh" });

    expect(port.postMessage).toHaveBeenCalledWith({
      type: "update-auth",
      jwtToken: "jwt-refresh",
    });
  });

  it("detaches for reconnect without shutting down the runtime sender", () => {
    const { runtime, transport } = testRuntime();
    const port = testPort();

    const bridge = new MessagePortRuntimeBridge(port, runtime);
    bridge.init();
    bridge.detachForReconnect();

    expect(transport.close).toHaveBeenCalledTimes(1);
    expect(port.postMessage).not.toHaveBeenCalledWith({ type: "close" });
  });

  it("registers auth failure callbacks on follower data port bridges", () => {
    const { runtime } = testRuntime();
    const port = testPort();
    const onAuthFailure = vi.fn();

    const bridge = new MessagePortRuntimeBridge(port, runtime);
    bridge.init();
    bridge.onAuthFailure(onAuthFailure);
    port.emit({ type: "auth-failure", reason: "unauthenticated" });

    expect(onAuthFailure).toHaveBeenCalledWith("unauthenticated");
  });
});

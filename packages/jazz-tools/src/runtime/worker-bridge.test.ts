import { describe, expect, it, vi } from "vitest";
import { MessagePortRuntimeBridge } from "./worker-bridge.js";
import type { Runtime } from "./client.js";

describe("MessagePortRuntimeBridge", () => {
  it("forwards auth updates over the follower data port bridge", () => {
    const handle = {
      detachForReconnect: vi.fn(),
      shutdown: vi.fn(),
      updateAuth: vi.fn(),
    };
    const runtime = {
      createMessagePortBridge: vi.fn(() => handle),
    } as unknown as Runtime;
    const port = {} as MessagePort;

    const bridge = new MessagePortRuntimeBridge(port, runtime);
    bridge.init();
    bridge.updateAuth({ jwtToken: "jwt-refresh" });

    expect(handle.updateAuth).toHaveBeenCalledWith("jwt-refresh");
  });

  it("detaches for reconnect without shutting down the runtime sender", () => {
    const handle = {
      detachForReconnect: vi.fn(),
      shutdown: vi.fn(),
      updateAuth: vi.fn(),
    };
    const runtime = {
      createMessagePortBridge: vi.fn(() => handle),
    } as unknown as Runtime;
    const port = {} as MessagePort;

    const bridge = new MessagePortRuntimeBridge(port, runtime);
    bridge.init();
    bridge.detachForReconnect();

    expect(handle.detachForReconnect).toHaveBeenCalledTimes(1);
    expect(handle.shutdown).not.toHaveBeenCalled();
  });
});

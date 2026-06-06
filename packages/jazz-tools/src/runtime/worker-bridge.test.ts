import { describe, expect, it, vi } from "vitest";
import { MessagePortRuntimeBridge } from "./worker-bridge.js";
import type { Runtime } from "./client.js";

describe("MessagePortRuntimeBridge", () => {
  it("forwards auth updates over the follower data port bridge", () => {
    const handle = {
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
});

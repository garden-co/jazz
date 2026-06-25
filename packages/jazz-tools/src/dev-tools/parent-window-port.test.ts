import { describe, it, expect, vi } from "vitest";
import { DEVTOOLS_BRIDGE_CHANNEL } from "./protocol.js";
import { createParentWindowBridgePort } from "./parent-window-port.js";

describe("createParentWindowBridgePort", () => {
  it("posts outbound messages to window.parent with the page origin", async () => {
    const parentPost = vi.fn();
    vi.stubGlobal("window", {
      parent: { postMessage: parentPost },
      location: { origin: "http://localhost:5173" },
      addEventListener: vi.fn(),
    } as unknown as Window);
    const port = await createParentWindowBridgePort();
    port.postMessage({ channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "request" });
    expect(parentPost).toHaveBeenCalledWith(
      { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "request" },
      "http://localhost:5173",
    );
  });

  it("delivers inbound messages only from window.parent on the bridge channel", async () => {
    let handler: (e: MessageEvent) => void = () => {};
    const parent = { postMessage: vi.fn() };
    vi.stubGlobal("window", {
      parent,
      location: { origin: "http://localhost:5173" },
      addEventListener: (t: string, cb: (e: MessageEvent) => void) => {
        if (t === "message") handler = cb;
      },
    } as unknown as Window);
    const port = await createParentWindowBridgePort();
    const received: unknown[] = [];
    port.onMessage.addListener((m) => received.push(m));
    handler({
      source: parent,
      data: { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "response" },
    } as unknown as MessageEvent);
    handler({
      source: {},
      data: { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "response" },
    } as unknown as MessageEvent);
    handler({
      source: parent,
      data: { channel: "other", kind: "response" },
    } as unknown as MessageEvent);
    expect(received).toEqual([{ channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "response" }]);
  });
});

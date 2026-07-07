import { describe, it, expect, vi } from "vitest";
import { DEVTOOLS_BRIDGE_CHANNEL } from "../../dev-tools/protocol.js";
import { createRelay } from "./relay.js";

const makeWindow = () => ({ postMessage: vi.fn() });
const opts = (top: unknown, iframe: unknown) => ({
  topWindow: top as Window,
  iframeWindow: iframe as Window,
  origin: "http://localhost:5173",
});

describe("inspector overlay relay", () => {
  it("re-injects an iframe request into the top window exactly once", () => {
    const top = makeWindow(),
      iframe = makeWindow();
    const relay = createRelay(opts(top, iframe));
    relay.handle({
      source: iframe,
      data: { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "request", requestId: "r1" },
    } as unknown as MessageEvent);
    expect(top.postMessage).toHaveBeenCalledTimes(1);
    expect(top.postMessage).toHaveBeenCalledWith(
      { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "request", requestId: "r1" },
      "*",
    );
    expect(iframe.postMessage).not.toHaveBeenCalled();
  });

  it("does not echo a re-injected request (no loop)", () => {
    const top = makeWindow(),
      iframe = makeWindow();
    const relay = createRelay(opts(top, iframe));
    const req = { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "request", requestId: "r1" };
    relay.handle({ source: iframe, data: req } as unknown as MessageEvent); // hop 1
    relay.handle({ source: top, data: req } as unknown as MessageEvent); // hop 2 (round-trip)
    expect(top.postMessage).toHaveBeenCalledTimes(1);
    expect(iframe.postMessage).not.toHaveBeenCalled();
  });

  it("does NOT re-inject a request from a foreign frame (NOTE: not the overlay+extension multi-peer case)", () => {
    const top = makeWindow(),
      iframe = makeWindow(),
      other = makeWindow();
    const relay = createRelay(opts(top, iframe));
    relay.handle({
      source: other,
      data: { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "request", requestId: "x" },
    } as unknown as MessageEvent);
    expect(top.postMessage).not.toHaveBeenCalled();
    expect(iframe.postMessage).not.toHaveBeenCalled();
  });

  it("forwards a runtime reply into the iframe and does not re-post to top", () => {
    const top = makeWindow(),
      iframe = makeWindow();
    const relay = createRelay(opts(top, iframe));
    relay.handle({
      source: top,
      data: { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "response", requestId: "r1", ok: true },
    } as unknown as MessageEvent);
    expect(iframe.postMessage).toHaveBeenCalledWith(
      { channel: DEVTOOLS_BRIDGE_CHANNEL, kind: "response", requestId: "r1", ok: true },
      "http://localhost:5173",
    );
    expect(top.postMessage).not.toHaveBeenCalled();
  });

  it("ignores non-bridge-channel messages", () => {
    const top = makeWindow(),
      iframe = makeWindow();
    const relay = createRelay(opts(top, iframe));
    relay.handle({
      source: iframe,
      data: { channel: "other", kind: "request" },
    } as unknown as MessageEvent);
    expect(top.postMessage).not.toHaveBeenCalled();
    expect(iframe.postMessage).not.toHaveBeenCalled();
  });
});

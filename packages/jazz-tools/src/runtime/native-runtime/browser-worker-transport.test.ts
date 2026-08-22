import { describe, expect, it, vi } from "vitest";
import { BrowserWorkerTransportPump } from "./browser-worker-transport.js";
import type { Transport } from "./native-runtime-adapter.js";

function runtime() {
  return {
    onPeerTransportWork: () => () => undefined,
  };
}

function transport(overrides: Partial<Transport> = {}): Transport {
  return {
    close: () => true,
    recvWireFrames: () => [],
    sendWireFrame: () => undefined,
    tick: () => 0,
    ...overrides,
  };
}

describe("BrowserWorkerTransportPump", () => {
  it("reports a rejected transport tick", async () => {
    const failure = new Error("tick failed");
    const onError = vi.fn();
    const pump = new BrowserWorkerTransportPump(
      runtime(),
      transport({ tick: () => Promise.reject(failure) }),
      () => undefined,
      onError,
    );

    await vi.waitFor(() => expect(onError).toHaveBeenCalledWith(failure));
    pump.close();
  });

  it("does not send frames when closed during a suspended tick", async () => {
    let release!: () => void;
    const sendFrames = vi.fn();
    const peer = transport({
      tick: () => new Promise<number>((resolve) => (release = () => resolve(1))),
      recvWireFrames: () => [Uint8Array.from([1, 7])],
    });
    const pump = new BrowserWorkerTransportPump(runtime(), peer, sendFrames, vi.fn());

    await Promise.resolve();
    pump.close();
    release();
    await Promise.resolve();
    await Promise.resolve();
    expect(sendFrames).not.toHaveBeenCalled();
  });
});

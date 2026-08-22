import { describe, expect, it, vi } from "vitest";
import { BrowserWorkerTransportPump } from "./browser-worker-transport.js";
import type { Transport } from "./native-runtime-adapter.js";

function runtime(peer: Transport) {
  let running: Promise<void> | null = null;
  return {
    onPeerTransportWork: () => () => undefined,
    progressPeerTransport: () =>
      (running ??= Promise.resolve(peer.tick()).then(() => undefined)).finally(() => {
        running = null;
      }),
    retirePeerTransport: async (transport: Transport) => {
      await running;
      transport.close();
    },
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
    const peer = transport({ tick: () => Promise.reject(failure) });
    const pump = new BrowserWorkerTransportPump(runtime(peer), peer, () => undefined, onError);

    await vi.waitFor(() => expect(onError).toHaveBeenCalledWith(failure));
    pump.close();
  });

  it("does not send frames when closed during a suspended tick", async () => {
    let release!: () => void;
    const sendFrames = vi.fn();
    const close = vi.fn(() => true);
    let reads = 0;
    const peer = transport({
      close,
      tick: () => new Promise<number>((resolve) => (release = () => resolve(1))),
      recvWireFrames: () => (++reads === 1 ? [] : [Uint8Array.from([1, 7])]),
    });
    const pump = new BrowserWorkerTransportPump(runtime(peer), peer, sendFrames, vi.fn());

    await Promise.resolve();
    pump.close();
    expect(close).not.toHaveBeenCalled();
    release();
    await Promise.resolve();
    await Promise.resolve();
    expect(sendFrames).not.toHaveBeenCalled();
    expect(close).toHaveBeenCalledOnce();
  });

  it("drains a durability receipt while the evaluator tick remains suspended", async () => {
    const receipt = Uint8Array.from([4, 2]);
    const sendFrames = vi.fn();
    const peer = transport({ recvWireFrames: () => [receipt] });
    const pump = new BrowserWorkerTransportPump(runtime(peer), peer, sendFrames, vi.fn());

    pump.drainOutboundFrames();

    expect(sendFrames).toHaveBeenCalledWith([receipt]);
    pump.close();
  });

  it("publishes an already-produced peer request before awaiting suspended evaluation", async () => {
    let release!: () => void;
    const request = Uint8Array.from([9, 3]);
    const sendFrames = vi.fn();
    let drained = false;
    const peer = transport({
      tick: () => new Promise<number>((resolve) => (release = () => resolve(0))),
      recvWireFrames: () => {
        if (drained) return [];
        drained = true;
        return [request];
      },
    });
    const pump = new BrowserWorkerTransportPump(runtime(peer), peer, sendFrames, vi.fn());

    await vi.waitFor(() => expect(sendFrames).toHaveBeenCalledWith([request]));
    release();
    await Promise.resolve();
    pump.close();
  });

  it("publishes a peer request produced by an evaluator that remains suspended", async () => {
    let release!: () => void;
    let notifyOutbound: () => void = () => undefined;
    const request = Uint8Array.from([6, 4]);
    const outbound: Uint8Array[] = [];
    const sendFrames = vi.fn();
    const peer = transport({
      setOutboundScheduler: (callback) => {
        notifyOutbound = callback;
      },
      tick: () => {
        outbound.push(request);
        notifyOutbound();
        return new Promise<number>((resolve) => (release = () => resolve(0)));
      },
      recvWireFrames: () => outbound.splice(0),
    });
    const pump = new BrowserWorkerTransportPump(runtime(peer), peer, sendFrames, vi.fn());

    await vi.waitFor(() => expect(sendFrames).toHaveBeenCalledWith([request]));
    release();
    await Promise.resolve();
    pump.close();
  });
});

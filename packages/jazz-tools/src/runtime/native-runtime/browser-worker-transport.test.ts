import { describe, expect, it, vi } from "vitest";
import {
  BrowserWorkerTransportPump,
  MAX_AUXILIARY_BYTES_PER_PORT_MESSAGE,
  MAX_AUXILIARY_FRAMES_PER_PORT_MESSAGE,
} from "./browser-worker-transport.js";
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

  it("routes worker chunk responses through the auxiliary lane", async () => {
    const chunkResponse = Uint8Array.from([7, 7]);
    const semanticFrame = Uint8Array.from([8, 8]);
    const sendWireFrame = vi.fn();
    const routeAuxiliaryWireFrame = vi.fn(async (frame: Uint8Array) => {
      if (frame[0] === 7) return undefined;
      return frame;
    });
    const peer = transport({
      routeAuxiliaryWireFrame,
      sendWireFrame,
    });
    const pump = new BrowserWorkerTransportPump(runtime(peer), peer, () => undefined, vi.fn());

    // The chunk response is consumed by the dedicated auxiliary lane; only
    // the following canonical frame reaches the normal transport queue.
    pump.receive([chunkResponse, semanticFrame]);

    await vi.waitFor(() => expect(routeAuxiliaryWireFrame).toHaveBeenCalledTimes(2));
    expect(sendWireFrame).toHaveBeenCalledWith(semanticFrame);
    pump.close();
  });

  it("forwards only opt-in bounded auxiliary relay diagnostics", () => {
    const setAuxiliaryTraceEnabled = vi.fn();
    const onTrace = vi.fn();
    const trace = {
      event: "outbound-request",
      role: "upstream" as const,
      connection: "7",
      requestId: "3",
      remainingHops: 6,
      objectHash: "9d1c2b3a4e5f",
      locatorFingerprint: "7a6b5c4d3e2f",
    };
    const peer = transport({
      recvAuxiliaryWireFrames: () => [Uint8Array.from([9])],
      takeAuxiliaryTrace: () => [trace],
      setAuxiliaryTraceEnabled,
    });
    const pump = new BrowserWorkerTransportPump(
      runtime(peer),
      peer,
      () => undefined,
      vi.fn(),
      onTrace,
    );

    pump.drainOutboundFrames();

    expect(setAuxiliaryTraceEnabled).toHaveBeenCalledWith(true);
    expect(onTrace).toHaveBeenCalledWith([trace]);
    pump.close();
    expect(setAuxiliaryTraceEnabled).toHaveBeenLastCalledWith(false);
  });

  it("preserves FIFO across MessagePort batches while auxiliary routing is pending", async () => {
    const auxiliary = Uint8Array.from([7, 7]);
    const semantic = Uint8Array.from([8, 8]);
    const sendWireFrame = vi.fn();
    let releaseAuxiliaryRoute!: (value: undefined) => void;
    const routeAuxiliaryWireFrame = vi.fn((frame: Uint8Array) => {
      if (frame[0] === 7) {
        return new Promise<undefined>((resolve) => (releaseAuxiliaryRoute = resolve));
      }
      return Promise.resolve(frame);
    });
    const peer = transport({ routeAuxiliaryWireFrame, sendWireFrame });
    const pump = new BrowserWorkerTransportPump(runtime(peer), peer, () => undefined, vi.fn());

    // A later port delivery must not overtake an earlier chunk response while
    // the latter is still resolving through the dedicated auxiliary lane.
    pump.receive([auxiliary]);
    await vi.waitFor(() => expect(routeAuxiliaryWireFrame).toHaveBeenCalledWith(auxiliary));
    pump.receive([semantic]);
    await Promise.resolve();
    expect(sendWireFrame).not.toHaveBeenCalled();

    releaseAuxiliaryRoute(undefined);
    await vi.waitFor(() => expect(sendWireFrame).toHaveBeenCalledWith(semantic));
    expect(routeAuxiliaryWireFrame.mock.calls.map(([frame]) => frame)).toEqual([
      auxiliary,
      semantic,
    ]);
    pump.close();
  });

  it("publishes an auxiliary chunk request while semantic evaluation is suspended", async () => {
    const request = Uint8Array.from([9, 4]);
    const sendFrames = vi.fn();
    let resolveAuxiliaryReady!: () => void;
    let auxiliaryFrames: Uint8Array[] = [];
    const peer = transport({
      tick: () => new Promise<number>(() => undefined),
      recvAuxiliaryWireFrames: () => auxiliaryFrames.splice(0),
      auxiliaryOutboundReady: () =>
        new Promise<void>((resolve) => (resolveAuxiliaryReady = resolve)),
    });
    const pump = new BrowserWorkerTransportPump(runtime(peer), peer, sendFrames, vi.fn());

    auxiliaryFrames = [request];
    resolveAuxiliaryReady();

    await vi.waitFor(() => expect(sendFrames).toHaveBeenCalledWith([request]));
    pump.close();
  });

  it("bounds auxiliary MessagePort batches, drains their FIFO tail, and yields to semantics", async () => {
    vi.useFakeTimers();
    try {
      const sent: string[] = [];
      const batches: Uint8Array[][] = [];
      const frames = Array.from({ length: 10 }, (_, index) => {
        const frame = new Uint8Array(400_000);
        frame[0] = index + 1;
        return frame;
      });
      let auxiliaryReady = false;
      let releaseAuxiliaryReady!: () => void;
      const recvAuxiliaryWireFrames = vi.fn((maxFrames = Infinity, maxBytes = Infinity) => {
        if (!auxiliaryReady) return [];
        const batch: Uint8Array[] = [];
        let bytes = 0;
        while (frames.length > 0 && batch.length < maxFrames) {
          const frame = frames[0]!;
          if (batch.length > 0 && bytes + frame.byteLength > maxBytes) break;
          frames.shift();
          batch.push(frame);
          bytes += frame.byteLength;
        }
        return batch;
      });
      const peer = transport({
        recvAuxiliaryWireFrames,
        auxiliaryOutboundReady: () =>
          auxiliaryReady
            ? Promise.resolve()
            : new Promise<void>((resolve) => (releaseAuxiliaryReady = resolve)),
        tick: () => new Promise<number>(() => undefined),
      });
      const pump = new BrowserWorkerTransportPump(
        {
          onPeerTransportWork: () => () => undefined,
          progressPeerTransport: () => new Promise<void>(() => undefined),
          retirePeerTransport: async () => undefined,
        },
        peer,
        (batch) => {
          batches.push(batch);
          sent.push(`aux:${batch.map((frame) => frame[0]).join(",")}`);
        },
        vi.fn(),
      );

      // Let the constructor's semantic pump enter its suspended tick before
      // making auxiliary traffic available; the watch path below is then the
      // only auxiliary drainer and its task yield is observable.
      await Promise.resolve();
      await Promise.resolve();
      auxiliaryReady = true;
      releaseAuxiliaryReady();
      await Promise.resolve();
      await Promise.resolve();
      expect(batches).toHaveLength(1);
      expect(recvAuxiliaryWireFrames).toHaveBeenLastCalledWith(
        MAX_AUXILIARY_FRAMES_PER_PORT_MESSAGE,
        MAX_AUXILIARY_BYTES_PER_PORT_MESSAGE,
      );

      const sendWireFrame = vi.fn((frame: Uint8Array) => sent.push(`semantic:${frame[0]}`));
      peer.sendWireFrame = sendWireFrame;
      pump.receive([Uint8Array.from([99])]);
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
      expect(sent).toEqual(["aux:1,2", "semantic:99"]);

      while (frames.length > 0) await vi.advanceTimersToNextTimerAsync();
      expect(batches.flat().map((frame) => frame[0])).toEqual(
        Array.from({ length: 10 }, (_, index) => index + 1),
      );
      for (const batch of batches) {
        expect(batch).toHaveLength(2);
        expect(batch.reduce((total, frame) => total + frame.byteLength, 0)).toBeLessThanOrEqual(
          MAX_AUXILIARY_BYTES_PER_PORT_MESSAGE,
        );
      }
      expect(sent[2]).toBe("aux:3,4");
      pump.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it("stops a scheduled bounded auxiliary continuation when closed", async () => {
    vi.useFakeTimers();
    try {
      const sent = vi.fn();
      const frames = [Uint8Array.from([1]), Uint8Array.from([2])];
      let auxiliaryReady = false;
      let releaseAuxiliaryReady!: () => void;
      const peer = transport({
        recvAuxiliaryWireFrames: () => (auxiliaryReady ? frames.splice(0, 1) : []),
        auxiliaryOutboundReady: () =>
          auxiliaryReady
            ? Promise.resolve()
            : new Promise<void>((resolve) => (releaseAuxiliaryReady = resolve)),
        tick: () => new Promise<number>(() => undefined),
      });
      const pump = new BrowserWorkerTransportPump(
        {
          onPeerTransportWork: () => () => undefined,
          progressPeerTransport: () => new Promise<void>(() => undefined),
          retirePeerTransport: async () => undefined,
        },
        peer,
        sent,
        vi.fn(),
      );

      await Promise.resolve();
      await Promise.resolve();
      auxiliaryReady = true;
      releaseAuxiliaryReady();
      await Promise.resolve();
      await Promise.resolve();
      expect(sent).toHaveBeenCalledTimes(1);
      pump.close();
      await vi.advanceTimersByTimeAsync(0);
      expect(sent).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
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

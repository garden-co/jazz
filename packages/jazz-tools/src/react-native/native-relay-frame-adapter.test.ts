import { describe, expect, it, vi } from "vitest";
import {
  ReactNativeRelayFrameAdapter,
  decodeNativeRelayResponse,
  encodeNativeRelayCommand,
  type NativeRelayCapability,
} from "./native-relay-frame-adapter.js";

const capability = Uint8Array.from({ length: 32 }, (_, index) => index) as NativeRelayCapability;
const base64 = (bytes: number[]) => btoa(String.fromCharCode(...bytes));

describe("ReactNativeRelayFrameAdapter", () => {
  it("uses only canonical peer frames and closes its admitted aliases", async () => {
    const work = new Set<() => void>();
    const runtime = {
      onPeerTransportWork: vi.fn((listener: () => void) => {
        work.add(listener);
        return () => work.delete(listener);
      }),
      notifyPeerTransportActivity: vi.fn(),
      progressPeerTransport: vi.fn(async () => undefined),
      retirePeerTransport: vi.fn(async () => undefined),
    };
    const received: Uint8Array[] = [];
    let outboundPending = true;
    const transport = {
      close: () => true,
      recvWireFrames: vi.fn(() => {
        if (!outboundPending) return [];
        outboundPending = false;
        return [Uint8Array.of(0xaa)];
      }),
      sendWireFrame: vi.fn((frame: Uint8Array) => received.push(frame)),
      setOutboundScheduler: vi.fn(),
      clearOutboundScheduler: vi.fn(),
      tick: () => 0,
    };
    const commands: number[] = [];
    const executor = {
      execute: vi.fn(async (request: string) => {
        const bytes = Uint8Array.from(atob(request), (character) => character.charCodeAt(0));
        commands.push(bytes[0]);
        // postcard tags: Open, Attach, SendClientFrame, Pump, Receive, CloseClient, CloseRelay
        return bytes[0] === 1
          ? base64([1, 9])
          : bytes[0] === 2
            ? base64([2, 7])
            : bytes[0] === 7
              ? base64([5, 1, 1, 0xbb])
              : bytes[0] === 3 || bytes[0] === 4
                ? base64([3, 1])
                : base64([4]);
      }),
    };
    const adapter = new ReactNativeRelayFrameAdapter(
      runtime,
      transport,
      executor,
      capability,
      vi.fn(),
    );

    await adapter.start();
    await Promise.resolve();
    await adapter.flush();
    expect(received).toContainEqual(Uint8Array.of(0xbb));
    expect(commands).toEqual(expect.arrayContaining([1, 2, 6, 5, 7]));
    expect(runtime.notifyPeerTransportActivity).toHaveBeenCalled();

    await adapter.shutdown();
    expect(commands).toEqual(expect.arrayContaining([3, 4]));
    expect(runtime.retirePeerTransport).toHaveBeenCalledWith(transport);
  });

  it("rejects malformed capabilities and incompatible response shapes before transport work", () => {
    expect(() =>
      encodeNativeRelayCommand({
        type: "open",
        capability: Uint8Array.of(1) as NativeRelayCapability,
      }),
    ).toThrow(/exactly 32/);
    expect(() => decodeNativeRelayResponse(base64([9]))).toThrow(/unknown response tag/);
  });

  it("preserves a backpressured frame for a later reconnect/work notification", async () => {
    let listener: (() => void) | undefined;
    const onError = vi.fn();
    let outbound = true;
    let rejectOnce = true;
    const runtime = {
      onPeerTransportWork: vi.fn((next: () => void) => {
        listener = next;
        return () => {
          listener = undefined;
        };
      }),
      progressPeerTransport: vi.fn(async () => undefined),
      retirePeerTransport: vi.fn(async () => undefined),
    };
    const transport = {
      close: () => true,
      recvWireFrames: () => {
        if (!outbound) return [];
        outbound = false;
        return [Uint8Array.of(0xac)];
      },
      sendWireFrame: vi.fn(),
      tick: () => 0,
    };
    const executor = {
      execute: vi.fn(async (request: string) => {
        const tag = atob(request).charCodeAt(0);
        if (tag === 1) return base64([1, 9]);
        if (tag === 2) return base64([2, 7]);
        if (tag === 6 && rejectOnce) {
          rejectOnce = false;
          throw new Error("Jazz native relay is backpressured");
        }
        if (tag === 7) return base64([5, 0]);
        return base64([4]);
      }),
    };
    const adapter = new ReactNativeRelayFrameAdapter(
      runtime,
      transport,
      executor,
      capability,
      onError,
    );
    await adapter.start();
    await adapter.flush();
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({
        message: expect.stringMatching(/backpressured/),
      }),
    );

    listener?.();
    await adapter.flush();
    const sent = executor.execute.mock.calls.filter(
      ([request]) => atob(request).charCodeAt(0) === 6,
    );
    expect(sent).toHaveLength(2);
    await adapter.shutdown();
  });

  it("closes a relay opened while shutdown races startup", async () => {
    let releaseOpen!: () => void;
    const openReady = new Promise<void>((resolve) => {
      releaseOpen = resolve;
    });
    const runtime = {
      onPeerTransportWork: () => () => undefined,
      progressPeerTransport: async () => undefined,
      retirePeerTransport: vi.fn(async () => undefined),
    };
    const transport = {
      close: () => true,
      recvWireFrames: () => [],
      sendWireFrame: vi.fn(),
      tick: () => 0,
    };
    const tags: number[] = [];
    const executor = {
      execute: vi.fn(async (request: string) => {
        const tag = atob(request).charCodeAt(0);
        tags.push(tag);
        if (tag === 1) {
          await openReady;
          return base64([1, 9]);
        }
        if (tag === 4) return base64([3, 1]);
        throw new Error(`unexpected command ${tag}`);
      }),
    };
    const adapter = new ReactNativeRelayFrameAdapter(
      runtime,
      transport,
      executor,
      capability,
      vi.fn(),
    );

    const start = adapter.start();
    const shutdown = adapter.shutdown();
    releaseOpen();
    await expect(start).resolves.toBeUndefined();
    await shutdown;
    expect(tags).toEqual([1, 4]);
    expect(runtime.retirePeerTransport).toHaveBeenCalledOnce();
  });

  it("closes an opened relay when attach has a wrong but valid response", async () => {
    const runtime = {
      onPeerTransportWork: () => () => undefined,
      progressPeerTransport: async () => undefined,
      retirePeerTransport: async () => undefined,
    };
    const transport = {
      close: () => true,
      recvWireFrames: () => [],
      sendWireFrame: vi.fn(),
      tick: () => 0,
    };
    const tags: number[] = [];
    const executor = {
      execute: vi.fn(async (request: string) => {
        const tag = atob(request).charCodeAt(0);
        tags.push(tag);
        return tag === 1 ? base64([1, 9]) : tag === 2 ? base64([4]) : base64([3, 1]);
      }),
    };
    const adapter = new ReactNativeRelayFrameAdapter(
      runtime,
      transport,
      executor,
      capability,
      vi.fn(),
    );

    await expect(adapter.start()).rejects.toThrow(/did not attach/);
    await adapter.shutdown();
    expect(tags).toEqual([1, 2, 4]);
  });

  it("redelivers native-drained frames in FIFO order after local delivery failure", async () => {
    let listener: (() => void) | undefined;
    let receiveCount = 0;
    let allowDelivery = false;
    const delivered: Uint8Array[] = [];
    const runtime = {
      onPeerTransportWork: (next: () => void) => {
        listener = next;
        return () => {
          listener = undefined;
        };
      },
      notifyPeerTransportActivity: vi.fn(),
      progressPeerTransport: async () => undefined,
      retirePeerTransport: async () => undefined,
    };
    const transport = {
      close: () => true,
      recvWireFrames: () => [],
      sendWireFrame: (frame: Uint8Array) => {
        if (!allowDelivery) {
          throw new Error("local transport backpressured");
        }
        delivered.push(frame);
      },
      tick: () => 0,
    };
    const onError = vi.fn();
    const executor = {
      execute: vi.fn(async (request: string) => {
        const tag = atob(request).charCodeAt(0);
        if (tag === 1) return base64([1, 9]);
        if (tag === 2) return base64([2, 7]);
        if (tag === 7) return base64(receiveCount++ === 0 ? [5, 2, 1, 0xbb, 1, 0xcc] : [5, 0]);
        if (tag === 3 || tag === 4) return base64([3, 1]);
        return base64([4]);
      }),
    };
    const adapter = new ReactNativeRelayFrameAdapter(
      runtime,
      transport,
      executor,
      capability,
      onError,
    );

    await adapter.start();
    await adapter.flush();
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({
        message: expect.stringMatching(/local transport/),
      }),
    );
    expect(delivered).toEqual([]);
    allowDelivery = true;
    listener?.();
    await adapter.flush();
    expect(delivered).toEqual([Uint8Array.of(0xbb), Uint8Array.of(0xcc)]);
    await adapter.shutdown();
  });
});

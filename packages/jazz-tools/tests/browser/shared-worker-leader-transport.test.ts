import { describe, expect, it } from "vitest";
import { MessagePortRuntimeTransport } from "../../src/runtime/shared-worker-leader/message-port-runtime-transport.js";
import type { Runtime } from "../../src/runtime/client.js";

interface StubCalls {
  applied: Uint8Array[];
  ticks: number;
}

function makeStubRuntime(calls: StubCalls): Runtime {
  return {
    installFollowerOutboxSender() {},
    setFollowerOutboxForwarder() {},
    replayFollowerServerEdge() {},
    applyIncomingFollowerPayload(payload: Uint8Array) {
      calls.applied.push(payload);
    },
    batchedTick() {
      calls.ticks += 1;
    },
  } as unknown as Runtime;
}

function flush(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 50));
}

describe("MessagePortRuntimeTransport", () => {
  it("ticks the runtime once after applying a leader-sync batch", async () => {
    const calls: StubCalls = { applied: [], ticks: 0 };
    const channel = new MessageChannel();
    const transport = new MessagePortRuntimeTransport({
      port: channel.port1,
      runtime: makeStubRuntime(calls),
    });
    transport.start();

    channel.port2.postMessage({
      type: "leader-sync",
      payload: [new Uint8Array([1, 2, 3]), new Uint8Array([4, 5])],
    });
    await flush();

    expect(calls.applied.length).toBe(2);
    // Without the tick, applied sync messages stay parked — queries never settle.
    expect(calls.ticks).toBe(1);

    transport.stop();
    channel.port2.close();
  });

  it("does not tick when a leader-sync batch carries no binary payloads", async () => {
    const calls: StubCalls = { applied: [], ticks: 0 };
    const channel = new MessageChannel();
    const transport = new MessagePortRuntimeTransport({
      port: channel.port1,
      runtime: makeStubRuntime(calls),
    });
    transport.start();

    channel.port2.postMessage({ type: "leader-sync", payload: [] });
    await flush();

    expect(calls.applied.length).toBe(0);
    expect(calls.ticks).toBe(0);

    transport.stop();
    channel.port2.close();
  });
});

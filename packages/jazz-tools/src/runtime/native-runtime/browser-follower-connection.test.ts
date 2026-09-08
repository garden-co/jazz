import { describe, expect, it, vi } from "vitest";
import { MessagePortBrowserFollowerConnection } from "./browser-follower-connection.js";
import {
  BrowserWorkerUnresponsiveError,
  serializeBrowserRelayError,
  type BrowserFollowerPortEvent,
  type BrowserFollowerPortRequest,
} from "./browser-worker-protocol.js";

class TestPort {
  readonly close = vi.fn();
  readonly start = vi.fn();
  readonly sent: BrowserFollowerPortRequest[] = [];
  onPostMessage: ((message: BrowserFollowerPortRequest) => void) | null = null;
  private readonly listeners = new Map<string, Set<(event: MessageEvent) => void>>();

  addEventListener(type: string, listener: (event: MessageEvent) => void): void {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: (event: MessageEvent) => void): void {
    this.listeners.get(type)?.delete(listener);
  }

  postMessage(message: BrowserFollowerPortRequest): void {
    this.sent.push(message);
    this.onPostMessage?.(message);
  }

  emit(event: BrowserFollowerPortEvent): void {
    for (const listener of this.listeners.get("message") ?? []) {
      listener({ data: event } as MessageEvent);
    }
  }
}

async function createWatchdogFollower(port = new TestPort()) {
  const transport = {
    recvWireFrames: () => [],
    sendWireFrame: () => undefined,
    tick: () => 0,
  };
  const runtime = {
    connectUpstreamPeer: vi.fn(() => transport),
    onPeerTransportWork: vi.fn(() => () => undefined),
    progressPeerTransport: vi.fn(async () => undefined),
    retirePeerTransport: vi.fn(async () => undefined),
    reportRemoteServerTransportError: vi.fn(),
    flushLocalSettlements: vi.fn(async () => undefined),
  };
  const onFailure = vi.fn();
  const connection = new MessagePortBrowserFollowerConnection(
    runtime as never,
    port as unknown as MessagePort,
    {},
    null,
    { onAuthFailure: vi.fn(), onAuthRestored: vi.fn(), onFailure },
  );
  const init = port.sent[port.sent.length - 1];
  if (!init || init.type !== "init") throw new Error("follower did not initialize");
  port.emit({ type: "result", id: init.id });
  await connection.ready();
  return { connection, port, runtime, onFailure };
}

describe("MessagePortBrowserFollowerConnection", () => {
  it.each([false, true])(
    "preserves an asynchronous peer installation across early frames (closed=%s)",
    async (closeBeforeConnected) => {
      const port = new TestPort();
      const transport = {
        recvWireFrames: () => [],
        sendWireFrame: vi.fn(),
        tick: () => 0,
      };
      let finishConnecting!: (value: typeof transport) => void;
      const pendingTransport = new Promise<typeof transport>((resolve) => {
        finishConnecting = resolve;
      });
      const runtime = {
        connectUpstreamPeer: vi.fn(() => pendingTransport),
        onPeerTransportWork: vi.fn(() => () => undefined),
        progressPeerTransport: vi.fn(async () => undefined),
        retirePeerTransport: vi.fn(async () => undefined),
        reportRemoteServerTransportError: vi.fn(),
      };
      const connection = new MessagePortBrowserFollowerConnection(
        runtime as never,
        port as unknown as MessagePort,
        {},
        null,
        { onAuthFailure: vi.fn(), onAuthRestored: vi.fn(), onFailure: vi.fn() },
      );
      const init = port.sent[0];
      if (!init || init.type !== "init") throw new Error("follower did not initialize");
      port.emit({ type: "result", id: init.id });
      port.emit({ type: "frames", frames: [Uint8Array.of(1)] });
      port.emit({ type: "frames", frames: [Uint8Array.of(2)] });
      expect(transport.sendWireFrame).not.toHaveBeenCalled();

      if (closeBeforeConnected) connection.detachForReconnect();
      finishConnecting(transport);
      if (closeBeforeConnected) {
        await expect(connection.ready()).rejects.toThrow("closed");
        expect(runtime.retirePeerTransport).toHaveBeenCalledExactlyOnceWith(transport);
        expect(runtime.onPeerTransportWork).not.toHaveBeenCalled();
        expect(transport.sendWireFrame).not.toHaveBeenCalled();
      } else {
        await connection.ready();
        await vi.waitFor(() => expect(transport.sendWireFrame).toHaveBeenCalledTimes(2));
        expect(transport.sendWireFrame.mock.calls).toEqual([
          [Uint8Array.of(1)],
          [Uint8Array.of(2)],
        ]);
        connection.detachForReconnect();
      }
    },
  );

  it("rejects a pending control operation when the established worker channel stays silent", async () => {
    vi.useFakeTimers();
    let connection: MessagePortBrowserFollowerConnection | undefined;
    try {
      const port = new TestPort();
      const transport = {
        recvWireFrames: () => [],
        sendWireFrame: () => undefined,
        tick: () => 0,
      };
      const runtime = {
        connectUpstreamPeer: vi.fn(() => transport),
        onPeerTransportWork: vi.fn(() => () => undefined),
        progressPeerTransport: vi.fn(async () => undefined),
        retirePeerTransport: vi.fn(async () => undefined),
        reportRemoteServerTransportError: vi.fn(),
      };
      const onFailure = vi.fn();
      connection = new MessagePortBrowserFollowerConnection(
        runtime as never,
        port as unknown as MessagePort,
        {},
        null,
        { onAuthFailure: vi.fn(), onAuthRestored: vi.fn(), onFailure },
      );
      const init = port.sent[0];
      if (!init || init.type !== "init") throw new Error("follower did not initialize");
      port.emit({ type: "result", id: init.id });
      await connection.ready();

      const outcome: {
        status: "pending" | "resolved" | "rejected";
        error?: unknown;
      } = { status: "pending" };
      void connection.waitForServerConnection().then(
        () => {
          outcome.status = "resolved";
        },
        (error: unknown) => {
          outcome.status = "rejected";
          outcome.error = error;
        },
      );
      await vi.advanceTimersByTimeAsync(0);
      expect(port.sent).toContainEqual({
        type: "wait-server",
        id: expect.any(Number),
      });

      // The channel supplies neither a result nor a messageerror. Allow the
      // pending-only probe interval and its full response window to elapse.
      await vi.advanceTimersByTimeAsync(30_000);
      await vi.advanceTimersByTimeAsync(29_999);
      expect(outcome.status).toBe("pending");
      expect(onFailure).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1);
      expect(outcome.status).toBe("rejected");
      expect(outcome.error).toBeInstanceOf(Error);
      expect(onFailure).toHaveBeenCalledExactlyOnceWith(outcome.error);
    } finally {
      // Release an unanswered baseline RPC only after observing its outcome.
      // Shutdown would send another unanswered request on this silent port.
      connection?.detachForReconnect();
      vi.useRealTimers();
    }
  });

  it.each(["wait-server", "flush-pending-writes"] as const)(
    "keeps a long %s operation alive with pongs and stops probing once idle",
    async (operation) => {
      vi.useFakeTimers();
      const { connection, port, onFailure } = await createWatchdogFollower();
      try {
        await vi.advanceTimersByTimeAsync(120_000);
        expect(port.sent.filter((message) => message.type === "runtime-probe")).toEqual([]);
        port.onPostMessage = (message) => {
          if (message.type === "runtime-probe") port.emit({ ...message, type: "runtime-pong" });
          if (message.type === "flush-local") port.emit({ type: "result", id: message.id });
        };
        const settled = vi.fn();
        const wait = (
          operation === "wait-server"
            ? connection.waitForServerConnection()
            : connection.waitForPendingWrites()
        ).then(settled);
        await vi.advanceTimersByTimeAsync(180_000);
        expect(settled).not.toHaveBeenCalled();
        expect(onFailure).not.toHaveBeenCalled();
        expect(port.sent.filter((message) => message.type === "runtime-probe")).toHaveLength(6);
        const request = port.sent.find((message) => message.type === operation);
        if (!request || !("id" in request) || request.id === undefined)
          throw new Error("missing pending operation");
        port.emit({ type: "result", id: request.id });
        await wait;
        expect(settled).toHaveBeenCalledOnce();
        await vi.advanceTimersByTimeAsync(120_000);
        expect(port.sent.filter((message) => message.type === "runtime-probe")).toHaveLength(6);
      } finally {
        connection.detachForReconnect();
        vi.useRealTimers();
      }
    },
  );

  it("rejects all pending controls once and fences a prior connection's pong", async () => {
    vi.useFakeTimers();
    const first = await createWatchdogFollower();
    let successorConnection: MessagePortBrowserFollowerConnection | undefined;
    try {
      const firstWait = first.connection.waitForServerConnection().catch((error) => error);
      await vi.advanceTimersByTimeAsync(30_000);
      const oldProbe = first.port.sent.find((message) => message.type === "runtime-probe")!;
      first.connection.detachForReconnect();
      await firstWait;
      const successor = await createWatchdogFollower(first.port);
      successorConnection = successor.connection;
      const failed = vi.fn();
      const firstPending = successor.connection.waitForServerConnection().catch(failed);
      const secondPending = successor.connection.disconnect().catch(failed);
      await vi.advanceTimersByTimeAsync(30_000);
      first.port.emit({ ...oldProbe, type: "runtime-pong" });
      await vi.advanceTimersByTimeAsync(30_000);
      await Promise.all([firstPending, secondPending]);
      expect(failed).toHaveBeenCalledTimes(2);
      const error = failed.mock.calls[0]![0];
      expect(error).toBeInstanceOf(BrowserWorkerUnresponsiveError);
      expect(error.message).toMatch(/outcomes are unknown.*not retried/);
      expect(failed.mock.calls[1]![0]).toBe(error);
      expect(successor.onFailure).toHaveBeenCalledExactlyOnceWith(error);
      expect(successor.runtime.retirePeerTransport).toHaveBeenCalledOnce();
      expect(first.port.close).toHaveBeenCalledTimes(2);
      for (const message of first.port.sent) {
        if (message.type === "runtime-probe") first.port.emit({ ...message, type: "runtime-pong" });
        if ("id" in message && message.id !== undefined)
          first.port.emit({ type: "result", id: message.id });
      }
      await vi.advanceTimersByTimeAsync(120_000);
      expect(failed).toHaveBeenCalledTimes(2);
      expect(successor.onFailure).toHaveBeenCalledOnce();
      expect(first.port.sent.filter((message) => message.type === "wait-server")).toHaveLength(2);
      expect(first.port.sent.filter((message) => message.type === "disconnect")).toHaveLength(1);
    } finally {
      first.connection.detachForReconnect();
      successorConnection?.detachForReconnect();
      vi.useRealTimers();
    }
  });

  it("gives a delayed expiry a fresh identified probe and a full reply grace", async () => {
    vi.useFakeTimers();
    const { connection, port, onFailure } = await createWatchdogFollower();
    try {
      const rejected = vi.fn();
      const wait = connection.waitForServerConnection().catch(rejected);
      await vi.advanceTimersByTimeAsync(30_000);
      const oldProbe = port.sent.find((message) => message.type === "runtime-probe")!;
      // Wall time passes without timer or message dispatch, as during sleep.
      vi.setSystemTime(Date.now() + 300_000);
      await vi.advanceTimersByTimeAsync(30_000);
      expect(onFailure).not.toHaveBeenCalled();
      const probes = port.sent.filter((message) => message.type === "runtime-probe");
      expect(probes).toHaveLength(2);
      expect(probes[1]!.nonce).not.toBe(oldProbe.nonce);
      port.emit({ ...oldProbe, type: "runtime-pong" });
      await vi.advanceTimersByTimeAsync(29_999);
      expect(rejected).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      await wait;
      expect(rejected).toHaveBeenCalledOnce();
      expect(onFailure).toHaveBeenCalledOnce();
    } finally {
      connection.detachForReconnect();
      vi.useRealTimers();
    }
  });

  it("starts a fresh reply grace when a hidden page becomes visible", async () => {
    vi.useFakeTimers();
    const page = Object.assign(new EventTarget(), { visibilityState: "hidden" });
    vi.stubGlobal("document", page);
    const { connection, port, onFailure } = await createWatchdogFollower();
    try {
      const rejected = vi.fn();
      const wait = connection.waitForServerConnection().catch(rejected);
      await vi.advanceTimersByTimeAsync(30_000);
      const oldProbe = port.sent.find((message) => message.type === "runtime-probe")!;
      await vi.advanceTimersByTimeAsync(29_000);
      page.visibilityState = "visible";
      page.dispatchEvent(new Event("visibilitychange"));
      port.emit({ ...oldProbe, type: "runtime-pong" });
      await vi.advanceTimersByTimeAsync(29_999);
      expect(rejected).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      await wait;
      expect(rejected).toHaveBeenCalledOnce();
      expect(onFailure).toHaveBeenCalledOnce();
      connection.detachForReconnect();
      const sent = port.sent.length;
      page.dispatchEvent(new Event("visibilitychange"));
      await vi.advanceTimersByTimeAsync(60_000);
      expect(port.sent).toHaveLength(sent);
    } finally {
      connection.detachForReconnect();
      vi.unstubAllGlobals();
      vi.useRealTimers();
    }
  });

  it("clears a remote peer failure after the worker confirms reconnect", async () => {
    const port = new TestPort();
    const transport = {
      recvWireFrames: () => [],
      sendWireFrame: () => undefined,
      tick: () => 0,
    };
    const runtime = {
      connectUpstreamPeer: vi.fn(() => transport),
      onPeerTransportWork: vi.fn(() => () => undefined),
      progressPeerTransport: vi.fn(async () => undefined),
      retirePeerTransport: vi.fn(async () => undefined),
      clearRemoteServerTransportError: vi.fn(),
      reportRemoteServerTransportError: vi.fn(),
      reportRemoteMutationError: vi.fn(),
    };
    const connection = new MessagePortBrowserFollowerConnection(
      runtime as never,
      port as unknown as MessagePort,
      {},
      null,
      {
        onAuthFailure: vi.fn(),
        onAuthRestored: vi.fn(),
        onFailure: vi.fn(),
      },
    );

    const init = port.sent[0];
    if (!init || init.type !== "init") throw new Error("follower did not initialize");
    port.emit({ type: "result", id: init.id });
    await connection.ready();

    port.onPostMessage = (request) => {
      if (request.type === "reconnect" || request.type === "wait-server") {
        port.emit({ type: "result", id: request.id });
      }
    };
    await connection.reconnect("{}", {});

    expect(runtime.clearRemoteServerTransportError).toHaveBeenCalledOnce();
    connection.detachForReconnect();
  });

  it("keeps the prior remote error when the replacement upstream negotiation fails", async () => {
    const port = new TestPort();
    const transport = {
      recvWireFrames: () => [],
      sendWireFrame: () => undefined,
      tick: () => 0,
    };
    const runtime = {
      connectUpstreamPeer: vi.fn(() => transport),
      onPeerTransportWork: vi.fn(() => () => undefined),
      progressPeerTransport: vi.fn(async () => undefined),
      retirePeerTransport: vi.fn(async () => undefined),
      clearRemoteServerTransportError: vi.fn(),
      reportRemoteServerTransportError: vi.fn(),
      reportRemoteMutationError: vi.fn(),
    };
    const connection = new MessagePortBrowserFollowerConnection(
      runtime as never,
      port as unknown as MessagePort,
      {},
      null,
      {
        onAuthFailure: vi.fn(),
        onAuthRestored: vi.fn(),
        onFailure: vi.fn(),
      },
    );

    const init = port.sent[0];
    if (!init || init.type !== "init") throw new Error("follower did not initialize");
    port.emit({ type: "result", id: init.id });
    await connection.ready();

    const negotiationFailure = new Error("websocket authentication failed");
    port.onPostMessage = (request) => {
      if (request.type === "reconnect") port.emit({ type: "result", id: request.id });
      if (request.type === "wait-server") {
        port.emit({
          type: "result",
          id: request.id,
          error: serializeBrowserRelayError(negotiationFailure),
        });
      }
    };

    await expect(connection.reconnect("{}", {})).rejects.toMatchObject({
      name: negotiationFailure.name,
      message: negotiationFailure.message,
      stack: negotiationFailure.stack,
    });
    expect(runtime.clearRemoteServerTransportError).not.toHaveBeenCalled();
    connection.detachForReconnect();
  });

  it("surfaces a foreground peer tick failure before disposing the follower", async () => {
    const port = new TestPort();
    const failure = new Error(
      "Protocol: maintained root occurrence sidecar length does not match root rows",
    );
    const retirementFailure = new Error("retiring the failed transport also failed");
    const transport = {
      recvWireFrames: () => [],
      sendWireFrame: () => undefined,
      tick: () => Promise.reject(failure),
    };
    const runtime = {
      connectUpstreamPeer: vi.fn(() => transport),
      onPeerTransportWork: vi.fn(() => () => undefined),
      progressPeerTransport: vi.fn(() => Promise.reject(failure)),
      retirePeerTransport: vi.fn(() => Promise.reject(retirementFailure)),
      reportRemoteServerTransportError: vi.fn(),
      reportRemoteMutationError: vi.fn(),
    };
    const onFailure = vi.fn();
    const connection = new MessagePortBrowserFollowerConnection(
      runtime as never,
      port as unknown as MessagePort,
      {},
      null,
      {
        onAuthFailure: vi.fn(),
        onAuthRestored: vi.fn(),
        onFailure,
      },
    );

    const init = port.sent[0];
    if (!init || init.type !== "init") throw new Error("follower did not initialize");
    port.emit({ type: "result", id: init.id });

    await vi.waitFor(() =>
      expect(runtime.reportRemoteServerTransportError).toHaveBeenCalledWith(failure),
    );
    await vi.waitFor(() => expect(runtime.retirePeerTransport).toHaveBeenCalledOnce());
    // The rejected retirement is delivered on a later microtask through the
    // same pump callback. Let that path run before asserting first-cause wins.
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(runtime.reportRemoteServerTransportError).toHaveBeenCalledTimes(1);
    expect(onFailure).toHaveBeenCalledWith(failure);
    expect(onFailure).toHaveBeenCalledTimes(1);
    expect(port.close).toHaveBeenCalled();

    await connection.shutdown();
  });

  it("records a relayed transport error before follower disposal without treating it as a mutation rejection", async () => {
    const port = new TestPort();
    const transport = {
      recvWireFrames: () => [],
      sendWireFrame: () => undefined,
      tick: () => 0,
    };
    const runtime = {
      connectUpstreamPeer: vi.fn(() => transport),
      onPeerTransportWork: vi.fn(() => () => undefined),
      progressPeerTransport: vi.fn(async () => undefined),
      retirePeerTransport: vi.fn(async () => undefined),
      reportRemoteServerTransportError: vi.fn(),
      reportRemoteMutationError: vi.fn(),
    };
    const onFailure = vi.fn();
    const connection = new MessagePortBrowserFollowerConnection(
      runtime as never,
      port as unknown as MessagePort,
      {},
      null,
      {
        onAuthFailure: vi.fn(),
        onAuthRestored: vi.fn(),
        onFailure,
      },
    );

    const init = port.sent[0];
    if (!init || init.type !== "init") throw new Error("follower did not initialize");
    port.emit({ type: "result", id: init.id });
    await connection.ready();

    const cause = Object.assign(new Error("maintained reader failed"), {
      name: "MaintainedReaderError",
      stack: "MaintainedReaderError: maintained reader failed\n    at worker-reader.wasm:42:7",
    });
    const failure = Object.assign(new Error("Protocol: terminal upstream failure", { cause }), {
      name: "WorkerTransportError",
      stack:
        "WorkerTransportError: Protocol: terminal upstream failure\n    at worker-core.ts:700:9",
    });
    port.emit({ type: "transport-error", error: serializeBrowserRelayError(failure) });

    expect(runtime.reportRemoteServerTransportError).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "WorkerTransportError",
        message: "Protocol: terminal upstream failure",
        stack: failure.stack,
        cause: expect.objectContaining({
          name: "MaintainedReaderError",
          message: "maintained reader failed",
          stack: cause.stack,
        }),
      }),
    );
    expect(runtime.reportRemoteMutationError).not.toHaveBeenCalled();
    expect(onFailure).not.toHaveBeenCalled();
    expect(port.close).not.toHaveBeenCalled();

    connection.detachForReconnect();
  });
  it("cancels a silent inspector control opening and protocol-closes its channel", async () => {
    const port = new TestPort();
    const transport = {
      recvWireFrames: () => [],
      sendWireFrame: () => undefined,
      tick: () => 0,
    };
    const runtime = {
      connectUpstreamPeer: vi.fn(() => transport),
      onPeerTransportWork: vi.fn(() => () => undefined),
      progressPeerTransport: vi.fn(async () => undefined),
      retirePeerTransport: vi.fn(async () => undefined),
      clearRemoteServerTransportError: vi.fn(),
      reportRemoteServerTransportError: vi.fn(),
      reportRemoteMutationError: vi.fn(),
    };
    const connection = new MessagePortBrowserFollowerConnection(
      runtime as never,
      port as unknown as MessagePort,
      {},
      null,
      {
        onAuthFailure: vi.fn(),
        onAuthRestored: vi.fn(),
        onFailure: vi.fn(),
      },
    );
    const init = port.sent[0];
    if (!init || init.type !== "init") throw new Error("follower did not initialize");
    port.emit({ type: "result", id: init.id });
    await connection.ready();

    const controller = new AbortController();
    const opening = connection.openInspectorControlPort(controller.signal);
    await vi.waitFor(() =>
      expect(port.sent.some((request) => request.type === "open-inspector-control")).toBe(true),
    );
    const request = port.sent.find((candidate) => candidate.type === "open-inspector-control");
    if (!request || request.type !== "open-inspector-control") {
      throw new Error("follower did not request an inspector control");
    }
    request.port.start();
    const closed = new Promise<unknown>((resolve) => {
      request.port.addEventListener("message", (event) => resolve(event.data), { once: true });
    });

    controller.abort();

    await expect(opening).rejects.toThrow("cancelled");
    await expect(closed).resolves.toEqual({ type: "close" });
    request.port.close();
    connection.detachForReconnect();
  });
  it("protocol-closes an inspector channel when follower disposal rejects its opening", async () => {
    const port = new TestPort();
    const transport = {
      recvWireFrames: () => [],
      sendWireFrame: () => undefined,
      tick: () => 0,
    };
    const runtime = {
      connectUpstreamPeer: vi.fn(() => transport),
      onPeerTransportWork: vi.fn(() => () => undefined),
      progressPeerTransport: vi.fn(async () => undefined),
      retirePeerTransport: vi.fn(async () => undefined),
      clearRemoteServerTransportError: vi.fn(),
      reportRemoteServerTransportError: vi.fn(),
      reportRemoteMutationError: vi.fn(),
    };
    const connection = new MessagePortBrowserFollowerConnection(
      runtime as never,
      port as unknown as MessagePort,
      {},
      null,
      {
        onAuthFailure: vi.fn(),
        onAuthRestored: vi.fn(),
        onFailure: vi.fn(),
      },
    );
    const init = port.sent[0];
    if (!init || init.type !== "init") throw new Error("follower did not initialize");
    port.emit({ type: "result", id: init.id });
    await connection.ready();

    const opening = connection.openInspectorControlPort();
    await vi.waitFor(() =>
      expect(port.sent.some((request) => request.type === "open-inspector-control")).toBe(true),
    );
    const request = port.sent.find((candidate) => candidate.type === "open-inspector-control");
    if (!request || request.type !== "open-inspector-control") {
      throw new Error("follower did not request an inspector control");
    }
    request.port.start();
    const closed = new Promise<unknown>((resolve) => {
      request.port.addEventListener("message", (event) => resolve(event.data), { once: true });
    });

    connection.detachForReconnect();

    await expect(opening).rejects.toThrow("reconnecting");
    await expect(closed).resolves.toEqual({ type: "close" });
    request.port.close();
  });
});

import { describe, expect, it, vi } from "vitest";
import { AuthorityChangedError, isAuthorityChangedError } from "../authority-changed-error.js";
import { MessagePortBrowserFollowerConnection } from "./browser-follower-connection.js";
import {
  BrowserWorkerUnresponsiveError,
  serializeBrowserRelayError,
  type BrowserFollowerPortEvent,
  type BrowserFollowerPortRequest,
  type BrowserWorkerLinkFate,
} from "./browser-worker-protocol.js";

/** A worker link fate: connected and failure-free unless `parts` say otherwise. */
function linkFate(parts: Partial<BrowserWorkerLinkFate>) {
  return {
    type: "link-fate" as const,
    fate: {
      state: "connected" as const,
      link: { state: "connected" } as BrowserWorkerLinkFate["link"],
      failure: null,
      authFailure: null,
      ...parts,
    } satisfies BrowserWorkerLinkFate,
  };
}

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
  const settlementListeners = new Set<(pending: boolean) => void>();
  const transport = {
    recvWireFrames: () => [],
    sendWireFrame: () => undefined,
    tick: () => 0,
  };
  const runtime = {
    connectUpstreamPeer: vi.fn(() => transport),
    onPeerTransportWork: vi.fn(() => () => undefined),
    onLocalSettlementChange: vi.fn((listener: (pending: boolean) => void) => {
      settlementListeners.add(listener);
      listener(false);
      return () => settlementListeners.delete(listener);
    }),
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
  return {
    connection,
    port,
    runtime,
    onFailure,
    setLocalSettlementsPending(pending: boolean) {
      for (const listener of settlementListeners) listener(pending);
    },
  };
}

describe("MessagePortBrowserFollowerConnection", () => {
  it("detects silent worker death with a mutation awaiting local durability and no control RPC", async () => {
    vi.useFakeTimers();
    const fixture = await createWatchdogFollower();
    try {
      fixture.setLocalSettlementsPending(true);
      await vi.advanceTimersByTimeAsync(59_999);
      expect(fixture.onFailure).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      expect(fixture.onFailure).toHaveBeenCalledExactlyOnceWith(
        expect.any(BrowserWorkerUnresponsiveError),
      );
      expect(fixture.port.sent.filter((message) => "id" in message)).toHaveLength(1);
      expect(fixture.runtime.retirePeerTransport).toHaveBeenCalledOnce();
    } finally {
      fixture.connection.detachForReconnect();
      vi.useRealTimers();
    }
  });

  it("keeps pending mutations alive with pongs and stops probing after local durability", async () => {
    vi.useFakeTimers();
    const fixture = await createWatchdogFollower();
    try {
      fixture.port.onPostMessage = (message) => {
        if (message.type === "runtime-probe")
          fixture.port.emit({ ...message, type: "runtime-pong" });
      };
      fixture.setLocalSettlementsPending(true);
      await vi.advanceTimersByTimeAsync(180_000);
      expect(fixture.onFailure).not.toHaveBeenCalled();
      const probes = fixture.port.sent.filter((message) => message.type === "runtime-probe").length;
      expect(probes).toBe(6);
      fixture.setLocalSettlementsPending(false);
      await vi.advanceTimersByTimeAsync(120_000);
      expect(fixture.port.sent.filter((message) => message.type === "runtime-probe")).toHaveLength(
        probes,
      );
    } finally {
      fixture.connection.detachForReconnect();
      vi.useRealTimers();
    }
  });

  it.each([false, true])(
    "probes a hidden idle follower once on resume (worker alive=%s)",
    async (alive) => {
      vi.useFakeTimers();
      const page = Object.assign(new EventTarget(), { visibilityState: "hidden" });
      vi.stubGlobal("document", page);
      const fixture = await createWatchdogFollower();
      try {
        fixture.port.onPostMessage = (message) => {
          if (alive && message.type === "runtime-probe")
            fixture.port.emit({ ...message, type: "runtime-pong" });
        };
        page.dispatchEvent(new Event("resume"));
        await vi.advanceTimersByTimeAsync(120_000);
        expect(
          fixture.port.sent.filter((message) => message.type === "runtime-probe"),
        ).toHaveLength(1);
        if (alive) expect(fixture.onFailure).not.toHaveBeenCalled();
        else
          expect(fixture.onFailure).toHaveBeenCalledExactlyOnceWith(
            expect.any(BrowserWorkerUnresponsiveError),
          );
      } finally {
        fixture.connection.detachForReconnect();
        vi.unstubAllGlobals();
        vi.useRealTimers();
      }
    },
  );

  // rc.4 adopter finding R66: a visible tab sitting idle when another tab
  // crashed the worker never noticed, so it stayed "connecting" and the dead
  // tab's writes waited for some tab to write or reload.
  it.each([false, true])(
    "keeps probing a visible idle follower (worker alive=%s)",
    async (alive) => {
      vi.useFakeTimers();
      const page = Object.assign(new EventTarget(), { visibilityState: "visible" });
      vi.stubGlobal("document", page);
      const fixture = await createWatchdogFollower();
      try {
        fixture.port.onPostMessage = (message) => {
          if (alive && message.type === "runtime-probe")
            fixture.port.emit({ ...message, type: "runtime-pong" });
        };
        await vi.advanceTimersByTimeAsync(120_000);
        const probes = fixture.port.sent.filter((message) => message.type === "runtime-probe");
        if (alive) {
          expect(probes).toHaveLength(4);
          expect(fixture.onFailure).not.toHaveBeenCalled();
        } else {
          // One probe, unanswered for its reply grace.
          expect(probes).toHaveLength(1);
          expect(fixture.onFailure).toHaveBeenCalledExactlyOnceWith(
            expect.any(BrowserWorkerUnresponsiveError),
          );
        }
      } finally {
        fixture.connection.detachForReconnect();
        vi.unstubAllGlobals();
        vi.useRealTimers();
      }
    },
  );
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
        onLocalSettlementChange: vi.fn(() => () => undefined),
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
        onLocalSettlementChange: vi.fn(() => () => undefined),
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
      onLocalSettlementChange: vi.fn(() => () => undefined),
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
      onLocalSettlementChange: vi.fn(() => () => undefined),
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
      onLocalSettlementChange: vi.fn(() => () => undefined),
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
      onLocalSettlementChange: vi.fn(() => () => undefined),
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
    port.emit(
      linkFate({ failure: { error: serializeBrowserRelayError(failure), retrying: false } }),
    );

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
      false,
    );
    expect(runtime.reportRemoteMutationError).not.toHaveBeenCalled();
    expect(onFailure).not.toHaveBeenCalled();
    expect(port.close).not.toHaveBeenCalled();

    // An outage the worker keeps retrying reaches the tab as one (#3923).
    port.emit(
      linkFate({
        failure: {
          error: serializeBrowserRelayError(new Error("websocket closed")),
          retrying: true,
        },
      }),
    );
    expect(runtime.reportRemoteServerTransportError).toHaveBeenLastCalledWith(
      expect.objectContaining({ message: "websocket closed" }),
      true,
    );

    connection.detachForReconnect();
  });
  it("applies the worker's link fate alike at admission and later (#4808)", async () => {
    const port = new TestPort();
    const transport = {
      recvWireFrames: () => [],
      sendWireFrame: () => undefined,
      tick: () => 0,
    };
    const runtime = {
      connectUpstreamPeer: vi.fn(() => transport),
      onPeerTransportWork: vi.fn(() => () => undefined),
      onLocalSettlementChange: vi.fn(() => () => undefined),
      progressPeerTransport: vi.fn(async () => undefined),
      retirePeerTransport: vi.fn(async () => undefined),
      clearRemoteServerTransportError: vi.fn(),
      reportRemoteServerTransportError: vi.fn(),
      reportRemoteMutationError: vi.fn(),
    };
    const onAuthFailure = vi.fn();
    const onAuthRestored = vi.fn();
    const onRemoteLinkChange = vi.fn();
    const connection = new MessagePortBrowserFollowerConnection(
      runtime as never,
      port as unknown as MessagePort,
      {},
      null,
      { onAuthFailure, onAuthRestored, onRemoteLinkChange, onFailure: vi.fn() },
    );
    const init = port.sent[0];
    if (!init || init.type !== "init") throw new Error("follower did not initialize");

    // A tab attaching to a worker whose upstream was already refused.
    const refused = linkFate({
      state: "unavailable",
      link: { state: "offline", reason: "authority-changed", willRetry: false } as never,
      failure: {
        error: serializeBrowserRelayError(new AuthorityChangedError("store recreated")),
        retrying: false,
      },
      authFailure: "expired",
    });
    port.emit(refused);
    port.emit({ type: "result", id: init.id });
    await connection.ready();
    expect(runtime.reportRemoteServerTransportError).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ code: "authority_changed" }),
      false,
    );
    expect(
      isAuthorityChangedError(runtime.reportRemoteServerTransportError.mock.calls[0]![0]),
    ).toBe(true);
    expect(onAuthFailure).toHaveBeenCalledExactlyOnceWith("expired");
    expect(onRemoteLinkChange).toHaveBeenLastCalledWith("unavailable", refused.fate.link);

    // The same fate again changes nothing in the tab's runtime.
    port.emit(refused);
    expect(runtime.reportRemoteServerTransportError).toHaveBeenCalledOnce();
    expect(onAuthFailure).toHaveBeenCalledOnce();

    // The worker recovered: the error and the auth refusal both clear.
    port.emit(linkFate({}));
    expect(runtime.clearRemoteServerTransportError).toHaveBeenCalledOnce();
    expect(onAuthRestored).toHaveBeenCalledOnce();
    expect(onRemoteLinkChange).toHaveBeenLastCalledWith("connected", { state: "connected" });

    connection.detachForReconnect();
  });

  it("clears an auth error a previous worker connection left when the new one has none", async () => {
    const port = new TestPort();
    const runtime = {
      connectUpstreamPeer: vi.fn(() => ({
        recvWireFrames: () => [],
        sendWireFrame: () => undefined,
        tick: () => 0,
      })),
      onPeerTransportWork: vi.fn(() => () => undefined),
      onLocalSettlementChange: vi.fn(() => () => undefined),
      progressPeerTransport: vi.fn(async () => undefined),
      retirePeerTransport: vi.fn(async () => undefined),
      clearRemoteServerTransportError: vi.fn(),
      reportRemoteServerTransportError: vi.fn(),
    };
    const onAuthFailure = vi.fn();
    const onAuthRestored = vi.fn();
    const connection = new MessagePortBrowserFollowerConnection(
      runtime as never,
      port as unknown as MessagePort,
      {},
      null,
      { onAuthFailure, onAuthRestored, onFailure: vi.fn() },
    );
    const init = port.sent[0];
    if (!init || init.type !== "init") throw new Error("follower did not initialize");
    port.emit(linkFate({}));
    port.emit({ type: "result", id: init.id });
    await connection.ready();
    expect(onAuthRestored).toHaveBeenCalledOnce();
    expect(onAuthFailure).not.toHaveBeenCalled();
    expect(runtime.reportRemoteServerTransportError).not.toHaveBeenCalled();
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
      onLocalSettlementChange: vi.fn(() => () => undefined),
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
      onLocalSettlementChange: vi.fn(() => () => undefined),
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

describe("MessagePortBrowserFollowerConnection.leaveForPageCache", () => {
  it("detaches an idle follower at once: a close that wants no reply, no failure", async () => {
    const { connection, port, onFailure } = await createWatchdogFollower();
    port.sent.length = 0;
    expect(connection.leaveForPageCache()).toBe(true);
    expect(port.sent).toEqual([{ type: "close", releaseContext: true }]);
    expect(port.close).toHaveBeenCalledOnce();
    expect(onFailure).not.toHaveBeenCalled();
    await expect(connection.flushLocal()).rejects.toThrow("Browser follower connection is closed");
    expect(connection.leaveForPageCache()).toBe(false);
  });

  it("declines while an operation waits on the worker, and keeps it", async () => {
    const { connection, port } = await createWatchdogFollower();
    port.sent.length = 0;
    const waiting = connection.waitForServerConnection();
    await vi.waitFor(() =>
      expect(port.sent.map((message) => message.type)).toContain("wait-server"),
    );
    expect(connection.leaveForPageCache()).toBe(false);
    expect(port.sent.map((message) => message.type)).not.toContain("close");
    expect(port.close).not.toHaveBeenCalled();
    const request = port.sent.find((message) => message.type === "wait-server");
    port.emit({ type: "result", id: (request as { id: number }).id });
    await waiting;
    expect(connection.leaveForPageCache()).toBe(true);
  });

  it("declines while a write waits for the worker to settle it, and keeps it", async () => {
    const { connection, port, setLocalSettlementsPending } = await createWatchdogFollower();
    port.sent.length = 0;
    setLocalSettlementsPending(true);
    expect(connection.leaveForPageCache()).toBe(false);
    expect(port.close).not.toHaveBeenCalled();
    setLocalSettlementsPending(false);
    expect(connection.leaveForPageCache()).toBe(true);
  });

  it("declines before the worker admitted it", () => {
    const port = new TestPort();
    const connection = new MessagePortBrowserFollowerConnection(
      {
        onLocalSettlementChange: () => () => undefined,
      } as never,
      port as unknown as MessagePort,
      {},
      null,
      { onAuthFailure: vi.fn(), onAuthRestored: vi.fn(), onFailure: vi.fn() },
    );
    expect(connection.leaveForPageCache()).toBe(false);
    expect(port.close).not.toHaveBeenCalled();
  });
});

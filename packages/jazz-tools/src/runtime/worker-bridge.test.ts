import { describe, expect, it } from "vitest";
import { WorkerBridge, type PeerSyncBatch } from "./worker-bridge.js";
import type { Runtime } from "./client.js";
import type { WorkerToMainMessage } from "../worker/worker-protocol.js";
import { OutboxDestinationKind } from "./sync-transport.js";

class MockWorker {
  onmessage: ((event: MessageEvent<WorkerToMainMessage>) => void) | null = null;
  posted: unknown[] = [];
  private readonly listeners = new Set<(event: MessageEvent<WorkerToMainMessage>) => void>();

  postMessage(message: unknown): void {
    this.posted.push(message);
  }

  addEventListener(
    type: string,
    listener: (event: MessageEvent<WorkerToMainMessage>) => void,
  ): void {
    if (type !== "message") return;
    this.listeners.add(listener);
  }

  removeEventListener(
    type: string,
    listener: (event: MessageEvent<WorkerToMainMessage>) => void,
  ): void {
    if (type !== "message") return;
    this.listeners.delete(listener);
  }

  emitFromWorker(message: WorkerToMainMessage): void {
    const event = { data: message } as MessageEvent<WorkerToMainMessage>;
    this.onmessage?.(event);
    for (const listener of this.listeners) {
      listener(event);
    }
  }
}

type SendSyncPayloadCallback = (
  destinationKind: OutboxDestinationKind,
  destinationId: string,
  payload: Uint8Array,
  isCatalogue: boolean,
) => void;

function createRuntimeMock(): {
  runtime: Runtime;
  emitSyncPayload: SendSyncPayloadCallback;
  receivedFromWorker: Uint8Array[];
  addServerCalls: { count: number };
  removeServerCalls: { count: number };
} {
  let onSyncToSend: SendSyncPayloadCallback | null = null;
  const receivedFromWorker: Uint8Array[] = [];
  const addServerCalls = { count: 0 };
  const removeServerCalls = { count: 0 };

  const runtime: Runtime = {
    insert: () => "id",
    update: () => undefined,
    delete: () => undefined,
    query: async () => [],
    subscribe: () => 1,
    unsubscribe: () => undefined,
    onSyncMessageReceived: (payload: Uint8Array) => {
      receivedFromWorker.push(payload);
    },
    onSyncMessageToSend: (callback: SendSyncPayloadCallback) => {
      onSyncToSend = callback;
    },
    addServer: () => {
      addServerCalls.count += 1;
    },
    removeServer: () => {
      removeServerCalls.count += 1;
    },
    addClient: () => "client-id",
    getSchema: () => ({}),
    getSchemaHash: () => "schema-hash",
  };

  return {
    runtime,
    emitSyncPayload: (
      destinationKind: OutboxDestinationKind,
      destinationId: string,
      payload: Uint8Array,
      isCatalogue = false,
    ) => {
      if (!onSyncToSend) {
        throw new Error("onSyncMessageToSend callback not registered");
      }
      onSyncToSend(destinationKind, destinationId, payload, isCatalogue);
    },
    receivedFromWorker,
    addServerCalls,
    removeServerCalls,
  };
}

describe("WorkerBridge", () => {
  const enc = (value: unknown): Uint8Array => new TextEncoder().encode(JSON.stringify(value));

  it("attaches runtime server and forwards worker sync payloads to runtime", () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();

    new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

    expect(runtimeMock.addServerCalls.count).toBe(1);

    worker.emitFromWorker({
      type: "sync",
      payload: [enc({ id: 1 }), enc({ id: 2 })],
    });

    expect(runtimeMock.receivedFromWorker).toEqual([enc({ id: 1 }), enc({ id: 2 })]);
  });

  it("batches server-bound runtime payloads into one worker sync message", async () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

    runtimeMock.emitSyncPayload("server", "server-1", enc({ id: 1 }), false);
    runtimeMock.emitSyncPayload("server", "server-2", enc({ id: 2 }), false);
    runtimeMock.emitSyncPayload("client", "client-1", enc({ ignored: true }), false);

    // Outgoing payloads are buffered until init completes.
    let syncMessages = worker.posted.filter(
      (entry): entry is { type: "sync"; payload: Uint8Array[] } =>
        typeof entry === "object" && entry !== null && (entry as { type?: string }).type === "sync",
    );
    expect(syncMessages).toHaveLength(0);

    const initPromise = bridge.init({
      schemaJson: '{"tables":[]}',
      appId: "app-1",
      env: "dev",
      userBranch: "main",
      dbName: "db-1",
    });
    worker.emitFromWorker({ type: "init-ok", clientId: "worker-client-123" });
    await initPromise;
    await Promise.resolve();

    syncMessages = worker.posted.filter(
      (entry): entry is { type: "sync"; payload: Uint8Array[] } =>
        typeof entry === "object" && entry !== null && (entry as { type?: string }).type === "sync",
    );

    expect(syncMessages).toHaveLength(1);
    expect(syncMessages[0]).toEqual({
      type: "sync",
      payload: [enc({ id: 1 }), enc({ id: 2 })],
    });
  });

  it("initializes worker and returns assigned client id", async () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

    const initPromise = bridge.init({
      schemaJson: '{"tables":[]}',
      appId: "app-1",
      env: "dev",
      userBranch: "main",
      dbName: "db-1",
      serverUrl: "http://localhost:3000",
    });

    expect(worker.posted[0]).toMatchObject({
      type: "init",
      appId: "app-1",
      dbName: "db-1",
    });

    worker.emitFromWorker({
      type: "init-ok",
      clientId: "worker-client-123",
    });

    await expect(initPromise).resolves.toBe("worker-client-123");
    expect(bridge.getWorkerClientId()).toBe("worker-client-123");
  });

  it("detaches runtime server on shutdown and stops forwarding after disposal", async () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

    const shutdownPromise = bridge.shutdown(worker as unknown as Worker);

    expect(runtimeMock.removeServerCalls.count).toBe(1);
    expect(worker.posted[0]).toEqual({ type: "shutdown" });

    worker.emitFromWorker({ type: "shutdown-ok" });
    await shutdownPromise;

    runtimeMock.emitSyncPayload("server", "server-1", enc({ dropped: true }), false);
    await Promise.resolve();

    const syncMessagesAfterShutdown = worker.posted.filter(
      (entry): entry is { type: "sync"; payload: Uint8Array[] } =>
        typeof entry === "object" && entry !== null && (entry as { type?: string }).type === "sync",
    );
    expect(syncMessagesAfterShutdown).toHaveLength(0);
  });

  it("supports peer channel control and peer-sync forwarding", () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);
    const peerBatches: PeerSyncBatch[] = [];

    bridge.onPeerSync((batch) => {
      peerBatches.push(batch);
    });

    bridge.openPeer("peer-a");
    bridge.sendPeerSync("peer-a", 9, [enc("payload-1"), enc("payload-2")]);
    bridge.closePeer("peer-a");

    expect(worker.posted).toEqual([
      { type: "peer-open", peerId: "peer-a" },
      {
        type: "peer-sync",
        peerId: "peer-a",
        term: 9,
        payload: [enc("payload-1"), enc("payload-2")],
      },
      { type: "peer-close", peerId: "peer-a" },
    ]);

    worker.emitFromWorker({
      type: "peer-sync",
      peerId: "peer-a",
      term: 9,
      payload: [enc("from-worker")],
    });

    expect(peerBatches).toEqual([
      {
        peerId: "peer-a",
        term: 9,
        payload: [enc("from-worker")],
      },
    ]);
  });

  it("can redirect outgoing server payloads and replay upstream connection", async () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);
    const redirected: Uint8Array[] = [];

    bridge.setServerPayloadForwarder((payload) => {
      redirected.push(payload);
    });
    runtimeMock.emitSyncPayload("server", "server-1", enc({ routed: "peer" }), false);
    await Promise.resolve();

    const workerSyncMessages = worker.posted.filter(
      (entry): entry is { type: "sync"; payload: Uint8Array[] } =>
        typeof entry === "object" && entry !== null && (entry as { type?: string }).type === "sync",
    );
    expect(workerSyncMessages).toHaveLength(0);
    expect(redirected).toEqual([enc({ routed: "peer" })]);

    bridge.replayServerConnection();
    expect(runtimeMock.removeServerCalls.count).toBe(1);
    expect(runtimeMock.addServerCalls.count).toBe(2);

    bridge.applyIncomingServerPayload(enc("from-peer-leader"));
    expect(runtimeMock.receivedFromWorker).toEqual([enc("from-peer-leader")]);
  });

  it("forwards lifecycle hints to worker", () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

    bridge.sendLifecycleHint("visibility-hidden");
    bridge.sendLifecycleHint("resume");

    expect(worker.posted).toMatchObject([
      {
        type: "lifecycle-hint",
        event: "visibility-hidden",
      },
      {
        type: "lifecycle-hint",
        event: "resume",
      },
    ]);
    expect((worker.posted[0] as any).sentAtMs).toEqual(expect.any(Number));
    expect((worker.posted[1] as any).sentAtMs).toEqual(expect.any(Number));
  });
});

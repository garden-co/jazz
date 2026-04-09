import { afterEach, describe, expect, it, vi } from "vitest";
import { JazzClient, type Runtime } from "./client.js";
import type { RuntimeSyncOutboxCallback } from "./sync-transport.js";

const textEncoder = new TextEncoder();

function encodeFrames(events: unknown[]): Uint8Array {
  const chunks = events.map((event) => {
    const payload = textEncoder.encode(JSON.stringify(event));
    const frame = new Uint8Array(4 + payload.length);
    new DataView(frame.buffer).setUint32(0, payload.length, false);
    frame.set(payload, 4);
    return frame;
  });

  const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const bytes = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  return bytes;
}

function openConnectedStream(clientId: string): { response: Response; close(): void } {
  let controllerRef: ReadableStreamDefaultController<Uint8Array> | null = null;
  const bytes = encodeFrames([{ type: "Connected", client_id: clientId }]);

  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      controllerRef = controller;
      controller.enqueue(bytes);
    },
  });

  return {
    response: {
      ok: true,
      status: 200,
      statusText: "OK",
      body,
    } as Response,
    close() {
      controllerRef?.close();
    },
  };
}

function unauthenticatedResponse(code: "expired" | "missing" | "invalid" | "disabled"): Response {
  return {
    ok: false,
    status: 401,
    statusText: "Unauthorized",
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => ({
      error: "unauthenticated",
      code,
      message: `auth failed: ${code}`,
    }),
  } as Response;
}

function okResponse(): Response {
  return {
    ok: true,
    status: 200,
    statusText: "OK",
    headers: new Headers(),
  } as Response;
}

function makeRuntime() {
  let outboxCallback: RuntimeSyncOutboxCallback | null = null;
  const addServer = vi.fn();
  const removeServer = vi.fn();
  const close = vi.fn();

  const runtime: Runtime = {
    insert: () => ({ id: "row-1", values: [] }),
    insertDurable: async () => ({ id: "row-1", values: [] }),
    update: () => {},
    updateDurable: async () => {},
    delete: () => {},
    deleteDurable: async () => {},
    query: async () => [],
    subscribe: () => 0,
    createSubscription: () => 0,
    executeSubscription: () => {},
    unsubscribe: () => {},
    onSyncMessageReceived: () => {},
    onSyncMessageToSend: (callback) => {
      outboxCallback = callback;
    },
    addServer,
    removeServer,
    close,
    addClient: () => "runtime-client-id",
    getSchema: () => ({}),
    getSchemaHash: () => "schema-hash",
  };

  return {
    runtime,
    addServer,
    removeServer,
    close,
    sendServerPayload(payload = '{"kind":"sync"}', isCatalogue = false) {
      if (!outboxCallback) {
        throw new Error("outbox callback not registered");
      }
      outboxCallback("server", "server-1", payload, isCatalogue);
    },
  };
}

describe("JazzClient sync auth handling", () => {
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    (globalThis as { fetch: typeof fetch }).fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it("detaches the active stream when /sync returns an auth failure", async () => {
    const stream = openConnectedStream("server-client-id");
    const fetchMock = vi.fn(async (input: string | URL) => {
      const url = String(input);
      if (url.includes("/events")) {
        return stream.response;
      }
      if (url.endsWith("/sync")) {
        return unauthenticatedResponse("expired");
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const { runtime, addServer, removeServer, sendServerPayload } = makeRuntime();
    const onAuthFailure = vi.fn();
    const client = JazzClient.connectWithRuntime(
      runtime,
      {
        appId: "test-app",
        schema: {},
        serverUrl: "http://localhost:3000",
        jwtToken: "expired-jwt",
      },
      { onAuthFailure },
    );

    await vi.waitFor(() => expect(addServer).toHaveBeenCalledWith(null, null));

    sendServerPayload();

    await vi.waitFor(() => expect(onAuthFailure).toHaveBeenCalledWith("expired"));
    expect(removeServer).toHaveBeenCalledTimes(1);

    await client.shutdown();
    stream.close();
  });

  it("waits for in-flight sync posts during shutdown and ignores new outbox work", async () => {
    const stream = openConnectedStream("server-client-id");
    let resolveSyncResponse: ((response: Response) => void) | null = null;
    const fetchMock = vi.fn((input: string | URL) => {
      const url = String(input);
      if (url.includes("/events")) {
        return Promise.resolve(stream.response);
      }
      if (url.endsWith("/sync")) {
        return new Promise<Response>((resolve) => {
          resolveSyncResponse = resolve;
        });
      }
      return Promise.reject(new Error(`Unexpected fetch: ${url}`));
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const { runtime, addServer, close, sendServerPayload } = makeRuntime();
    const client = JazzClient.connectWithRuntime(runtime, {
      appId: "test-app",
      schema: {},
      serverUrl: "http://localhost:3000",
      backendSecret: "backend-secret",
    });

    await vi.waitFor(() => expect(addServer).toHaveBeenCalledWith(null, null));

    sendServerPayload('{"kind":"first"}');
    await vi.waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        "http://localhost:3000/sync",
        expect.objectContaining({ method: "POST" }),
      ),
    );

    let shutdownResolved = false;
    const shutdownPromise = client.shutdown().then(() => {
      shutdownResolved = true;
    });

    await Promise.resolve();
    expect(shutdownResolved).toBe(false);
    expect(close).not.toHaveBeenCalled();

    sendServerPayload('{"kind":"second"}');
    expect(fetchMock.mock.calls.filter(([url]) => String(url).endsWith("/sync"))).toHaveLength(1);

    resolveSyncResponse?.(okResponse());
    await shutdownPromise;

    expect(close).toHaveBeenCalledTimes(1);
    stream.close();
  });
});

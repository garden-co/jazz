import { afterEach, describe, expect, it, vi } from "vitest";
import {
  buildEndpointUrl,
  buildEventsUrl,
  createRuntimeSyncStreamController,
  createSyncOutboxRouter,
  generateClientId,
  isExpectedFetchAbortError,
  linkExternalIdentity,
  normalizePathPrefix,
  readBinaryFrames,
  sendSyncPayload,
  SyncStreamController,
} from "./sync-transport.js";

describe("sync-transport", () => {
  const originalFetch = globalThis.fetch;
  const textEncoder = new TextEncoder();
  const pingPayload = textEncoder.encode('{"Ping":{}}');
  const pongPayload = textEncoder.encode('{"Pong":{}}');

  function encodeFrames(events: unknown[]): Uint8Array {
    const chunks: Uint8Array[] = events.map((event) => {
      const payload = encodeEventFrame(event as any);
      const frame = new Uint8Array(4 + payload.length);
      new DataView(frame.buffer).setUint32(0, payload.length, false);
      frame.set(payload, 4);
      return frame;
    });

    const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const all = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      all.set(chunk, offset);
      offset += chunk.length;
    }
    return all;
  }

  function encodeEventFrame(event: {
    type: string;
    client_id?: string;
    payload?: Uint8Array;
    seq?: number | null;
    next_sync_seq?: number | null;
  }): Uint8Array {
    if (event.type === "Connected") {
      const clientIdBytes = textEncoder.encode(event.client_id ?? "");
      const hasNext = event.next_sync_seq != null;
      const payload = new Uint8Array(1 + 8 + 1 + (hasNext ? 8 : 0) + 4 + clientIdBytes.length);
      let offset = 0;
      payload[offset++] = 1; // Connected
      offset += 8; // connection_id ignored by JS parser in tests
      payload[offset++] = hasNext ? 1 : 0;
      if (hasNext) {
        new DataView(payload.buffer).setBigUint64(offset, BigInt(event.next_sync_seq!), false);
        offset += 8;
      }
      new DataView(payload.buffer).setUint32(offset, clientIdBytes.length, false);
      offset += 4;
      payload.set(clientIdBytes, offset);
      return payload;
    }

    if (event.type === "SyncUpdate") {
      const messagePayload = event.payload ?? new Uint8Array();
      const hasSeq = event.seq != null;
      const payload = new Uint8Array(1 + 1 + (hasSeq ? 8 : 0) + messagePayload.length);
      let offset = 0;
      payload[offset++] = 3; // SyncUpdate
      payload[offset++] = hasSeq ? 1 : 0;
      if (hasSeq) {
        new DataView(payload.buffer).setBigUint64(offset, BigInt(event.seq!), false);
        offset += 8;
      }
      payload.set(messagePayload, offset);
      return payload;
    }

    if (event.type === "Heartbeat") {
      return Uint8Array.of(5);
    }

    throw new Error(`Unsupported test event type: ${event.type}`);
  }

  function streamResponse(events: unknown[]): Response {
    const bytes = encodeFrames(events);
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(bytes);
        controller.close();
      },
    });
    return {
      ok: true,
      status: 200,
      statusText: "OK",
      body,
    } as Response;
  }

  afterEach(() => {
    (globalThis as { fetch: typeof fetch }).fetch = originalFetch;
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("generateClientId returns UUIDv4 format", () => {
    const id = generateClientId();
    expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  });

  it("uses a stable non-zero fallback client_id when none is provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await sendSyncPayload("http://localhost:3000", pingPayload, false, {});
    await sendSyncPayload("http://localhost:3000", pongPayload, false, {});

    expect(fetchMock).toHaveBeenCalledTimes(2);

    const firstUrl = new URL(fetchMock.mock.calls[0][0] as string);
    const secondUrl = new URL(fetchMock.mock.calls[1][0] as string);
    const firstClientId = firstUrl.searchParams.get("client_id");
    const secondClientId = secondUrl.searchParams.get("client_id");
    expect(firstClientId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    expect(firstClientId).not.toBe("00000000-0000-0000-0000-000000000000");
    expect(secondClientId).toBe(firstClientId);
    expect(fetchMock.mock.calls[0][1].body).toEqual(pingPayload);
    expect(fetchMock.mock.calls[1][1].body).toEqual(pongPayload);
  });

  it("uses provided client_id when supplied", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const providedClientId = "11111111-2222-4333-8444-555555555555";
    await sendSyncPayload("http://localhost:3000", pingPayload, false, {
      clientId: providedClientId,
      jwtToken: "token",
    });

    const url = new URL(fetchMock.mock.calls[0][0] as string);
    expect(url.searchParams.get("client_id")).toBe(providedClientId);
  });

  it("throws on non-2xx sync POST responses", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue({ ok: false, status: 503, statusText: "Service Unavailable" });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await expect(sendSyncPayload("http://localhost:3000", pingPayload, false, {})).rejects.toThrow(
      "Sync POST failed: 503 Service Unavailable",
    );
  });

  it("throws when fetch rejects", async () => {
    const fetchMock = vi.fn().mockRejectedValue(new Error("network down"));
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await expect(sendSyncPayload("http://localhost:3000", pingPayload, false, {})).rejects.toThrow(
      "Sync POST failed: network down",
    );
  });

  it("posts to path-prefixed sync route when provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await sendSyncPayload("http://localhost:3000/", pingPayload, false, {
      jwtToken: "token",
      pathPrefix: "apps/app-123/",
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0][0]).toMatch(
      /^http:\/\/localhost:3000\/apps\/app-123\/sync\?client_id=/,
    );
  });

  it("skips catalogue payload sync when admin secret is missing", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await sendSyncPayload("http://localhost:3000", pingPayload, true, { jwtToken: "jwt-token" });

    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("posts catalogue payloads with admin secret header", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await sendSyncPayload("http://localhost:3000", pingPayload, true, {
      adminSecret: "admin-secret",
      jwtToken: "jwt-token",
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0][1].headers).toMatchObject({
      "Content-Type": "application/octet-stream",
      "X-Jazz-Admin-Secret": "admin-secret",
    });
    expect(fetchMock.mock.calls[0][1].headers).not.toHaveProperty("Authorization");
    expect(fetchMock.mock.calls[0][1].headers).not.toHaveProperty("X-Jazz-Local-Mode");
    expect(fetchMock.mock.calls[0][1].headers).not.toHaveProperty("X-Jazz-Local-Token");
  });

  it("posts link-external with bearer and local auth headers", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        principal_id: "local:abc",
        issuer: "https://issuer.example",
        subject: "user-1",
        created: true,
      }),
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const result = await linkExternalIdentity("http://localhost:3000/", {
      jwtToken: "jwt-token",
      localAuthMode: "anonymous",
      localAuthToken: "device-token",
      pathPrefix: "apps/app-123/",
    });

    expect(result.created).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0][0]).toBe(
      "http://localhost:3000/apps/app-123/auth/link-external",
    );
    expect(fetchMock.mock.calls[0][1].method).toBe("POST");
    expect(fetchMock.mock.calls[0][1].headers).toMatchObject({
      Authorization: "Bearer jwt-token",
      "X-Jazz-Local-Mode": "anonymous",
      "X-Jazz-Local-Token": "device-token",
    });
  });

  it("normalizes route prefixes and endpoint URLs", () => {
    expect(normalizePathPrefix(undefined)).toBe("");
    expect(normalizePathPrefix("")).toBe("");
    expect(normalizePathPrefix("apps/app-1/")).toBe("/apps/app-1");
    expect(normalizePathPrefix("/apps/app-1/")).toBe("/apps/app-1");

    expect(buildEndpointUrl("http://localhost:1625/", "/sync", "apps/app-1")).toBe(
      "http://localhost:1625/apps/app-1/sync",
    );
    expect(buildEventsUrl("http://localhost:1625", "client#1", "/apps/app-1")).toBe(
      "http://localhost:1625/apps/app-1/events?client_id=client%231",
    );
  });

  it("stream controller attaches on Connected and forwards sync payloads", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      streamResponse([
        { type: "Connected", client_id: "server-client-1" },
        { type: "SyncUpdate", payload: pingPayload },
      ]),
    );
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const onConnected = vi.fn();
    const onDisconnected = vi.fn();
    const onSyncMessage = vi.fn();
    let clientId = "initial-client-id";

    const controller = new SyncStreamController({
      getAuth: () => ({}),
      getClientId: () => clientId,
      setClientId: (nextClientId) => {
        clientId = nextClientId;
      },
      onConnected,
      onDisconnected,
      onSyncMessage,
    });

    controller.start("http://localhost:3000");

    await vi.waitFor(() => expect(onConnected).toHaveBeenCalledTimes(1));
    await vi.waitFor(() => expect(onSyncMessage).toHaveBeenCalledWith(pingPayload));
    expect(clientId).toBe("server-client-1");
    await vi.waitFor(() => expect(onDisconnected).toHaveBeenCalledTimes(1));

    controller.stop();
  });

  it("stream controller retries after non-OK connect responses", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(0);

    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({ ok: false, status: 503 } as Response)
      .mockResolvedValueOnce(streamResponse([{ type: "Connected", client_id: "server-client-2" }]));
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const onConnected = vi.fn();
    let clientId = "initial-client-id";

    const controller = new SyncStreamController({
      getAuth: () => ({}),
      getClientId: () => clientId,
      setClientId: (nextClientId) => {
        clientId = nextClientId;
      },
      onConnected,
      onDisconnected: vi.fn(),
      onSyncMessage: vi.fn(),
    });

    controller.start("http://localhost:3000");
    await Promise.resolve();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(300);
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    await vi.waitFor(() => expect(onConnected).toHaveBeenCalledTimes(1));
    expect(clientId).toBe("server-client-2");

    controller.stop();
  });

  it("classifies canceled fetch errors as expected aborts", () => {
    expect(
      isExpectedFetchAbortError(new Error("fetch failed: Fetch request has been canceled")),
    ).toBe(true);
    expect(
      isExpectedFetchAbortError({
        message: "outer",
        cause: new Error("fetch failed: Fetch request has been cancelled"),
      }),
    ).toBe(true);
    expect(isExpectedFetchAbortError(new Error("network down"))).toBe(false);
  });

  it("suppresses expected canceled-fetch errors when stopping an in-flight stream", async () => {
    const fetchMock = vi.fn().mockImplementation((_url, init) => {
      const signal = (init as RequestInit).signal as AbortSignal;
      return new Promise<Response>((_resolve, reject) => {
        signal.addEventListener(
          "abort",
          () => {
            reject(new Error("fetch failed: Fetch request has been canceled"));
          },
          { once: true },
        );
      });
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const controller = new SyncStreamController({
      getAuth: () => ({}),
      getClientId: () => "initial-client-id",
      setClientId: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      onSyncMessage: vi.fn(),
    });

    controller.start("http://localhost:3000");
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    controller.stop();
    await Promise.resolve();

    expect(errorSpy).not.toHaveBeenCalledWith(
      expect.stringContaining("Stream connect error:"),
      expect.anything(),
    );
  });

  it("labels callback failures separately from parse failures", async () => {
    const response = streamResponse([{ type: "Connected", client_id: "server-client-4" }]);
    const reader = response.body!.getReader();
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    await readBinaryFrames(
      reader,
      {
        onSyncMessage: vi.fn(),
        onConnected: () => {
          throw new Error("callback blew up");
        },
      },
      "[client] ",
    );

    expect(errorSpy).toHaveBeenCalledWith("[client] Stream callback error:", expect.any(Error));
    expect(errorSpy).not.toHaveBeenCalledWith("[client] Stream parse error:", expect.any(Error));
  });

  it("runtime-bound stream controller maps stream events to runtime hooks", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      streamResponse([
        { type: "Connected", client_id: "server-client-3" },
        { type: "SyncUpdate", payload: pingPayload },
      ]),
    );
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const runtime = {
      addServer: vi.fn(),
      removeServer: vi.fn(),
      onSyncMessageReceived: vi.fn(),
    };
    let clientId = "initial-client-id";

    const controller = createRuntimeSyncStreamController({
      getRuntime: () => runtime,
      getAuth: () => ({}),
      getClientId: () => clientId,
      setClientId: (nextClientId) => {
        clientId = nextClientId;
      },
    });

    controller.start("http://localhost:3000");

    await vi.waitFor(() => expect(runtime.addServer).toHaveBeenCalledTimes(1));
    await vi.waitFor(() => expect(runtime.onSyncMessageReceived).toHaveBeenCalledWith(pingPayload));
    expect(clientId).toBe("server-client-3");
    await vi.waitFor(() => expect(runtime.removeServer).toHaveBeenCalledTimes(1));

    controller.stop();
  });

  it("sync outbox router routes server and client destinations", async () => {
    const onServerPayload = vi.fn().mockResolvedValue(undefined);
    const onClientPayload = vi.fn();
    const router = createSyncOutboxRouter({
      onServerPayload,
      onClientPayload,
    });

    router("server", "upstream-1", pingPayload, false);
    router("client", "client-1", pongPayload, false);

    await vi.waitFor(() => expect(onServerPayload).toHaveBeenCalledWith(pingPayload, false));
    expect(onClientPayload).toHaveBeenCalledWith(pongPayload);
  });

  it("sync outbox router surfaces server-send failures", async () => {
    const error = new Error("network down");
    const onServerPayload = vi.fn().mockRejectedValue(error);
    const onServerPayloadError = vi.fn();
    const router = createSyncOutboxRouter({
      onServerPayload,
      onServerPayloadError,
    });

    router("server", "upstream-1", pingPayload, false);

    await vi.waitFor(() => expect(onServerPayloadError).toHaveBeenCalledWith(error));
  });
});

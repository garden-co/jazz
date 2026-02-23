import { afterEach, describe, expect, it, vi } from "vitest";
import {
  buildEndpointUrl,
  buildEventsUrl,
  generateClientId,
  linkExternalIdentity,
  normalizePathPrefix,
  sendSyncPayload,
  SyncStreamController,
} from "./sync-transport.js";

describe("sync-transport", () => {
  const originalFetch = globalThis.fetch;
  const textEncoder = new TextEncoder();

  function encodeFrames(events: unknown[]): Uint8Array {
    const chunks: Uint8Array[] = events.map((event) => {
      const payload = textEncoder.encode(JSON.stringify(event));
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

    await sendSyncPayload("http://localhost:3000", { Ping: {} }, {});
    await sendSyncPayload("http://localhost:3000", { Pong: {} }, {});

    expect(fetchMock).toHaveBeenCalledTimes(2);

    const firstBody = JSON.parse(fetchMock.mock.calls[0][1].body as string);
    const secondBody = JSON.parse(fetchMock.mock.calls[1][1].body as string);

    expect(firstBody.client_id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    expect(firstBody.client_id).not.toBe("00000000-0000-0000-0000-000000000000");
    expect(secondBody.client_id).toBe(firstBody.client_id);
  });

  it("uses provided client_id when supplied", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const providedClientId = "11111111-2222-4333-8444-555555555555";
    await sendSyncPayload(
      "http://localhost:3000",
      { Ping: {} },
      { clientId: providedClientId, jwtToken: "token" },
    );

    const body = JSON.parse(fetchMock.mock.calls[0][1].body as string);
    expect(body.client_id).toBe(providedClientId);
  });

  it("throws on non-2xx sync POST responses", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue({ ok: false, status: 503, statusText: "Service Unavailable" });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await expect(sendSyncPayload("http://localhost:3000", { Ping: {} }, {})).rejects.toThrow(
      "Sync POST failed: 503 Service Unavailable",
    );
  });

  it("throws when fetch rejects", async () => {
    const fetchMock = vi.fn().mockRejectedValue(new Error("network down"));
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await expect(sendSyncPayload("http://localhost:3000", { Ping: {} }, {})).rejects.toThrow(
      "Sync POST failed: network down",
    );
  });

  it("posts to path-prefixed sync route when provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await sendSyncPayload(
      "http://localhost:3000/",
      { Ping: {} },
      { jwtToken: "token", pathPrefix: "apps/app-123/" },
    );

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0][0]).toBe("http://localhost:3000/apps/app-123/sync");
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
        { type: "SyncUpdate", payload: { Ping: {} } },
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
    await vi.waitFor(() =>
      expect(onSyncMessage).toHaveBeenCalledWith(JSON.stringify({ Ping: {} })),
    );
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
});

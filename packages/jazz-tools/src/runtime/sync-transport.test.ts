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
  sendSyncPayloadBatch,
  SyncStreamController,
  type RuntimeSyncOutboxCallback,
} from "./sync-transport.js";

describe("sync-transport", () => {
  const originalFetch = globalThis.fetch;
  const textEncoder = new TextEncoder();
  const outboxInvokers: Array<
    [
      name: string,
      invoke: (
        router: RuntimeSyncOutboxCallback,
        destinationKind: "server" | "client",
        destinationId: string,
        payloadJson: string,
        isCatalogue: boolean,
      ) => void,
    ]
  > = [
    [
      "wasm/rn",
      (
        router: RuntimeSyncOutboxCallback,
        destinationKind: "server" | "client",
        destinationId: string,
        payloadJson: string,
        isCatalogue: boolean,
      ) => router(destinationKind, destinationId, payloadJson, isCatalogue),
    ],
    [
      "napi-callee-handled",
      (
        router: RuntimeSyncOutboxCallback,
        destinationKind: "server" | "client",
        destinationId: string,
        payloadJson: string,
        isCatalogue: boolean,
      ) => router(null, destinationKind, destinationId, payloadJson, isCatalogue),
    ],
    [
      "napi-nested",
      (
        router: RuntimeSyncOutboxCallback,
        destinationKind: "server" | "client",
        destinationId: string,
        payloadJson: string,
        isCatalogue: boolean,
      ) => router(null, [destinationKind, destinationId, payloadJson, isCatalogue]),
    ],
  ];

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

    await sendSyncPayload("http://localhost:3000", JSON.stringify({ Ping: {} }), false, {});
    await sendSyncPayload("http://localhost:3000", JSON.stringify({ Pong: {} }), false, {});

    expect(fetchMock).toHaveBeenCalledTimes(2);

    const firstBody = JSON.parse(fetchMock.mock.calls[0]![1].body as string);
    const secondBody = JSON.parse(fetchMock.mock.calls[1]![1].body as string);

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
    await sendSyncPayload("http://localhost:3000", JSON.stringify({ Ping: {} }), false, {
      clientId: providedClientId,
      jwtToken: "token",
    });

    const body = JSON.parse(fetchMock.mock.calls[0]![1].body as string);
    expect(body.client_id).toBe(providedClientId);
  });

  it("throws on non-2xx sync POST responses", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue({ ok: false, status: 503, statusText: "Service Unavailable" });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await expect(
      sendSyncPayload("http://localhost:3000", JSON.stringify({ Ping: {} }), false, {}),
    ).rejects.toThrow("Sync POST failed: 503 Service Unavailable");
  });

  it("throws when fetch rejects", async () => {
    const fetchMock = vi.fn().mockRejectedValue(new Error("network down"));
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await expect(
      sendSyncPayload("http://localhost:3000", JSON.stringify({ Ping: {} }), false, {}),
    ).rejects.toThrow("Sync POST failed: network down");
  });

  it("posts to path-prefixed sync route when provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await sendSyncPayload("http://localhost:3000/", JSON.stringify({ Ping: {} }), false, {
      jwtToken: "token",
      pathPrefix: "apps/app-123/",
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]![0]).toBe("http://localhost:3000/apps/app-123/sync");
  });

  it("posts non-catalogue payloads with backend secret when provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await sendSyncPayload("http://localhost:3000", JSON.stringify({ Ping: {} }), false, {
      backendSecret: "backend-secret",
      jwtToken: "jwt-token",
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]![1].headers).toMatchObject({
      "Content-Type": "application/json",
      "X-Jazz-Backend-Secret": "backend-secret",
    });
    expect(fetchMock.mock.calls[0]![1].headers).not.toHaveProperty("Authorization");
    expect(fetchMock.mock.calls[0]![1].headers).not.toHaveProperty("X-Jazz-Local-Mode");
    expect(fetchMock.mock.calls[0]![1].headers).not.toHaveProperty("X-Jazz-Local-Token");
  });

  it("skips catalogue payload sync when admin secret is missing", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await sendSyncPayload(
      "http://localhost:3000",
      JSON.stringify({
        ObjectUpdated: {
          metadata: {
            metadata: {
              type: "catalogue_schema",
            },
          },
        },
      }),
      true,
      { jwtToken: "jwt-token" },
    );

    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("posts catalogue payloads with admin secret header", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await sendSyncPayload(
      "http://localhost:3000",
      JSON.stringify({
        ObjectUpdated: {
          metadata: {
            metadata: {
              type: "catalogue_lens",
            },
          },
        },
      }),
      true,
      { adminSecret: "admin-secret", jwtToken: "jwt-token" },
    );

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]![1].headers).toMatchObject({
      "Content-Type": "application/json",
      "X-Jazz-Admin-Secret": "admin-secret",
    });
    expect(fetchMock.mock.calls[0]![1].headers).not.toHaveProperty("Authorization");
    expect(fetchMock.mock.calls[0]![1].headers).not.toHaveProperty("X-Jazz-Local-Mode");
    expect(fetchMock.mock.calls[0]![1].headers).not.toHaveProperty("X-Jazz-Local-Token");
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
    expect(fetchMock.mock.calls[0]![0]).toBe(
      "http://localhost:3000/apps/app-123/auth/link-external",
    );
    expect(fetchMock.mock.calls[0]![1].method).toBe("POST");
    expect(fetchMock.mock.calls[0]![1].headers).toMatchObject({
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
        {
          type: "Connected",
          client_id: "server-client-1",
          catalogue_state_hash: "catalogue-1",
        },
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
    expect(onConnected).toHaveBeenCalledWith("catalogue-1");
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

  it("stream controller uses backend secret auth when provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue(streamResponse([]));
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const controller = new SyncStreamController({
      getAuth: () => ({ backendSecret: "backend-secret" }),
      getClientId: () => "initial-client-id",
      setClientId: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      onSyncMessage: vi.fn(),
    });

    controller.start("http://localhost:3000");
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));

    expect(fetchMock.mock.calls[0]![1].headers).toMatchObject({
      Accept: "application/octet-stream",
      "X-Jazz-Backend-Secret": "backend-secret",
    });
    expect(fetchMock.mock.calls[0]![1].headers).not.toHaveProperty("Authorization");

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
    const response = streamResponse([
      {
        type: "Connected",
        client_id: "server-client-4",
        catalogue_state_hash: "catalogue-4",
      },
    ]);
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

  it("logs schema warnings that arrive over the stream", async () => {
    const response = streamResponse([
      {
        type: "SyncUpdate",
        payload: {
          SchemaWarning: {
            queryId: 7,
            tableName: "todos",
            rowCount: 3,
            fromHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            toHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
          },
        },
      },
    ]);
    const reader = response.body!.getReader();
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const onSyncMessage = vi.fn();

    await readBinaryFrames(
      reader,
      {
        onSyncMessage,
      },
      "[client] ",
    );

    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining("[client] Detected 3 rows of todos with differing schema versions."),
    );
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining(
        "npx jazz-tools migrations create aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      ),
    );
    expect(onSyncMessage).toHaveBeenCalledWith(
      JSON.stringify({
        SchemaWarning: {
          queryId: 7,
          tableName: "todos",
          rowCount: 3,
          fromHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          toHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        },
      }),
    );
  });

  it("runtime-bound stream controller maps stream events to runtime hooks", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      streamResponse([
        {
          type: "Connected",
          client_id: "server-client-3",
          catalogue_state_hash: "catalogue-3",
        },
        { type: "SyncUpdate", payload: { Ping: {} } },
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
    expect(runtime.addServer).toHaveBeenCalledWith("catalogue-3");
    await vi.waitFor(() =>
      expect(runtime.onSyncMessageReceived).toHaveBeenCalledWith(JSON.stringify({ Ping: {} })),
    );
    expect(clientId).toBe("server-client-3");
    await vi.waitFor(() => expect(runtime.removeServer).toHaveBeenCalledTimes(1));

    controller.stop();
  });

  it.each(outboxInvokers)(
    "sync outbox router routes server and client destinations (%s)",
    async (_name, invoke) => {
      const onServerPayload = vi.fn().mockResolvedValue(undefined);
      const onClientPayload = vi.fn();
      const router = createSyncOutboxRouter({
        onServerPayload,
        onClientPayload,
      });

      invoke(router, "server", "upstream-1", JSON.stringify({ Ping: {} }), false);
      invoke(router, "client", "client-1", JSON.stringify({ Pong: {} }), false);

      await vi.waitFor(() =>
        expect(onServerPayload).toHaveBeenCalledWith(JSON.stringify({ Ping: {} }), false),
      );
      expect(onClientPayload).toHaveBeenCalledWith(JSON.stringify({ Pong: {} }));
    },
  );

  it("sync outbox router accepts the real nested NAPI callback shape", async () => {
    const onServerPayload = vi.fn().mockResolvedValue(undefined);
    const router = createSyncOutboxRouter({
      onServerPayload,
    });

    router(null, ["server", "upstream-1", JSON.stringify({ Ping: {} }), false]);

    await vi.waitFor(() =>
      expect(onServerPayload).toHaveBeenCalledWith(JSON.stringify({ Ping: {} }), false),
    );
  });

  it.each(outboxInvokers)(
    "sync outbox router posts server payloads via sendSyncPayload (%s)",
    async (_name, invoke) => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200, statusText: "OK" });
      (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

      const router = createSyncOutboxRouter({
        onServerPayload: (payload, isCatalogue) =>
          sendSyncPayload("http://localhost:3000", payload as string, isCatalogue, {
            backendSecret: "backend-secret",
          }),
      });

      const payloadJson = JSON.stringify({ QuerySubscription: { id: "q-1" } });
      invoke(router, "server", "upstream-1", payloadJson, false);

      await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));

      const requestBody = JSON.parse(fetchMock.mock.calls[0]![1].body as string) as {
        payloads: unknown[];
      };
      expect(requestBody.payloads).toEqual([JSON.parse(payloadJson)]);
      expect(fetchMock.mock.calls[0]![0]).toBe("http://localhost:3000/sync");
      expect(fetchMock.mock.calls[0]![1].headers).toMatchObject({
        "X-Jazz-Backend-Secret": "backend-secret",
      });
    },
  );

  it("sync outbox router surfaces server-send failures", async () => {
    const error = new Error("network down");
    const onServerPayload = vi.fn().mockRejectedValue(error);
    const onServerPayloadError = vi.fn();
    const router = createSyncOutboxRouter({
      onServerPayload,
      onServerPayloadError,
    });

    router("server", "upstream-1", JSON.stringify({ Ping: {} }), false);

    await vi.waitFor(() => expect(onServerPayloadError).toHaveBeenCalledWith(error));
  });

  // ---------------------------------------------------------------------------
  // sendSyncPayloadBatch
  //
  // RED: sendSyncPayloadBatch does not exist yet — these tests will fail at
  // import time until it is exported from sync-transport.ts.
  // ---------------------------------------------------------------------------

  describe("sendSyncPayloadBatch", () => {
    const playerPayload = (id: string) =>
      JSON.stringify({
        ObjectUpdated: { object_id: id, branch_name: "main", commits: [] },
      });

    it("sends all payloads in a single POST using the always-array wire format", async () => {
      // alice updates her position 3 times in one tick — should be 1 fetch call,
      // not 3
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
      (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

      const payloads = [playerPayload("id-1"), playerPayload("id-2"), playerPayload("id-3")];

      await sendSyncPayloadBatch("http://localhost:3000", payloads, {
        jwtToken: "alice-token",
        clientId: "client-alice",
      });

      expect(fetchMock).toHaveBeenCalledTimes(1);

      const body = JSON.parse(fetchMock.mock.calls[0]![1].body as string);
      expect(body.payloads).toHaveLength(3);
      expect(body.client_id).toBe("client-alice");
      // Each element is the parsed payload, not the raw JSON string
      expect(body.payloads[0]!).toEqual(JSON.parse(payloads[0]!));
      expect(body.payloads[1]!).toEqual(JSON.parse(payloads[1]!));
      expect(body.payloads[2]!).toEqual(JSON.parse(payloads[2]!));
    });

    it("preserves payload order in the POST body", async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
      (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

      // bob's collected flag goes false→true→false in one tick; order must be preserved
      const p1 = JSON.stringify({
        ObjectUpdated: { object_id: "p1", branch_name: "main", commits: [] },
      });
      const p2 = JSON.stringify({
        ObjectUpdated: { object_id: "p2", branch_name: "main", commits: [] },
      });
      const p3 = JSON.stringify({
        ObjectUpdated: { object_id: "p3", branch_name: "main", commits: [] },
      });

      await sendSyncPayloadBatch("http://localhost:3000", [p1, p2, p3], {
        jwtToken: "bob-token",
      });

      const body = JSON.parse(fetchMock.mock.calls[0]![1].body as string);
      expect(body.payloads[0]!.ObjectUpdated.object_id).toBe("p1");
      expect(body.payloads[1]!.ObjectUpdated.object_id).toBe("p2");
      expect(body.payloads[2]!.ObjectUpdated.object_id).toBe("p3");
    });

    it("applies JWT auth header", async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
      (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

      await sendSyncPayloadBatch("http://localhost:3000", [playerPayload("id-1")], {
        jwtToken: "alice-jwt",
      });

      expect(fetchMock.mock.calls[0]![1].headers).toMatchObject({
        Authorization: "Bearer alice-jwt",
      });
    });

    it("posts to path-prefixed route when provided", async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
      (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

      await sendSyncPayloadBatch("http://localhost:3000", [playerPayload("id-1")], {
        jwtToken: "token",
        pathPrefix: "apps/app-42",
      });

      expect(fetchMock.mock.calls[0]![0]).toBe("http://localhost:3000/apps/app-42/sync");
    });

    it("throws on non-2xx response", async () => {
      const fetchMock = vi
        .fn()
        .mockResolvedValue({ ok: false, status: 503, statusText: "Service Unavailable" });
      (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

      await expect(
        sendSyncPayloadBatch("http://localhost:3000", [playerPayload("id-1")], {}),
      ).rejects.toThrow("503");
    });
  });
});

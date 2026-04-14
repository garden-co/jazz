import { afterEach, describe, expect, it, vi } from "vitest";
import {
  buildEndpointUrl,
  buildWsUrl,
  createSyncOutboxRouter,
  generateClientId,
  isExpectedFetchAbortError,
  linkExternalIdentity,
  normalizePathPrefix,
  type RuntimeSyncOutboxCallback,
} from "./sync-transport.js";

describe("sync-transport", () => {
  const originalFetch = globalThis.fetch;
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

  afterEach(() => {
    (globalThis as { fetch: typeof fetch }).fetch = originalFetch;
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("generateClientId returns UUIDv4 format", () => {
    const id = generateClientId();
    expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
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
  });

  it("buildWsUrl converts http to ws and appends /ws endpoint", () => {
    expect(buildWsUrl("http://localhost:3000")).toBe("ws://localhost:3000/ws");
    expect(buildWsUrl("https://api.example.com/")).toBe("wss://api.example.com/ws");
    expect(buildWsUrl("http://localhost:3000", "apps/app-1")).toBe(
      "ws://localhost:3000/apps/app-1/ws",
    );
    expect(buildWsUrl("https://example.com", "/apps/app-1/")).toBe(
      "wss://example.com/apps/app-1/ws",
    );
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
});

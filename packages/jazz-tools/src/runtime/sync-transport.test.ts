import { afterEach, describe, expect, it, vi } from "vitest";
import {
  buildEndpointUrl,
  buildEventsUrl,
  generateClientId,
  normalizePathPrefix,
  sendSyncPayload,
} from "./sync-transport.js";

describe("sync-transport", () => {
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    (globalThis as { fetch: typeof fetch }).fetch = originalFetch;
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
});

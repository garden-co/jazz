import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchSchemaHashes, fetchStoredWasmSchema } from "./schema-fetch.js";
import { fetchServerSubscriptions } from "./introspection-fetch.js";

describe("schema-fetch", () => {
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    (globalThis as { fetch: typeof fetch }).fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it("fetches the schema endpoint with admin secret header", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: "OK",
      json: async () => ({ users: { columns: [] } }),
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const result = await fetchStoredWasmSchema("http://localhost:1625/", {
      adminSecret: "admin-secret",
      pathPrefix: "/apps/app-123",
      schemaHash: hash,
    });

    expect(result.schema.users).toBeDefined();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0][0]).toBe(`http://localhost:1625/apps/app-123/schema/${hash}`);
    expect(fetchMock.mock.calls[0][1]).toMatchObject({
      method: "GET",
      headers: {
        "X-Jazz-Admin-Secret": "admin-secret",
      },
    });
  });

  it("throws a descriptive error on non-2xx responses", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 404,
      statusText: "Not Found",
      text: async () => '{"error":"missing"}',
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await expect(
      fetchStoredWasmSchema("http://localhost:1625", {
        adminSecret: "admin-secret",
        schemaHash:
          "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
      }),
    ).rejects.toThrow('Schema fetch failed: 404 Not Found - {"error":"missing"}');
  });

  it("fetches schema hashes with admin secret header", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: "OK",
      json: async () => ({
        hashes: ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
      }),
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const result = await fetchSchemaHashes("http://localhost:1625/", {
      adminSecret: "admin-secret",
      pathPrefix: "/apps/app-123",
    });

    expect(result.hashes).toEqual([
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ]);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0][0]).toBe("http://localhost:1625/apps/app-123/schemas");
    expect(fetchMock.mock.calls[0][1]).toMatchObject({
      method: "GET",
      headers: {
        "X-Jazz-Admin-Secret": "admin-secret",
      },
    });
  });

  it("fetches the requested schema hash when provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: "OK",
      json: async () => ({ users: { columns: [] } }),
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await fetchStoredWasmSchema("http://localhost:1625/", {
      adminSecret: "admin-secret",
      schemaHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    });

    expect(fetchMock.mock.calls[0][0]).toBe(
      "http://localhost:1625/schema/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );
  });

  it("fetches grouped server subscriptions with admin secret and app id", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: "OK",
      json: async () => ({
        appId: "app-123",
        generatedAt: 1741600800000,
        queries: [
          {
            groupKey: "group-1",
            count: 2,
            table: "todos",
            query: '{"table":"todos"}',
            branches: ["main"],
            propagation: "full",
          },
        ],
      }),
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const result = await fetchServerSubscriptions("http://localhost:1625/", {
      adminSecret: "admin-secret",
      appId: "test-app",
      pathPrefix: "/apps/app-123",
    });

    expect(result.appId).toBe("app-123");
    expect(result.queries).toHaveLength(1);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0][0]).toBe(
      "http://localhost:1625/apps/app-123/admin/introspection/subscriptions?appId=test-app",
    );
    expect(fetchMock.mock.calls[0][1]).toMatchObject({
      method: "GET",
      headers: {
        "X-Jazz-Admin-Secret": "admin-secret",
      },
    });
  });

  it("throws a descriptive error when server subscription fetch fails", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 401,
      statusText: "Unauthorized",
      text: async () => '{"error":"bad secret"}',
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await expect(
      fetchServerSubscriptions("http://localhost:1625", {
        adminSecret: "wrong-secret",
        appId: "test-app",
      }),
    ).rejects.toThrow(
      'Server subscriptions fetch failed: 401 Unauthorized - {"error":"bad secret"}',
    );
  });
});

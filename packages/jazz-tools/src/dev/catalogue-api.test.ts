import { schema as s } from "../index.js";
import { wasmSchemasEqual } from "./schema-utils.js";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  fetchPublishedBundles,
  fetchSchemaHashes,
  fetchStoredWasmSchema,
} from "./catalogue-api.js";
import { fetchServerSubscriptions } from "../runtime/introspection-fetch.js";

describe("catalogue-api", () => {
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
      json: async () => ({
        schema: { tables: { users: { columns: [] } } },
        publishedAt: 1_744_011_200_000,
      }),
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const result = await fetchStoredWasmSchema("http://localhost:1625/", {
      appId: "app-123",
      adminSecret: "admin-secret",
      schemaHash: hash,
    });

    expect(result.schema.users).toBeDefined();
    expect(result.publishedAt).toBe(1_744_011_200_000);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]![0]).toBe(`http://localhost:1625/apps/app-123/schema/${hash}`);
    expect(fetchMock.mock.calls[0]![1]).toMatchObject({
      method: "GET",
      headers: {
        "X-Jazz-Admin-Secret": "admin-secret",
      },
    });
  });

  it("preserves millisecond publishedAt values", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: "OK",
      json: async () => ({
        schema: { tables: { users: { columns: [] } } },
        publishedAt: 1_744_011_200_000,
      }),
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const result = await fetchStoredWasmSchema("http://localhost:1625/", {
      appId: "app-123",
      adminSecret: "admin-secret",
      schemaHash: hash,
    });

    expect(result.publishedAt).toBe(1_744_011_200_000);
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
        appId: "test-app",
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
      appId: "app-123",
      adminSecret: "admin-secret",
    });

    expect(result.hashes).toEqual([
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ]);
    expect(result.schemas).toEqual([]);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]![0]).toBe("http://localhost:1625/apps/app-123/schemas");
    expect(fetchMock.mock.calls[0]![1]).toMatchObject({
      method: "GET",
      headers: {
        "X-Jazz-Admin-Secret": "admin-secret",
      },
    });
  });

  it("fetches schema hash upload metadata when present", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: "OK",
      json: async () => ({
        hashes: ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        schemas: [
          {
            hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            publishedAt: 1_744_011_200_000,
          },
        ],
      }),
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const result = await fetchSchemaHashes("http://localhost:1625/", {
      appId: "app-123",
      adminSecret: "admin-secret",
    });

    expect(result.hashes).toEqual([
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ]);
    expect(result.schemas).toEqual([
      {
        hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        publishedAt: 1_744_011_200_000,
      },
    ]);
  });

  it("fetches the requested schema hash when provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: "OK",
      json: async () => ({
        schema: { tables: { users: { columns: [] } } },
        publishedAt: null,
      }),
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await fetchStoredWasmSchema("http://localhost:1625/", {
      appId: "test-app",
      adminSecret: "admin-secret",
      schemaHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    });

    expect(fetchMock.mock.calls[0]![0]).toBe(
      "http://localhost:1625/apps/test-app/schema/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );
  });

  it("lists published bundles with admin secret header", async () => {
    const bundle = {
      bundle: "b".repeat(64),
      schemaHash: "a".repeat(64),
      publishedAt: 1_744_011_200_000,
      permissions: { users: { select: { using: { type: "True" } } } },
    };
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: "OK",
      json: async () => ({ bundles: [bundle] }),
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const result = await fetchPublishedBundles("http://localhost:1625/", {
      appId: "app-123",
      adminSecret: "admin-secret",
    });

    expect(result).toEqual([bundle]);
    expect(fetchMock.mock.calls[0]![0]).toBe("http://localhost:1625/apps/app-123/admin/bundles");
    expect(fetchMock.mock.calls[0]![1]).toMatchObject({
      method: "GET",
      headers: { "X-Jazz-Admin-Secret": "admin-secret" },
    });
  });

  it("throws a descriptive error when listing bundles fails", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 401,
      statusText: "Unauthorized",
      text: async () => '{"error":"bad secret"}',
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    await expect(
      fetchPublishedBundles("http://localhost:1625", {
        appId: "test-app",
        adminSecret: "admin-secret",
      }),
    ).rejects.toThrow('Bundles fetch failed: 401 Unauthorized - {"error":"bad secret"}');
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
          },
        ],
      }),
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const result = await fetchServerSubscriptions("http://localhost:1625/", {
      adminSecret: "admin-secret",
      appId: "test-app",
    });

    expect(result.appId).toBe("app-123");
    expect(result.queries).toHaveLength(1);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]![0]).toBe(
      "http://localhost:1625/apps/test-app/admin/introspection/subscriptions?appId=test-app",
    );
    expect(fetchMock.mock.calls[0]![1]).toMatchObject({
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

import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchSchemaHashes, fetchStoredWasmSchema } from "./schema-fetch.js";

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
      json: async () => ({ tables: { users: { columns: [] } } }),
    });
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    const hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const result = await fetchStoredWasmSchema("http://localhost:1625/", {
      adminSecret: "admin-secret",
      pathPrefix: "/apps/app-123",
      schemaHash: hash,
    });

    expect(result.schema.tables.users).toBeDefined();
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
      json: async () => ({ tables: { users: { columns: [] } } }),
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
});

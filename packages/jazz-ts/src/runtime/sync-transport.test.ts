import { afterEach, describe, expect, it, vi } from "vitest";
import { isCataloguePayload, sendSyncPayload } from "./sync-transport.js";

const originalFetch = globalThis.fetch;

describe("sync transport", () => {
  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  it("uses provided client ID in sync payload", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    await sendSyncPayload(
      "https://example.test",
      { ObjectUpdated: { metadata: { metadata: { type: "todo" } } } },
      { clientId: "550e8400-e29b-41d4-a716-446655440000" },
    );

    const init = fetchMock.mock.calls[0][1] as RequestInit;
    const body = JSON.parse(init.body as string);
    expect(body.client_id).toBe("550e8400-e29b-41d4-a716-446655440000");
  });

  it("uses JWT auth for non-catalogue payloads", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    await sendSyncPayload(
      "https://example.test",
      { ObjectUpdated: { metadata: { metadata: { type: "todo" } } } },
      {
        jwtToken: "jwt-token",
        clientId: "550e8400-e29b-41d4-a716-446655440000",
      },
    );

    const init = fetchMock.mock.calls[0][1] as RequestInit;
    const headers = init.headers as Record<string, string>;
    expect(headers["Authorization"]).toBe("Bearer jwt-token");
  });

  it("uses admin secret for catalogue payloads", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const payload = {
      ObjectUpdated: {
        metadata: {
          metadata: {
            type: "catalogue_schema",
          },
        },
      },
    };

    expect(isCataloguePayload(payload)).toBe(true);

    await sendSyncPayload("https://example.test", payload, {
      adminSecret: "admin-secret",
      jwtToken: "jwt-token",
      clientId: "550e8400-e29b-41d4-a716-446655440000",
    });

    const init = fetchMock.mock.calls[0][1] as RequestInit;
    const headers = init.headers as Record<string, string>;
    expect(headers["X-Jazz-Admin-Secret"]).toBe("admin-secret");
    expect(headers["Authorization"]).toBeUndefined();
  });
});

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

  it("uses a generated fallback client ID when none is provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    await sendSyncPayload(
      "https://example.test",
      { ObjectUpdated: { metadata: { metadata: { type: "todo" } } } },
      {},
    );
    await sendSyncPayload(
      "https://example.test",
      { ObjectUpdated: { metadata: { metadata: { type: "todo" } } } },
      {},
    );

    const firstBody = JSON.parse((fetchMock.mock.calls[0][1] as RequestInit).body as string);
    const secondBody = JSON.parse((fetchMock.mock.calls[1][1] as RequestInit).body as string);

    expect(firstBody.client_id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    );
    expect(firstBody.client_id).not.toBe("00000000-0000-0000-0000-000000000000");
    expect(secondBody.client_id).toBe(firstBody.client_id);
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

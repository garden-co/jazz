import { describe, expect, it, vi } from "vitest";
import {
  createTestJwt,
  expectAllowed,
  expectDenied,
  requestForClaims,
  scopedClientForClaims,
  seedRows,
} from "./index.js";

function decodeJwtPayload(token: string): Record<string, unknown> {
  const payload = token.split(".")[1];
  const base64 = payload.replace(/-/g, "+").replace(/_/g, "/");
  const padded = base64 + "=".repeat((4 - (base64.length % 4)) % 4);
  return JSON.parse(Buffer.from(padded, "base64").toString("utf8")) as Record<string, unknown>;
}

describe("testing helpers", () => {
  it("creates a request with bearer token claims", () => {
    const request = requestForClaims({
      sub: "user-123",
      claims: { role: "admin" },
    });

    const auth = (request.headers as Record<string, string>).authorization;
    expect(auth).toMatch(/^Bearer /);
    const token = auth.slice("Bearer ".length);
    expect(decodeJwtPayload(token)).toEqual({
      sub: "user-123",
      claims: { role: "admin" },
    });
  });

  it("creates a scoped client via forRequest", () => {
    const scoped = { query: vi.fn() };
    const forRequest = vi.fn((request: unknown) => {
      void request;
      return scoped;
    });
    const client = { forRequest } as unknown as Parameters<typeof scopedClientForClaims>[0];

    const result = scopedClientForClaims(client, { sub: "user-9" });

    expect(result).toBe(scoped);
    expect(forRequest).toHaveBeenCalledTimes(1);
    const firstCall = forRequest.mock.calls.at(0);
    expect(firstCall).toBeDefined();
    const request = firstCall![0] as { headers: Record<string, string> };
    expect(request.headers.authorization).toMatch(/^Bearer /);
  });

  it("supports allow/deny assertions", async () => {
    await expect(expectAllowed(async () => "ok")).resolves.toBeUndefined();
    await expect(
      expectDenied(async () => {
        throw new Error("permission denied");
      }),
    ).resolves.toBeInstanceOf(Error);

    await expect(
      expectDenied(
        async () => {
          throw new Error("denied by policy");
        },
        { match: /policy/ },
      ),
    ).resolves.toBeInstanceOf(Error);

    await expect(expectDenied(async () => "ok")).rejects.toThrow(
      "Expected operation to be denied, but it succeeded.",
    );
  });

  it("seeds rows through db.insert", async () => {
    const insert = vi.fn((_table: unknown, row: unknown) => JSON.stringify(row));
    const db = { insert } as unknown as Parameters<typeof seedRows>[0];
    const table = { _table: "todos" } as unknown as Parameters<typeof seedRows>[1];

    const ids = await seedRows(db, table, [
      { title: "a", owner_id: "u1" },
      { title: "b", owner_id: "u2" },
    ]);

    expect(insert).toHaveBeenCalledTimes(2);
    expect(ids).toEqual([
      JSON.stringify({ title: "a", owner_id: "u1" }),
      JSON.stringify({ title: "b", owner_id: "u2" }),
    ]);
  });

  it("creates unsigned JWT fixtures", () => {
    const token = createTestJwt({ sub: "user-1", claims: { org: "acme" } });
    expect(token.split(".")).toHaveLength(3);
    expect(decodeJwtPayload(token)).toEqual({
      sub: "user-1",
      claims: { org: "acme" },
    });
  });
});

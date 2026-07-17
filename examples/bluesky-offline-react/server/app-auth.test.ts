import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  createBffSession: vi.fn(),
  currentSession: vi.fn(),
  invalidateBffSession: vi.fn(),
  jazzToken: vi.fn(),
  oauthAuthorize: vi.fn(),
  oauthCallback: vi.fn(),
  oauthRevoke: vi.fn(),
  projectThread: vi.fn(),
  projectTimelinePage: vi.fn(),
  getTimelineProjectionStatus: vi.fn(),
  reconcileOperations: vi.fn(),
}));

vi.mock("./auth.js", () => ({
  bffSessionCookie: "bff-session",
  createBffSession: mocks.createBffSession,
  currentSession: mocks.currentSession,
  invalidateBffSession: mocks.invalidateBffSession,
  jazzJwks: { keys: [] },
  jazzToken: mocks.jazzToken,
  oauth: {
    authorize: mocks.oauthAuthorize,
    callback: mocks.oauthCallback,
    revoke: mocks.oauthRevoke,
  },
  oauthScope: "atproto transition:generic",
}));

vi.mock("./bridge.js", () => {
  class OperationError extends Error {
    constructor(message: string, readonly status: 400 | 502) {
      super(message);
    }
  }
  return {
    OperationError,
    projectThread: mocks.projectThread,
    projectTimelinePage: mocks.projectTimelinePage,
    getTimelineProjectionStatus: mocks.getTimelineProjectionStatus,
    reconcileOperations: mocks.reconcileOperations,
  };
});

import { OperationError } from "./bridge.js";
import { createServer } from "./app.js";

const authenticatedSession = {
  did: "did:plc:alice",
  session: { fetchHandler: vi.fn() },
};

describe("BFF authentication routes", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.createBffSession.mockResolvedValue("opaque-session-id");
    mocks.currentSession.mockResolvedValue(null);
    mocks.invalidateBffSession.mockResolvedValue(undefined);
    mocks.jazzToken.mockResolvedValue("jazz-jwt");
    mocks.oauthCallback.mockResolvedValue({ session: { did: "did:plc:alice" } });
    mocks.oauthRevoke.mockResolvedValue(undefined);
    mocks.projectTimelinePage.mockResolvedValue({ count: 1 });
  });

  it("sets an opaque HTTP-only session cookie with origin-appropriate security", async () => {
    const httpResponse = await createServer("http://127.0.0.1:5173")
      .request("/api/auth/callback?code=code&state=state");
    const httpCookie = httpResponse.headers.get("set-cookie") ?? "";

    expect(httpCookie).toContain("bff-session=opaque-session-id");
    expect(httpCookie).not.toContain("did:plc:alice");
    expect(httpCookie).toContain("HttpOnly");
    expect(httpCookie).toContain("SameSite=Lax");
    expect(httpCookie).toContain("Path=/");
    expect(httpCookie).not.toContain("Secure");

    const httpsResponse = await createServer("https://example.test")
      .request("/api/auth/callback?code=code&state=state");
    expect(httpsResponse.headers.get("set-cookie")).toContain("Secure");
  });

  it("rejects unknown session IDs before any authenticated route runs", async () => {
    const requests = [
      new Request("http://localhost/api/session", { headers: { cookie: "bff-session=forged-session-id" } }),
      new Request("http://localhost/api/timeline", { headers: { cookie: "bff-session=forged-session-id" } }),
      new Request("http://localhost/api/thread?uri=at://did:plc:alice/app.bsky.feed.post/one", {
        headers: { cookie: "bff-session=forged-session-id" },
      }),
      new Request("http://localhost/api/operations", {
        method: "POST",
        headers: { cookie: "bff-session=forged-session-id", "content-type": "application/json" },
        body: "[]",
      }),
    ];

    for (const request of requests) {
      const response = await createServer().request(request);
      expect(response.status).toBe(401);
      expect(await response.json()).toEqual({ error: "not signed in" });
    }
    expect(mocks.projectTimelinePage).not.toHaveBeenCalled();
    expect(mocks.projectThread).not.toHaveBeenCalled();
    expect(mocks.reconcileOperations).not.toHaveBeenCalled();
  });

  it("invalidates the server-side mapping and cookie on logout", async () => {
    mocks.invalidateBffSession.mockResolvedValue("did:plc:alice");

    const response = await createServer().request("/api/auth/logout", {
      method: "POST",
      headers: { cookie: "bff-session=opaque-session-id" },
    });

    expect(mocks.invalidateBffSession).toHaveBeenCalledWith("opaque-session-id");
    expect(mocks.oauthRevoke).toHaveBeenCalledWith("did:plc:alice");
    expect(response.headers.get("set-cookie")).toContain("bff-session=");
    expect(response.headers.get("set-cookie")).toContain("Path=/");
  });

  it("shares the session guard across authenticated routes", async () => {
    mocks.currentSession.mockResolvedValue(authenticatedSession);

    const response = await createServer().request("/api/timeline", {
      headers: { cookie: "bff-session=opaque-session-id" },
    });

    expect(response.status).toBe(200);
    expect(mocks.projectTimelinePage).toHaveBeenCalledWith(
      "did:plc:alice",
      authenticatedSession.session,
      undefined,
    );
  });

  it("exposes asynchronous projection status behind the same session guard", async () => {
    mocks.currentSession.mockResolvedValue(authenticatedSession);
    mocks.getTimelineProjectionStatus.mockReturnValue({
      id: "projection-id",
      ownerDid: "did:plc:alice",
      state: "projecting",
      acceptedAt: "2026-07-16T18:00:00.000Z",
    });

    const response = await createServer().request("/api/timeline/status", {
      headers: { cookie: "bff-session=opaque-session-id" },
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ id: "projection-id", state: "projecting" });
  });

  it("maps expected operation errors and unexpected bridge failures centrally", async () => {
    mocks.currentSession.mockResolvedValue(authenticatedSession);
    mocks.projectTimelinePage.mockRejectedValueOnce(new OperationError("invalid request", 400));
    const expected = await createServer().request("/api/timeline", {
      headers: { cookie: "bff-session=opaque-session-id" },
    });

    mocks.projectTimelinePage.mockRejectedValueOnce(new Error("upstream failed"));
    const unexpected = await createServer().request("/api/timeline", {
      headers: { cookie: "bff-session=opaque-session-id" },
    });

    expect(expected.status).toBe(400);
    expect(await expected.json()).toEqual({ error: "invalid request" });
    expect(unexpected.status).toBe(502);
    expect(await unexpected.json()).toEqual({ error: "upstream failed" });
  });

  it("validates and decodes queued operations before reconciliation", async () => {
    mocks.currentSession.mockResolvedValue(authenticatedSession);
    const malformed = await createServer().request("/api/operations", {
      method: "POST",
      headers: { cookie: "bff-session=opaque-session-id", "content-type": "application/json" },
      body: JSON.stringify([{ kind: "post" }]),
    });
    expect(malformed.status).toBe(400);
    expect(mocks.reconcileOperations).not.toHaveBeenCalled();

    const row = {
      id: "00000000-0000-0000-0000-000000000001",
      ownerDid: "did:plc:alice",
      kind: "post",
      rkey: "3mpost",
      payload: JSON.stringify({ text: "Hello", createdAt: "2026-07-16T18:00:00.000Z" }),
      state: "queued",
      createdAt: "2026-07-16T18:00:00.000Z",
    };
    const accepted = await createServer().request("/api/operations", {
      method: "POST",
      headers: { cookie: "bff-session=opaque-session-id", "content-type": "application/json" },
      body: JSON.stringify([row]),
    });
    expect(accepted.status).toBe(200);
    expect(mocks.reconcileOperations).toHaveBeenCalledWith(
      "did:plc:alice",
      authenticatedSession.session,
      [expect.objectContaining({ kind: "post", payload: { text: "Hello", createdAt: row.createdAt } })],
    );
  });

  it("rejects operation rows owned by another Jazz user", async () => {
    mocks.currentSession.mockResolvedValue(authenticatedSession);
    const response = await createServer().request("/api/operations", {
      method: "POST",
      headers: { cookie: "bff-session=opaque-session-id", "content-type": "application/json" },
      body: JSON.stringify([{
        id: "00000000-0000-0000-0000-000000000001",
        ownerDid: "did:plc:mallory",
        kind: "post",
        rkey: "3mpost",
        payload: JSON.stringify({ text: "Hello", createdAt: "2026-07-16T18:00:00.000Z" }),
        state: "queued",
        createdAt: "2026-07-16T18:00:00.000Z",
      }]),
    });
    expect(response.status).toBe(403);
    expect(mocks.reconcileOperations).not.toHaveBeenCalled();
  });
});

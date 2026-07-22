import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  activateTimeline: vi.fn(),
  createJazzToken: vi.fn(),
  projectNextTimelinePage: vi.fn(),
  projectThread: vi.fn(),
  reconcileOperations: vi.fn(),
  restoreBffSession: vi.fn(),
}));

vi.mock("../../server/auth.js", () => ({
  bffSessionCookie: "bff-session",
  createBffSession: vi.fn(),
  createJazzToken: mocks.createJazzToken,
  invalidateBffSession: vi.fn(),
  jazzJwks: { keys: [] },
  oauth: { authorize: vi.fn(), callback: vi.fn(), revoke: vi.fn() },
  oauthScope: "atproto transition:generic",
  restoreBffSession: mocks.restoreBffSession,
}));

vi.mock("../../server/bridge.js", () => ({
  activateTimeline: mocks.activateTimeline,
  projectNextTimelinePage: mocks.projectNextTimelinePage,
  projectThread: mocks.projectThread,
  reconcileOperations: mocks.reconcileOperations,
}));

import { createServer } from "../../server/app.js";

const authenticatedSession = {
  did: "did:plc:alice",
  session: { fetchHandler: vi.fn() },
};

const queuedPost = {
  id: "00000000-0000-0000-0000-000000000001",
  ownerDid: "did:plc:alice",
  kind: "post",
  rkey: "3mpost",
  payload: JSON.stringify({ text: "Hello", createdAt: "2026-07-16T18:00:00.000Z" }),
  state: "queued",
  createdAt: "2026-07-16T18:00:00.000Z",
};

describe("BFF routes", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mocks.restoreBffSession.mockResolvedValue(authenticatedSession);
    mocks.createJazzToken.mockResolvedValue("jazz-jwt");
  });

  it("exchanges the BFF session for the matching Jazz identity", async () => {
    const response = await createServer().request("/api/session", {
      headers: { cookie: "bff-session=opaque-session-id" },
    });

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ did: "did:plc:alice", token: "jazz-jwt" });
    expect(mocks.restoreBffSession).toHaveBeenCalledWith("opaque-session-id");
    expect(mocks.createJazzToken).toHaveBeenCalledWith("did:plc:alice");
    expect(mocks.activateTimeline).toHaveBeenCalledWith(
      "did:plc:alice",
      authenticatedSession.session,
    );
  });

  it("asks the BFF for the next page without exposing its AppView cursor", async () => {
    const metadata = {
      cursor: "next-page",
      hasMore: true,
      count: 20,
      projection: { id: "projection-id", state: "accepted" },
    };
    mocks.projectNextTimelinePage.mockResolvedValue(metadata);

    const response = await createServer().request("/api/timeline/more", {
      headers: { cookie: "bff-session=opaque-session-id" },
    });

    expect(await response.json()).toEqual({
      cursor: "next-page",
      hasMore: true,
      count: 20,
    });
    expect(mocks.projectNextTimelinePage).toHaveBeenCalledWith(
      "did:plc:alice",
      authenticatedSession.session,
    );
  });

  it("decodes Jazz outbox rows before reconciling them with ATProto", async () => {
    const response = await createServer().request("/api/operations", {
      method: "POST",
      headers: { cookie: "bff-session=opaque-session-id", "content-type": "application/json" },
      body: JSON.stringify([queuedPost]),
    });

    expect(response.status).toBe(200);
    expect(mocks.reconcileOperations).toHaveBeenCalledWith(
      "did:plc:alice",
      authenticatedSession.session,
      [
        expect.objectContaining({
          kind: "post",
          payload: { text: "Hello", createdAt: queuedPost.createdAt },
        }),
      ],
    );
  });

  it("rejects Jazz outbox rows owned by another authenticated user", async () => {
    const response = await createServer().request("/api/operations", {
      method: "POST",
      headers: { cookie: "bff-session=opaque-session-id", "content-type": "application/json" },
      body: JSON.stringify([{ ...queuedPost, ownerDid: "did:plc:mallory" }]),
    });

    expect(response.status).toBe(403);
    expect(mocks.reconcileOperations).not.toHaveBeenCalled();
  });

  it.each([
    ["/api/auth/login", undefined],
    ["/api/operations", "bff-session=opaque-session-id"],
  ])("rejects malformed JSON sent to %s", async (path, cookie) => {
    const response = await createServer().request(path, {
      method: "POST",
      headers: {
        ...(cookie ? { cookie } : {}),
        "content-type": "application/json",
      },
      body: "{",
    });

    expect(response.status).toBe(400);
  });

  it.each(["at://incomplete", "at://did:plc:alice/app.bsky.feed.like/3mlike"])(
    "rejects invalid post URI %s",
    async (uri) => {
      const response = await createServer().request(`/api/thread?uri=${encodeURIComponent(uri)}`, {
        headers: { cookie: "bff-session=opaque-session-id" },
      });

      expect(response.status).toBe(400);
      expect(mocks.projectThread).not.toHaveBeenCalled();
    },
  );

  it("does not expose unexpected server errors", async () => {
    mocks.projectNextTimelinePage.mockRejectedValue(new Error("database password leaked"));
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);

    const response = await createServer().request("/api/timeline/more", {
      headers: { cookie: "bff-session=opaque-session-id" },
    });

    expect(response.status).toBe(502);
    expect(await response.json()).toEqual({ error: "Unexpected server error" });
    expect(consoleError).toHaveBeenCalledWith(expect.any(Error));
    consoleError.mockRestore();
  });

  it("keeps projection jobs out of the client API", async () => {
    const response = await createServer().request("/api/timeline/status", {
      headers: { cookie: "bff-session=opaque-session-id" },
    });

    expect(response.status).toBe(404);
  });

  it("serves the built frontend without hiding missing API routes", async () => {
    const directory = mkdtempSync(join(process.cwd(), ".static-app-test-"));
    mkdirSync(join(directory, "assets"));
    writeFileSync(join(directory, "index.html"), "<main>Jazz app</main>");
    writeFileSync(join(directory, "assets", "app.js"), "console.log('Jazz app')");

    try {
      const application = createServer({
        staticRoot: `./${basename(directory)}`,
        webOrigin: "http://127.0.0.1:3001",
      });

      const asset = await application.request("/assets/app.js");
      expect(asset.status).toBe(200);
      expect(await asset.text()).toBe("console.log('Jazz app')");

      const navigation = await application.request("/thread/example");
      expect(navigation.status).toBe(200);
      expect(await navigation.text()).toBe("<main>Jazz app</main>");

      const missingApiRoute = await application.request("/api/missing");
      expect(missingApiRoute.status).toBe(404);
      expect(await missingApiRoute.text()).not.toContain("Jazz app");
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});

import type { NodeSavedSession } from "@atproto/oauth-client-node";
import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { createJazzContext } from "jazz-tools/backend";
import permissions from "../permissions.js";
import { app } from "../schema.js";
import { createOAuthSessionStore } from "./oauth-session-store.js";

describe("OAuth session storage", () => {
  it("persists sessions for the backend without exposing them to users", async () => {
    const context = createJazzContext({
      appId: randomUUID(),
      app,
      permissions,
      driver: { type: "memory" },
      env: "test",
      userBranch: "main",
    });
    const store = createOAuthSessionStore(context.asBackend());
    const did = "did:plc:alice";
    const session = {
      dpopJwk: { kty: "EC", crv: "P-256", x: "x", y: "y", d: "d" },
      tokenSet: { sub: did, access_token: "access", refresh_token: "refresh" },
    } as unknown as NodeSavedSession;

    try {
      await store.set(did, session);

      expect(await store.get(did)).toEqual(session);
      expect(await context.forSession({ user_id: did, claims: {} })
        .all(app.oauthSessions.where({}))).toEqual([]);

      await store.del(did);
      expect(await store.get(did)).toBeUndefined();
    } finally {
      await context.shutdown();
    }
  });
});

import type { NodeSavedSession } from "@atproto/oauth-client-node";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createJazzContext } from "jazz-tools/backend";
import permissions from "../permissions.js";
import { app } from "../schema.js";
import { createOAuthSessionStore } from "./oauth-session-store.js";

describe("OAuth session storage", () => {
  it("persists sessions for the backend without exposing them to users", async () => {
    const dataDirectory = mkdtempSync(join(tmpdir(), "jazz-oauth-sessions-"));
    const context = createJazzContext({
      appId: randomUUID(),
      app,
      permissions,
      driver: { type: "persistent", dataPath: join(dataDirectory, "jazz.db") },
      env: "test",
      userBranch: "main",
    });
    const store = createOAuthSessionStore(context.db());
    const did = "did:plc:alice";
    const session = {
      dpopJwk: { kty: "EC", crv: "P-256", x: "x", y: "y", d: "d" },
      tokenSet: { sub: did, access_token: "access", refresh_token: "refresh" },
    } as unknown as NodeSavedSession;

    try {
      await store.set(did, session);

      expect(await store.get(did)).toEqual(session);
      expect(await context.forSession({ user_id: did, claims: {}, authMode: "external" })
        .all(app.oauthSessions.where({}))).toEqual([]);

      await store.del(did);
      expect(await store.get(did)).toBeUndefined();
    } finally {
      await context.shutdown();
      rmSync(dataDirectory, { recursive: true, force: true });
    }
  });
});

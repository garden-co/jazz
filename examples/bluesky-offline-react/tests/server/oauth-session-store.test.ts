import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { app } from "../../schema.js";
import {
  createBffSessionStore,
  createEncryptedValueStore,
} from "../../server/oauth-session-store.js";
import { withTestDatabase } from "./support/database.js";

describe("encrypted authentication storage", () => {
  beforeEach(() => vi.stubEnv("OAUTH_SESSION_ENCRYPTION_KEY", "00".repeat(32)));
  afterEach(() => vi.unstubAllEnvs());

  it("persists opaque BFF sessions until they are invalidated", async () => {
    await withTestDatabase(async (database) => {
      const sessions = createBffSessionStore(database);
      const did = "did:plc:alice";
      const sessionId = await sessions.create(did);

      expect(sessionId).not.toContain(did);
      expect(await createBffSessionStore(database).resolve(sessionId)).toBe(did);
      expect(await sessions.resolve("forged-session-id")).toBeUndefined();

      expect(await sessions.invalidate(sessionId)).toBe(did);
      expect(await sessions.resolve(sessionId)).toBeUndefined();
    });
  });

  it("encrypts updates, deletes values, and rejects tampered ciphertext", async () => {
    await withTestDatabase(async (database) => {
      const store = createEncryptedValueStore<{ token: string }>(database, "test:");
      await store.set("alice", { token: "first" });
      await store.set("alice", { token: "second" });

      const row = await database.one(
        app.oauthSessions.where({
          sessionKey: { eq: "test:alice" },
        }),
      );
      expect(row?.sessionJson).not.toContain("second");
      expect(await store.get("alice")).toEqual({ token: "second" });

      if (!row) throw new Error("Expected encrypted row");
      database.update(app.oauthSessions, row.id, {
        sessionJson: `${row.sessionJson}tampered`,
      });
      await expect(store.get("alice")).rejects.toThrow();

      await store.del("alice");
      expect(await store.get("alice")).toBeUndefined();
    });
  });
});

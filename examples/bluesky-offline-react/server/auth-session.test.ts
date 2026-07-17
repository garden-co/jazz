import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createJazzContext } from "jazz-tools/backend";
import type { NodeSavedState } from "@atproto/oauth-client-node";
import permissions from "../permissions.js";
import { app } from "../schema.js";
import {
  createBffSessionStore,
  createEncryptedValueStore,
  createOAuthStateStore,
} from "./oauth-session-store.js";
import { loadOrCreateJwtKeys, type StoredJwtKeys } from "./signing-keys.js";

const encryptionKey = "00".repeat(32);

describe("server authentication persistence", () => {
  afterEach(() => {
    vi.doUnmock("./jazz.js");
    vi.resetModules();
  });

  it("uses opaque BFF session IDs that persist server-side and can be invalidated", async () => {
    process.env.OAUTH_SESSION_ENCRYPTION_KEY = encryptionKey;
    const dataDirectory = mkdtempSync(join(tmpdir(), "jazz-bff-sessions-"));
    const context = createJazzContext({
      appId: randomUUID(),
      app,
      permissions,
      driver: { type: "persistent", dataPath: join(dataDirectory, "jazz.db") },
      env: "test",
      userBranch: "main",
    });
    const did = "did:plc:alice";

    try {
      const sessions = createBffSessionStore(context.db());
      const sessionId = await sessions.create(did);

      expect(sessionId).not.toContain(did);
      expect(sessionId).toMatch(/^[A-Za-z0-9_-]{43}$/);
      expect(await createBffSessionStore(context.db()).resolve(sessionId)).toBe(did);
      expect(await sessions.resolve("forged-session-id")).toBeUndefined();

      expect(await sessions.invalidate(sessionId)).toBe(did);
      expect(await sessions.resolve(sessionId)).toBeUndefined();
    } finally {
      await context.shutdown();
      rmSync(dataDirectory, { recursive: true, force: true });
    }
  });

  it("persists OAuth callback state across store instances", async () => {
    process.env.OAUTH_SESSION_ENCRYPTION_KEY = encryptionKey;
    const dataDirectory = mkdtempSync(join(tmpdir(), "jazz-oauth-state-"));
    const context = createJazzContext({
      appId: randomUUID(),
      app,
      permissions,
      driver: { type: "persistent", dataPath: join(dataDirectory, "jazz.db") },
      env: "test",
      userBranch: "main",
    });
    const state: NodeSavedState = {
      iss: "https://issuer.example",
      authMethod: { method: "none" },
      verifier: "secret",
      dpopJwk: { kty: "EC", crv: "P-256", x: "x", y: "y", d: "d" },
    };

    try {
      await createOAuthStateStore(context.db()).set("state-id", state);
      const restartedStore = createOAuthStateStore(context.db());
      expect(await restartedStore.get("state-id")).toEqual(state);
      await restartedStore.del("state-id");
      expect(await restartedStore.get("state-id")).toBeUndefined();
    } finally {
      await context.shutdown();
      rmSync(dataDirectory, { recursive: true, force: true });
    }
  });

  it("encrypts updates, deletes values, and rejects tampered ciphertext", async () => {
    process.env.OAUTH_SESSION_ENCRYPTION_KEY = encryptionKey;
    const dataDirectory = mkdtempSync(join(tmpdir(), "jazz-encrypted-store-"));
    const context = createJazzContext({
      appId: randomUUID(),
      app,
      permissions,
      driver: { type: "persistent", dataPath: join(dataDirectory, "jazz.db") },
      env: "test",
      userBranch: "main",
    });
    const database = context.db();
    const store = createEncryptedValueStore<{ token: string }>(database, "test:");

    try {
      await store.set("alice", { token: "first" });
      await store.set("alice", { token: "second" });
      const row = await database.one(app.oauthSessions.where({ sessionKey: { eq: "test:alice" } }));
      expect(row?.sessionJson).not.toContain("second");
      expect(await store.get("alice")).toEqual({ token: "second" });

      if (!row) throw new Error("Expected encrypted row");
      database.update(app.oauthSessions, row.id, { sessionJson: `${row.sessionJson}tampered` });
      await expect(store.get("alice")).rejects.toThrow();

      await store.del("alice");
      expect(await store.get("alice")).toBeUndefined();
    } finally {
      await context.shutdown();
      rmSync(dataDirectory, { recursive: true, force: true });
    }
  });

  it("reuses the Jazz signing key after an auth module restart", async () => {
    process.env.OAUTH_SESSION_ENCRYPTION_KEY = encryptionKey;
    const dataDirectory = mkdtempSync(join(tmpdir(), "jazz-signing-key-"));
    const context = createJazzContext({
      appId: randomUUID(),
      app,
      permissions,
      driver: { type: "persistent", dataPath: join(dataDirectory, "jazz.db") },
      env: "test",
      userBranch: "main",
    });

    try {
      const firstStore = createEncryptedValueStore<StoredJwtKeys>(
        context.db(),
        "jazz-signing-key:",
      );
      const firstKeys = await loadOrCreateJwtKeys(firstStore);

      const restartedStore = createEncryptedValueStore<StoredJwtKeys>(
        context.db(),
        "jazz-signing-key:",
      );
      const restartedKeys = await loadOrCreateJwtKeys(restartedStore);

      expect(restartedKeys).toEqual(firstKeys);
    } finally {
      await context.shutdown();
      rmSync(dataDirectory, { recursive: true, force: true });
    }
  });
});

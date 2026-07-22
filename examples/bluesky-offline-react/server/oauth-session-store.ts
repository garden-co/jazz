import type { NodeSavedSessionStore, NodeSavedState } from "@atproto/oauth-client-node";
import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import type { Db } from "jazz-tools";
import { app } from "../schema.js";

type EncryptedSession = {
  v: 1;
  iv: string;
  ciphertext: string;
  tag: string;
};

export type EncryptedValueStore<Value> = {
  set(key: string, value: Value): Promise<void>;
  get(key: string): Promise<Value | undefined>;
  del(key: string): Promise<void>;
};

function encryptionKey() {
  const value = process.env.OAUTH_SESSION_ENCRYPTION_KEY;
  if (!value || !/^[0-9a-f]{64}$/i.test(value)) {
    throw new Error(
      "OAUTH_SESSION_ENCRYPTION_KEY must be a 64-character hexadecimal key; generate one with `openssl rand -hex 32`.",
    );
  }
  return Buffer.from(value, "hex");
}

function encryptSession(key: Buffer, sessionKey: string, value: unknown) {
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  cipher.setAAD(Buffer.from(sessionKey));
  const ciphertext = Buffer.concat([cipher.update(JSON.stringify(value), "utf8"), cipher.final()]);
  const envelope: EncryptedSession = {
    v: 1,
    iv: iv.toString("base64url"),
    ciphertext: ciphertext.toString("base64url"),
    tag: cipher.getAuthTag().toString("base64url"),
  };
  return JSON.stringify(envelope);
}

function decryptSession(key: Buffer, sessionKey: string, stored: string) {
  const envelope = JSON.parse(stored) as EncryptedSession;
  if (envelope.v !== 1) {
    throw new Error(`Unsupported OAuth session encryption version: ${envelope.v}`);
  }
  const decipher = createDecipheriv("aes-256-gcm", key, Buffer.from(envelope.iv, "base64url"));
  decipher.setAAD(Buffer.from(sessionKey));
  decipher.setAuthTag(Buffer.from(envelope.tag, "base64url"));
  return JSON.parse(
    Buffer.concat([
      decipher.update(Buffer.from(envelope.ciphertext, "base64url")),
      decipher.final(),
    ]).toString("utf8"),
  );
}

export function createEncryptedValueStore<Value>(
  db: Db,
  namespace = "",
): EncryptedValueStore<Value> {
  const encryptionKeyBuffer = encryptionKey();

  return {
    async set(key, value) {
      const sessionKey = `${namespace}${key}`;
      const session = {
        sessionKey,
        sessionJson: encryptSession(encryptionKeyBuffer, sessionKey, value),
        updatedAt: new Date().toISOString(),
      };
      const existing = await db.one(app.oauthSessions.where({ sessionKey: { eq: sessionKey } }));
      if (existing) {
        db.update(app.oauthSessions, existing.id, session);
      } else {
        db.insert(app.oauthSessions, session);
      }
    },
    async get(key) {
      const sessionKey = `${namespace}${key}`;
      const row = await db.one(app.oauthSessions.where({ sessionKey: { eq: sessionKey } }));
      return row
        ? (decryptSession(encryptionKeyBuffer, sessionKey, row.sessionJson) as Value)
        : undefined;
    },
    async del(key) {
      const sessionKey = `${namespace}${key}`;
      const rows = await db.all(app.oauthSessions.where({ sessionKey: { eq: sessionKey } }));
      for (const row of rows) db.delete(app.oauthSessions, row.id);
    },
  };
}

export function createOAuthSessionStore(db: Db): NodeSavedSessionStore {
  return createEncryptedValueStore(db);
}

export function createOAuthStateStore(db: Db) {
  return createEncryptedValueStore<NodeSavedState>(db, "oauth-state:");
}

export function createBffSessionStore(db: Db) {
  const sessions = createEncryptedValueStore<string>(db, "bff-session:");

  return {
    async create(did: string) {
      const sessionId = randomBytes(32).toString("base64url");
      await sessions.set(sessionId, did);
      return sessionId;
    },
    resolve(sessionId: string) {
      return sessions.get(sessionId);
    },
    async invalidate(sessionId: string) {
      const did = await sessions.get(sessionId);
      await sessions.del(sessionId);
      return did;
    },
  };
}

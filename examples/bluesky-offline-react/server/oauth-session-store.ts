import type { NodeSavedSessionStore } from "@atproto/oauth-client-node";
import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import type { Db } from "jazz-tools";
import { app } from "../schema.js";
import { legacyObjectId, stableObjectId } from "./timeline.js";

type EncryptedSession = {
  v: 1;
  iv: string;
  ciphertext: string;
  tag: string;
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
  const ciphertext = Buffer.concat([
    cipher.update(JSON.stringify(value), "utf8"),
    cipher.final(),
  ]);
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
  const decipher = createDecipheriv(
    "aes-256-gcm",
    key,
    Buffer.from(envelope.iv, "base64url"),
  );
  decipher.setAAD(Buffer.from(sessionKey));
  decipher.setAuthTag(Buffer.from(envelope.tag, "base64url"));
  return JSON.parse(
    Buffer.concat([
      decipher.update(Buffer.from(envelope.ciphertext, "base64url")),
      decipher.final(),
    ]).toString("utf8"),
  );
}

export function createOAuthSessionStore(db: Db): NodeSavedSessionStore {
  const encryptionKeyBuffer = encryptionKey();
  const rowId = (key: string) => stableObjectId("oauth-session", key);
  const legacyRowId = (key: string) => legacyObjectId("oauth-session", key);

  return {
    async set(sessionKey, value) {
      db.upsert(app.oauthSessions, {
        sessionKey,
        sessionJson: encryptSession(encryptionKeyBuffer, sessionKey, value),
        updatedAt: new Date().toISOString(),
      }, { id: rowId(sessionKey) });
    },
    async get(sessionKey) {
      const row = await db.one(app.oauthSessions.where({ id: { eq: rowId(sessionKey) } }))
        ?? await db.one(app.oauthSessions.where({ id: { eq: legacyRowId(sessionKey) } }));
      return row
        ? decryptSession(encryptionKeyBuffer, sessionKey, row.sessionJson)
        : undefined;
    },
    async del(sessionKey) {
      const ids = [rowId(sessionKey), legacyRowId(sessionKey)];
      for (const id of ids) {
        if (await db.one(app.oauthSessions.where({ id: { eq: id } }))) {
          db.delete(app.oauthSessions, id);
        }
      }
    },
  };
}

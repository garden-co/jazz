import type { NodeSavedSessionStore } from "@atproto/oauth-client-node";
import type { Db } from "jazz-tools";
import { app } from "../schema.js";
import { stableObjectId } from "./timeline.js";

export function createOAuthSessionStore(db: Db): NodeSavedSessionStore {
  const rowId = (key: string) => stableObjectId("oauth-session", key);

  return {
    async set(key, value) {
      db.upsert(app.oauthSessions, {
        sessionKey: key,
        sessionJson: JSON.stringify(value),
        updatedAt: new Date().toISOString(),
      }, { id: rowId(key) });
    },
    async get(key) {
      const row = await db.one(app.oauthSessions.where({ id: { eq: rowId(key) } }));
      return row ? JSON.parse(row.sessionJson) : undefined;
    },
    async del(key) {
      db.delete(app.oauthSessions, rowId(key));
    },
  };
}

import { createJazzSession } from "../../src/backend/create-jazz-session.js";
import {
  largeValueRewriteApp as app,
  largeValueRewritePermissions,
} from "./large-value-rewrite-schema.js";
import type { JazzServerInfo } from "./testing-server.js";

type Session = Awaited<ReturnType<typeof createJazzSession>>;
const sessions = new Map<string, Session>();

function backendDb(appId: string) {
  const snapshot = sessions.get(appId)?.getSnapshot();
  if (!snapshot || snapshot.status !== "ready" || !snapshot.client) {
    throw snapshot?.error ?? new Error("Large-value rewrite backend is not open");
  }
  return snapshot.client.db;
}

export async function largeValueRewriteBackendOpen(info: JazzServerInfo): Promise<void> {
  const session = await createJazzSession({
    appId: info.appId,
    serverUrl: info.serverUrl,
    initial: { backendSecret: "jazz-browser-test-backend" },
    app,
    permissions: largeValueRewritePermissions,
    driver: { type: "memory" },
    tier: "global",
    defaultDurabilityTier: "global",
  });
  sessions.set(info.appId, session);
}

/** Insert a large attachment and an empty reply into the conversation. */
export async function largeValueRewriteBackendSeed(
  appId: string,
  conversationId: string,
  attachmentBytes: number,
): Promise<string> {
  const db = backendDb(appId);
  const payload = new Uint8Array(attachmentBytes);
  for (let i = 0; i < payload.length; i++) payload[i] = (i * 31) & 0xff;
  await db
    .insert(app.attachments, {
      conversation_id: conversationId,
      filename: "rough-mix.wav",
      payload,
    })
    .wait({ tier: "global" });
  const turn = await db
    .insert(app.turns, { conversation_id: conversationId, body: "" })
    .wait({ tier: "global" });
  return turn.id;
}

/**
 * Append each word to the reply in its own exclusive read-modify-write
 * transaction, as MusicAgent's runner streams a model reply.
 */
export async function largeValueRewriteBackendAppend(
  appId: string,
  turnId: string,
  words: string[],
  delayMs: number,
): Promise<string> {
  const db = backendDb(appId);
  for (const word of words) {
    const write = await db.exclusiveTransaction(async (tx) => {
      const turn = await tx.one(app.turns.where({ id: turnId }));
      tx.update(app.turns, turnId, { body: (turn?.body ?? "") + word });
    });
    await write.wait();
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
  const turn = await db.one(app.turns.where({ id: turnId }), {
    tier: "remote",
  });
  return turn?.body ?? "";
}

export async function largeValueRewriteBackendClose(appId: string): Promise<void> {
  const session = sessions.get(appId);
  await session?.close();
  if (sessions.get(appId) === session) sessions.delete(appId);
}

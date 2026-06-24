import { nanoid } from "nanoid";
import { useDb, useSession } from "jazz-tools/react";
import { app } from "../../schema.js";

export function useCreateRoom() {
  const db = useDb();
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;

  return async function createRoom(): Promise<string> {
    if (!sessionUserId) throw new Error("Cannot create a room before the session is ready");

    const shareToken = nanoid();
    const now = new Date();
    const { value: room } = db.insert(app.rooms, {
      shareToken,
      title: "Untitled",
      editorLanguage: "plaintext",
      creator_session_user_id: sessionUserId,
      createdAt: now,
    });

    await db
      .insert(app.roomParticipants, {
        room_id: room.id,
        session_user_id: sessionUserId,
        lastAccessedAt: now,
      })
      .wait({ tier: "edge" });

    return shareToken;
  };
}

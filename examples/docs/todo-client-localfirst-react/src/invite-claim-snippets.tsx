import { useEffect, useRef, useState } from "react";
import { schema as s } from "jazz-tools";
import { useDb, useSession } from "jazz-tools/react";

const schema = {
  chats: s.table({
    joinCode: s.string().optional(),
  }),
  chatMembers: s.table({
    chatId: s.ref("chats"),
    user_id: s.string(),
    joinCode: s.string().optional(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

function navigate(to: string) {
  window.location.hash = to;
}

// #region invite-handler-jwt-claim
export function InviteHandlerWithClaim({ chatId, code }: { chatId: string; code: string }) {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id ?? null;
  const [chatLoaded, setChatLoaded] = useState(false);
  const handled = useRef(false);

  useEffect(() => {
    if (!userId) return;
    return db.subscribeAll(
      app.chats.where({ id: chatId }),
      (delta) => {
        if (delta.all.length > 0) setChatLoaded(true);
      },
      undefined,
      { user_id: userId, claims: { join_code: code } },
    );
  }, [db, userId, chatId, code]);

  useEffect(() => {
    if (!chatLoaded || handled.current || !userId) return;
    handled.current = true;

    db.insert(app.chatMembers, { chatId, user_id: userId, joinCode: code })
      .wait({ tier: "local" })
      .then(() => navigate(`/chat/${chatId}`))
      .catch((err) => {
        console.error("failed to join", err);
        handled.current = false;
      });
  }, [chatLoaded, db, userId, chatId, code]);

  return <p>Joining…</p>;
}
// #endregion invite-handler-jwt-claim

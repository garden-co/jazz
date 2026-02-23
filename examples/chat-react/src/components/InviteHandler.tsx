import { useEffect, useRef } from "react";
import { useDb, useSession, useAll } from "jazz-tools/react";
import { navigate } from "@/hooks/useRouter";
import { useMyProfile } from "@/hooks/useMyProfile";
import { app } from "../../schema/app.js";

interface InviteHandlerProps {
  chatId: string;
  code: string;
}

export function InviteHandler({ chatId, code }: InviteHandlerProps) {
  const db = useDb();
  const session = useSession();
  const handled = useRef(false);

  const userId = session?.user_id ?? null;
  const myProfile = useMyProfile();

  // Chats are now publicly readable. Subscribe so the row syncs locally,
  // satisfying the FK check before we insert the chatMembers row.
  const chats = useAll(app.chats.where({ id: chatId }));
  const chat = chats[0] ?? null;

  useEffect(() => {
    if (handled.current || !userId || !myProfile || !chat) return;
    handled.current = true;

    db.insert(app.chatMembers, {
      chat: chatId,
      userId,
      joinCode: code,
    });

    navigate(`/#/chat/${chatId}`);
  }, [db, userId, myProfile, chatId, code, chat]);

  return (
    <div id="joining-chat" className="p-8 text-center text-muted-foreground italic">
      Joining chat...
    </div>
  );
}

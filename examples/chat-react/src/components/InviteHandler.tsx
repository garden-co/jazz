import { useEffect, useRef, useState } from "react";
import { useDb, useSession } from "jazz-tools/react";
import { navigate } from "@/hooks/useRouter";
import { useMyProfile } from "@/hooks/useMyProfile";
import { app } from "../../schema.js";

interface InviteHandlerProps {
  chatId: string;
  code: string;
}

export function InviteHandler({ chatId, code }: InviteHandlerProps) {
  const db = useDb();
  const session = useSession();
  const handled = useRef(false);
  const [chatLoaded, setChatLoaded] = useState(false);
  const sharedWriteOptions = db.getConfig().serverUrl ? { tier: "edge" as const } : undefined;

  const userId = session?.user_id ?? null;
  const myProfile = useMyProfile();

  useEffect(() => {
    if (!userId) return;
    const unsubscribe = db.subscribeAll(
      app.chats.where({ id: chatId }),
      (delta) => {
        if (delta.all.length > 0) setChatLoaded(true);
      },
      undefined,
      { user_id: userId, claims: { join_code: code } },
    );
    return unsubscribe;
  }, [db, userId, chatId, code]);

  useEffect(() => {
    if (!chatLoaded || handled.current || !userId || !myProfile) return;
    handled.current = true;

    void db
      .insertDurable(
        app.chatMembers,
        {
          chatId,
          userId,
          joinCode: code,
        },
        sharedWriteOptions,
      )
      .then(() => {
        navigate(`/#/chat/${chatId}`);
      })
      .catch((error) => {
        console.error("failed to accept invite", error);
        handled.current = false;
      });
  }, [chatLoaded, db, userId, myProfile, chatId, code, sharedWriteOptions]);

  return (
    <div id="joining-chat" className="p-8 text-center text-muted-foreground italic">
      Joining chat...
    </div>
  );
}

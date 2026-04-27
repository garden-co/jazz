import { useEffect, useRef, useState } from "react";
import { useDb, useSession } from "jazz-tools/react";
import { navigate } from "@/hooks/useRouter";
import { useMyProfile } from "@/hooks/useMyProfile";
import { app } from "../../schema.js";
import { type Db, DurabilityTier } from "jazz-tools";

interface InviteHandlerProps {
  chatId: string;
  code: string;
}

export function InviteHandler({ chatId, code }: InviteHandlerProps) {
  const db = useDb();
  const session = useSession();
  const handled = useRef(false);
  const [chatLoaded, setChatLoaded] = useState(false);
  const sharedWriteOptions: { tier: DurabilityTier } = {
    tier: db.getConfig().serverUrl ? "edge" : "local",
  };

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
      { user_id: userId, claims: { join_code: code }, authMode: "external" },
    );
    return unsubscribe;
  }, [db, userId, chatId, code]);

  useEffect(() => {
    if (!chatLoaded || handled.current || !userId || !myProfile) return;
    handled.current = true;

    void db
      .insert(app.chatMembers, {
        chatId,
        userId,
        joinCode: code,
      })
      .wait(sharedWriteOptions)
      .then(() => waitForInvitedMessages(db, chatId, code, userId, sharedWriteOptions.tier))
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

function waitForInvitedMessages(
  db: Db,
  chatId: string,
  code: string,
  userId: string,
  tier: DurabilityTier,
): Promise<void> {
  return new Promise((resolve) => {
    let unsubscribe: (() => void) | undefined;
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      window.clearTimeout(timeout);
      queueMicrotask(() => unsubscribe?.());
      resolve();
    };
    const timeout = window.setTimeout(finish, 3000);

    unsubscribe = db.subscribeAll(
      app.messages.where({ chatId }),
      (delta) => {
        if (delta.all.length > 0) finish();
      },
      { tier, visibility: "hidden_from_live_query_list" },
      { user_id: userId, claims: { join_code: code }, authMode: "external" },
    );
  });
}

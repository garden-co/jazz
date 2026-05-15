import { useEffect, useRef, useState } from "react";
import { reatomComponent } from "@reatom/react";
import type { DurabilityTier } from "jazz-tools";
import { jazz } from "@/jazz";
import { myProfile } from "@/model/my-profile";
import { chatRoute } from "@/routes";
import { app } from "../../schema.js";

interface InviteHandlerProps {
  chatId: string;
  code: string;
}

export const InviteHandler = reatomComponent(({ chatId, code }: InviteHandlerProps) => {
  const { db, session } = jazz();
  const profile = myProfile();
  const handled = useRef(false);
  const [chatLoaded, setChatLoaded] = useState(false);
  const sharedWriteOptions: { tier: DurabilityTier } = {
    tier: db.getConfig().serverUrl ? "edge" : "local",
  };

  const userId = session?.user_id ?? null;

  // Session override (claims/authMode) is not supported by reatomQueryAll,
  // so we drive this subscription imperatively via db.subscribeAll.
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
    if (!chatLoaded || handled.current || !userId || !profile) return;
    handled.current = true;

    void db
      .insert(app.chatMembers, {
        chatId,
        userId,
        joinCode: code,
      })
      .wait(sharedWriteOptions)
      .then(() => db.all(app.messages.where({ chatId }), { tier: sharedWriteOptions.tier }))
      .then(() => {
        chatRoute.go({ chatId });
      })
      .catch((error) => {
        console.error("failed to accept invite", error);
        handled.current = false;
      });
  }, [chatLoaded, db, userId, profile, chatId, code, sharedWriteOptions]);

  return (
    <div id="joining-chat" className="p-8 text-center text-muted-foreground italic">
      Joining chat...
    </div>
  );
}, "InviteHandler");

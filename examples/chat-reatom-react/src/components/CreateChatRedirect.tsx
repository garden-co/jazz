import { useEffect, useRef } from "react";
import { reatomComponent } from "@reatom/react";
import { wrap } from "@reatom/core";
import type { DurabilityTier } from "jazz-tools";
import { jazz } from "@/jazz";
import { myProfile } from "@/model/my-profile";
import { chatRoute } from "@/routes";
import { app } from "../../schema.js";

export const CreateChatRedirect = reatomComponent(() => {
  const { db, session } = jazz();
  const profile = myProfile();
  const initialized = useRef(false);
  const userId = session?.user_id ?? null;
  const sharedWriteOptions: { tier: DurabilityTier } = {
    tier: db.getConfig().serverUrl ? "edge" : "local",
  };

  useEffect(() => {
    if (initialized.current || !userId || !profile) return;
    initialized.current = true;

    void (async () => {
      const chat = await wrap(
        db
          .insert(app.chats, {
            isPublic: true,
            createdBy: userId,
          })
          .wait(sharedWriteOptions),
      );

      await wrap(db.insert(app.chatMembers, { chatId: chat.id, userId }).wait(sharedWriteOptions));

      await wrap(
        db
          .insert(app.messages, {
            chatId: chat.id,
            text: "Hello world",
            senderId: profile.id,
            createdAt: new Date(),
          })
          .wait(sharedWriteOptions),
      );

      chatRoute.go({ chatId: chat.id });
    })().catch((error) => {
      console.error("failed to create initial chat", error);
      initialized.current = false;
    });
  }, [db, userId, profile, sharedWriteOptions]);

  return (
    <div className="flex-1 overflow-y-auto flex flex-col-reverse">
      <article>Creating chat...</article>
    </div>
  );
}, "CreateChatRedirect");

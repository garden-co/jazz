import { useEffect, useRef } from "react";
import { useDb, useSession } from "jazz-tools/react";
import { useRouter } from "@/hooks/useRouter";
import { useMyProfile } from "@/hooks/useMyProfile";
import { app } from "../../schema.js";
import { DurabilityTier } from "jazz-tools";

export const CreateChatRedirect = () => {
  const db = useDb();
  const session = useSession();
  const { navigate } = useRouter();
  const initialized = useRef(false);
  const sharedWriteOptions: { tier: DurabilityTier } = {
    tier: db.getConfig().serverUrl ? "edge" : "local",
  };

  const userId = session?.user_id ?? null;
  const myProfile = useMyProfile();

  useEffect(() => {
    if (initialized.current || !userId || !myProfile) return;
    initialized.current = true;

    void (async () => {
      const seeded = db.transaction((tx) => {
        const chat = tx.insert(app.chats, {
          isPublic: true,
          createdBy: userId,
        });

        tx.insert(app.chatMembers, { chatId: chat.id, userId });

        tx.insert(app.messages, {
          chatId: chat.id,
          text: "Hello world",
          senderId: myProfile.id,
          createdAt: new Date(),
        });

        return chat;
      });
      const chat = seeded.value;
      await seeded.wait({ tier: "local" });

      void seeded.wait(sharedWriteOptions).catch((error) => {
        console.error("failed to persist initial chat", error);
      });

      navigate(`/#/chat/${chat.id}`);
    })().catch((error) => {
      console.error("failed to create initial chat", error);
      initialized.current = false;
    });
  }, [db, userId, myProfile, navigate, sharedWriteOptions]);

  return (
    <div className="flex-1 overflow-y-auto flex flex-col-reverse">
      <article>Creating chat...</article>
    </div>
  );
};

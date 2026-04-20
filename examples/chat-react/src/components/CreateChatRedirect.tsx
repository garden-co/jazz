import { useEffect, useRef } from "react";
import { useDb, useSession } from "jazz-tools/react";
import { useRouter } from "@/hooks/useRouter";
import { useMyProfile } from "@/hooks/useMyProfile";
import { app } from "../../schema.js";

export const CreateChatRedirect = () => {
  const db = useDb();
  const session = useSession();
  const { navigate } = useRouter();
  const initialized = useRef(false);
  const sharedWriteOptions = db.getConfig().serverUrl ? { tier: "edge" as const } : undefined;

  const userId = session?.user_id ?? null;
  const myProfile = useMyProfile();

  useEffect(() => {
    if (initialized.current || !userId || !myProfile) return;
    initialized.current = true;

    void (async () => {
      const chat = await db.insertDurable(
        app.chats,
        {
          isPublic: true,
          createdBy: userId,
        },
        sharedWriteOptions,
      );

      await db.insertDurable(app.chatMembers, { chatId: chat.id, userId }, sharedWriteOptions);

      await db.insertDurable(
        app.messages,
        {
          chatId: chat.id,
          text: "Hello world",
          senderId: myProfile.id,
          createdAt: new Date(),
        },
        sharedWriteOptions,
      );

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

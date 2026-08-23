import { useEffect, useMemo, useRef } from "react";
import { useDb, useSession } from "jazz-tools/react";
import { useRouter } from "@/hooks/useRouter";
import { useMyProfile } from "@/hooks/useMyProfile";
import { waitForWrite } from "@/lib/db-write";
import { app } from "../../schema.js";
import { DurabilityTier } from "jazz-tools";

export const CreateChatRedirect = () => {
  const db = useDb();
  const session = useSession();
  const { navigate } = useRouter();
  const initialized = useRef(false);
  const sharedWriteOptions: { tier: DurabilityTier } = useMemo(
    () => ({ tier: db.getConfig().serverUrl ? "edge" : "local" }),
    [db],
  );

  const userId = session?.user_id ?? null;
  const myProfile = useMyProfile();

  useEffect(() => {
    if (initialized.current || !userId || !myProfile) return;
    initialized.current = true;

    void (async () => {
      const chatWrite = await Promise.resolve(
        db.insert(app.chats, {
          isPublic: true,
          createdBy: userId,
        }),
      );
      const chat = chatWrite.value;
      await chatWrite.wait(sharedWriteOptions);

      await waitForWrite(
        db.insert(app.chatMembers, { chatId: chat.id, userId }),
        sharedWriteOptions,
      );

      await waitForWrite(
        db.insert(app.messages, {
          chatId: chat.id,
          text: "Hello world",
          senderId: myProfile.id,
          createdAt: new Date(),
        }),
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

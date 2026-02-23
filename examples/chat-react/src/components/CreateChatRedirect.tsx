import { useEffect, useRef } from "react";
import { useDb, useSession } from "jazz-tools/react";
import { useRouter } from "@/hooks/useRouter";
import { useMyProfile } from "@/hooks/useMyProfile";
import { app } from "../../schema/app.js";

export const CreateChatRedirect = () => {
  const db = useDb();
  const session = useSession();
  const { navigate } = useRouter();
  const initialized = useRef(false);

  const userId = session?.user_id ?? null;
  const myProfile = useMyProfile();

  useEffect(() => {
    if (initialized.current || !userId || !myProfile) return;
    initialized.current = true;

    const chatId = db.insert(app.chats, {
      isPublic: true,
      createdBy: userId,
    });

    db.insert(app.chatMembers, { chat: chatId, userId });

    db.insert(app.messages, {
      chat: chatId,
      text: "Hello world",
      sender: myProfile.id,
      senderId: userId,
      createdAt: Math.floor(Date.now() / 1000),
    });

    navigate(`/#/chat/${chatId}`);
  }, [db, userId, myProfile, navigate]);

  return (
    <div className="flex-1 overflow-y-auto flex flex-col-reverse">
      <article>Creating chat...</article>
    </div>
  );
};

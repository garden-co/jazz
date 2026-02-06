import { useEffect, useRef } from "react";
import { useAcceptInvite, useSuspenseAccount } from "jazz-tools/react";
import { toast } from "sonner";
import { useRouter } from "@/hooks/useRouter";
import { Chat, ChatAccount } from "@/schema";

export const CreateChatRedirect = () => {
  const me = useSuspenseAccount(ChatAccount, {
    resolve: {
      root: {
        chats: true,
      },
    },
  });
  useAcceptInvite({
    invitedObjectSchema: Chat,
    onAccept: async (chatId) => {
      const chat = await Chat.load(chatId);
      if (!chat.$isLoaded) toast.error("Failed to load chat");
      navigate(`/chat/${chatId}`);
    },
  });
  const { navigate } = useRouter();
  const initialized = useRef(false);

  useEffect(() => {
    if (initialized.current) return;
    initialized.current = true;
    const chat = Chat.create([]);
    chat.$jazz.push({ text: "Hello world", reactions: [] });
    me.root.chats.$jazz.set(chat.$jazz.id, chat);
    navigate(`/#/chat/${chat.$jazz.id}`);
  }, [navigate, me.root.chats]);

  return (
    <div className="flex-1 overflow-y-auto flex flex-col-reverse">
      <article>Creating chat...</article>
    </div>
  );
};

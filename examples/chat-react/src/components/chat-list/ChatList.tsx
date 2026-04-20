import { useDb, useAll, useSession } from "jazz-tools/react";
import { LockIcon, MessageSquarePlusIcon } from "lucide-react";
import { ChatListItem } from "@/components/chat-list/ChatListItem";
import { Button } from "@/components/ui/button";
import { useMyProfile } from "@/hooks/useMyProfile";
import { navigate } from "@/hooks/useRouter";
import { app } from "../../../schema.js";

export const ChatList = () => {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id ?? null;
  const sharedWriteOptions = db.getConfig().serverUrl ? { tier: "edge" as const } : undefined;

  const myProfile = useMyProfile();

  const memberships =
    useAll(app.chatMembers.where({ userId: userId ?? "__none__" }).include({ chat: true })) ?? [];

  const createPublicChat = async () => {
    if (!userId || !myProfile) return;

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
  };

  const createPrivateChat = async () => {
    if (!userId || !myProfile) return;

    const shareCode = crypto.randomUUID().slice(0, 8);

    const chat = await db.insertDurable(
      app.chats,
      {
        isPublic: false,
        createdBy: userId,
        joinCode: shareCode,
      },
      sharedWriteOptions,
    );
    await db.insertDurable(
      app.chatMembers,
      {
        chatId: chat.id,
        userId,
        joinCode: shareCode,
      },
      sharedWriteOptions,
    );
    await db.insertDurable(
      app.messages,
      {
        chatId: chat.id,
        text: "This is a private chat.",
        senderId: myProfile.id,
        createdAt: new Date(),
      },
      sharedWriteOptions,
    );
    navigate(`/#/chat/${chat.id}`);
  };

  return (
    <div className="p-2 flex flex-col gap-2">
      <div className="grid grid-cols-2 gap-2">
        <Button onClick={() => void createPublicChat()}>
          <MessageSquarePlusIcon /> New Chat
        </Button>
        <Button variant="outline" onClick={() => void createPrivateChat()}>
          <LockIcon /> New Private Chat
        </Button>
      </div>

      {memberships.map((membership) => {
        // useAll erases .include() type info; chat is Chat at runtime
        const chat = membership.chat as unknown as
          | { id: string; isPublic: boolean; name?: string }
          | undefined;
        return (
          <ChatListItem
            key={membership.id}
            chatId={chat?.id ?? membership.id}
            chat={chat}
            onDelete={() => db.delete(app.chatMembers, membership.id)}
          />
        );
      })}
    </div>
  );
};

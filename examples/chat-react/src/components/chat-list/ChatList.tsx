import { useDb, useAll, useSession } from "jazz-tools/react";
import { LockIcon, MessageSquarePlusIcon } from "lucide-react";
import { ChatListItem } from "@/components/chat-list/ChatListItem";
import { Button } from "@/components/ui/button";
import { useMyProfile } from "@/hooks/useMyProfile";
import { navigate } from "@/hooks/useRouter";
import { app } from "../../../schema/app.js";

export const ChatList = () => {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id ?? null;

  const myProfile = useMyProfile();

  const memberships =
    useAll(app.chatMembers.where({ userId: userId ?? "__none__" }).include({ chat: true })) ?? [];

  const createPublicChat = () => {
    if (!userId || !myProfile) return;

    const chat = db.insert(app.chats, {
      isPublic: true,
      createdBy: userId,
    });
    db.insert(app.chatMembers, { chat: chat.id, userId });
    db.insert(app.messages, {
      chat: chat.id,
      text: "Hello world",
      sender: myProfile.id,
      senderId: userId,
      createdAt: new Date(),
    });
    navigate(`/#/chat/${chat.id}`);
  };

  const createPrivateChat = () => {
    if (!userId || !myProfile) return;

    const shareCode = crypto.randomUUID().slice(0, 8);

    const chat = db.insert(app.chats, {
      isPublic: false,
      createdBy: userId,
    });
    db.insert(app.chatMembers, {
      chat: chat.id,
      userId,
      joinCode: shareCode,
    });
    db.insert(app.messages, {
      chat: chat.id,
      text: "This is a private chat.",
      sender: myProfile.id,
      senderId: userId,
      createdAt: new Date(),
    });
    navigate(`/#/chat/${chat.id}`);
  };

  return (
    <div className="p-2 flex flex-col gap-2">
      <div className="grid grid-cols-2 gap-2">
        <Button onClick={createPublicChat}>
          <MessageSquarePlusIcon /> New Chat
        </Button>
        <Button variant="outline" onClick={createPrivateChat}>
          <LockIcon /> New Private Chat
        </Button>
      </div>

      {memberships.map((membership) => {
        // useAll erases .include() type info; chat is Chat at runtime
        const chat = membership.chat as unknown as { id: string; isPublic: boolean } | undefined;
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

import { co } from "jazz-tools";
import { useSuspenseAccount } from "jazz-tools/react";
import { LockIcon, MessageSquarePlusIcon } from "lucide-react";
import { ChatListItem } from "@/components/chat-list/ChatListItem";
import { Button } from "@/components/ui/button";
import { navigate } from "@/hooks/useRouter";
import { Chat, ChatAccount } from "@/schema";

export const ChatList = () => {
  const me = useSuspenseAccount(ChatAccount, {
    resolve: {
      root: {
        chats: {
          $each: true,
        },
      },
    },
  });

  const chats = Object.values(me.root.chats).sort((a, b) => {
    return (
      new Date(b.$jazz.createdAt).getTime() -
      new Date(a.$jazz.createdAt).getTime()
    );
  });

  const createPrivateChat = async () => {
    const privateGroup = co.group().create();
    const chat = Chat.create(
      [
        {
          text: "This is a private chat.",
          reactions: [],
        },
      ],
      {
        owner: privateGroup,
      },
    );
    me.root.chats.$jazz.set(chat.$jazz.id, chat);
    navigate(`/chat/${chat.$jazz.id}`);
  };

  return (
    <div className="p-2 flex flex-col gap-2">
      <div className="grid grid-cols-2 gap-2">
        <Button onClick={() => navigate("/")}>
          <MessageSquarePlusIcon /> New Chat
        </Button>
        <Button variant="outline" onClick={createPrivateChat}>
          <LockIcon /> New Private Chat
        </Button>
      </div>

      {chats.map((chat) => (
        <ChatListItem
          key={chat.$jazz.id}
          chatId={chat.$jazz.id}
          onDelete={() => me.root.chats.$jazz.delete(chat.$jazz.id)}
        />
      ))}
    </div>
  );
};

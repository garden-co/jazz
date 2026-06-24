import { reatomComponent } from "@reatom/react";
import { wrap } from "@reatom/core";
import { LockIcon, MessageSquarePlusIcon } from "lucide-react";
import type { DurabilityTier } from "jazz-tools";
import { ChatListItem } from "@/components/chat-list/ChatListItem";
import { Button } from "@/components/ui/button";
import { jazz } from "@/jazz";
import { myProfile } from "@/model/my-profile";
import { myMembershipsQuery } from "@/model/queries";
import { chatRoute } from "@/routes";
import { app } from "../../../schema.js";

export const ChatList = reatomComponent(() => {
  const { db, session } = jazz();
  const profile = myProfile();
  const userId = session?.user_id ?? null;
  const sharedWriteOptions: { tier: DurabilityTier } = {
    tier: db.getConfig().serverUrl ? "edge" : "local",
  };

  const memberships = myMembershipsQuery();

  const createPublicChat = wrap(async () => {
    if (!userId || !profile) return;

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
  });

  const createPrivateChat = wrap(async () => {
    if (!userId || !profile) return;

    const shareCode = crypto.randomUUID().slice(0, 8);

    const chat = await wrap(
      db
        .insert(app.chats, {
          isPublic: false,
          createdBy: userId,
          joinCode: shareCode,
        })
        .wait(sharedWriteOptions),
    );
    await wrap(
      db
        .insert(app.chatMembers, {
          chatId: chat.id,
          userId,
          joinCode: shareCode,
        })
        .wait(sharedWriteOptions),
    );
    await wrap(
      db
        .insert(app.messages, {
          chatId: chat.id,
          text: "This is a private chat.",
          senderId: profile.id,
          createdAt: new Date(),
        })
        .wait(sharedWriteOptions),
    );

    chatRoute.go({ chatId: chat.id });
  });

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
        // .include() type info is erased at runtime; chat is Chat at runtime
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
}, "ChatList");

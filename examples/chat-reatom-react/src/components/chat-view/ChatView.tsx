import { useCallback, useDeferredValue, useEffect, useRef, useState } from "react";
import { reatomComponent } from "@reatom/react";
import { wrap } from "@reatom/core";
import { Loader2Icon } from "lucide-react";
import type { DurabilityTier } from "jazz-tools";
import { ChatMessage } from "@/components/chat/ChatMessage";
import { ChatHeader } from "@/components/chat-view/ChatHeader";
import { MessageComposer } from "@/components/composer/MessageComposer";
import { Button } from "@/components/ui/button";
import { jazz } from "@/jazz";
import { myProfile } from "@/model/my-profile";
import { getChatMessagesQuery, getChatRowsQuery, myMembershipForChatQuery } from "@/model/queries";
import { app } from "../../../schema.js";

const INITIAL_MESSAGES_TO_SHOW = 20;
const LOAD_MORE_STEP = 20;

interface ChatViewProps {
  chatId: string;
}

export const ChatView = reatomComponent(({ chatId }: ChatViewProps) => {
  const { db, session } = jazz();
  const userId = session?.user_id ?? null;
  const profile = myProfile();
  const sharedWriteOptions: { tier: DurabilityTier } = {
    tier: db.getConfig().serverUrl ? "edge" : "local",
  };

  const [showNLastMessages, setShowNLastMessages] = useState(INITIAL_MESSAGES_TO_SHOW);

  const chatRows = getChatRowsQuery(chatId)();
  const chatKnown = chatRows.length > 0;

  const myMemberships = myMembershipForChatQuery(chatId)();
  const isMember = myMemberships.length > 0;

  const autoJoinPending = useRef(false);
  const autoJoined = useRef(false);
  const [membershipReady, setMembershipReady] = useState(false);

  useEffect(() => {
    if (isMember && !autoJoinPending.current) {
      setMembershipReady(true);
      return;
    }

    if (!userId || !chatKnown || isMember || autoJoined.current) return;
    autoJoined.current = true;
    autoJoinPending.current = true;

    db.insert(app.chatMembers, { chatId, userId })
      .wait(sharedWriteOptions)
      .then(() => {
        autoJoinPending.current = false;
        setMembershipReady(true);
      })
      .catch((error) => {
        console.error("auto-join failed", error);
        autoJoined.current = false;
        autoJoinPending.current = false;
      });
  }, [userId, chatKnown, isMember, chatId, db, sharedWriteOptions]);

  const observer = useRef<IntersectionObserver | null>(null);

  const observerTargetCallback = useCallback((node: HTMLButtonElement | null) => {
    if (observer.current) observer.current.disconnect();

    if (node) {
      observer.current = new IntersectionObserver(
        (entries) => {
          if (entries[0].isIntersecting) {
            setShowNLastMessages((prev) => prev + LOAD_MORE_STEP);
          }
        },
        { threshold: 0.1, rootMargin: "100px" },
      );
      observer.current.observe(node);
    }
  }, []);

  const messages = getChatMessagesQuery(chatId, showNLastMessages + 1)();
  const deferredMessages = useDeferredValue(messages);
  const hasMore = deferredMessages.length > showNLastMessages;

  const handleDelete = wrap((messageId: string) => {
    db.delete(app.messages, messageId);
  });

  if (!chatKnown && userId) {
    return (
      <div className="flex-1 flex items-center justify-center p-8 text-center text-muted-foreground">
        <p>You don't have permission to access this chat.</p>
      </div>
    );
  }

  return (
    <>
      <ChatHeader chatId={chatId} />
      <div className="h-full flex-1 overflow-y-auto flex flex-col-reverse p-2 gap-8 pb-6">
        {deferredMessages.length > 0 ? (
          deferredMessages
            .slice(0, showNLastMessages)
            .map((msg) => (
              <ChatMessage
                key={msg.id}
                message={msg}
                sender={msg.sender ?? undefined}
                isMe={msg.senderId === profile?.id || msg.sender?.userId === userId}
                onDelete={() => handleDelete(msg.id)}
              />
            ))
        ) : (
          <div className="flex flex-col items-center justify-center py-10">
            <p className="text-muted-foreground text-sm">No messages yet</p>
          </div>
        )}

        {hasMore && (
          <Button
            ref={observerTargetCallback}
            variant="ghost"
            onClick={() => setShowNLastMessages((prev) => prev + LOAD_MORE_STEP)}
          >
            <Loader2Icon className="mr-2 animate-spin" />
            Loading older messages...
          </Button>
        )}
      </div>

      <MessageComposer chatId={chatId} disabled={!membershipReady} />
    </>
  );
}, "ChatView");

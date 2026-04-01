import { useCallback, useEffect, useRef, useState } from "react";
import { Loader2Icon } from "lucide-react";
import { useDb, useAll, useSession } from "jazz-tools/react";
import { ChatMessage } from "@/components/chat/ChatMessage";
import { ChatHeader } from "@/components/chat-view/ChatHeader";
import { MessageComposer } from "@/components/composer/MessageComposer";
import { Button } from "@/components/ui/button";
import { app } from "../../../schema.js";

const INITIAL_MESSAGES_TO_SHOW = 20;
const LOAD_MORE_STEP = 20;

interface ChatViewProps {
  chatId: string;
}

export const ChatView = ({ chatId }: ChatViewProps) => {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id ?? null;

  const [showNLastMessages, setShowNLastMessages] = useState(INITIAL_MESSAGES_TO_SHOW);

  // After a brief sync window, if the chat row is still not visible to this
  // user, we know they don't have permission (private chat, not a member).
  const chatRows = useAll(app.chats.where({ id: chatId })) ?? [];
  const chat = chatRows[0];
  const chatKnown = chatRows.length > 0;

  // Auto-join: if the user can see the chat but isn't a member yet, insert a
  // chatMember row so they appear in the member list and can send messages.
  const myMemberships =
    useAll(app.chatMembers.where({ chatId, userId: userId ?? "__none__" })) ?? [];
  const isMember = myMemberships.length > 0;
  const autoJoined = useRef(false);

  useEffect(() => {
    if (!userId || !chatKnown || isMember || autoJoined.current) return;
    autoJoined.current = true;
    db.insert(app.chatMembers, { chatId, userId });
  }, [userId, chatKnown, isMember, chatId, db]);

  const [accessChecked, setAccessChecked] = useState(false);
  useEffect(() => {
    setAccessChecked(false);
    autoJoined.current = false;
    const timer = setTimeout(() => setAccessChecked(true), 1500);
    return () => clearTimeout(timer);
  }, [chatId]);

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

  const messages =
    useAll(
      app.messages
        .where({ chatId })
        .include({ sender: true })
        .orderBy("createdAt", "desc")
        .limit(showNLastMessages + 1),
    ) ?? [];

  const hasMore = messages.length > showNLastMessages;

  const handleDelete = (messageId: string) => {
    db.delete(app.messages, messageId);
  };

  if (accessChecked && !chatKnown && userId) {
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
        {messages.length > 0 ? (
          messages
            .slice(0, showNLastMessages)
            .map((msg) => (
              <ChatMessage
                key={msg.id}
                message={msg}
                sender={msg.sender}
                isMe={msg.sender.userId === userId}
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

      <MessageComposer chatId={chatId} />
    </>
  );
};

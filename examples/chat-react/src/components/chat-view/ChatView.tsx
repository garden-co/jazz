import { useCallback, useEffect, useRef, useState } from "react";
import { deleteCoValues, type ID } from "jazz-tools";
import {
  useCoStates,
  useSuspenseAccount,
  useSuspenseCoState,
} from "jazz-tools/react";
import { Loader2Icon } from "lucide-react";
import { ChatMessage } from "@/components/chat/ChatMessage";
import { MessageComposer } from "@/components/composer/MessageComposer";
import { Button } from "@/components/ui/button";
import { Chat, ChatAccount, Message } from "@/schema";

const INITIAL_MESSAGES_TO_SHOW = 20;
const LOAD_MORE_STEP = 20;

interface ChatViewProps {
  chatId: ID<typeof Chat>;
}

export const ChatView = ({ chatId }: ChatViewProps) => {
  const me = useSuspenseAccount(ChatAccount, {
    resolve: { root: { chats: true }, profile: true },
  });
  const chat = useSuspenseCoState(Chat, chatId);
  const [showNLastMessages, setShowNLastMessages] = useState(
    INITIAL_MESSAGES_TO_SHOW,
  );

  const observer = useRef<IntersectionObserver | null>(null);

  // Sync chat to account root in case it's not already there
  useEffect(() => {
    if (me.$isLoaded && !me.root.chats.$jazz.has(chatId)) {
      me.root.chats.$jazz.set(chatId, chat);
    }
  }, [me, chat, chatId]);

  const observerTargetCallback = useCallback(
    (node: HTMLButtonElement | null) => {
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
    },
    [],
  );

  const messageIds = Array.from(chat.$jazz.refs)
    .slice(-showNLastMessages)
    .reverse()
    .map((msgRef) => msgRef.id);

  const messages = useCoStates(Message, messageIds, {
    resolve: {
      text: true,
      attachment: true,
      reactions: true,
    },
  });

  const allLoaded = messages.every(
    (msg) => msg && msg.$jazz.loadingState !== "loading",
  );
  const hasMore = chat.length > showNLastMessages;

  const onDelete = async (id: ID<typeof Message>) => {
    chat.$jazz.remove((msg) => msg.$jazz.id === id);
    await deleteCoValues(Message, id);
  };

  return (
    <>
      <div className="h-full flex-1 overflow-y-auto flex flex-col-reverse p-2 gap-8 pb-6">
        {messages.length > 0 ? (
          messages.map((msg) =>
            msg.$isLoaded ? (
              <ChatMessage
                key={msg.$jazz.id}
                message={msg}
                onDelete={onDelete}
              />
            ) : null,
          )
        ) : (
          <div className="flex flex-col items-center justify-center py-10">
            <p className="text-muted-foreground text-sm">No messages yet</p>
          </div>
        )}

        {allLoaded && hasMore && (
          <Button
            ref={observerTargetCallback}
            variant="ghost"
            onClick={() =>
              setShowNLastMessages(showNLastMessages + LOAD_MORE_STEP)
            }
          >
            <Loader2Icon className="mr-2 animate-spin" />
            Loading older messages...
          </Button>
        )}
      </div>

      <MessageComposer chat={chat} />
    </>
  );
};

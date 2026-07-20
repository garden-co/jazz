import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Loader2Icon } from "lucide-react";
import { useDb, useAll, useSession } from "jazz-tools/react";
import { ChatMessage } from "@/components/chat/ChatMessage";
import { ChatHeader } from "@/components/chat-view/ChatHeader";
import { MessageComposer } from "@/components/composer/MessageComposer";
import { Button } from "@/components/ui/button";
import { useMyProfile } from "@/hooks/useMyProfile";
import { fireAndReport, waitForWrite } from "@/lib/db-write";
import { app } from "../../../schema.js";
import { type DurabilityTier } from "jazz-tools";

const INITIAL_MESSAGES_TO_SHOW = 20;
const LOAD_MORE_STEP = 20;

interface ChatViewProps {
  chatId: string;
}

export const ChatView = ({ chatId }: ChatViewProps) => {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id ?? null;
  const myProfile = useMyProfile();
  const sharedWriteOptions: { tier: DurabilityTier } = useMemo(
    () => ({ tier: db.getConfig().serverUrl ? "edge" : "local" }),
    [db],
  );

  const [showNLastMessages, setShowNLastMessages] = useState(INITIAL_MESSAGES_TO_SHOW);

  const chatRowsResult = useAll(app.chats.where({ id: chatId }));
  const chatRows = chatRowsResult.data ?? [];
  const chat = chatRows[0];
  const chatKnown = chatRows.length > 0;

  // Auto-join: if the user can see the chat but isn't a member yet, insert a
  // chatMember row so they appear in the member list and can send messages.
  const myMembershipsResult = useAll(
    app.chatMembers.where({ chatId, userId: userId ?? "__none__" }),
    sharedWriteOptions,
  );
  const myMemberships = myMembershipsResult.data ?? [];
  const membershipKnown = myMembershipsResult.data !== undefined;
  const isMember = myMemberships.length > 0;
  // autoJoinPending: true while we've started the insert but haven't yet
  // received server acknowledgement.  Used to suppress the isMember shortcut
  // so a local-only membership row can't unlock the composer prematurely.
  const autoJoinPending = useRef(false);
  const autoJoined = useRef(false);

  // membershipReady gates the composer: true when we know the server has
  // acknowledged this user's membership.  Starts true if the user was already
  // a member before this component mounted (e.g. returning to a chat they
  // joined in a previous session); otherwise becomes true only after the
  // auto-join insert is durably persisted at edge tier.
  const [membershipReady, setMembershipReady] = useState(false);
  const [autoJoinFailed, setAutoJoinFailed] = useState(false);

  useEffect(() => {
    autoJoined.current = false;
    autoJoinPending.current = false;
    setMembershipReady(false);
    setAutoJoinFailed(false);
  }, [chatId]);

  useEffect(() => {
    // If the local store already shows membership AND we haven't just inserted
    // it ourselves (i.e. this is a pre-existing membership), unlock the
    // composer immediately — no insert needed.
    if (isMember && !autoJoinPending.current) {
      setMembershipReady(true);
      return;
    }

    const canRequestMembership = chatKnown && chat?.isPublic;

    if (!userId || !canRequestMembership || !membershipKnown || isMember || autoJoined.current)
      return;
    autoJoined.current = true;
    autoJoinPending.current = true;
    setAutoJoinFailed(false);

    waitForWrite(db.insert(app.chatMembers, { chatId, userId }), sharedWriteOptions)
      .then(() => {
        autoJoinPending.current = false;
        setMembershipReady(true);
      })
      .catch((error) => {
        console.error("auto-join failed", error);
        autoJoined.current = false;
        autoJoinPending.current = false;
        setAutoJoinFailed(true);
      });
  }, [
    userId,
    chatKnown,
    chat?.isPublic,
    membershipKnown,
    isMember,
    chatId,
    db,
    sharedWriteOptions,
  ]);

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

  const canRenderChat = chatKnown || membershipKnown;
  const canReadChatContents = chatKnown || membershipReady;
  const { data: messages = [] } = useAll(
    app.messages
      .where({ chatId: canReadChatContents ? chatId : "00000000-0000-0000-0000-000000000000" })
      .include({ sender: true })
      .orderBy("createdAt", "desc")
      .limit(showNLastMessages + 1),
  );

  const hasMore = messages.length > showNLastMessages;

  const handleDelete = (messageId: string) => {
    fireAndReport(db.delete(app.messages, messageId), "failed to delete message");
  };

  if (chatRowsResult !== undefined && !chatKnown && userId && autoJoinFailed) {
    return (
      <div className="flex-1 flex items-center justify-center p-8 text-center text-muted-foreground">
        <p>You don't have permission to access this chat.</p>
      </div>
    );
  }

  if (!canRenderChat) {
    return (
      <div className="flex-1 grid place-items-center p-8 text-center text-muted-foreground italic">
        <div className="flex gap-2">
          <Loader2Icon className="animate-spin" />
          Loading chat...
        </div>
      </div>
    );
  }

  if (chat && !chat.isPublic && membershipKnown && !isMember) {
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
                sender={msg.sender ?? undefined}
                isMe={msg.senderId === myProfile?.id || msg.sender?.userId === userId}
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
};

import type { CoFeedEntry, ID } from "jazz-tools";
import { Suspense } from "react";
import clsx from "clsx";
import { useSuspenseCoState } from "jazz-tools/react";
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/ui/hover-card";
import { type ChatAccount, ChatAccountWithProfile, Message } from "@/schema";

interface ReactingUserProps {
  userId: ID<typeof ChatAccount>;
  currentUserId?: ID<typeof ChatAccount>;
}

const ReactingUser = ({ userId, currentUserId }: ReactingUserProps) => {
  const user = useSuspenseCoState(ChatAccountWithProfile, userId);

  return (
    <div>
      <span className={user.$jazz.id === currentUserId ? "font-semibold" : ""}>
        {user.$jazz.id === currentUserId
          ? "You"
          : user.profile?.name || "Anonymous"}
      </span>
    </div>
  );
};

interface ReactorsListProps {
  instances: { by?: { $jazz: { id: ID<typeof ChatAccount> } } | null }[];
  currentUserId: ID<typeof ChatAccount> | undefined;
}

const ReactorsList = ({ instances, currentUserId }: ReactorsListProps) => {
  return (
    <div className="flex flex-col gap-1">
      {instances?.map((r) => {
        if (!r.by) return null;
        return (
          <ReactingUser
            key={r.by.$jazz.id}
            userId={r.by.$jazz.id}
            currentUserId={currentUserId}
          />
        );
      })}
    </div>
  );
};

interface ReactionPillProps {
  emoji: string;
  count: number;
  iReacted: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}

const ReactionPill = ({
  emoji,
  count,
  iReacted,
  onToggle,
  children,
}: ReactionPillProps) => {
  return (
    <HoverCard openDelay={200} closeDelay={100}>
      <HoverCardTrigger asChild>
        <button
          type="button"
          onPointerDown={(e) => e.stopPropagation()}
          onClick={(e) => {
            e.stopPropagation();
            onToggle();
          }}
          className={clsx(
            "text-xs px-1.5 py-0.5 rounded-full border shadow-sm transition-colors text-nowrap",
            iReacted
              ? "bg-primary text-primary-foreground border-primary-foreground"
              : "bg-background hover:bg-muted",
          )}
        >
          {emoji}
          {count > 1 && <span className="ml-1 opacity-75"> {count}</span>}
        </button>
      </HoverCardTrigger>

      <HoverCardContent
        side="top"
        className="w-fit min-w-25 p-2 text-xs"
        // Prevent clicking the card from triggering the message bubble dropdown
        onPointerDown={(e) => e.stopPropagation()}
      >
        <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
      </HoverCardContent>
    </HoverCard>
  );
};

interface MessageReactionsProps {
  messageId: ID<typeof Message>;
  currentUserId: ID<typeof ChatAccount> | undefined;
  isMe: boolean;
}

export const MessageReactions = ({
  messageId,
  currentUserId,
  isMe,
}: MessageReactionsProps) => {
  const message = useSuspenseCoState(Message, messageId, {
    resolve: {
      reactions: true,
    },
  });

  const reactions = Object.values(message.reactions.perAccount).reduce<
    Record<string, CoFeedEntry<string>[]>
  >((acc, item) => {
    const key = item.value;
    if (!acc[key]) acc[key] = [];
    acc[key].push(item);
    return acc;
  }, {});

  const myReaction = message.reactions.byMe?.value;

  const handleToggle = (emoji: string) => {
    const newValue = myReaction === emoji ? "" : emoji;
    message.reactions.$jazz.push(newValue);
  };

  const reactionEntries = Object.entries(reactions)
    .filter(([value, count]) => value && count && count.length > 0)
    .sort();

  if (reactionEntries.length === 0) return null;

  return (
    <div
      className={clsx(
        "absolute bottom-0 transform translate-y-[90%] flex gap-1 z-10 w-fit",
        isMe ? "right-0" : "left-0",
      )}
    >
      {reactionEntries.map(([emoji, instances]) => {
        if (!instances) return null;

        const count = instances.length;
        const iReacted = instances.some(
          (r) => r.by?.$jazz.id === currentUserId,
        );

        return (
          <ReactionPill
            key={emoji}
            emoji={emoji}
            count={count}
            iReacted={iReacted}
            onToggle={() => handleToggle(emoji)}
          >
            <ReactorsList instances={instances} currentUserId={currentUserId} />
          </ReactionPill>
        );
      })}
    </div>
  );
};

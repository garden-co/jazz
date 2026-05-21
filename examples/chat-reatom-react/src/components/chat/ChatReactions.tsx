import { Suspense } from "react";
import { reatomComponent } from "@reatom/react";
import clsx from "clsx";
import { HoverCard, HoverCardContent, HoverCardTrigger } from "@/components/ui/hover-card";
import { jazz } from "@/jazz";
import { getMessageReactionsQuery, getProfilesByUserIdQuery } from "@/model/queries";
import { app } from "../../../schema.js";

const ReactorName = reatomComponent(
  ({ userId, currentUserId }: { userId: string; currentUserId?: string }) => {
    const profiles = getProfilesByUserIdQuery(userId)();
    const profile = profiles[0];

    if (userId === currentUserId) return <li>You</li>;
    return <li>{profile?.name ?? "Unknown"}</li>;
  },
  "ReactorName",
);

function ReactorsList({
  reactorUserIds,
  currentUserId,
}: {
  reactorUserIds: string[];
  currentUserId?: string;
}) {
  return (
    <ul className="space-y-0.5">
      {reactorUserIds.map((uid) => (
        <Suspense key={uid} fallback={<li>...</li>}>
          <ReactorName userId={uid} currentUserId={currentUserId} />
        </Suspense>
      ))}
    </ul>
  );
}

interface ReactionPillProps {
  emoji: string;
  count: number;
  iReacted: boolean;
  reactorUserIds: string[];
  currentUserId?: string;
  onToggle: () => void;
}

const ReactionPill = ({
  emoji,
  count,
  iReacted,
  reactorUserIds,
  currentUserId,
  onToggle,
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
        onPointerDown={(e) => e.stopPropagation()}
      >
        <Suspense fallback={<div>Loading...</div>}>
          <ReactorsList reactorUserIds={reactorUserIds} currentUserId={currentUserId} />
        </Suspense>
      </HoverCardContent>
    </HoverCard>
  );
};

interface MessageReactionsProps {
  messageId: string;
  isMe: boolean;
}

export const MessageReactions = reatomComponent(({ messageId, isMe }: MessageReactionsProps) => {
  const { db, session } = jazz();
  const userId = session?.user_id;

  const reactions = getMessageReactionsQuery(messageId)();

  if (reactions.length === 0) return null;

  const grouped: Record<string, typeof reactions> = {};
  for (const r of reactions) {
    if (!grouped[r.emoji]) grouped[r.emoji] = [];
    grouped[r.emoji].push(r);
  }

  const handleToggle = (emoji: string) => {
    if (!userId) return;
    const myReaction = reactions.find((r) => r.emoji === emoji && r.userId === userId);
    if (myReaction) {
      db.delete(app.reactions, myReaction.id);
    } else {
      db.insert(app.reactions, {
        messageId,
        userId,
        emoji,
      });
    }
  };

  const entries = Object.entries(grouped).sort();

  return (
    <div
      className={clsx(
        "absolute bottom-0 transform translate-y-[90%] flex gap-1 z-10 w-fit",
        isMe ? "right-0" : "left-0",
      )}
    >
      {entries.map(([emoji, instances]) => {
        const count = instances.length;
        const iReacted = instances.some((r) => r.userId === userId);
        const reactorUserIds = instances.map((r) => r.userId);

        return (
          <ReactionPill
            key={emoji}
            emoji={emoji}
            count={count}
            iReacted={iReacted}
            reactorUserIds={reactorUserIds}
            currentUserId={userId}
            onToggle={() => handleToggle(emoji)}
          />
        );
      })}
    </div>
  );
}, "MessageReactions");

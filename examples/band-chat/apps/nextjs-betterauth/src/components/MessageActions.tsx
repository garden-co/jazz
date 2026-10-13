"use client";

import { useState } from "react";
import { useDb } from "jazz-tools/react";
import {
  Button,
  HStack,
  MoreMenu,
  Popover,
  TextInput,
  ToggleButton,
  VStack,
} from "@astryxdesign/core";
import { app, type Reaction } from "../../schema";
import { displayNameFor, useDirectory } from "../lib/profiles";
import type { MessageSummary } from "./RoomView";

const QUICK_REACTIONS = ["👍", "❤️", "🔥", "😂", "🎸", "🥁", "🎤", "👏"];
const EMOJI = /^(\p{Extended_Pictographic}|\p{Emoji_Component}|\p{Emoji_Presentation})+$/u;

/**
 * Reaction toggles, the reaction picker and, for your own messages, who read
 * it and delete.
 */
export function MessageActions({
  message,
  reactions,
  author,
  isMine,
  onShowReadBy,
}: {
  message: MessageSummary;
  reactions: Reaction[];
  author: string;
  isMine: boolean;
  onShowReadBy: () => void;
}) {
  const db = useDb();
  const directory = useDirectory();
  const byEmoji = new Map<string, Reaction[]>();
  for (const reaction of reactions) {
    const list = byEmoji.get(reaction.emoji) ?? [];
    list.push(reaction);
    byEmoji.set(reaction.emoji, list);
  }

  function toggle(emoji: string) {
    const mine = reactions.find(
      (reaction) => reaction.emoji === emoji && reaction.author === author,
    );
    if (mine) db.delete(app.reactions, mine.id);
    else db.insert(app.reactions, { roomId: message.roomId, messageId: message.id, author, emoji });
  }

  return (
    <HStack gap={1} wrap="wrap" vAlign="center" className="message-actions">
      {[...byEmoji.entries()].map(([emoji, list]) => {
        const names = list.map((reaction) =>
          displayNameFor(directory, { author: reaction.author }),
        );
        return (
          <ToggleButton
            key={emoji}
            size="sm"
            label={`${emoji} ${list.length}`}
            tooltip={names.join(", ")}
            isPressed={list.some((reaction) => reaction.author === author)}
            onPressedChange={() => toggle(emoji)}
          />
        );
      })}
      <ReactionPicker onPick={toggle} />
      {isMine ? (
        <MoreMenu
          label="Message options"
          size="sm"
          items={[
            { label: "Read by", onClick: onShowReadBy },
            {
              label: "Delete message",
              variant: "destructive",
              onClick: () => db.delete(app.messages, message.id),
            },
          ]}
        />
      ) : null}
    </HStack>
  );
}

function ReactionPicker({ onPick }: { onPick: (emoji: string) => void }) {
  const [isOpen, setOpen] = useState(false);
  const [custom, setCustom] = useState("");
  function pick(emoji: string) {
    onPick(emoji);
    setOpen(false);
    setCustom("");
  }
  const trimmed = custom.trim();
  return (
    <Popover
      isOpen={isOpen}
      onOpenChange={setOpen}
      label="Add a reaction"
      placement="above"
      content={
        <VStack gap={2} padding={2}>
          <HStack gap={1} wrap="wrap">
            {QUICK_REACTIONS.map((emoji) => (
              <Button key={emoji} label={emoji} variant="ghost" onClick={() => pick(emoji)} />
            ))}
          </HStack>
          <HStack gap={1} vAlign="end">
            <TextInput
              label="Other emoji"
              size="sm"
              value={custom}
              onChange={setCustom}
              onEnter={() => EMOJI.test(trimmed) && pick(trimmed)}
            />
            <Button
              label="Add"
              size="sm"
              isDisabled={!EMOJI.test(trimmed)}
              onClick={() => pick(trimmed)}
            />
          </HStack>
        </VStack>
      }
    >
      {(trigger) => (
        <Button
          ref={trigger.ref}
          label="React"
          size="sm"
          variant="ghost"
          onClick={trigger.onClick}
          aria-haspopup={trigger["aria-haspopup"]}
          aria-expanded={trigger["aria-expanded"]}
          aria-controls={trigger["aria-controls"]}
        />
      )}
    </Popover>
  );
}

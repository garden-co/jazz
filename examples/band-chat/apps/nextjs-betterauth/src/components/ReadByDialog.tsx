"use client";

import { useAll } from "jazz-tools/react";
import { Dialog, DialogHeader, List, ListItem, Text, VStack } from "@astryxdesign/core";
import { app } from "../../schema";
import { ProfileAvatar, useDirectory } from "../lib/profiles";
import type { MessageSummary } from "./RoomView";

/**
 * Who has read a message: the room's current members whose read marker is at
 * or past it. A marker is one "read up to" position per member, with no
 * history, so there is no read time to show, only names.
 */
export function ReadByDialog({
  message,
  onOpenChange,
}: {
  /** The message to show readers for; the dialog is closed when undefined. */
  message: MessageSummary | undefined;
  onOpenChange: (isOpen: boolean) => void;
}) {
  const directory = useDirectory();
  const { data: members } = useAll(
    message ? app.roomMembers.where({ roomId: message.roomId }) : undefined,
  );
  const { data: markers } = useAll(
    message
      ? app.readMarkers.where({ roomId: message.roomId, lastReadAt: { gte: message.$createdAt } })
      : undefined,
  );
  const readerAuthors = new Set((markers ?? []).map((marker) => marker.reader));
  const readers = (members ?? []).flatMap((member) => {
    if (!readerAuthors.has(member.memberAuthor)) return [];
    const profile =
      (member.memberProfileId ? directory.byId.get(member.memberProfileId) : undefined) ??
      directory.byAuthor.get(member.memberAuthor);
    // The sender has read their own message; listing them adds nothing.
    if (profile?.id === message?.senderId) return [];
    return [{ member, profile, name: profile?.displayName ?? "Bandmate" }];
  });
  readers.sort((a, b) => a.name.localeCompare(b.name));

  return (
    <Dialog isOpen={!!message} onOpenChange={onOpenChange} width={420}>
      <DialogHeader title="Read by" onOpenChange={onOpenChange} />
      <VStack gap={4} padding={4}>
        {!members || !markers ? null : readers.length === 0 ? (
          <Text color="secondary">Nobody has read this yet.</Text>
        ) : (
          <List hasDividers>
            {readers.map(({ member, profile, name }) => (
              <ListItem
                key={member.id}
                startContent={<ProfileAvatar profile={profile} size="md" />}
                label={name}
              />
            ))}
          </List>
        )}
      </VStack>
    </Dialog>
  );
}

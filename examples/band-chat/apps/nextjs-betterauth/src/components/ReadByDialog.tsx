"use client";

import { useAll } from "jazz-tools/react";
import { Dialog, DialogHeader, List, ListItem, Text, Timestamp, VStack } from "@astryxdesign/core";
import { app } from "../../schema";
import { ProfileAvatar, useDirectory } from "../lib/profiles";
import type { MessageSummary } from "./RoomView";

/**
 * Who has read a message, and when. Each membership brings the first entry
 * of its read journal that reaches the message; that entry was written when
 * the member's marker moved past it, so its `$createdAt` is the read date.
 * A member who jumped straight to the end read every skipped message at once.
 *
 * The sheet is a one-room, members-sized read, and BandChat applies no limit
 * on room size or message age to it.
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
    message
      ? app.roomMembers.where({ roomId: message.roomId }).include({
          progressViaMember: app.readProgress
            .select("upToAt", "$createdAt")
            .where({ upToAt: { gte: message.$createdAt } })
            .orderBy("upToAt", "asc")
            .limit(1),
        })
      : undefined,
  );
  const readers = (members ?? [])
    .flatMap((member) => {
      const first = member.progressViaMember[0];
      const profile =
        (member.memberProfileId ? directory.byId.get(member.memberProfileId) : undefined) ??
        directory.byAuthor.get(member.memberAuthor);
      // The sender has read their own message; listing them adds nothing.
      if (!first || profile?.id === message?.senderId) return [];
      return [{ member, profile, readAt: first.$createdAt }];
    })
    .sort((a, b) => a.readAt.getTime() - b.readAt.getTime());

  return (
    <Dialog isOpen={!!message} onOpenChange={onOpenChange} width={420}>
      <DialogHeader title="Read by" onOpenChange={onOpenChange} />
      <VStack gap={4} padding={4}>
        {!members ? null : readers.length === 0 ? (
          <Text color="secondary">Nobody has read this yet.</Text>
        ) : (
          <List hasDividers>
            {readers.map(({ member, profile, readAt }) => (
              <ListItem
                key={member.id}
                startContent={<ProfileAvatar profile={profile} size="md" />}
                label={profile?.displayName ?? "Bandmate"}
                endContent={<Timestamp value={readAt.toISOString()} format="relative_short" />}
              />
            ))}
          </List>
        )}
      </VStack>
    </Dialog>
  );
}

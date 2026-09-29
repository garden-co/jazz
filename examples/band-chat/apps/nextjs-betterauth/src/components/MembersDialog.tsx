"use client";

import { useState } from "react";
import { useDb } from "jazz-tools/react";
import {
  Badge,
  Button,
  Dialog,
  DialogHeader,
  HStack,
  List,
  ListItem,
  Text,
  TextInput,
  VStack,
} from "@astryxdesign/core";
import { app, type JoinRequest, type RoomMember } from "../../schema";
import { ProfileAvatar, useDirectory } from "../lib/profiles";
import { roomLink } from "../lib/url-state";

/**
 * Admission stays with the room creator. A room link only lets someone *ask*
 * to join; the creator admits requests or adds people they already share a
 * room with. Anyone may leave; only the creator removes others.
 */
export function MembersDialog({
  isOpen,
  onOpenChange,
  roomId,
  author,
  isCreator,
  members,
  requests,
}: {
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
  roomId: string;
  author: string;
  isCreator: boolean;
  members: RoomMember[];
  requests: JoinRequest[];
}) {
  const db = useDb();
  const directory = useDirectory();
  const memberAuthors = new Set(members.map((member) => member.memberAuthor));
  const known = directory.others.filter((profile) => !memberAuthors.has(profile.author));
  const pending = requests.filter((request) => !memberAuthors.has(request.requester));

  // Admission and clearing the request commit together. Both policies check
  // only the committed room, profile and request, never each other.
  function admit(memberAuthor: string, memberProfileId: string) {
    void db
      .transaction((tx) => {
        tx.insert(app.roomMembers, { roomId, memberAuthor, memberProfileId });
        for (const request of requests)
          if (request.requester === memberAuthor) tx.delete(app.joinRequests, request.id);
      })
      .catch((error: unknown) => console.error("Could not admit member", error));
  }

  return (
    <Dialog isOpen={isOpen} onOpenChange={onOpenChange} width={520}>
      <DialogHeader
        title={isCreator ? "Members and invites" : "Members"}
        onOpenChange={onOpenChange}
      />
      <VStack gap={6} padding={4}>
        {isCreator ? <InviteLink roomId={roomId} /> : null}
        {isCreator && pending.length > 0 ? (
          <List header={<SectionTitle>Asking to join</SectionTitle>} hasDividers>
            {pending.map((request) => {
              const profile = directory.byId.get(request.profileId);
              return (
                <ListItem
                  key={request.id}
                  startContent={<ProfileAvatar profile={profile} size="md" />}
                  label={profile?.displayName ?? "Someone"}
                  endContent={
                    <HStack gap={1}>
                      <Button
                        label="Decline"
                        size="sm"
                        variant="ghost"
                        onClick={() => db.delete(app.joinRequests, request.id)}
                      />
                      <Button
                        label="Admit"
                        size="sm"
                        variant="primary"
                        onClick={() => admit(request.requester, request.profileId)}
                      />
                    </HStack>
                  }
                />
              );
            })}
          </List>
        ) : null}
        <List header={<SectionTitle>{`${members.length} members`}</SectionTitle>} hasDividers>
          {members.map((member) => {
            const profile =
              (member.memberProfileId ? directory.byId.get(member.memberProfileId) : undefined) ??
              directory.byAuthor.get(member.memberAuthor);
            const isMe = member.memberAuthor === author;
            return (
              <ListItem
                key={member.id}
                data-member-author={member.memberAuthor}
                startContent={<ProfileAvatar profile={profile} size="md" />}
                label={
                  isMe
                    ? `${profile?.displayName ?? "You"} (you)`
                    : (profile?.displayName ?? "Bandmate")
                }
                endContent={
                  isMe && isCreator ? (
                    <Badge label="Creator" />
                  ) : isCreator ? (
                    <Button
                      label="Remove"
                      size="sm"
                      variant="ghost"
                      onClick={() => db.delete(app.roomMembers, member.id)}
                    />
                  ) : undefined
                }
              />
            );
          })}
        </List>
        {isCreator && known.length > 0 ? (
          <List header={<SectionTitle>People you know</SectionTitle>} hasDividers>
            {known.map((profile) => (
              <ListItem
                key={profile.id}
                startContent={<ProfileAvatar profile={profile} size="md" />}
                label={profile.displayName}
                endContent={
                  <Button label="Add" size="sm" onClick={() => admit(profile.author, profile.id)} />
                }
              />
            ))}
          </List>
        ) : null}
      </VStack>
    </Dialog>
  );
}

function SectionTitle({ children }: { children: string }) {
  return (
    <Text type="label" color="secondary">
      {children}
    </Text>
  );
}

function InviteLink({ roomId }: { roomId: string }) {
  const link = roomLink(roomId);
  const [copied, setCopied] = useState(false);
  return (
    <VStack gap={2}>
      <HStack gap={2} vAlign="end">
        <TextInput
          label="Room link"
          description="Anyone with this link can ask to join. You decide who gets in."
          value={link}
          isReadOnly
          width="100%"
        />
        <Button
          label={copied ? "Copied" : "Copy link"}
          onClick={() =>
            void navigator.clipboard
              .writeText(link)
              .then(() => setCopied(true))
              .catch(() => setCopied(false))
          }
        />
      </HStack>
    </VStack>
  );
}

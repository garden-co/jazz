import { useState } from "react";
import { id } from "@instantdb/react";
import {
  Badge,
  Banner,
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
import { db, transact, type JoinRequest, type RoomMember } from "../lib/db";
import { ProfileAvatar, useDirectory } from "../lib/profiles";
import { roomLink } from "../lib/url-state";

/**
 * Admission stays with the room creator. A room link only lets someone *ask*
 * to join, and the creator admits requests. Anyone may leave; only the
 * creator removes others.
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
  const directory = useDirectory();
  const memberAuthors = new Set(members.map((member) => member.memberAuthor));
  const pending = requests.filter((request) => !memberAuthors.has(request.requester));
  const [error, setError] = useState<string | null>(null);

  // Admission, then clearing the request. Instant checks the membership's
  // rule against the state after its transaction, which must still hold the
  // request, so the two can't commit together.
  function admit(memberAuthor: string, memberProfileId: string) {
    setError(null);
    transact(
      db.tx.roomMembers[id()]
        .create({ memberAuthor })
        .link({ room: roomId, memberProfile: memberProfileId }),
    )
      .then(() =>
        transact(
          requests
            .filter((request) => request.requester === memberAuthor)
            .map((request) => db.tx.joinRequests[request.id].delete()),
        ),
      )
      .catch((cause: unknown) =>
        setError(`Could not admit them: ${cause instanceof Error ? cause.message : String(cause)}`),
      );
  }

  return (
    <Dialog isOpen={isOpen} onOpenChange={onOpenChange} width={520}>
      <DialogHeader
        title={isCreator ? "Members and invites" : "Members"}
        onOpenChange={onOpenChange}
      />
      <VStack gap={6} padding={4}>
        {error ? (
          <Banner status="error" title={error} container="section" collapsible={false} />
        ) : null}
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
                        onClick={() => transact(db.tx.joinRequests[request.id].delete())}
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
                      onClick={() => transact(db.tx.roomMembers[member.id].delete())}
                    />
                  ) : undefined
                }
              />
            );
          })}
        </List>
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

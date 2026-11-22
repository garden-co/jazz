import { useState } from "react";
import { useDb } from "jazz-tools/react";
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
import { app, type E2eeMember, type JoinRequest } from "../../jazz.generated";
import { admitMember, removeMember, renewRoomKey } from "../lib/membership";
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
  /** The room's member rows; undefined while they load. */
  members: E2eeMember[] | undefined;
  requests: JoinRequest[];
}) {
  const db = useDb();
  const directory = useDirectory();
  const memberAuthors = new Set((members ?? []).map((member) => member.accountId));
  const pending = requests.filter((request) => !memberAuthors.has(request.requester));
  const [error, setError] = useState<string | null>(null);
  // What the last admission or removal left undone, each with its retry.
  const [notice, setNotice] = useState<"delivery" | "rotation" | null>(null);

  const describe = (cause: unknown) => (cause instanceof Error ? cause.message : String(cause));

  // The member row and the profile membership commit with the cleared
  // request, then keys are delivered. A delivery that reports a failure
  // leaves the person admitted but unable to read yet.
  function admit(memberAuthor: string, memberProfileId: string) {
    setError(null);
    setNotice(null);
    admitMember({
      db,
      roomId,
      memberAuthor,
      memberProfileId,
      requestIds: requests.filter((r) => r.requester === memberAuthor).map((r) => r.id),
    }).then(
      (delivery) => setNotice(delivery.failed > 0 ? "delivery" : null),
      (cause: unknown) => setError(`Admission refused: ${describe(cause)}`),
    );
  }

  function retryDelivery() {
    setNotice(null);
    db.encryption.keys.deliver().then(
      (delivery) => setNotice(delivery.failed > 0 ? "delivery" : null),
      () => setNotice("delivery"),
    );
  }

  function renewKey() {
    setNotice(null);
    renewRoomKey(db, roomId).catch(() => setNotice("rotation"));
  }

  // Removal waits for the authority; only then is the room's key renewed, so
  // the removed member reads nothing written later.
  function remove(accountId: string) {
    setError(null);
    setNotice(null);
    removeMember({ db, roomId, accountId }).then(
      () => renewKey(),
      (cause: unknown) => setError(`Could not remove them: ${describe(cause)}`),
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
        {notice === "delivery" ? (
          <Banner
            status="warning"
            title="Key not delivered yet"
            description="They are admitted, but cannot read the room until their key arrives."
            container="section"
            collapsible={false}
            endContent={<Button label="Retry" size="sm" onClick={retryDelivery} />}
          />
        ) : null}
        {notice === "rotation" ? (
          <Banner
            status="warning"
            title="Removed, but the room's key is not renewed yet"
            description="They stay able to read what is written until the key is renewed."
            container="section"
            collapsible={false}
            endContent={<Button label="Retry" size="sm" onClick={renewKey} />}
          />
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
        {members ? (
          <List header={<SectionTitle>{`${members.length} members`}</SectionTitle>} hasDividers>
            {members.map((member) => {
              const profile = directory.byAuthor.get(member.accountId);
              const isMe = member.accountId === author;
              return (
                <ListItem
                  key={member.id}
                  data-member-author={member.accountId}
                  startContent={<ProfileAvatar profile={profile} size="md" />}
                  label={
                    isMe
                      ? `${profile?.displayName ?? "You"} (you)`
                      : (profile?.displayName ?? "Unknown")
                  }
                  endContent={
                    isMe && isCreator ? (
                      <Badge label="Creator" />
                    ) : isCreator ? (
                      <Button
                        label="Remove"
                        size="sm"
                        variant="ghost"
                        onClick={() => remove(member.accountId)}
                      />
                    ) : undefined
                  }
                />
              );
            })}
          </List>
        ) : (
          <Text type="label" color="secondary">
            Loading members…
          </Text>
        )}
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

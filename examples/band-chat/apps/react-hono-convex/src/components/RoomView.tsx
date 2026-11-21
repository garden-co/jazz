import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "convex/react";
import {
  AvatarGroup,
  Badge,
  Banner,
  Button,
  Center,
  ChatLayout,
  ChatMessage,
  ChatMessageBubble,
  ChatMessageList,
  ChatMessageMetadata,
  ChatSystemMessage,
  Divider,
  EmptyState,
  Heading,
  HStack,
  MobileNavToggle,
  MoreMenu,
  Spinner,
  StackItem,
  Timestamp,
  VStack,
} from "@astryxdesign/core";
import { api } from "@/convex/_generated/api";
import type { Doc } from "@/convex/_generated/dataModel";
import type { Id, Reaction } from "../lib/model";
import { ProfileAvatar, useDirectory } from "../lib/profiles";
import { Attachment } from "./Attachment";
import { Composer } from "./Composer";
import { MembersDialog } from "./MembersDialog";
import { MessageActions } from "./MessageActions";
import { RenameRoomDialog } from "./RenameRoomDialog";
import type { RoomSummary } from "./RoomNav";
import { SketchCanvas } from "./SketchCanvas";

// Consecutive messages from one sender within this window share a group.
const GROUP_WINDOW_MS = 5 * 60 * 1000;

/** A message row (its attachment is a file-storage id, fetched when needed). */
export type MessageSummary = Doc<"messages">;

/**
 * Opens a room's queries before its view mounts. The Convex client shares a
 * subscription between identical queries, so when the room id is already
 * known (from the URL) its messages load alongside the profile and room list.
 */
export function usePrefetchRoom(roomId: string | null) {
  const args = roomId ? { roomId: roomId as Id<"rooms"> } : "skip";
  useQuery(api.messages.page, args);
  useQuery(api.reactions.byRoom, args);
  useQuery(api.members.byRoom, args);
}

export function RoomView({ summary, author }: { summary: RoomSummary; author: string }) {
  const directory = useDirectory();
  const { room, isCreator } = summary;
  const roomId = room._id;
  const newestFirst = useQuery(api.messages.page, { roomId });
  // Undefined until the first result: "No messages yet" only for a room that has none.
  const isLoadingMessages = newestFirst === undefined;
  const messages = useMemo(() => [...(newestFirst ?? [])].reverse(), [newestFirst]);
  const reactions = useQuery(api.reactions.byRoom, { roomId }) ?? [];
  const loadedMembers = useQuery(api.members.byRoom, { roomId });
  const members = loadedMembers ?? [];
  const requests = useQuery(api.joinRequests.byRoom, isCreator ? { roomId } : "skip") ?? [];
  const allMarkers = useQuery(api.readMarkers.mine);
  const loadedMarkers = allMarkers?.filter((marker) => marker.roomId === roomId);
  const removeMember = useMutation(api.members.remove);
  const admit = useMutation(api.members.admit);
  const createSketch = useMutation(api.messages.startSketch);
  const [isMembersOpen, setMembersOpen] = useState(false);
  const [isRenameOpen, setRenameOpen] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const reportFailure = (action: string) => (cause: unknown) =>
    setActionError(`${action}: ${cause instanceof Error ? cause.message : String(cause)}`);

  useMarkRead({
    roomId,
    author,
    summary,
    markerIds: loadedMarkers?.map((marker) => marker._id),
  });

  const reactionsByMessage = useMemo(() => {
    const grouped = new Map<string, Reaction[]>();
    for (const reaction of reactions) {
      const list = grouped.get(reaction.messageId) ?? [];
      list.push(reaction);
      grouped.set(reaction.messageId, list);
    }
    return grouped;
  }, [reactions]);

  function leave() {
    const mine = members.find((member) => member.memberAuthor === author);
    if (!mine) return;
    // Leaving and dropping this reader's private markers commit together.
    setActionError(null);
    removeMember({ memberId: mine._id }).catch(reportFailure("Could not leave the room"));
  }

  // A creator without a membership (a room created before the room and its
  // membership were written in one transaction) can still read the room;
  // this puts them back in.
  const isMember = members.some((member) => member.memberAuthor === author);
  function rejoin() {
    setActionError(null);
    admit({ roomId, memberAuthor: author as Id<"users">, memberProfileId: directory.me._id }).catch(
      reportFailure("Could not join the room"),
    );
  }

  function startSketch() {
    // The canvas, its message and the room's activity commit together.
    setActionError(null);
    createSketch({ roomId }).catch(reportFailure("Could not start a sketch"));
  }

  const menuItems = [
    { label: isCreator ? "Members and invites" : "Members", onClick: () => setMembersOpen(true) },
    { label: "Start a sketch", onClick: startSketch },
    ...(isCreator
      ? [{ label: "Rename room", onClick: () => setRenameOpen(true) }]
      : [{ label: "Leave room", onClick: leave, variant: "destructive" as const }]),
  ];

  return (
    <VStack height="100%" className="room-view">
      <HStack gap={2} vAlign="center" paddingInline={4} paddingBlock={2}>
        <MobileNavToggle />
        <StackItem size="fill">
          <Heading level={2} maxLines={1}>
            {room.name}
          </Heading>
        </StackItem>
        <AvatarGroup size="sm" aria-label={`${members.length} members`}>
          {members.slice(0, 4).map((member) => (
            <ProfileAvatar
              key={member._id}
              size="sm"
              profile={
                (member.memberProfileId ? directory.byId.get(member.memberProfileId) : undefined) ??
                directory.byAuthor.get(member.memberAuthor)
              }
            />
          ))}
        </AvatarGroup>
        {isCreator ? (
          <Button
            label="Invite"
            // The badge's count runs into the label's text otherwise.
            aria-label={requests.length > 0 ? `Invite ${requests.length}` : undefined}
            size="sm"
            onClick={() => setMembersOpen(true)}
            endContent={
              requests.length > 0 ? (
                <Badge variant="info" label={String(requests.length)} />
              ) : undefined
            }
          />
        ) : null}
        <MoreMenu label="Room options" size="sm" alignment="end" items={menuItems} />
      </HStack>
      <Divider />
      {isCreator && loadedMembers && !isMember ? (
        <Banner
          status="warning"
          title="You are not a member of this room yet"
          container="section"
          collapsible={false}
          endContent={<Button label="Join room" size="sm" onClick={rejoin} />}
        />
      ) : null}
      {actionError ? (
        <Banner
          status="error"
          title={actionError}
          container="section"
          onDismiss={() => setActionError(null)}
        />
      ) : null}
      <StackItem size="fill" className="room-chat">
        <ChatLayout
          composer={
            <Composer
              roomId={roomId}
              roomName={room.name}
              profileId={directory.me._id}
              onStartSketch={startSketch}
            />
          }
          emptyState={
            isLoadingMessages ? (
              <Center axis="both" padding={4}>
                <Spinner label="Loading messages…" />
              </Center>
            ) : (
              <EmptyState
                title="No messages yet"
                description={
                  isCreator
                    ? "Say hello, or invite bandmates with the room link."
                    : "Say hello to the band."
                }
                actions={
                  isCreator ? (
                    <Button label="Invite bandmates" onClick={() => setMembersOpen(true)} />
                  ) : undefined
                }
              />
            )
          }
        >
          {messages.length > 0 ? (
            <ChatMessageList aria-label="Messages">
              {groupMessages(messages).map((group) =>
                group.kind === "day" ? (
                  <ChatSystemMessage key={group.key} variant="divider">
                    <Timestamp value={group.at.toISOString()} format="date_weekday" />
                  </ChatSystemMessage>
                ) : (
                  <MessageGroup
                    key={group.key}
                    messages={group.messages}
                    reactionsByMessage={reactionsByMessage}
                    author={author}
                  />
                ),
              )}
            </ChatMessageList>
          ) : null}
        </ChatLayout>
      </StackItem>
      <MembersDialog
        isOpen={isMembersOpen}
        onOpenChange={setMembersOpen}
        roomId={roomId}
        author={author}
        isCreator={isCreator}
        members={members}
        requests={requests}
      />
      {isCreator ? (
        <RenameRoomDialog
          isOpen={isRenameOpen}
          onOpenChange={setRenameOpen}
          roomId={roomId}
          name={room.name}
        />
      ) : null}
    </VStack>
  );
}

type Group =
  | { kind: "day"; key: string; at: Date }
  | { kind: "messages"; key: string; messages: MessageSummary[] };

function groupMessages(messages: MessageSummary[]): Group[] {
  const groups: Group[] = [];
  let current: MessageSummary[] | null = null;
  let previous: MessageSummary | undefined;
  for (const message of messages) {
    const at = new Date(message._creationTime);
    if (!previous || new Date(previous._creationTime).toDateString() !== at.toDateString()) {
      groups.push({ kind: "day", key: `day-${message._id}`, at });
      current = null;
    }
    const continues =
      current &&
      previous &&
      previous.senderId === message.senderId &&
      message._creationTime - previous._creationTime < GROUP_WINDOW_MS;
    if (!continues) {
      current = [];
      groups.push({ kind: "messages", key: message._id, messages: current });
    }
    current!.push(message);
    previous = message;
  }
  return groups;
}

function MessageGroup({
  messages,
  reactionsByMessage,
  author,
}: {
  messages: MessageSummary[];
  reactionsByMessage: Map<string, Reaction[]>;
  author: string;
}) {
  const directory = useDirectory();
  const senderId = messages[0]!.senderId;
  const isMine = senderId === directory.me._id;
  const sender = directory.byId.get(senderId);
  const name = sender?.displayName ?? "Bandmate";
  const last = messages.length - 1;
  return (
    <ChatMessage
      sender={isMine ? "user" : "assistant"}
      avatar={isMine ? undefined : <ProfileAvatar profile={sender} size="md" />}
    >
      {messages.map((message, index) => {
        const hasMedia = !!message.attachmentName || !!message.canvasId;
        return (
          <ChatMessageBubble
            key={message._id}
            data-message-id={message._id}
            className="message-bubble"
            variant={message.text || !hasMedia ? "filled" : "ghost"}
            width={message.canvasId ? "100%" : undefined}
            group={
              messages.length === 1
                ? undefined
                : index === 0
                  ? "first"
                  : index === last
                    ? "last"
                    : "middle"
            }
            name={!isMine && index === 0 ? name : undefined}
            metadata={
              <ChatMessageMetadata
                timestamp={
                  index === last ? (
                    <Timestamp value={new Date(message._creationTime).toISOString()} format="time" />
                  ) : undefined
                }
                footer={
                  <MessageActions
                    message={message}
                    reactions={reactionsByMessage.get(message._id) ?? []}
                    author={author}
                    isMine={isMine}
                  />
                }
              />
            }
          >
            <VStack gap={2}>
              {message.text ? <span className="message-text">{message.text}</span> : null}
              {message.attachmentName ? <Attachment message={message} /> : null}
              {message.canvasId ? (
                <SketchCanvas canvasId={message.canvasId} roomId={message.roomId} author={author} />
              ) : null}
            </VStack>
          </ChatMessageBubble>
        );
      })}
    </ChatMessage>
  );
}

/**
 * Moves this reader's private marker past the room's last activity while the
 * room is open and the page is visible. Only this account can read the marker.
 */
function useMarkRead({
  roomId,
  author,
  summary,
  markerIds,
}: {
  roomId: string;
  author: string;
  summary: RoomSummary;
  /** Undefined until this reader's markers have loaded. */
  markerIds: string[] | undefined;
}) {
  const mark = useMutation(api.readMarkers.mark);
  const [isVisible, setVisible] = useState(
    () => typeof document === "undefined" || document.visibilityState === "visible",
  );
  useEffect(() => {
    const update = () => setVisible(document.visibilityState === "visible");
    document.addEventListener("visibilitychange", update);
    return () => document.removeEventListener("visibilitychange", update);
  }, []);
  const needsMarker = !!markerIds && (summary.hasUnread || markerIds.length === 0);
  const markerId = markerIds?.[0];
  const activity = summary.room.lastActivityAt ?? 0;
  useEffect(() => {
    if (!isVisible || !needsMarker) return;
    // Another device's clock may run ahead; never leave the marker behind it.
    const lastReadAt = Math.max(Date.now(), activity);
    void mark({ roomId: roomId as Id<"rooms">, lastReadAt });
  }, [mark, isVisible, needsMarker, markerId, activity, roomId, author]);
}

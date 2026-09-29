"use client";

import { useEffect, useMemo, useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
import {
  AvatarGroup,
  Badge,
  Button,
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
  StackItem,
  Timestamp,
  VStack,
} from "@astryxdesign/core";
import { app, type Reaction } from "../../schema";
import { ProfileAvatar, useDirectory } from "../lib/profiles";
import { Attachment } from "./Attachment";
import { Composer } from "./Composer";
import { MembersDialog } from "./MembersDialog";
import { MessageActions } from "./MessageActions";
import { RenameRoomDialog } from "./RenameRoomDialog";
import type { RoomSummary } from "./RoomNav";
import { SketchCanvas } from "./SketchCanvas";

// The newest page of a room's history. Attachment bytes are not selected here:
// each attachment loads its own bytes only when it is shown or downloaded.
const HISTORY_PAGE = 200;
// Consecutive messages from one sender within this window share a group.
const GROUP_WINDOW_MS = 5 * 60 * 1000;

export type MessageSummary = {
  id: string;
  roomId: string;
  senderId: string;
  text: string;
  attachmentName?: string | null;
  attachmentType?: string | null;
  attachmentSize?: number | null;
  canvasId?: string | null;
  $createdAt: Date;
};

export function RoomView({ summary, author }: { summary: RoomSummary; author: string }) {
  const db = useDb();
  const directory = useDirectory();
  const { room, isCreator } = summary;
  const roomId = room.id;
  const { data: newestFirst = [] } = useAll(
    app.messages
      .where({ roomId })
      .select(
        "id",
        "roomId",
        "senderId",
        "text",
        "attachmentName",
        "attachmentType",
        "attachmentSize",
        "canvasId",
        "$createdAt",
      )
      .orderBy("$createdAt", "desc")
      .limit(HISTORY_PAGE),
  );
  const messages = useMemo(() => [...newestFirst].reverse(), [newestFirst]);
  const { data: reactions = [] } = useAll(app.reactions.where({ roomId }));
  const { data: members = [] } = useAll(app.roomMembers.where({ roomId }));
  const { data: requests = [] } = useAll(
    isCreator ? app.joinRequests.where({ roomId }) : undefined,
  );
  const { data: loadedMarkers } = useAll(app.readMarkers.where({ roomId, reader: author }));
  const markers = loadedMarkers ?? [];
  const [isMembersOpen, setMembersOpen] = useState(false);
  const [isRenameOpen, setRenameOpen] = useState(false);

  useMarkRead({
    roomId,
    author,
    summary,
    markerIds: loadedMarkers?.map((marker) => marker.id),
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
    for (const marker of markers) db.delete(app.readMarkers, marker.id);
    db.delete(app.roomMembers, mine.id);
  }

  function startSketch() {
    const canvas = db.insert(app.canvases, { roomId, title: "Sketch" }).value;
    db.insert(app.messages, {
      roomId,
      senderId: directory.me.id,
      text: "",
      canvasId: canvas.id,
    });
    db.update(app.rooms, roomId, { lastActivityAt: new Date() });
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
              key={member.id}
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
      <StackItem size="fill" className="room-chat">
        <ChatLayout
          composer={
            <Composer
              roomId={roomId}
              roomName={room.name}
              profileId={directory.me.id}
              onStartSketch={startSketch}
            />
          }
          emptyState={
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
    if (!previous || previous.$createdAt.toDateString() !== message.$createdAt.toDateString()) {
      groups.push({ kind: "day", key: `day-${message.id}`, at: message.$createdAt });
      current = null;
    }
    const continues =
      current &&
      previous &&
      previous.senderId === message.senderId &&
      message.$createdAt.getTime() - previous.$createdAt.getTime() < GROUP_WINDOW_MS;
    if (!continues) {
      current = [];
      groups.push({ kind: "messages", key: message.id, messages: current });
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
  const isMine = senderId === directory.me.id;
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
            key={message.id}
            data-message-id={message.id}
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
                    <Timestamp value={message.$createdAt.toISOString()} format="time" />
                  ) : undefined
                }
                footer={
                  <MessageActions
                    message={message}
                    reactions={reactionsByMessage.get(message.id) ?? []}
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
  const db = useDb();
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
  const activity = summary.room.lastActivityAt?.getTime() ?? 0;
  useEffect(() => {
    if (!isVisible || !needsMarker) return;
    // Another device's clock may run ahead; never leave the marker behind it.
    const lastReadAt = new Date(Math.max(Date.now(), activity));
    if (markerId) db.update(app.readMarkers, markerId, { lastReadAt });
    else db.insert(app.readMarkers, { roomId, reader: author, lastReadAt });
  }, [db, isVisible, needsMarker, markerId, activity, roomId, author]);
}

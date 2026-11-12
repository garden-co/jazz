"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
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
import { app, type Reaction, type ReadMarker } from "../../schema";
import { ProfileAvatar, useDirectory } from "../lib/profiles";
import { Attachment } from "./Attachment";
import { Composer } from "./Composer";
import { MembersDialog } from "./MembersDialog";
import { MessageActions } from "./MessageActions";
import { ReadByDialog } from "./ReadByDialog";
import { RenameRoomDialog } from "./RenameRoomDialog";
import type { RoomSummary } from "./RoomNav";
import { SketchCanvas } from "./SketchCanvas";

// A room opens on its newest page and loads older pages before a cursor, so
// a page costs the same at any depth. Attachment bytes are not selected here:
// an image or audio attachment loads its bytes once it scrolls near the
// viewport, and a file attachment only when it is downloaded.
const HISTORY_PAGE = 50;
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

export type ShownMessage = MessageSummary & { reactionsViaMessage: Reaction[] };

export type HistoryCursor = { at: Date; offset: number };

/** Messages of a room, newest first. A cursor includes rows sharing its timestamp. */
export function historyQuery(roomId: string, bound: { before?: HistoryCursor; from?: Date }) {
  const query = app.messages
    .where(
      bound.before
        ? { roomId, $createdAt: { lte: bound.before.at } }
        : bound.from
          ? { roomId, $createdAt: { gte: bound.from } }
          : { roomId },
    )
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
    .include({ reactionsViaMessage: true })
    .orderBy("$createdAt", "desc")
    .orderBy("id", "desc");
  if (bound.from) return query;
  return bound.before
    ? query.offset(bound.before.offset).limit(HISTORY_PAGE)
    : query.limit(HISTORY_PAGE);
}

/** One message older than `cursor`, if there is any: whether to offer "Load older". */
function olderProbeQuery(roomId: string, cursor: HistoryCursor) {
  return app.messages
    .where({ roomId, $createdAt: { lte: cursor.at } })
    .select("id")
    .orderBy("$createdAt", "desc")
    .orderBy("id", "desc")
    .offset(cursor.offset)
    .limit(1);
}

function cursorKey(cursor: HistoryCursor) {
  return `${cursor.at.getTime()}:${cursor.offset}`;
}

export function mergeHistoryPages(...pages: ShownMessage[][]) {
  const byId = new Map<string, ShownMessage>();
  for (const page of pages) {
    for (const message of page) byId.set(message.id, message);
  }
  return [...byId.values()].reverse();
}

export const roomMembersQuery = (roomId: string) => app.roomMembers.where({ roomId });

/**
 * Opens a room's queries before its view mounts. Queries are shared by key,
 * so when the room id is already known (from the URL) the room's messages
 * load alongside the profile and room list instead of after them.
 */
export function usePrefetchRoom(roomId: string | null) {
  // The live window as the view first opens it: the newest page.
  useAll(roomId ? historyQuery(roomId, {}) : undefined);
  useAll(roomId ? roomMembersQuery(roomId) : undefined);
}

export function RoomView({ summary, author }: { summary: RoomSummary; author: string }) {
  const directory = useDirectory();
  const { room, isCreator } = summary;
  const roomId = room.id;
  // Once older pages are loaded, the live window stops sliding: it keeps
  // every message from its oldest one on, so nothing falls between it and
  // the first older page when new messages arrive.
  const [pinnedFrom, setPinnedFrom] = useState<Date | undefined>(undefined);
  const { data: liveWindow, db } = useAll(historyQuery(roomId, { from: pinnedFrom }));
  // Undefined until the first result: "No messages yet" only for a room that has none.
  const isLoadingMessages = liveWindow === undefined;
  const newestFirst = useMemo(() => liveWindow ?? [], [liveWindow]);
  const [olderCursors, setOlderCursors] = useState<HistoryCursor[]>([]);
  const [olderPages, setOlderPages] = useState<ReadonlyMap<string, ShownMessage[]>>(new Map());
  const onOlderPage = useCallback((cursor: HistoryCursor, rows: ShownMessage[]) => {
    setOlderPages((pages) => new Map(pages).set(cursorKey(cursor), rows));
  }, []);
  const messages = useMemo(() => {
    return mergeHistoryPages(
      newestFirst,
      ...olderCursors.map((cursor) => olderPages.get(cursorKey(cursor)) ?? []),
    );
  }, [newestFirst, olderCursors, olderPages]);
  const lastCursor = olderCursors.at(-1);
  const lastPage = lastCursor ? olderPages.get(cursorKey(lastCursor)) : newestFirst;
  // Where the next older page starts: before the oldest shown message,
  // skipping the messages already shown at its timestamp.
  const nextCursor = useMemo((): HistoryCursor | undefined => {
    const oldest = messages[0];
    if (!oldest) return undefined;
    const at = oldest.$createdAt;
    const offset = messages.filter(
      (message) => message.$createdAt.getTime() === at.getTime(),
    ).length;
    return { at, offset };
  }, [messages]);
  // A full last page may still be the last one: ask for one more message
  // before offering "Load older".
  const { data: olderProbe } = useAll(
    lastPage && lastPage.length >= HISTORY_PAGE && nextCursor
      ? olderProbeQuery(roomId, nextCursor)
      : undefined,
  );
  const hasOlder = (olderProbe?.length ?? 0) > 0;
  function loadOlder() {
    if (!nextCursor) return;
    if (!pinnedFrom) setPinnedFrom(nextCursor.at);
    setOlderCursors((cursors) => [...cursors, nextCursor]);
  }
  const { data: loadedMembers } = useAll(roomMembersQuery(roomId));
  const members = loadedMembers ?? [];
  const { data: requests = [] } = useAll(
    isCreator ? app.joinRequests.where({ roomId }) : undefined,
  );
  // Every member's marker: this reader's moves forward, the others' draw the
  // check marks under this reader's messages.
  const { data: loadedMarkers } = useAll(app.readMarkers.where({ roomId }));
  const markers = useMemo(
    () => (loadedMarkers ?? []).filter((marker) => marker.reader === author),
    [loadedMarkers, author],
  );
  const readUpTo = useMemo(
    () => othersReadUpTo(loadedMarkers ?? [], loadedMembers ?? [], author),
    [loadedMarkers, loadedMembers, author],
  );
  const [readByMessage, setReadByMessage] = useState<MessageSummary | undefined>(undefined);
  const [isMembersOpen, setMembersOpen] = useState(false);
  const [isRenameOpen, setRenameOpen] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const reportFailure = (action: string) => (cause: unknown) =>
    setActionError(`${action}: ${cause instanceof Error ? cause.message : String(cause)}`);

  const isMember = members.some((member) => member.memberAuthor === author);
  useMarkRead({
    roomId,
    author,
    newestAt: newestFirst[0]?.$createdAt,
    markers: loadedMarkers ? markers : undefined,
    isMember,
  });

  function leave() {
    const mine = members.find((member) => member.memberAuthor === author);
    if (!mine) return;
    // Leaving and dropping this reader's marker commit together: an
    // ex-member leaves no read position behind.
    setActionError(null);
    db.transaction((tx) => {
      for (const marker of markers) tx.delete(app.readMarkers, marker.id);
      tx.delete(app.roomMembers, mine.id);
    })
      .then((result) => result.wait({ tier: "global" }))
      .catch(reportFailure("Could not leave the room"));
  }

  // A creator without a membership (a room created before the room and its
  // membership were written in one transaction) can still read the room;
  // this puts them back in.
  function rejoin() {
    setActionError(null);
    db.insert(app.roomMembers, { roomId, memberAuthor: author, memberProfileId: directory.me.id })
      .wait({ tier: "global" })
      .catch(reportFailure("Could not join the room"));
  }

  function startSketch() {
    // The canvas and its message commit together: the message policy's
    // check that the canvas exists in this room sees the canvas inserted
    // earlier in the same transaction.
    setActionError(null);
    db.transaction((tx) => {
      const canvas = tx.insert(app.canvases, { roomId, title: "Sketch" });
      tx.insert(app.messages, {
        roomId,
        senderId: directory.me.id,
        text: "",
        canvasId: canvas.id,
      });
    })
      .then((result) => result.wait({ tier: "global" }))
      .catch(reportFailure("Could not start a sketch"));
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
              profileId={directory.me.id}
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
          {olderCursors.map((cursor) => (
            <OlderPage
              key={cursorKey(cursor)}
              roomId={roomId}
              cursor={cursor}
              onRows={onOlderPage}
            />
          ))}
          {messages.length > 0 ? (
            <ChatMessageList aria-label="Messages">
              {hasOlder ? (
                <ChatSystemMessage key="older">
                  <Button
                    label="Load older messages"
                    size="sm"
                    variant="ghost"
                    onClick={loadOlder}
                  />
                </ChatSystemMessage>
              ) : null}
              {groupMessages(messages).map((group) =>
                group.kind === "day" ? (
                  <ChatSystemMessage key={group.key} variant="divider">
                    <Timestamp value={group.at.toISOString()} format="date_weekday" />
                  </ChatSystemMessage>
                ) : (
                  <MessageGroup
                    key={group.key}
                    messages={group.messages}
                    author={author}
                    readUpTo={readUpTo}
                    onShowReadBy={setReadByMessage}
                  />
                ),
              )}
            </ChatMessageList>
          ) : null}
        </ChatLayout>
      </StackItem>
      <ReadByDialog
        message={readByMessage}
        onOpenChange={(isOpen) => {
          if (!isOpen) setReadByMessage(undefined);
        }}
      />
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

/** Loads one older page and hands its rows up; it renders nothing itself. */
function OlderPage({
  roomId,
  cursor,
  onRows,
}: {
  roomId: string;
  cursor: HistoryCursor;
  onRows: (cursor: HistoryCursor, rows: ShownMessage[]) => void;
}) {
  const { data } = useAll(historyQuery(roomId, { before: cursor }));
  useEffect(() => {
    if (data) onRows(cursor, data);
  }, [data, cursor, onRows]);
  return null;
}

/**
 * The newest marker among the room's other current members: a message at or
 * before it has two check marks. A marker left by someone no longer in the
 * room (written before markers were removed with their member) counts for
 * nothing.
 */
function othersReadUpTo(
  markers: readonly ReadMarker[],
  members: readonly { memberAuthor: string }[],
  author: string,
): number {
  const others = new Set(members.map((member) => member.memberAuthor));
  others.delete(author);
  let newest = 0;
  for (const marker of markers)
    if (others.has(marker.reader)) newest = Math.max(newest, marker.lastReadAt.getTime());
  return newest;
}

type Group =
  | { kind: "day"; key: string; at: Date }
  | { kind: "messages"; key: string; messages: ShownMessage[] };

function groupMessages(messages: ShownMessage[]): Group[] {
  const groups: Group[] = [];
  let current: ShownMessage[] | null = null;
  let previous: ShownMessage | undefined;
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
  author,
  readUpTo,
  onShowReadBy,
}: {
  messages: ShownMessage[];
  author: string;
  /** The newest marker among other members, in epoch milliseconds. */
  readUpTo: number;
  onShowReadBy: (message: MessageSummary) => void;
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
                    <HStack gap={1} vAlign="center">
                      <Timestamp value={message.$createdAt.toISOString()} format="time" />
                      {isMine ? (
                        <ReadMark isRead={readUpTo >= message.$createdAt.getTime()} />
                      ) : null}
                    </HStack>
                  ) : undefined
                }
                footer={
                  <MessageActions
                    message={message}
                    reactions={message.reactionsViaMessage}
                    author={author}
                    isMine={isMine}
                    onShowReadBy={() => onShowReadBy(message)}
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

/** One check mark once the message is sent, two once another member read it. */
function ReadMark({ isRead }: { isRead: boolean }) {
  return (
    <span className="read-mark" aria-label={isRead ? "Read" : "Sent"}>
      {isRead ? "✓✓" : "✓"}
    </span>
  );
}

/** How often, at most, this reader's marker is written while a room is open. */
export const MARK_READ_INTERVAL_MS = 5_000;

/**
 * Moves this reader's marker to the room's newest message while the room is
 * open, the page is visible and the reader is a member. The marker is one row,
 * updated in place, never a history, and it only moves forward.
 *
 * Writes are throttled: at most one every {@link MARK_READ_INTERVAL_MS} (the
 * last position always lands, trailing), plus one at once when the reader
 * switches rooms or hides the page, so co-members' check marks catch up.
 */
function useMarkRead({
  roomId,
  author,
  newestAt,
  markers,
  isMember,
}: {
  roomId: string;
  author: string;
  /** `$createdAt` of the room's newest message, if it has one. */
  newestAt: Date | undefined;
  /** Undefined until this reader's markers have loaded. */
  markers: ReadMarker[] | undefined;
  /** Only members keep a marker: the policy refuses anyone else's. */
  isMember: boolean;
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

  // This room's write state, read when a write runs rather than when it is
  // scheduled. A new object per room: the flush that runs as the reader
  // leaves a room writes that room's marker, not the next one's.
  const room = useMemo(
    () => ({
      markerId: undefined as string | undefined,
      writtenUpTo: 0,
      pendingUpTo: undefined as number | undefined,
      lastWriteAt: 0,
      timer: undefined as ReturnType<typeof setTimeout> | undefined,
    }),
    [roomId],
  );
  const marker = markers?.[0];
  if (marker) {
    room.markerId = marker.id;
    room.writtenUpTo = Math.max(room.writtenUpTo, marker.lastReadAt.getTime());
  }

  // Writes the pending position, if it moves the marker forward.
  const flush = useCallback(() => {
    clearTimeout(room.timer);
    room.timer = undefined;
    const upTo = room.pendingUpTo;
    room.pendingUpTo = undefined;
    if (upTo === undefined || upTo <= room.writtenUpTo) return;
    room.lastWriteAt = Date.now();
    room.writtenUpTo = upTo;
    const lastReadAt = new Date(upTo);
    if (room.markerId) {
      db.update(app.readMarkers, room.markerId, { lastReadAt });
    } else {
      room.markerId = db.insert(app.readMarkers, { roomId, reader: author, lastReadAt }).value.id;
    }
  }, [db, room, roomId, author]);

  const isLoaded = markers !== undefined;
  const upTo = newestAt?.getTime();
  useEffect(() => {
    if (!isVisible || !isLoaded || !isMember || upTo === undefined) return;
    if (upTo <= room.writtenUpTo) return;
    room.pendingUpTo = upTo;
    const wait = room.lastWriteAt + MARK_READ_INTERVAL_MS - Date.now();
    if (wait <= 0) flush();
    else room.timer ??= setTimeout(flush, wait);
  }, [isVisible, isLoaded, isMember, upTo, room, flush]);

  // Hiding the page or leaving the room writes what is pending at once.
  useEffect(() => {
    if (!isVisible) flush();
  }, [isVisible, flush]);
  useEffect(() => flush, [flush]);
}

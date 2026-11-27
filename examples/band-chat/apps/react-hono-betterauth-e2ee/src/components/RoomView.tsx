import { useEffect, useMemo, useState } from "react";
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
import { app, type Reaction } from "../../jazz.generated";
import { failureOf, isWaitingForApproval, useDeviceStateIfAny } from "../lib/device-state";
import { useMarkRead } from "../lib/mark-read";
import { removeMember, renewRoomKey, useRoomMembers } from "../lib/membership";
import { ProfileAvatar, useDirectory } from "../lib/profiles";
import { Attachment } from "./Attachment";
import { Composer } from "./Composer";
import { useNewestMessage } from "./RoomActivity";
import { MembersDialog } from "./MembersDialog";
import { MessageActions } from "./MessageActions";
import { RenameRoomDialog } from "./RenameRoomDialog";
import type { RoomSummary } from "./RoomNav";
import { SketchCanvas } from "./SketchCanvas";

// The newest page of a room's history. Attachment bytes are not selected here:
// an image or audio attachment loads its bytes once it scrolls near the
// viewport, and a file attachment only when it is downloaded.
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

/** A room's newest messages, newest first. */
export const roomMessagesQuery = (roomId: string) =>
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
    .limit(HISTORY_PAGE);
export const roomReactionsQuery = (roomId: string) => app.reactions.where({ roomId });

/**
 * Opens a room's queries before its view mounts. Queries are shared by key,
 * so when the room id is already known (from the URL) the room's messages
 * load alongside the profile and room list instead of after them.
 */
export function usePrefetchRoom(roomId: string | null) {
  useAll(roomId ? roomMessagesQuery(roomId) : undefined);
  useAll(roomId ? roomReactionsQuery(roomId) : undefined);
}

export function RoomView({ summary, author }: { summary: RoomSummary; author: string }) {
  const directory = useDirectory();
  const { room, isCreator } = summary;
  const roomId = room.id;
  // #region bandchat-locked-count
  const { data: newestFirst, unreadable, db } = useAll(roomMessagesQuery(roomId));
  // Messages this browser holds no key for yet are left out of the read.
  const lockedCount = unreadable.length;
  // #endregion bandchat-locked-count
  const keyOnItsWay = useKeyOnItsWay(unreadable);
  // Undefined until the first result: "No messages yet" only for a room that has none.
  const isLoadingMessages = newestFirst === undefined;
  const messages = useMemo(() => [...(newestFirst ?? [])].reverse(), [newestFirst]);
  const { data: reactions = [] } = useAll(roomReactionsQuery(roomId));
  const { members: loadedMembers, loadFailure } = useRoomMembers(roomId);
  const members = loadedMembers ?? [];
  const { data: requests = [] } = useAll(
    isCreator ? app.joinRequests.where({ roomId }) : undefined,
  );
  const { data: loadedMarkers, unreadable: unreadableMarkers } = useAll(
    app.readMarkers.where({ roomId, reader: author }),
  );
  const newest = useNewestMessage(roomId);
  const [isMembersOpen, setMembersOpen] = useState(false);
  const [isRenameOpen, setRenameOpen] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [rotationFailure, setRotationFailure] = useState<string>();
  useEffect(() => {
    return db.encryption.onReport((report) => {
      if (report.type !== "rotation-failed" || typeof report.reason !== "string") return;
      const space = report.space as { table?: string; id?: string } | null | undefined;
      if (space?.table === "rooms" && space.id === roomId) setRotationFailure(report.reason);
    });
  }, [db, roomId]);

  function retryRotation() {
    setRotationFailure(undefined);
    renewRoomKey(db, roomId).catch((error: unknown) =>
      setRotationFailure(failureOf(error).message),
    );
  }
  const reportFailure = (action: string) => (cause: unknown) =>
    setActionError(`${action}: ${failureOf(cause).message}`);

  const decryptedIds = useMemo(
    () => (newestFirst ? new Set(newestFirst.map((message) => message.id)) : undefined),
    [newestFirst],
  );
  useMarkRead({
    roomId,
    author,
    newest,
    decryptedIds,
    markers: loadedMarkers,
    markersUnreadable: unreadableMarkers.length,
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
    // The member rows, the profile membership and this reader's markers go in
    // one serializable transaction; the screen waits for the authority.
    setActionError(null);
    removeMember({ db, roomId, accountId: author, deleteMarkers: true }).catch(
      reportFailure("Could not leave the room"),
    );
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
              profile={directory.byAuthor.get(member.accountId)}
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
      {rotationFailure !== undefined ? (
        <Banner
          status="warning"
          title="The room key could not be renewed"
          description={rotationFailure}
          container="section"
          collapsible={false}
          endContent={<Button label="Retry" size="sm" onClick={retryRotation} />}
        />
      ) : null}
      {
        // #region bandchat-locked-notice
        lockedCount > 0 && keyOnItsWay === "arriving" ? (
          <Center axis="both" padding={2}>
            <Spinner
              label={`Decrypting ${lockedCount} ${lockedCount === 1 ? "message" : "messages"}…`}
            />
          </Center>
        ) : lockedCount > 0 && keyOnItsWay === "slow" ? (
          <Banner
            status="info"
            title={`${lockedCount} ${lockedCount === 1 ? "message is" : "messages are"} waiting for the room's key`}
            description="It usually comes from another member's device once one that holds it is online. If no device holds it any more, these messages stay locked."
            container="section"
            collapsible={false}
          />
        ) : lockedCount > 0 ? (
          <Banner
            status="warning"
            title={`${lockedCount} ${lockedCount === 1 ? "message is" : "messages are"} locked on this browser`}
            description="This browser does not hold the room's key yet. They show here once it arrives, for example after another of your devices approves this one."
            container="section"
            collapsible={false}
          />
        ) : null
        // #endregion bandchat-locked-notice
      }
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
            ) : lockedCount > 0 ? null : (
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
        members={loadedMembers}
        loadFailure={loadFailure}
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

// #region bandchat-key-on-its-way
/** How long a key may be on its way before the room says it is waiting for one. */
const KEY_ARRIVING_MS = 10_000;

/**
 * Whether the messages this browser cannot read are only waiting for the
 * room's key: "arriving" for the first few seconds (the key, or the room's
 * key record, is usually still syncing), then "slow" while this account is a
 * member still waiting for the key, which another member's device usually
 * shares, though none may hold it any more. `null` when something else keeps them locked: this
 * browser waits for approval or holds no access, or after those seconds the
 * room still has no key record to share a key from.
 */
function useKeyOnItsWay(unreadable: readonly { code: string }[]): "arriving" | "slow" | null {
  const status = useDeviceStateIfAny()?.status;
  const waitingForApproval = isWaitingForApproval(status ?? null);
  const onItsWay =
    !waitingForApproval &&
    unreadable.length > 0 &&
    unreadable.every((row) => row.code === "key-pending" || row.code === "space-pending");
  const [slow, setSlow] = useState(false);
  useEffect(() => {
    setSlow(false);
    if (!onItsWay) return;
    const timer = setTimeout(() => setSlow(true), KEY_ARRIVING_MS);
    return () => clearTimeout(timer);
  }, [onItsWay]);
  if (!onItsWay) return null;
  if (!slow) return "arriving";
  // Only a browser known to be active is waiting for a member's key: one whose
  // status is still unknown may be waiting for approval instead.
  return status === "active" && unreadable.every((row) => row.code === "key-pending")
    ? "slow"
    : null;
}
// #endregion bandchat-key-on-its-way

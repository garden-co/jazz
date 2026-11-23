import { useEffect } from "react";
import { useAll } from "jazz-tools/react";
import { app } from "../../jazz.generated";
import type { NewestMessage } from "../lib/room-summaries";

/**
 * A room's newest message through plain columns only: no key, nothing
 * decrypted, one index entry (messages are indexed on room and creation time).
 */
// #region bandchat-newest-message
export const newestMessageQuery = (roomId: string) =>
  app.messages
    .where({ roomId })
    .select("id", "senderId", "$createdAt")
    .orderBy("$createdAt", "desc")
    .limit(1);
// #endregion bandchat-newest-message

/** The newest message of a room; undefined while loading, null when it has none. */
export function useNewestMessage(roomId: string): NewestMessage | null | undefined {
  const { data } = useAll(newestMessageQuery(roomId));
  return data ? (data[0] ?? null) : undefined;
}

/**
 * One small live query per listed room, reporting each room's newest
 * message. Its cost grows with the number of rooms, never with their history.
 */
export function RoomActivityProbes({
  roomIds,
  onNewest,
}: {
  roomIds: readonly string[];
  onNewest: (roomId: string, newest: NewestMessage | null | undefined) => void;
}) {
  return (
    <>
      {roomIds.map((roomId) => (
        <Probe key={roomId} roomId={roomId} onNewest={onNewest} />
      ))}
    </>
  );
}

function Probe({
  roomId,
  onNewest,
}: {
  roomId: string;
  onNewest: (roomId: string, newest: NewestMessage | null | undefined) => void;
}) {
  const newest = useNewestMessage(roomId);
  const id = newest?.id;
  const time = newest?.$createdAt.getTime();
  useEffect(() => {
    onNewest(roomId, newest);
    // `newest` is a new object each render; its id and time are the content.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [roomId, id, time, newest === undefined, onNewest]);
  return null;
}

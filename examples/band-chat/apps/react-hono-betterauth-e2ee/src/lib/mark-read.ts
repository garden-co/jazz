import { useEffect, useRef, useState } from "react";
import { useDb } from "jazz-tools/react";
import { app } from "../../jazz.generated";

export interface MarkerState {
  id: string;
  lastReadAt: Date;
}

/**
 * Whether to write a read marker now. Only on positive proof that this
 * browser can read the room: the decrypted message list holds the row whose
 * ID is the plain newest-message query's, and it is newer than the marker.
 * An empty read, a locked newest message or an older readable one prove
 * nothing, so none of them write.
 */
export function markerToWrite({
  newest,
  decryptedIds,
  markers,
  markersUnreadable,
}: {
  /** The plain newest message; undefined while loading, null when the room has none. */
  newest: { id: string; $createdAt: Date } | null | undefined;
  /** IDs of the messages this browser decrypted; undefined while loading. */
  decryptedIds: ReadonlySet<string> | undefined;
  /** This reader's markers for the room; undefined while loading. */
  markers: readonly MarkerState[] | undefined;
  /** Markers the read left out because they could not be decrypted. */
  markersUnreadable: number;
}): { markerId: string | undefined; lastReadAt: Date } | null {
  if (!newest || !decryptedIds || !markers) return null;
  if (!decryptedIds.has(newest.id)) return null;
  // A marker that exists but cannot be read here is not "no marker".
  if (markersUnreadable > 0) return null;
  const current = markers.reduce<MarkerState | undefined>(
    (best, marker) => (!best || best.lastReadAt < marker.lastReadAt ? marker : best),
    undefined,
  );
  if (current && current.lastReadAt.getTime() >= newest.$createdAt.getTime()) return null;
  const now = new Date();
  return {
    markerId: current?.id,
    lastReadAt: now.getTime() > newest.$createdAt.getTime() ? now : newest.$createdAt,
  };
}

/**
 * Moves this reader's private marker to the newest message's time (or now)
 * while the room is open and the page is visible. A refused write is not
 * retried until a newer message arrives.
 */
export function useMarkRead({
  roomId,
  author,
  newest,
  decryptedIds,
  markers,
  markersUnreadable,
}: {
  roomId: string;
  author: string;
  newest: { id: string; $createdAt: Date } | null | undefined;
  decryptedIds: ReadonlySet<string> | undefined;
  markers: readonly MarkerState[] | undefined;
  markersUnreadable: number;
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
  const attempted = useRef<string | null>(null);
  // The marker this hook inserted, until this reader's query shows it: a newer message can
  // arrive first, and must move that marker rather than insert a second one.
  const inserted = useRef<{ key: string; id: string } | null>(null);
  // Once the query shows it, the query is the record: if the marker is later deleted (leaving
  // the room deletes it), the next write inserts a new one.
  useEffect(() => {
    const id = inserted.current?.id;
    if (id && markers?.some((marker) => marker.id === id)) inserted.current = null;
  }, [markers]);
  const write = isVisible
    ? markerToWrite({ newest, decryptedIds, markers, markersUnreadable })
    : null;
  const newestId = newest?.id;
  const markerId = write?.markerId;
  const hasWrite = !!write;
  useEffect(() => {
    if (!hasWrite || !newestId || attempted.current === newestId) return;
    attempted.current = newestId;
    const lastReadAt = write!.lastReadAt;
    const key = `${roomId}/${author}`;
    const existing = markerId ?? (inserted.current?.key === key ? inserted.current.id : undefined);
    const insertId = existing ? undefined : crypto.randomUUID();
    // A refused insert leaves no marker to move.
    const forget = () => {
      if (insertId && inserted.current?.id === insertId) inserted.current = null;
    };
    if (insertId) inserted.current = { key, id: insertId };
    try {
      const written = existing
        ? db.update(app.readMarkers, existing, { lastReadAt })
        : db.insert(app.readMarkers, { roomId, reader: author, lastReadAt }, { id: insertId });
      // Refused: leave it until a newer message arrives.
      void Promise.resolve(written?.wait?.({ tier: "global" })).catch(forget);
    } catch {
      // Same: no retry for this message.
      forget();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [db, hasWrite, newestId, markerId, roomId, author]);
}

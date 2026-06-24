import { useEffect, useMemo, useRef, useState } from "react";
import { nanoid } from "nanoid";
import { useAll, useDb, useSession } from "jazz-tools/react";
import * as Y from "yjs";
import { app, type RoomYjsSnapshot } from "../../schema.js";
import { hashString } from "../lib/hash.js";
import { toUint8Array } from "../lib/yjsUpdate.js";

type UseJazzYjsDocumentArgs = {
  roomId: string | null;
};

type Runtime = {
  roomId: string | null;
  didBootstrap: boolean;
  appliedUpdateIds: Set<string>;
};

const snapshotDebounceMs = 2_000;
const snapshotCoalesceMs = 2 * 60 * 1_000;

function latestSnapshot(snapshots: RoomYjsSnapshot[]): RoomYjsSnapshot | null {
  return snapshots.reduce<RoomYjsSnapshot | null>((latest, snapshot) => {
    if (!latest || snapshot.createdAt.getTime() > latest.createdAt.getTime()) return snapshot;
    return latest;
  }, null);
}

export function useJazzYjsDocument({ roomId }: UseJazzYjsDocumentArgs): {
  ydoc: Y.Doc;
  isReady: boolean;
} {
  const db = useDb();
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;
  const [{ ydoc, providerInstanceId }] = useState(() => ({
    ydoc: new Y.Doc(),
    providerInstanceId: nanoid(),
  }));
  const remoteOrigin = useRef({ provider: "jazz" }).current;
  const runtimeRef = useRef<Runtime>({
    roomId: null,
    didBootstrap: false,
    appliedUpdateIds: new Set(),
  });
  const [readyRoomId, setReadyRoomId] = useState<string | null>(null);

  const snapshotRows = useAll(roomId ? app.roomYjsSnapshots.where({ room_id: roomId }) : undefined);
  const updateRows = useAll(roomId ? app.roomYjsUpdates.where({ room_id: roomId }) : undefined);

  useEffect(() => {
    return () => ydoc.destroy();
  }, [ydoc]);

  useEffect(() => {
    const runtime = runtimeRef.current;
    if (runtime.roomId !== roomId) {
      ydoc.transact(
        () => ydoc.getText("monaco").delete(0, ydoc.getText("monaco").length),
        remoteOrigin,
      );
      runtime.roomId = roomId;
      runtime.didBootstrap = false;
      runtime.appliedUpdateIds = new Set();
      setReadyRoomId(null);
    }
  }, [remoteOrigin, roomId, ydoc]);

  useEffect(() => {
    if (!roomId || !snapshotRows || !updateRows) return;

    const runtime = runtimeRef.current;
    if (!runtime.didBootstrap) {
      const snapshot = latestSnapshot(snapshotRows);
      if (snapshot) {
        Y.applyUpdate(ydoc, toUint8Array(snapshot.state), remoteOrigin);
      }
      runtime.didBootstrap = true;
    }

    for (const row of updateRows) {
      if (runtime.appliedUpdateIds.has(row.id)) continue;
      runtime.appliedUpdateIds.add(row.id);
      if (row.provider_instance_id === providerInstanceId) continue;

      Y.applyUpdate(ydoc, toUint8Array(row.update), remoteOrigin);
    }

    setReadyRoomId(roomId);
  }, [providerInstanceId, remoteOrigin, roomId, snapshotRows, updateRows, ydoc]);

  useEffect(() => {
    if (readyRoomId !== roomId || !roomId || !sessionUserId) return;

    const onUpdate = (update: Uint8Array, origin: unknown) => {
      if (origin === remoteOrigin) return;

      db.insert(app.roomYjsUpdates, {
        room_id: roomId,
        update: new Uint8Array(update),
        session_user_id: sessionUserId,
        provider_instance_id: providerInstanceId,
        createdAt: new Date(),
      })
        .wait({ tier: "edge" })
        .catch((error: unknown) => {
          console.error("Failed to persist Yjs update", error);
        });
    };

    ydoc.on("update", onUpdate);
    return () => ydoc.off("update", onUpdate);
  }, [db, providerInstanceId, readyRoomId, remoteOrigin, roomId, sessionUserId, ydoc]);

  const latestSnapshotRow = useMemo(() => latestSnapshot(snapshotRows ?? []), [snapshotRows]);

  useEffect(() => {
    if (readyRoomId !== roomId || !roomId || !sessionUserId) return;

    const ytext = ydoc.getText("monaco");
    let timeoutId: number | undefined;

    const saveSnapshot = async () => {
      const text = ytext.toString();
      const textHash = await hashString(text);
      const now = new Date();

      if (
        latestSnapshotRow?.textHash === textHash &&
        now.getTime() - latestSnapshotRow.createdAt.getTime() < snapshotCoalesceMs
      ) {
        return;
      }

      await db
        .insert(app.roomYjsSnapshots, {
          room_id: roomId,
          state: Y.encodeStateAsUpdate(ydoc),
          textHash,
          session_user_id: sessionUserId,
          createdAt: now,
        })
        .wait({ tier: "local" });
    };

    const onTextChange = (_event: Y.YTextEvent, transaction: Y.Transaction) => {
      if (transaction.origin === remoteOrigin) return;
      if (timeoutId) window.clearTimeout(timeoutId);
      timeoutId = window.setTimeout(() => {
        saveSnapshot().catch((error: unknown) => {
          console.error("Failed to persist Yjs snapshot", error);
        });
      }, snapshotDebounceMs);
    };

    ytext.observe(onTextChange);
    return () => {
      if (timeoutId) window.clearTimeout(timeoutId);
      ytext.unobserve(onTextChange);
    };
  }, [db, latestSnapshotRow, readyRoomId, remoteOrigin, roomId, sessionUserId, ydoc]);

  return { ydoc, isReady: readyRoomId === roomId && !!roomId };
}

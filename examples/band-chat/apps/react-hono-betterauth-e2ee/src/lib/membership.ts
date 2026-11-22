import { useEffect, useState } from "react";
import { useAll } from "jazz-tools/react";
import { PersistedWriteRejectedError, type Db } from "jazz-tools";
import { app, type E2eeMember } from "../../jazz.generated";

const roomSpace = (roomId: string) => ({ table: "rooms", id: roomId });

/**
 * The accounts holding a member row of the room, read from Jazz's own member
 * table (the one membership truth); undefined until they have loaded. A
 * profile or a `roomMembers` row is not needed to be listed.
 */
export function useRoomMembers(roomId: string): {
  members: E2eeMember[] | undefined;
  loadFailure: { reason: string; retry: () => void } | undefined;
} {
  const [filter, setFilter] = useState<Awaited<ReturnType<Db["encryption"]["members"]["where"]>>>();
  const [error, setError] = useState<string>();
  const [attempt, setAttempt] = useState(0);
  const { data, db } = useAll(filter ? app.__e2ee_members.where(filter) : undefined);
  useEffect(() => {
    let isLive = true;
    setFilter(undefined);
    setError(undefined);
    db.encryption.members.where(roomSpace(roomId)).then(
      (found) => isLive && setFilter(found),
      (cause: unknown) =>
        isLive && setError(cause instanceof Error ? cause.message : String(cause)),
    );
    return () => {
      isLive = false;
    };
  }, [attempt, db, roomId]);
  return {
    members: data,
    loadFailure:
      error === undefined ? undefined : { reason: error, retry: () => setAttempt((n) => n + 1) },
  };
}

const ATTEMPTS = 3;

/**
 * Takes `accountId` out of the room: its member rows and its `roomMembers`
 * rows go in one serializable transaction that reads both inside it, and this
 * resolves only when the authority has answered. Resolves with the number of
 * member rows removed. A conflict with a concurrent write (for example an
 * admission of the same account) is run again on fresh rows; a refusal is
 * thrown. `alsoDelete` names further rows of the departing account (a
 * leaver's read markers) to drop in the same transaction.
 */
export async function removeMember({
  db,
  roomId,
  accountId,
  deleteMarkers = false,
}: {
  db: Db;
  roomId: string;
  accountId: string;
  deleteMarkers?: boolean;
}): Promise<number> {
  const filter = await db.encryption.members.where(roomSpace(roomId), accountId);
  for (let attempt = 1; ; attempt++) {
    try {
      const written = await db.serializableTransaction(async (tx) => {
        const memberRows = await tx.all(app.__e2ee_members.where(filter), { tier: "remote" });
        const profileRows = await tx.all(
          app.roomMembers.where({ roomId, memberAuthor: accountId }),
          { tier: "remote" },
        );
        for (const row of memberRows) tx.delete(app.__e2ee_members, row.id);
        for (const row of profileRows) tx.delete(app.roomMembers, row.id);
        if (deleteMarkers) {
          const markers = await tx.all(app.readMarkers.where({ roomId, reader: accountId }), {
            tier: "remote",
          });
          for (const marker of markers) tx.delete(app.readMarkers, marker.id);
        }
        return memberRows.length;
      });
      return await written.wait();
    } catch (error) {
      if (
        attempt >= ATTEMPTS ||
        !(error instanceof PersistedWriteRejectedError) ||
        (error.code !== "serialization_conflict" && error.code !== "transaction_conflict")
      )
        throw error;
    }
  }
}

/**
 * Admits an account: its member row and `roomMembers` row go in the one
 * transaction that clears its join requests, then keys are delivered.
 * Resolves with what delivery reported.
 */
export async function admitMember({
  db,
  roomId,
  memberAuthor,
  memberProfileId,
  requestIds,
}: {
  db: Db;
  roomId: string;
  memberAuthor: string;
  memberProfileId: string;
  requestIds: string[];
}): Promise<{ delivered: number; failed: number }> {
  const memberRow = await db.encryption.members.row(roomSpace(roomId), memberAuthor);
  const written = await db.transaction((tx) => {
    tx.insert(app.__e2ee_members, memberRow);
    tx.insert(app.roomMembers, { roomId, memberAuthor, memberProfileId });
    for (const id of requestIds) tx.delete(app.joinRequests, id);
  });
  await written.wait({ tier: "global" });
  return db.encryption.keys.deliver();
}

/**
 * Renews the room's key after a removal, so the removed member reads nothing
 * written later. Called after every removal: a room with no key yet (nothing
 * encrypted written) has nothing to renew, and Jazz answers 0. Resolves with
 * the deliveries written; any failure is the caller's to show, with a retry.
 */
export function renewRoomKey(db: Db, roomId: string): Promise<number> {
  return db.encryption.keys.rotate(roomSpace(roomId));
}

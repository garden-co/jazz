import { init, type InstaQLEntity, type TransactionChunk } from "@instantdb/react";
import schema, { type AppSchema } from "../../instant.schema";

export const db = init({
  appId: import.meta.env.VITE_INSTANT_APP_ID,
  schema,
  useDateObjects: true,
});

// Rows in the shapes the UI works with (the Jazz app's row types): linked
// rows' ids are flattened into `roomId`, `senderId` and so on.
type Entity<E extends keyof AppSchema["entities"], Q extends object = {}> = InstaQLEntity<
  AppSchema,
  E,
  Q,
  undefined,
  true
>;
export type Profile = Entity<"profiles">;
export type Room = Entity<"rooms">;
export type RoomMember = {
  id: string;
  roomId: string;
  memberAuthor: string;
  memberProfileId: string | null;
};
export type JoinRequest = { id: string; roomId: string; requester: string; profileId: string };
export type ReadMarker = { id: string; roomId: string; reader: string; lastReadAt: Date };
export type Reaction = { id: string; messageId: string; author: string; emoji: string };

/** `db.transact`, the shape every write here goes through. */
export function transact(chunks: TransactionChunk<any, any> | TransactionChunk<any, any>[]) {
  return db.transact(chunks);
}

type Ref = { id: string } | undefined;

export const toMember = (row: {
  id: string;
  memberAuthor: string;
  room?: Ref;
  memberProfile?: Ref;
}): RoomMember => ({
  id: row.id,
  roomId: row.room?.id ?? "",
  memberAuthor: row.memberAuthor,
  memberProfileId: row.memberProfile?.id ?? null,
});

export const toRequest = (row: {
  id: string;
  requester: string;
  room?: Ref;
  profile?: Ref;
}): JoinRequest => ({
  id: row.id,
  roomId: row.room?.id ?? "",
  requester: row.requester,
  profileId: row.profile?.id ?? "",
});

export const toMarker = (row: {
  id: string;
  reader: string;
  lastReadAt: Date;
  room?: Ref;
}): ReadMarker => ({
  id: row.id,
  roomId: row.room?.id ?? "",
  reader: row.reader,
  lastReadAt: row.lastReadAt,
});

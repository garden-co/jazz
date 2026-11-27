import type { Doc, Id } from "@/convex/_generated/dataModel";

export type Profile = Doc<"profiles">;
export type Room = Doc<"rooms">;
export type RoomMember = Doc<"roomMembers">;
export type JoinRequest = Doc<"joinRequests">;
export type Reaction = Doc<"reactions">;
export type { Id };

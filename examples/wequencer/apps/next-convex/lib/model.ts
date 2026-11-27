import type { Doc, Id } from "../convex/_generated/dataModel";

export type Session = Doc<"sessions">;
export type Track = Doc<"tracks">;
export type Step = Doc<"steps">;
export type Pattern = Doc<"patterns">;
export type Instrument = Track["instrument"];
export type MemberRole = Doc<"session_members">["role"];
export type { Id };

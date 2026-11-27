// Wequencer's rows as the API returns them (the tables are in db/001_init.sql).
export type Instrument = "kick" | "snare" | "clap" | "closed_hat" | "open_hat" | "tom" | "bass" | "lead";
export type MemberRole = "owner" | "editor" | "viewer";

export type Profile = { id: string; author: string; displayName: string };
export type Session = { id: string; title: string; tempo_bpm: number; created_by: string; created_at: number };
export type SessionMember = { id: string; session_id: string; member_author: string; role: MemberRole };
export type Track = {
  id: string;
  session_id: string;
  position: number;
  name: string;
  color: string;
  instrument: Instrument;
  volume: number;
  muted: boolean;
  solo: boolean;
};
export type Pattern = { id: string; session_id: string; position: number; name: string; length: number };
/** Sparse: a pad without a row is off. The id is derived from (track, pattern, position). */
export type Step = {
  id: string;
  session_id: string;
  track_id: string;
  pattern_id: string;
  position: number;
  enabled: boolean;
  velocity: number;
  probability: number;
};
export type TransportObservation = {
  id: string;
  session_id: string;
  playing: boolean;
  bar: number;
  observed_at: number;
  tempo_bpm: number;
  pattern_id: string | null;
};
export type Presence = { id: string; session_id: string; profile_id: string; cursor_step: number; heartbeat_at: number };

/** What the WebSocket tells subscribers: these tables changed under this topic. */
export type ChangeEvent = { type: "change"; topic: string; tables: string[] };

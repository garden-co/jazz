-- Wequencer on Neon: the tables the live queries read. Managed Better Auth keeps
-- its users in the branch's "neon_auth" schema. No RLS (Realtime rejects it).
-- Times are epoch milliseconds.

CREATE TABLE profiles (
  id text PRIMARY KEY,
  author text NOT NULL UNIQUE,
  "displayName" text NOT NULL
);

CREATE TABLE sessions (
  id text PRIMARY KEY,
  title text NOT NULL,
  -- The starting tempo; the newest transport observation overrides it.
  tempo_bpm integer NOT NULL,
  -- The creator: administers the session whatever their membership row says.
  created_by text NOT NULL,
  created_at double precision NOT NULL
);

CREATE TABLE session_members (
  id text PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  member_author text NOT NULL,
  role text NOT NULL CHECK (role IN ('owner', 'editor', 'viewer'))
);
CREATE INDEX ON session_members (session_id, member_author);
CREATE INDEX ON session_members (member_author);

CREATE TABLE tracks (
  id text PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  position integer NOT NULL,
  name text NOT NULL,
  color text NOT NULL,
  instrument text NOT NULL DEFAULT 'kick'
    CHECK (instrument IN ('kick', 'snare', 'clap', 'closed_hat', 'open_hat', 'tom', 'bass', 'lead')),
  volume integer NOT NULL DEFAULT 80,
  muted boolean NOT NULL DEFAULT false,
  solo boolean NOT NULL DEFAULT false
);
CREATE INDEX ON tracks (session_id, position);

CREATE TABLE patterns (
  id text PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  position integer NOT NULL,
  name text NOT NULL,
  length integer NOT NULL
);
CREATE INDEX ON patterns (session_id, position);

-- Sparse: a pad without a row is off. The id is derived from (track,
-- pattern, position), so concurrent first presses write the same row.
CREATE TABLE steps (
  id text PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  track_id text NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
  pattern_id text NOT NULL REFERENCES patterns(id) ON DELETE CASCADE,
  position integer NOT NULL,
  enabled boolean NOT NULL,
  velocity integer NOT NULL,
  probability integer NOT NULL
);
CREATE INDEX ON steps (track_id, pattern_id, position);
CREATE INDEX ON steps (session_id);

CREATE TABLE transport_observations (
  id text PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  playing boolean NOT NULL,
  bar integer NOT NULL,
  observed_at double precision NOT NULL,
  tempo_bpm integer NOT NULL DEFAULT 120,
  pattern_id text REFERENCES patterns(id) ON DELETE SET NULL
);
CREATE INDEX ON transport_observations (session_id, observed_at);

CREATE TABLE presence (
  id text PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  profile_id text NOT NULL REFERENCES profiles(id),
  cursor_step integer NOT NULL,
  heartbeat_at double precision NOT NULL
);
CREATE INDEX ON presence (session_id, heartbeat_at);

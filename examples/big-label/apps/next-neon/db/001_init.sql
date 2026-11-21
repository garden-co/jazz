-- BigLabel on Neon: the tables the live queries read. Managed Better Auth keeps
-- its users in the branch's "neon_auth" schema; user_id columns hold its ids.
-- No RLS (Realtime rejects it): reads are filtered in the sealed queries,
-- writes checked in the write handlers. Release dates are epoch milliseconds.
--
-- Every tenant row carries organization_id, and every reference between
-- tenant rows is a composite foreign key on (organization_id, id), so a row
-- can only point at rows of its own label, whoever writes it.

CREATE TABLE organizations (
  id text PRIMARY KEY,
  name text NOT NULL,
  slug text NOT NULL UNIQUE
);

CREATE TABLE people (
  id text PRIMARY KEY,
  user_id text NOT NULL UNIQUE,
  name text NOT NULL,
  UNIQUE (id, user_id)
);

-- Sign-in emails, written by the bootstrap route and read only by the
-- server's member lookup. No live query reads this table.
CREATE TABLE person_emails (
  id text PRIMARY KEY,
  person_id text NOT NULL UNIQUE REFERENCES people(id),
  email text NOT NULL
);
CREATE INDEX ON person_emails (email);

CREATE TABLE memberships (
  id text PRIMARY KEY,
  organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  person_id text NOT NULL REFERENCES people(id),
  -- Denormalized from people, so a membership check is one indexed lookup.
  user_id text NOT NULL,
  role text NOT NULL CHECK (role IN ('admin', 'editor', 'viewer')),
  UNIQUE (organization_id, id),
  UNIQUE (organization_id, person_id),
  FOREIGN KEY (person_id, user_id) REFERENCES people(id, user_id)
);
CREATE INDEX ON memberships (user_id, organization_id);

CREATE TABLE teams (
  id text PRIMARY KEY,
  organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  name text NOT NULL,
  UNIQUE (organization_id, id)
);
CREATE INDEX ON teams (organization_id, name);

CREATE TABLE team_assignments (
  id text PRIMARY KEY,
  organization_id text NOT NULL,
  team_id text NOT NULL,
  membership_id text NOT NULL,
  role text NOT NULL,
  FOREIGN KEY (organization_id, team_id) REFERENCES teams(organization_id, id) ON DELETE CASCADE,
  FOREIGN KEY (organization_id, membership_id) REFERENCES memberships(organization_id, id) ON DELETE CASCADE
);
CREATE INDEX ON team_assignments (organization_id, team_id);
CREATE INDEX ON team_assignments (organization_id, membership_id);

CREATE TABLE artists (
  id text PRIMARY KEY,
  organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  name text NOT NULL,
  genre text NOT NULL,
  status text NOT NULL,
  -- Lower-cased name and genre, for case-insensitive search.
  search_key text NOT NULL,
  UNIQUE (organization_id, id)
);
CREATE INDEX ON artists (organization_id, name, id);

CREATE TABLE catalogues (
  id text PRIMARY KEY,
  organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  name text NOT NULL,
  code text NOT NULL,
  UNIQUE (organization_id, id)
);

CREATE TABLE releases (
  id text PRIMARY KEY,
  organization_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  artist_id text NOT NULL,
  catalogue_id text,
  catalog_number text NOT NULL,
  -- The catalogue number's trailing digits, so "NFC-1000" sorts after "NFC-999".
  catalog_sequence integer,
  title text NOT NULL,
  format text NOT NULL,
  release_date double precision NOT NULL,
  status text NOT NULL,
  -- Lower-cased title and catalogue number, for case-insensitive search.
  search_key text NOT NULL,
  UNIQUE (organization_id, id),
  -- Catalogue numbers are unique per label.
  UNIQUE (organization_id, catalog_number),
  -- An artist with releases can't be deleted (NO ACTION).
  FOREIGN KEY (organization_id, artist_id) REFERENCES artists(organization_id, id),
  -- MATCH SIMPLE: a release without a catalogue skips the check.
  FOREIGN KEY (organization_id, catalogue_id) REFERENCES catalogues(organization_id, id)
);
CREATE INDEX ON releases (organization_id, release_date, id);
CREATE INDEX ON releases (organization_id, artist_id, release_date);
CREATE INDEX ON releases (organization_id, catalogue_id, catalog_sequence);

-- Which teams work on a release.
CREATE TABLE release_teams (
  id text PRIMARY KEY,
  organization_id text NOT NULL,
  release_id text NOT NULL,
  team_id text NOT NULL,
  FOREIGN KEY (organization_id, release_id) REFERENCES releases(organization_id, id) ON DELETE CASCADE,
  FOREIGN KEY (organization_id, team_id) REFERENCES teams(organization_id, id) ON DELETE CASCADE
);
CREATE INDEX ON release_teams (organization_id, release_id);
CREATE INDEX ON release_teams (organization_id, team_id);

-- Per-person release roles. Admin-only; the UI doesn't show them yet.
CREATE TABLE release_assignments (
  id text PRIMARY KEY,
  organization_id text NOT NULL,
  release_id text NOT NULL,
  membership_id text NOT NULL,
  role text NOT NULL,
  FOREIGN KEY (organization_id, release_id) REFERENCES releases(organization_id, id) ON DELETE CASCADE,
  FOREIGN KEY (organization_id, membership_id) REFERENCES memberships(organization_id, id) ON DELETE CASCADE
);

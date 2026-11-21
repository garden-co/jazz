-- BigLabel on Zero: the tenant tables zero-cache replicates (public schema).
-- Better Auth's tables live in "auth" and sign-in emails in "private": Zero
-- doesn't replicate either, so no client query can reach them.
-- Times are epoch milliseconds.

CREATE TABLE organizations (
  id text PRIMARY KEY,
  name text NOT NULL,
  slug text NOT NULL UNIQUE
);

CREATE TABLE people (
  id text PRIMARY KEY,
  "userId" text NOT NULL UNIQUE,
  name text NOT NULL
);

CREATE SCHEMA private;
-- Sign-in emails, written only by the bootstrap route and read only by the
-- add-member route.
CREATE TABLE private."personEmails" (
  id text PRIMARY KEY,
  "personId" text NOT NULL UNIQUE REFERENCES people(id),
  email text NOT NULL
);
CREATE INDEX ON private."personEmails" (email);

CREATE TABLE memberships (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  "personId" text NOT NULL REFERENCES people(id),
  -- Denormalized from the person, so membership checks are one indexed lookup.
  "userId" text NOT NULL,
  role text NOT NULL CHECK (role IN ('admin', 'editor', 'viewer')),
  UNIQUE ("organizationId", "personId")
);
CREATE INDEX ON memberships ("userId");
CREATE INDEX ON memberships ("organizationId", "userId");

CREATE TABLE teams (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  name text NOT NULL
);
CREATE INDEX ON teams ("organizationId", name);

CREATE TABLE "teamAssignments" (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  "teamId" text NOT NULL REFERENCES teams(id),
  "membershipId" text NOT NULL REFERENCES memberships(id),
  role text NOT NULL
);
CREATE INDEX ON "teamAssignments" ("organizationId", "teamId");
CREATE INDEX ON "teamAssignments" ("membershipId");

CREATE TABLE artists (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  name text NOT NULL,
  genre text NOT NULL,
  status text NOT NULL,
  -- Lower-cased name and genre, for case-insensitive search.
  "searchKey" text NOT NULL
);
CREATE INDEX ON artists ("organizationId", name, id);

CREATE TABLE catalogues (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  name text NOT NULL,
  code text NOT NULL
);
CREATE INDEX ON catalogues ("organizationId", name);

CREATE TABLE releases (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  -- An artist with releases can't be deleted.
  "artistId" text NOT NULL REFERENCES artists(id) ON DELETE RESTRICT,
  "catalogueId" text REFERENCES catalogues(id),
  "catalogNumber" text NOT NULL,
  -- The catalogue number's trailing digits, so "NFC-1000" sorts after "NFC-999".
  "catalogSequence" integer,
  title text NOT NULL,
  format text NOT NULL,
  "releaseDate" double precision NOT NULL,
  status text NOT NULL,
  -- Lower-cased title and catalogue number, for case-insensitive search.
  "searchKey" text NOT NULL,
  -- Catalogue numbers are unique per label.
  UNIQUE ("organizationId", "catalogNumber")
);
CREATE INDEX ON releases ("organizationId", "releaseDate", id);
CREATE INDEX ON releases ("organizationId", "artistId", "releaseDate");
CREATE INDEX ON releases ("organizationId", "catalogueId", "releaseDate");

-- Which teams work on a release. Both sides belong to the same tenant.
CREATE TABLE "releaseTeams" (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  "releaseId" text NOT NULL REFERENCES releases(id),
  "teamId" text NOT NULL REFERENCES teams(id)
);
CREATE INDEX ON "releaseTeams" ("organizationId", "releaseId");
CREATE INDEX ON "releaseTeams" ("organizationId", "teamId");

CREATE TABLE "releaseAssignments" (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  "releaseId" text NOT NULL REFERENCES releases(id),
  "membershipId" text NOT NULL REFERENCES memberships(id),
  role text NOT NULL
);
CREATE INDEX ON "releaseAssignments" ("organizationId", "releaseId");
CREATE INDEX ON "releaseAssignments" ("membershipId");

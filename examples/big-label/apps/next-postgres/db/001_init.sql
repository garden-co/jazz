-- BigLabel on plain Postgres. Better Auth keeps its tables in the "auth"
-- schema (scripts/db.ts). Every tenant row carries "organizationId"; composite
-- foreign keys on ("organizationId", <ref>) keep references inside the label.

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

-- Sign-in emails: written by the bootstrap, read by the add-member route. No
-- read endpoint exposes this table.
CREATE TABLE "personEmails" (
  id text PRIMARY KEY,
  "personId" text NOT NULL UNIQUE REFERENCES people(id),
  email text NOT NULL
);
CREATE INDEX ON "personEmails" (email);

CREATE TABLE memberships (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  "personId" text NOT NULL REFERENCES people(id),
  "userId" text NOT NULL,
  role text NOT NULL CHECK (role IN ('admin', 'editor', 'viewer')),
  UNIQUE ("organizationId", "personId"),
  UNIQUE ("organizationId", id)
);
CREATE INDEX ON memberships ("userId");

CREATE TABLE teams (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  name text NOT NULL,
  UNIQUE ("organizationId", id)
);

CREATE TABLE "teamAssignments" (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL,
  "teamId" text NOT NULL,
  "membershipId" text NOT NULL,
  role text NOT NULL,
  FOREIGN KEY ("organizationId", "teamId") REFERENCES teams("organizationId", id) ON DELETE CASCADE,
  FOREIGN KEY ("organizationId", "membershipId") REFERENCES memberships("organizationId", id) ON DELETE CASCADE
);
CREATE INDEX ON "teamAssignments" ("organizationId", "teamId");

CREATE TABLE artists (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  name text NOT NULL,
  genre text NOT NULL,
  status text NOT NULL,
  -- Lower-cased name and genre, for case-insensitive search.
  "searchKey" text NOT NULL,
  UNIQUE ("organizationId", id)
);
CREATE INDEX ON artists ("organizationId", name, id);

CREATE TABLE catalogues (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  name text NOT NULL,
  code text NOT NULL,
  UNIQUE ("organizationId", id)
);

CREATE TABLE releases (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  "artistId" text NOT NULL,
  "catalogueId" text,
  "catalogNumber" text NOT NULL,
  -- The catalogue number's trailing digits, so "NFC-1000" sorts after "NFC-999".
  "catalogSequence" integer,
  title text NOT NULL,
  format text NOT NULL,
  "releaseDate" timestamptz NOT NULL,
  status text NOT NULL,
  -- Lower-cased title and catalogue number, for case-insensitive search.
  "searchKey" text NOT NULL,
  UNIQUE ("organizationId", id),
  -- Catalogue numbers are unique per label.
  UNIQUE ("organizationId", "catalogNumber"),
  -- An artist with releases can't be deleted.
  FOREIGN KEY ("organizationId", "artistId") REFERENCES artists("organizationId", id) ON DELETE RESTRICT,
  FOREIGN KEY ("organizationId", "catalogueId") REFERENCES catalogues("organizationId", id) ON DELETE SET NULL ("catalogueId")
);
CREATE INDEX ON releases ("organizationId", "releaseDate", id);
CREATE INDEX ON releases ("organizationId", "artistId", "releaseDate");
CREATE INDEX ON releases ("organizationId", "catalogueId", "catalogSequence");

CREATE TABLE "releaseTeams" (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL,
  "releaseId" text NOT NULL,
  "teamId" text NOT NULL,
  FOREIGN KEY ("organizationId", "releaseId") REFERENCES releases("organizationId", id) ON DELETE CASCADE,
  FOREIGN KEY ("organizationId", "teamId") REFERENCES teams("organizationId", id) ON DELETE CASCADE
);
CREATE INDEX ON "releaseTeams" ("organizationId", "releaseId");
CREATE INDEX ON "releaseTeams" ("organizationId", "teamId");

CREATE TABLE "releaseAssignments" (
  id text PRIMARY KEY,
  "organizationId" text NOT NULL,
  "releaseId" text NOT NULL,
  "membershipId" text NOT NULL,
  role text NOT NULL,
  FOREIGN KEY ("organizationId", "releaseId") REFERENCES releases("organizationId", id) ON DELETE CASCADE,
  FOREIGN KEY ("organizationId", "membershipId") REFERENCES memberships("organizationId", id) ON DELETE CASCADE
);

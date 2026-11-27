-- BandChat on plain Postgres: the tables the app server reads and writes.
-- Better Auth keeps its own tables in the "auth" schema (created by
-- scripts/db.ts). Times are epoch milliseconds.
-- Attachments and avatar images are left out (see NOTES.md).

CREATE TABLE "profiles" (
  "id" text PRIMARY KEY,
  "author" text NOT NULL,
  "displayName" text NOT NULL,
  "createdAt" double precision NOT NULL
);
CREATE INDEX ON "profiles" ("author", "createdAt");

CREATE TABLE "rooms" (
  "id" text PRIMARY KEY,
  "name" text NOT NULL,
  "createdBy" text NOT NULL,
  "createdAt" double precision NOT NULL,
  -- Denormalized "last activity", written by members when they post.
  "lastActivityAt" double precision
);
CREATE INDEX ON "rooms" ("createdBy");

CREATE TABLE "roomMembers" (
  "id" text PRIMARY KEY,
  "roomId" text NOT NULL REFERENCES "rooms"("id") ON DELETE CASCADE,
  "memberAuthor" text NOT NULL,
  "memberProfileId" text REFERENCES "profiles"("id"),
  UNIQUE ("roomId", "memberAuthor")
);
CREATE INDEX ON "roomMembers" ("memberAuthor");

CREATE TABLE "joinRequests" (
  "id" text PRIMARY KEY,
  "roomId" text NOT NULL REFERENCES "rooms"("id") ON DELETE CASCADE,
  "requester" text NOT NULL,
  "profileId" text NOT NULL REFERENCES "profiles"("id")
);
CREATE INDEX ON "joinRequests" ("roomId", "requester");

CREATE TABLE "readMarkers" (
  "id" text PRIMARY KEY,
  "roomId" text NOT NULL REFERENCES "rooms"("id") ON DELETE CASCADE,
  "reader" text NOT NULL,
  "lastReadAt" double precision NOT NULL
);
CREATE INDEX ON "readMarkers" ("reader", "roomId");

CREATE TABLE "canvases" (
  "id" text PRIMARY KEY,
  "roomId" text NOT NULL REFERENCES "rooms"("id") ON DELETE CASCADE,
  "title" text NOT NULL
);

CREATE TABLE "messages" (
  "id" text PRIMARY KEY,
  "roomId" text NOT NULL REFERENCES "rooms"("id") ON DELETE CASCADE,
  "senderId" text NOT NULL REFERENCES "profiles"("id"),
  "text" text NOT NULL,
  "canvasId" text REFERENCES "canvases"("id"),
  "createdAt" double precision NOT NULL
);
CREATE INDEX ON "messages" ("roomId", "createdAt");
CREATE INDEX ON "messages" ("senderId");

CREATE TABLE "reactions" (
  "id" text PRIMARY KEY,
  "roomId" text NOT NULL REFERENCES "rooms"("id") ON DELETE CASCADE,
  "messageId" text NOT NULL REFERENCES "messages"("id") ON DELETE CASCADE,
  "author" text NOT NULL,
  "emoji" text NOT NULL
);
CREATE INDEX ON "reactions" ("roomId");
CREATE INDEX ON "reactions" ("messageId");

CREATE TABLE "strokes" (
  "id" text PRIMARY KEY,
  "canvasId" text NOT NULL REFERENCES "canvases"("id") ON DELETE CASCADE,
  "roomId" text NOT NULL REFERENCES "rooms"("id") ON DELETE CASCADE,
  "author" text NOT NULL,
  "color" text NOT NULL,
  "width" integer NOT NULL,
  -- Flat [x0, y0, x1, y1, ...] in canvas units (0-1000 on both axes).
  "points" jsonb NOT NULL,
  "createdAt" double precision NOT NULL
);
CREATE INDEX ON "strokes" ("canvasId", "createdAt");

-- StagePlan on Neon: the Postgres schema the live queries read. No RLS: Realtime
-- rejects tables with row-level security; the server scopes each query instead.
-- Times are epoch milliseconds (double precision), as in Zero's samples.

CREATE TABLE "crew" (
  "id" text PRIMARY KEY,
  "account" text NOT NULL UNIQUE,
  "name" text NOT NULL
);

CREATE TABLE "shows" (
  "id" text PRIMARY KEY,
  "name" text NOT NULL,
  "venue" text NOT NULL,
  "date" text NOT NULL,
  "doors" text NOT NULL,
  "chiefAccount" text NOT NULL,
  "createdAt" double precision NOT NULL
);

CREATE TABLE "showCrew" (
  "id" text PRIMARY KEY,
  "showId" text NOT NULL REFERENCES "shows"("id") ON DELETE CASCADE,
  "crewId" text NOT NULL REFERENCES "crew"("id"),
  "account" text NOT NULL,
  "role" text NOT NULL CHECK ("role" IN ('chief', 'crew')),
  UNIQUE ("showId", "account")
);
CREATE INDEX ON "showCrew" ("account");

CREATE TABLE "showInvites" (
  "id" text PRIMARY KEY,
  "showId" text NOT NULL REFERENCES "shows"("id") ON DELETE CASCADE,
  "code" text NOT NULL
);
CREATE INDEX ON "showInvites" ("showId");

CREATE TABLE "tasks" (
  "id" text PRIMARY KEY,
  "showId" text NOT NULL REFERENCES "shows"("id") ON DELETE CASCADE,
  "title" text NOT NULL,
  "status" text NOT NULL CHECK ("status" IN ('todo', 'doing', 'done', 'blocked')),
  "assigneeId" text REFERENCES "crew"("id"),
  "notes" text,
  "rank" double precision NOT NULL,
  "createdBy" text NOT NULL,
  "createdAt" double precision NOT NULL,
  "updatedAt" double precision NOT NULL
);
CREATE INDEX ON "tasks" ("showId", "rank");

CREATE TABLE "comments" (
  "id" text PRIMARY KEY,
  "taskId" text NOT NULL REFERENCES "tasks"("id") ON DELETE CASCADE,
  "authorId" text NOT NULL REFERENCES "crew"("id"),
  "body" text NOT NULL,
  "createdAt" double precision NOT NULL
);
CREATE INDEX ON "comments" ("taskId", "createdAt");

-- Activity outlives deleted tasks, so taskId has no foreign key.
CREATE TABLE "activity" (
  "id" text PRIMARY KEY,
  "showId" text NOT NULL REFERENCES "shows"("id") ON DELETE CASCADE,
  "taskId" text NOT NULL,
  "actorId" text NOT NULL REFERENCES "crew"("id"),
  "kind" text NOT NULL,
  "detail" text,
  "createdAt" double precision NOT NULL
);
CREATE INDEX ON "activity" ("showId", "createdAt");
CREATE INDEX ON "activity" ("taskId", "createdAt");

CREATE TABLE "checklistItems" (
  "id" text PRIMARY KEY,
  "title" text NOT NULL,
  "done" boolean NOT NULL,
  "ownerAccount" text NOT NULL
);
CREATE INDEX ON "checklistItems" ("ownerAccount", "title");

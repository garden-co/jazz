// Resets the database and writes the StagePlan fixture straight into
// Postgres, as the fixture's seed chief. Prints the ids the harness needs.
import pg from "pg";
import { BIG_SHOW, stagePlanFixture } from "../../../../../dev/app-comparisons/fixtures/stage-plan.mjs";
import { DATABASE_URL, resetDatabase } from "./db.js";

const fixture = stagePlanFixture();
const chief = "00000000-0000-4000-8000-000000000001";
const ids = new Map<string, string>();
const idOf = (key: string) => {
  if (!ids.has(key)) ids.set(key, crypto.randomUUID());
  return ids.get(key)!;
};

await resetDatabase();
const db = new pg.Client({ connectionString: DATABASE_URL });
await db.connect();
const insert = (table: string, rows: object[]) =>
  db.query(`INSERT INTO "${table}" SELECT * FROM json_populate_recordset(null::"${table}", $1)`, [
    JSON.stringify(rows),
  ]);

await db.query("BEGIN");
await insert("crew", [{ id: chief, account: chief, name: fixture.users.chief.name }]);
await insert(
  "shows",
  fixture.shows.map((show) => ({
    id: idOf(show.key),
    name: show.name,
    venue: show.venue,
    date: show.date,
    doors: show.doors,
    chiefAccount: chief,
    createdAt: show.createdAtMs,
  })),
);
await insert(
  "showCrew",
  fixture.shows.map((show) => ({
    id: `${idOf(show.key)}:${chief}`,
    showId: idOf(show.key),
    crewId: chief,
    account: chief,
    role: "chief",
  })),
);
await insert(
  "showInvites",
  fixture.invites.map((invite) => ({ id: crypto.randomUUID(), showId: idOf(invite.showKey), code: invite.code })),
);
await insert(
  "tasks",
  fixture.tasks.map((task) => ({
    id: idOf(task.key),
    showId: idOf(task.showKey),
    title: task.title,
    status: task.status,
    rank: task.rank,
    createdBy: chief,
    createdAt: task.createdAtMs,
    updatedAt: task.createdAtMs,
  })),
);
await insert(
  "comments",
  fixture.comments.map((comment) => ({
    id: crypto.randomUUID(),
    taskId: idOf(comment.taskKey),
    authorId: chief,
    body: comment.body,
    createdAt: comment.createdAtMs,
  })),
);
await db.query("COMMIT");
await db.end();

const big = fixture.invites.find((invite) => invite.showKey === BIG_SHOW)!;
console.log(JSON.stringify({ showId: idOf(BIG_SHOW), code: big.code }));

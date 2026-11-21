// Resets the Instant app and writes the StagePlan fixture through the admin
// SDK (no permission checks), as the fixture's seed chief. Prints the ids the
// harness needs as JSON.
import { init, id } from "@instantdb/admin";
import schema from "./instant.schema.ts";
import { stagePlanFixture, BIG_SHOW } from "../../../../dev/app-comparisons/fixtures/stage-plan.mjs";

const db = init({
  appId: process.env.VITE_INSTANT_APP_ID!,
  adminToken: process.env.INSTANT_APP_ADMIN_TOKEN!,
  schema,
});
const fixture = stagePlanFixture();

/** Temporary apps rate-limit bursts of admin calls: wait and try again. */
async function retry<T>(call: () => Promise<T>): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await call();
    } catch (error) {
      if ((error as { status?: number }).status !== 429 || attempt > 30) throw error;
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

// Reset: delete every row the app has.
for (const ns of [
  "shows",
  "showCrew",
  "showInvites",
  "tasks",
  "comments",
  "activity",
  "crew",
  "checklistItems",
] as const) {
  for (;;) {
    const rows = (
      await retry(() => db.query({ [ns]: { $: { limit: 500, fields: ["id"] } } } as any))
    )[ns] as {
      id: string;
    }[];
    if (rows.length === 0) break;
    await retry(() => db.transact(rows.map((row) => (db.tx as any)[ns][row.id].delete())));
  }
}

const chiefAccount = "00000000-0000-4000-8000-000000000001";
await retry(() =>
  db.transact(
    db.tx.crew[chiefAccount].create({ account: chiefAccount, name: fixture.users.chief.name }),
  ),
);
const ids: Record<string, string> = {};
for (const show of fixture.shows) {
  const showId = id();
  ids[show.key] = showId;
  const invite = fixture.invites.find((i) => i.showKey === show.key)!;
  const chunks = [
    db.tx.shows[showId].create({
      name: show.name,
      venue: show.venue,
      date: show.date,
      doors: show.doors,
      chiefAccount,
    }),
    db.tx.showCrew[id()]
      .create({ account: chiefAccount, role: "chief" })
      .link({ show: showId, crew: chiefAccount }),
    db.tx.showInvites[id()].create({ code: invite.code }).link({ show: showId }),
  ];
  for (const task of fixture.tasks.filter((t) => t.showKey === show.key)) {
    const taskId = id();
    chunks.push(
      db.tx.tasks[taskId]
        .create({
          title: task.title,
          status: task.status as never,
          rank: task.rank,
          creatorAccount: chiefAccount,
          updatedAt: task.createdAtMs,
        })
        .link({ show: showId }) as never,
    );
    for (const comment of fixture.comments.filter((c) => c.taskKey === task.key)) {
      chunks.push(
        db.tx.comments[id()]
          .create({ body: comment.body, createdAt: comment.createdAtMs })
          .link({ task: taskId, author: chiefAccount }) as never,
      );
    }
  }
  await retry(() => db.transact(chunks));
}
const big = fixture.invites.find((i) => i.showKey === BIG_SHOW)!;
console.log(JSON.stringify({ showId: ids[BIG_SHOW], code: big.code }));

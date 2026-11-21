// Writes the StagePlan fixture through the trusted backend (no policy checks),
// as the fixture's seed chief. Prints the ids the harness needs as JSON.
import { createJazz, sqlite } from "jazz-tools/backend";
import { app } from "../jazz.generated.js";
import permissions from "../jazz.permissions.json" with { type: "json" };
import { stagePlanFixture, BIG_SHOW } from "../../../../../dev/app-comparisons/fixtures/stage-plan.mjs";

const dataDir = process.env.JAZZ_DATA_DIR ?? ".jazz";
const fixture = stagePlanFixture();
const jazz = createJazz({ app, permissions, store: sqlite({ path: dataDir }) });
const db = jazz.db;

const chiefAccount = "00000000-0000-4000-8000-000000000001";
const chief = await db.insert(app.crew, { account: chiefAccount, name: fixture.users.chief.name }).wait({ tier: "local" });
const ids: Record<string, string> = {};
for (const show of fixture.shows) {
  const write = db.transaction((tx) => {
    const row = tx.insert(app.shows, {
      name: show.name,
      venue: show.venue,
      date: show.date,
      doors: show.doors,
      chiefAccount,
    });
    tx.insert(app.showCrew, { showId: row.id, crewId: chief.id, account: chiefAccount, role: "chief" });
    const invite = fixture.invites.find((i) => i.showKey === show.key)!;
    tx.insert(app.showInvites, { showId: row.id, code: invite.code });
    for (const task of fixture.tasks.filter((t) => t.showKey === show.key)) {
      const taskRow = tx.insert(app.tasks, {
        showId: row.id,
        title: task.title,
        status: task.status as never,
        rank: task.rank,
      });
      for (const comment of fixture.comments.filter((c) => c.taskKey === task.key)) {
        tx.insert(app.comments, { taskId: taskRow.id, authorId: chief.id, body: comment.body });
      }
    }
    return row;
  });
  const { value } = await write;
  ids[show.key] = value.id;
}
await jazz.close();
const big = fixture.invites.find((i) => i.showKey === BIG_SHOW)!;
console.log(JSON.stringify({ showId: ids[BIG_SHOW], code: big.code }));

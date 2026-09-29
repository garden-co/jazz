import type { Db } from "jazz-tools";
import { app, type Crew, type TaskStatus } from "../../schema.js";

/**
 * The demo show every new account starts with. The content is fixed, so every
 * first run looks the same and a screenshot or test can rely on it.
 */
export const DEMO_SHOW = {
  name: "The Late Lanterns: album launch",
  venue: "Old Pump House",
  date: "2026-11-14",
  doors: "19:30",
};

const DEMO_TASKS: { title: string; status: TaskStatus; mine?: boolean; notes?: string }[] = [
  {
    title: "Load-in",
    status: "done",
    mine: true,
    notes: "Dock opens at 14:00. Four cases and the drum riser.",
  },
  { title: "Confirm stage plot with the venue", status: "done" },
  { title: "Set the backline", status: "doing", mine: true },
  { title: "Line check", status: "doing", notes: "16 inputs. Vocals on channels 1 to 3." },
  { title: "Soundcheck with the band", status: "todo", mine: true },
  { title: "Print setlists and tape them down", status: "todo" },
  { title: "Walk-in playlist on the house system", status: "todo" },
  {
    title: "Hazer approval from the venue",
    status: "blocked",
    notes: "Waiting on the fire officer.",
  },
];

/** Finds or creates the crew profile for an account. */
export async function ensureProfile(
  db: Db,
  account: string,
): Promise<{ profile: Crew; isNew: boolean }> {
  const existing = await db.one(app.crew.where({ account }), { tier: "local-first-unless-empty" });
  if (existing) return { profile: existing, isNew: false };
  const profile = db.insert(app.crew, {
    account,
    name: `Stagehand ${account.slice(-4).toUpperCase()}`,
  });
  return { profile: profile.value, isNew: true };
}

/** Creates the demo show for a new account, with its board, a comment and activity. */
export async function seedDemoShow(db: Db, account: string, profile: Crew) {
  await db.transaction((tx) => {
    const show = tx.insert(app.shows, { ...DEMO_SHOW, chiefAccount: account });
    tx.insert(app.showCrew, { showId: show.id, crewId: profile.id, account, role: "chief" });
    tx.insert(app.showInvites, { showId: show.id, code: crypto.randomUUID() });

    DEMO_TASKS.forEach((demo, index) => {
      const task = tx.insert(app.tasks, {
        showId: show.id,
        title: demo.title,
        status: demo.status,
        assigneeId: demo.mine ? profile.id : null,
        notes: demo.notes ?? null,
        rank: index + 1,
      });
      tx.insert(app.activity, {
        showId: show.id,
        taskId: task.id,
        actorId: profile.id,
        kind: "created",
      });
      if (demo.title === "Line check") {
        tx.insert(app.comments, {
          taskId: task.id,
          authorId: profile.id,
          body: "Channel 7 crackles. Swapping the DI box before soundcheck.",
        });
        tx.insert(app.activity, {
          showId: show.id,
          taskId: task.id,
          actorId: profile.id,
          kind: "commented",
        });
      }
    });
  });
}

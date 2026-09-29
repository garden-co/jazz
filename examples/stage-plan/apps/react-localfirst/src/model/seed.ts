import type { Db, WriteResult } from "jazz-tools";
import { app, type Crew, type TaskStatus } from "../../schema.js";
import { addComment, createShow, ensureChiefSetup, type Me } from "./actions.js";

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

const DEMO_COMMENT = "Channel 7 crackles. Swapping the DI box before soundcheck.";

type Writes = WriteResult<unknown>[];

/**
 * Makes sure an account has a crew profile and gives a new account the demo
 * show, unless it arrived through an invite link. Only local reads and
 * writes, so it works offline. Returns the writes it made, so the caller can
 * check that the server accepted them.
 */
export async function setUpAccount(db: Db, account: string, { withDemo }: { withDemo: boolean }) {
  const { profile, isNew } = await ensureProfile(db, account);
  const me = { account, profile };
  const writes = isNew && withDemo ? await seedDemoShow(db, me) : [];
  return { me, isNew, writes };
}

/**
 * Writes the demo show: the show, its crew chief and invite, the board with
 * its activity, and a first comment. Each write only depends on earlier
 * writes, never on rows in its own transaction
 * (https://github.com/garden-co/jazz/issues/3755).
 */
export async function seedDemoShow(db: Db, me: Me): Promise<Writes> {
  const created = await createShow(db, me, DEMO_SHOW);
  const board = await writeBoard(db, me, created.show.id);
  const comment = await addComment(db, me, board.lineCheck, DEMO_COMMENT);
  return [...created.writes, ...board.writes, comment];
}

/**
 * Finishes a demo show that was only partly saved, for example because the
 * server rejected one of its writes. The show's activity log records which steps already ran,
 * so tasks you delete later don't come back.
 */
export async function resumeDemoShow(db: Db, me: Me): Promise<Writes> {
  const show = await db.one(app.shows.where({ chiefAccount: me.account, name: DEMO_SHOW.name }), {
    tier: "local-first-unless-empty",
  });
  if (!show) return [];
  const writes = await ensureChiefSetup(db, me, show.id);
  const log = await db.all(app.activity.where({ showId: show.id }), {
    tier: "local-first-unless-empty",
  });
  if (log.length === 0) {
    const board = await writeBoard(db, me, show.id);
    writes.push(...board.writes, await addComment(db, me, board.lineCheck, DEMO_COMMENT));
  } else if (!log.some((entry) => entry.kind === "commented")) {
    const lineCheck = await db.one(app.tasks.where({ showId: show.id, title: "Line check" }), {
      tier: "local-first-unless-empty",
    });
    if (lineCheck) writes.push(await addComment(db, me, lineCheck, DEMO_COMMENT));
  }
  return writes;
}

/** The demo board: eight tasks, each with its "created" activity. */
async function writeBoard(db: Db, me: Me, showId: string) {
  const board = await db.transaction((tx) =>
    DEMO_TASKS.map((demo, index) => {
      const task = tx.insert(app.tasks, {
        showId,
        title: demo.title,
        status: demo.status,
        assigneeId: demo.mine ? me.profile.id : null,
        notes: demo.notes ?? null,
        rank: index + 1,
      });
      tx.insert(app.activity, { showId, taskId: task.id, actorId: me.profile.id, kind: "created" });
      return task;
    }),
  );
  const lineCheck = board.value.find((task) => task.title === "Line check")!;
  return { lineCheck, writes: [board] as Writes };
}

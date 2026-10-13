import type { Db, WriteResult } from "jazz-tools";
import { app, type Crew, type TaskStatus } from "../../schema.js";
import {
  addComment,
  ensureChiefSetup,
  stageComment,
  stageShow,
  type Me,
  type Tx,
} from "./actions.js";

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

/**
 * How long to wait for the server's copy of the profile before creating one.
 * The wait ends as soon as the server answers or the link drops, and never
 * applies offline, so it is only this long when the server is connected but
 * slow. Giving up early would create a second profile for a returning account.
 */
const PROFILE_SERVER_WAIT_MS = 60_000;

/** Finds or creates the crew profile for an account. */
export async function ensureProfile(
  db: Db,
  account: string,
): Promise<{ profile: Crew; isNew: boolean }> {
  const existing = await db.one(app.crew.where({ account }), {
    tier: "local-first",
    firstLoadRemoteWaitMs: PROFILE_SERVER_WAIT_MS,
  });
  if (existing) return { profile: existing, isNew: false };
  const profile = db.insert(app.crew, {
    account,
    name: `Stagehand ${account.slice(-4).toUpperCase()}`,
  });
  return { profile: profile.value, isNew: true };
}

const DEMO_COMMENT = "Channel 7 crackles. Swapping the DI box before soundcheck.";

type Writes = WriteResult<unknown>[];

/** A demo show and the writes that made or repaired it. */
export type DemoShow = { showId: string; writes: Writes };

/**
 * Makes sure an account has a crew profile and gives a new account the demo
 * show, unless it arrived through an invite link. Only local reads and
 * writes, so it works offline. Returns the demo show's writes, so the caller
 * can check that the server accepted them.
 */
export async function setUpAccount(db: Db, account: string, { withDemo }: { withDemo: boolean }) {
  const { profile, isNew } = await ensureProfile(db, account);
  const me = { account, profile };
  const demo = isNew && withDemo ? await seedDemoShow(db, me) : undefined;
  return { me, demo };
}

/**
 * Writes the demo show in one transaction: the show, its crew chief and
 * invite, the board with its activity, and a first comment. Each row's policy
 * sees the rows staged before it in the same transaction.
 */
export async function seedDemoShow(db: Db, me: Me): Promise<DemoShow> {
  const seeded = await db.transaction(async (tx) => {
    const show = await stageShow(tx, me, DEMO_SHOW);
    const lineCheck = stageBoard(tx, me, show.id);
    stageComment(tx, me, lineCheck, DEMO_COMMENT);
    return show;
  });
  return { showId: seeded.value.id, writes: [seeded] as Writes };
}

/**
 * Finishes a demo show after the server rejected some of its writes. Call it
 * only once every write of the earlier attempt has settled: it reads the
 * server's copy to see what's missing, and the show's activity log records
 * which steps went through.
 */
export async function resumeDemoShow(db: Db, me: Me, showId: string): Promise<DemoShow> {
  const server = { tier: "remote" } as const;
  const show = await db.one(app.shows.where({ id: showId }), server);
  if (!show) return seedDemoShow(db, me);

  const writes = await ensureChiefSetup(db, me, showId);
  const log = await db.all(app.activity.where({ showId }), server);
  if (log.length === 0) {
    const board = await writeBoard(db, me, showId);
    writes.push(...board.writes, await addComment(db, me, board.lineCheck, DEMO_COMMENT));
  } else if (!log.some((entry) => entry.kind === "commented")) {
    const lineCheck = await db.one(app.tasks.where({ showId, title: "Line check" }), server);
    if (lineCheck) writes.push(await addComment(db, me, lineCheck, DEMO_COMMENT));
  }
  return { showId, writes };
}

/** The demo board: eight tasks, each with its "created" activity. */
async function writeBoard(db: Db, me: Me, showId: string) {
  const board = await db.transaction((tx) => stageBoard(tx, me, showId));
  return { lineCheck: board.value, writes: [board] as Writes };
}

/** Stages the demo board into `tx` and returns its "Line check" task. */
function stageBoard(tx: Tx, me: Me, showId: string) {
  const tasks = DEMO_TASKS.map((demo, index) => {
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
  });
  return tasks.find((task) => task.title === "Line check")!;
}

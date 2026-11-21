import { id } from "@instantdb/react";
import { db, transact, type Crew, type TaskStatus } from "./db.js";
import { stageComment, stageShow, type Me } from "./actions.js";

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

const DEMO_COMMENT = "Channel 7 crackles. Swapping the DI box before soundcheck.";

/**
 * Finds or creates the crew profile for an account. The profile's id is the
 * account's id, so two tabs can't create two profiles.
 */
export async function ensureProfile(account: string): Promise<{ profile: Crew; isNew: boolean }> {
  const { data } = await db.queryOnce({ crew: { $: { where: { id: account } } } });
  const existing = data.crew[0];
  if (existing) return { profile: existing, isNew: false };
  const profile = { id: account, account, name: `Stagehand ${account.slice(-4).toUpperCase()}` };
  await transact(db.tx.crew[account].create({ account, name: profile.name }));
  return { profile, isNew: true };
}

/** A demo show and the write that made it. */
export type DemoShow = { showId: string; write: Promise<unknown> };

/**
 * Makes sure an account has a crew profile and gives a new account the demo
 * show, unless it arrived through an invite link.
 */
export async function setUpAccount(account: string, { withDemo }: { withDemo: boolean }) {
  const { profile, isNew } = await ensureProfile(account);
  const me = { account, profile };
  const demo = isNew && withDemo ? seedDemoShow(me) : undefined;
  return { me, demo };
}

/**
 * Writes the demo show in one transaction: the show, its crew chief and
 * invite, the board with its activity, and a first comment. Instant applies
 * it all or none of it, so a retry writes the whole show again.
 */
export function seedDemoShow(me: Me): DemoShow {
  const showId = id();
  const now = Date.now();
  const tasks = DEMO_TASKS.map((demo, index) => ({ ...demo, id: id(), showId, rank: index + 1 }));
  const lineCheck = tasks.find((task) => task.title === "Line check")!;
  const write = transact([
    ...stageShow(me, showId, DEMO_SHOW),
    ...tasks.flatMap((task) => [
      db.tx.tasks[task.id]
        .create({
          title: task.title,
          status: task.status,
          assigneeId: task.mine ? me.profile.id : null,
          notes: task.notes ?? null,
          rank: task.rank,
          creatorAccount: me.account,
          updatedAt: now,
        })
        .link({ show: showId }),
      db.tx.activity[id()]
        .create({ taskId: task.id, kind: "created", createdAt: now })
        .link({ show: showId, actor: me.profile.id }),
    ]),
    ...stageComment(me, lineCheck, DEMO_COMMENT),
  ]);
  return { showId, write };
}

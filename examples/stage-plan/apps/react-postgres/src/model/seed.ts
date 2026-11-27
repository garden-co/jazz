import { call } from "../data/api.js";
import type { Db } from "../data/live.js";
import type { Crew } from "../../shared/types.js";
import { createShow, type Me, type Write } from "./actions.js";

/** The demo show every new account starts with (its board is filled in by the server). */
export const DEMO_SHOW = {
  name: "The Late Lanterns: album launch",
  venue: "Old Pump House",
  date: "2026-11-14",
  doors: "19:30",
};

/** A demo show and the write that made it. */
export type DemoShow = { showId: string; write: Write };

/** Finds or creates the crew profile for an account (the session already says whether it has one). */
export async function ensureProfile(db: Db, account: string): Promise<{ profile: Crew; isNew: boolean }> {
  const existing = db.getQueryData<Crew | null>(["me"]);
  if (existing) return { profile: existing, isNew: false };
  const name = `Stagehand ${account.slice(-4).toUpperCase()}`;
  const profile = await call<Crew>("crew.ensure", { name });
  db.setQueryData(["me"], profile);
  return { profile, isNew: true };
}

/**
 * Makes sure an account has a crew profile and gives a new account the demo
 * show, unless it arrived through an invite link. The demo show is one call:
 * the show, its crew chief and invite, the board with its activity, and a
 * first comment.
 */
export async function setUpAccount(db: Db, account: string, { withDemo }: { withDemo: boolean }) {
  const { profile, isNew } = await ensureProfile(db, account);
  const me = { account, profile };
  const demo = isNew && withDemo ? await seedDemoShow(db, me) : undefined;
  return { me, demo };
}

export async function seedDemoShow(db: Db, me: Me): Promise<DemoShow> {
  const { show, write } = await createShow(db, me, DEMO_SHOW, true);
  return { showId: show.id, write };
}

/** Writes the demo show again after the server rejected it. */
export function resumeDemoShow(db: Db, me: Me): Promise<DemoShow> {
  return seedDemoShow(db, me);
}

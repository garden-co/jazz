import type { MutatorResult, Zero } from "@rocicorp/zero";
import { DEMO_SHOW, mutators } from "../../mutators.js";
import { queries } from "../../queries.js";
import type { Crew } from "../../schema.js";
import { createShow, type Me } from "./actions.js";

export { DEMO_SHOW };

/** A demo show and the write that made it. */
export type DemoShow = { showId: string; write: MutatorResult };

/**
 * Finds or creates the crew profile for an account. It waits for the
 * server's answer first: deciding too early would give a returning account a
 * second demo show.
 */
export async function ensureProfile(db: Zero, account: string): Promise<{ profile: Crew; isNew: boolean }> {
  const existing = await db.run(queries.crew.mine(), { type: "complete" });
  if (existing) return { profile: existing, isNew: false };
  const name = `Stagehand ${account.slice(-4).toUpperCase()}`;
  await db.mutate(mutators.crew.ensure({ name })).client;
  return { profile: { id: account, account, name }, isNew: true };
}

/**
 * Makes sure an account has a crew profile and gives a new account the demo
 * show, unless it arrived through an invite link. The demo show is one
 * mutation: the show, its crew chief and invite, the board with its activity,
 * and a first comment.
 */
export async function setUpAccount(db: Zero, account: string, { withDemo }: { withDemo: boolean }) {
  const { profile, isNew } = await ensureProfile(db, account);
  const me = { account, profile };
  const demo = isNew && withDemo ? seedDemoShow(db, me) : undefined;
  return { me, demo };
}

export function seedDemoShow(db: Zero, me: Me): DemoShow {
  const { show, write } = createShow(db, me, DEMO_SHOW, true);
  return { showId: show.id, write };
}

/** Writes the demo show again after the server rejected it. */
export function resumeDemoShow(db: Zero, me: Me): DemoShow {
  return seedDemoShow(db, me);
}

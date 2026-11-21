import { DEMO_SHOW } from "../../demo.js";
import type { Crew } from "../../schema.js";
import { createShow, type Db, type Me } from "./actions.js";
import { mutate, type Write } from "./live.js";

export { DEMO_SHOW };

/** A demo show and the write that made it. */
export type DemoShow = { showId: string; write: Write };

/**
 * Finds or creates the crew profile for an account. The server decides
 * whether it is new, so a returning account never gets a second demo show.
 */
export async function ensureProfile(_db: Db, account: string): Promise<{ profile: Crew; isNew: boolean }> {
  const name = `Stagehand ${account.slice(-4).toUpperCase()}`;
  const result = await mutate("crew.ensure", { name }).server;
  if (result.type === "error") throw new Error(result.error.message);
  return result.result as { profile: Crew; isNew: boolean };
}

/**
 * Makes sure an account has a crew profile and gives a new account the demo
 * show, unless it arrived through an invite link. The demo show is one
 * transaction: the show, its crew chief and invite, the board with its
 * activity, and a first comment.
 */
export async function setUpAccount(db: Db, account: string, { withDemo }: { withDemo: boolean }) {
  const { profile, isNew } = await ensureProfile(db, account);
  const me = { account, profile };
  const demo = isNew && withDemo ? seedDemoShow(db, me) : undefined;
  return { me, demo };
}

export function seedDemoShow(db: Db, me: Me): DemoShow {
  const { show, write } = createShow(db, me, DEMO_SHOW, true);
  return { showId: show.id, write };
}

/** Writes the demo show again after the server rejected it. */
export function resumeDemoShow(db: Db, me: Me): DemoShow {
  return seedDemoShow(db, me);
}

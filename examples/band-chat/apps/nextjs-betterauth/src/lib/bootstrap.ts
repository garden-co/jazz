import { app } from "../../schema";
import { authJazzContext } from "./auth-jazz-context";

/**
 * Explicitly provision the signed-in user's profile in a server route/layout.
 * Read hooks must never call this: ordinary UI reads are side-effect free.
 */
export async function ensureProfile(userId: string, displayName: string) {
  const db = authJazzContext().asBackend(app);
  const existing = await db.one(app.profiles.where({ userId }));
  if (existing) return existing;

  try {
    return await db
      .insert(app.profiles, { userId, displayName }, { id: userId })
      .wait({ tier: "edge" });
  } catch (error) {
    const raced = await db.one(app.profiles.where({ userId }));
    if (raced) return raced;
    throw error;
  }
}

import { app } from "../../schema";
import { authJazzContext } from "./auth-jazz-context";
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

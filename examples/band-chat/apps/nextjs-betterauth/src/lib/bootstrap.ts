import { createHash } from "node:crypto";
import { app } from "../../schema";
import { authJazzContext } from "./auth-jazz-context";
import { authorForSession } from "./identity";

/**
 * Explicitly provision the signed-in user's profile in a server route/layout.
 * Read hooks must never call this: ordinary UI reads are side-effect free.
 */
export async function ensureProfile(issuer: string, userId: string, displayName: string) {
  const author = authorForSession(issuer, userId);
  const db = authJazzContext().asBackend(app);
  const existing = await db.one(app.profiles.where({ author }));
  if (existing) return existing;

  try {
    return await db
      .insert(app.profiles, { author, displayName }, { id: profileId(author) })
      .wait({ tier: "edge" });
  } catch (error) {
    const raced = await db.one(app.profiles.where({ author }));
    if (raced) return raced;
    throw error;
  }
}

function profileId(author: string): string {
  const hex = createHash("sha256").update(author).digest("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-a${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}

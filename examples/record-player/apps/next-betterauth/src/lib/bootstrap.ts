import { app } from "../../schema";
import { authJazzContext } from "./auth-jazz-context";

/**
 * Verify Better Auth's trusted backend row before mounting Jazz. RecordPlayer
 * intentionally seeds no playlist or profile: first playlists remain normal
 * client writes under the app's ordinary permissions.
 */
export async function ensureAccountBootstrap(userId: string): Promise<void> {
  const db = authJazzContext().asBackend(app);
  const user = await db.one(app.better_auth_user.where({ id: userId }));
  if (!user) throw new Error("authenticated Better Auth user is missing from trusted storage");
}

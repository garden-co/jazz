import { createHash } from "node:crypto";
import { adminDb } from "@/lib/instant.server";

/** Provision the signed-in profile outside the application's read path. */
export async function ensureProfile(author: string, displayName: string) {
  const { profiles } = await adminDb.query({ profiles: { $: { where: { author } } } });
  const existing = profiles[0];
  if (existing) {
    if (existing.displayName !== displayName)
      await adminDb.transact(adminDb.tx.profiles[existing.id].update({ displayName }));
    return existing;
  }
  // A stable id per author: two concurrent bootstraps write the same row.
  const profileId = createHash("sha256").update(author).digest("hex");
  const id = `${profileId.slice(0, 8)}-${profileId.slice(8, 12)}-5${profileId.slice(13, 16)}-a${profileId.slice(17, 20)}-${profileId.slice(20, 32)}`;
  await adminDb.transact(adminDb.tx.profiles[id].update({ author, displayName }));
  return { id, author, displayName };
}

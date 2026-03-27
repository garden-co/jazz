import { useEffect } from "react";
import { useDb, useSession, useAll } from "jazz-tools/react";
import { getRandomUsername } from "@/lib/utils";
import { app, type Profile } from "../../schema/app.js";

// Module-level guard: the in-memory WASM store settles synchronously with []
// before OPFS data streams in from the worker. Without this guard, every page
// load would create a duplicate profile.
const createdForUser = new Set<string>();

export function useMyProfile(): Profile | null {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id ?? null;

  const profiles = useAll(app.profiles.where({ userId: userId ?? "__none__" }));

  // Deterministic: always pick the first profile by ID
  const sorted = profiles ? [...profiles].sort((a, b) => a.id.localeCompare(b.id)) : [];
  const canonical = sorted[0] ?? null;

  useEffect(() => {
    if (!userId || !profiles || profiles.length > 0 || createdForUser.has(userId)) return;
    createdForUser.add(userId);
    db.insert(app.profiles, { userId, name: getRandomUsername() });
  }, [userId, profiles, db]);

  return canonical;
}

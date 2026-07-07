import { useEffect, useMemo, useState } from "react";
import { useDb, useSession, useAll } from "jazz-tools/react";
import { getRandomUsername } from "@/lib/utils";
import { app, type Profile } from "../../schema.js";

// Module-level guard: the in-memory WASM store settles synchronously with []
// before OPFS data streams in from the worker. Without this guard, every page
// load would create a duplicate profile.
const createdForUser = new Set<string>();

/** Reset the module-level guard. Needed in tests that remount with different appIds. */
export function resetProfileGuard() {
  createdForUser.clear();
}

export function useMyProfile(): Profile | null {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id ?? null;
  const [optimisticProfile, setOptimisticProfile] = useState<Profile | null>(null);
  const [confirmedProfileId, setConfirmedProfileId] = useState<string | null>(null);
  const sharedWriteOptions = useMemo(
    () => (db.getConfig().serverUrl ? { tier: "edge" as const } : undefined),
    [db],
  );

  const profiles = useAll(app.profiles.where({ userId: userId ?? "__none__" }));

  // Deterministic: always pick the first profile by ID
  const sorted = profiles ? [...profiles].sort((a, b) => a.id.localeCompare(b.id)) : [];
  const canonical = sorted[0] ?? null;
  const localProfile = optimisticProfile?.userId === userId ? optimisticProfile : null;
  const profile = canonical ?? localProfile;
  const profileConfirmed =
    !sharedWriteOptions ||
    (profile && (profile.id === confirmedProfileId || profile.id !== localProfile?.id));

  useEffect(() => {
    if (!userId || !profiles || profiles.length > 0 || createdForUser.has(userId)) return;
    createdForUser.add(userId);

    void (async () => {
      const profile = { userId, name: getRandomUsername() };
      const created = await Promise.resolve(db.insert(app.profiles, profile));
      setOptimisticProfile(created.value);
      if (!sharedWriteOptions) {
        setConfirmedProfileId(created.value.id);
        return;
      }

      await created.wait(sharedWriteOptions);
      setConfirmedProfileId(created.value.id);
    })().catch(() => {
      createdForUser.delete(userId);
    });
  }, [userId, profiles, db, sharedWriteOptions]);

  return profileConfirmed ? profile : null;
}

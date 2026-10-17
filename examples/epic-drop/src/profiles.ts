import * as React from "react";
import { useAll, useDb } from "jazz-tools/react";
import { app } from "../schema.js";

export function defaultName(userId: string): string {
  return `Guest ${userId.replace(/-/g, "").slice(-4).toUpperCase()}`;
}

/**
 * Give a new anonymous account a readable name once. The profile row's id is
 * the account id, so tabs that race here upsert the same row instead of
 * creating duplicates.
 */
export function useEnsureProfile(userId: string | undefined) {
  const db = useDb();
  const { data: mine, isLoading } = useAll(userId ? app.profiles.where({ id: userId }) : undefined);
  React.useEffect(() => {
    if (!userId || isLoading || !mine || mine.length > 0) return;
    db.upsert(app.profiles, userId, { name: defaultName(userId) });
  }, [db, isLoading, mine, userId]);
  return mine?.[0];
}

/**
 * Names for the given accounts. Permissions show a name only across a
 * membership, so anyone else falls back to a generated one.
 */
export function useNames(userIds: readonly string[], currentUserId: string | undefined) {
  const ids = [...new Set(userIds)].sort();
  const { data: profiles = [] } = useAll(
    ids.length > 0 ? app.profiles.where({ id: { in: ids } }) : undefined,
  );
  return React.useCallback(
    (userId: string) => {
      if (userId === currentUserId) return "You";
      return profiles.find((profile) => profile.id === userId)?.name ?? defaultName(userId);
    },
    [currentUserId, profiles],
  );
}

import * as React from "react";
import { useAll, useDb } from "jazz-tools/react";
import { app } from "../schema.js";

export function defaultName(userId: string): string {
  return `Guest ${userId.replace(/-/g, "").slice(-4).toUpperCase()}`;
}

/** Give a new anonymous account a readable name once. */
export function useEnsureProfile(userId: string | undefined) {
  const db = useDb();
  const { data: mine, isLoading } = useAll(
    userId ? app.profiles.where({ user_id: userId }) : undefined,
  );
  const created = React.useRef(false);
  React.useEffect(() => {
    if (!userId || isLoading || !mine || mine.length > 0 || created.current) return;
    created.current = true;
    db.insert(app.profiles, { user_id: userId, name: defaultName(userId) });
  }, [db, isLoading, mine, userId]);
  return mine?.[0];
}

/** Names for the given accounts, falling back to a generated one. */
export function useNames(userIds: readonly string[], currentUserId: string | undefined) {
  const ids = [...new Set(userIds)].sort();
  const { data: profiles = [] } = useAll(
    ids.length > 0 ? app.profiles.where({ user_id: { in: ids } }) : undefined,
  );
  return React.useCallback(
    (userId: string) => {
      if (userId === currentUserId) return "You";
      return profiles.find((profile) => profile.user_id === userId)?.name ?? defaultName(userId);
    },
    [currentUserId, profiles],
  );
}

export type ReactionIntent = {
  active: boolean;
  syncedActive: boolean;
  keepPending: boolean;
};

export function nextReactionIntent(
  currentActive: boolean,
  queued?: { syncedActive?: boolean },
): ReactionIntent {
  const active = !currentActive;
  const syncedActive = queued?.syncedActive ?? currentActive;
  return { active, syncedActive, keepPending: active !== syncedActive };
}

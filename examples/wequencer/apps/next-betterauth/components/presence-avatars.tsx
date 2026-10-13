"use client";

import { useEffect, useState } from "react";
import { useAll } from "jazz-tools/react";
import { Avatar } from "@astryxdesign/core/Avatar";
import { AvatarGroup, AvatarGroupOverflow } from "@astryxdesign/core/AvatarGroup";
import { app } from "@/schema";
import { PRESENCE_HEARTBEAT_INTERVAL_MS } from "@/components/presence-heartbeat";

/** Three missed heartbeats and a bandmate no longer counts as here. */
const PRESENCE_WINDOW_MS = PRESENCE_HEARTBEAT_INTERVAL_MS * 3;
const MAX_VISIBLE = 4;

export function usePresence(sessionId: string) {
  const { data = [] } = useAll(
    app.presence.where({ session_id: sessionId }).include({ profile: true }),
  );
  return data;
}

export type PresenceRow = ReturnType<typeof usePresence>[number];

function useNow(intervalMs: number) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(timer);
  }, [intervalMs]);
  return now;
}

/**
 * Bandmates whose heartbeat arrived recently. Presence is advisory: a row can
 * be stale after a disconnect, and it never grants access to anything.
 */
export function PresenceAvatars({ presence }: { presence: PresenceRow[] }) {
  const now = useNow(PRESENCE_HEARTBEAT_INTERVAL_MS);
  const here = new Map<string, string>();
  for (const row of presence) {
    if (now - row.heartbeat_at.getTime() > PRESENCE_WINDOW_MS) continue;
    here.set(row.profile_id, row.profile?.displayName ?? "Bandmate");
  }
  const people = [...here.entries()];
  if (people.length === 0) return null;
  const visible = people.slice(0, MAX_VISIBLE);

  return (
    <div aria-label={`${people.length} here now`} role="group">
      <AvatarGroup size="sm">
        {visible.map(([id, name]) => (
          <Avatar key={id} name={name} tooltip={`${name} is here`} />
        ))}
        {people.length > MAX_VISIBLE ? (
          <AvatarGroupOverflow count={people.length - MAX_VISIBLE} />
        ) : null}
      </AvatarGroup>
    </div>
  );
}

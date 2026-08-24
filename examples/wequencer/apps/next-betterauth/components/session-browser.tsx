"use client";

import { useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
import { app } from "@/schema";
import { SequencerSession } from "@/components/sequencer-session";

const TRACK_COLORS = ["#ff7a59", "#f5c451", "#5dd6c0", "#7998ff"];
const INSTRUMENTS = ["Kick", "Snare", "Closed hat", "Bass"];

export function SessionBrowser({ userId, displayName }: { userId: string; displayName: string }) {
  const db = useDb();
  const { data: sessions = [], isLoading } = useAll(app.sessions.orderBy("$createdAt", "desc"));
  const { data: profiles = [] } = useAll(app.profiles.where({ user_id: userId }));
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [createdProfileId, setCreatedProfileId] = useState<string | null>(null);

  function createSession() {
    // This is an explicit user action, not account bootstrap on the initial
    // read-through path. A production app's server route may additionally
    // create default shared structures with its trusted backend credential.
    const profile =
      profiles[0] ?? db.insert(app.profiles, { user_id: userId, display_name: displayName }).value;
    const session = db.insert(app.sessions, {
      title: "Late-night rehearsal",
      tempo_bpm: 124,
      loop_steps: 16,
    });
    db.insert(app.session_members, {
      session_id: session.value.id,
      user_id: userId,
      role: "owner",
    });
    for (const [position, name] of INSTRUMENTS.entries()) {
      const track = db.insert(app.tracks, {
        session_id: session.value.id,
        position,
        name,
        color: TRACK_COLORS[position],
      });
      for (let step = 0; step < 16; step += 1) {
        db.insert(app.steps, {
          track_id: track.value.id,
          position: step,
          enabled: step % (position + 2) === 0,
          velocity: position === 0 ? 112 : 88,
          probability: 100,
        });
      }
    }
    setCreatedProfileId(profile.id);
    setSelectedId(session.value.id);
  }

  const selected = sessions.find((session) => session.id === selectedId) ?? sessions[0];
  const profileId = createdProfileId ?? profiles[0]?.id;
  if (selected && profileId)
    return <SequencerSession sessionId={selected.id} userId={userId} profileId={profileId} />;

  return (
    <section className="session-empty">
      <p className="eyebrow">COLLABORATIVE STEP SEQUENCER</p>
      <h2>{isLoading ? "Finding your sessions…" : "Start a rehearsal"}</h2>
      <p>
        Every pad is an ordinary local-first row. Jazz keeps independent edits responsive offline
        and converges them after a reconnect.
      </p>
      <button className="btn-primary" type="button" onClick={createSession} disabled={isLoading}>
        Create a 4-track session
      </button>
    </section>
  );
}

"use client";

import { useEffect, useMemo } from "react";
import { useAll, useDb } from "jazz-tools/react";
import { app } from "@/schema";
import { TrackLane } from "@/components/track-lane";

export function SequencerSession({
  sessionId,
  userId,
  profileId,
}: {
  sessionId: string;
  userId: string;
  profileId: string;
}) {
  const db = useDb();
  const { data: sessions = [] } = useAll(app.sessions.where({ id: sessionId }));
  const { data: tracks = [] } = useAll(
    app.tracks.where({ session_id: sessionId }).orderBy("position", "asc"),
  );
  const { data: observations = [] } = useAll(
    app.transport_observations.where({ session_id: sessionId }).orderBy("observed_at", "desc"),
  );
  const { data: presence = [] } = useAll(app.presence.where({ session_id: sessionId }));
  const transport = observations[0];
  const session = sessions[0];
  const playhead = transport?.bar ?? 0;
  const title = session?.title ?? "Loading session…";

  const observableTransport = useMemo(
    () => ({ playing: transport?.playing ?? false, bar: playhead }),
    [playhead, transport?.playing],
  );

  useEffect(() => {
    const ownPresence = presence.find((value) => value.profile_id === profileId);
    const writeHeartbeat = () => {
      const heartbeat_at = new Date();
      if (ownPresence) db.update(app.presence, ownPresence.id, { heartbeat_at });
      else
        db.insert(app.presence, {
          session_id: sessionId,
          profile_id: profileId,
          cursor_step: playhead,
          heartbeat_at,
        });
    };
    writeHeartbeat();
    const interval = window.setInterval(writeHeartbeat, 5_000);
    return () => window.clearInterval(interval);
  }, [db, playhead, presence, profileId, sessionId]);

  function toggleTransport() {
    db.insert(app.transport_observations, {
      session_id: sessionId,
      playing: !observableTransport.playing,
      bar: observableTransport.playing ? observableTransport.bar + 1 : observableTransport.bar,
      observed_at: new Date(),
    });
  }

  return (
    <section className="sequencer">
      <header className="sequencer-toolbar">
        <div>
          <p className="eyebrow">SHARED SESSION</p>
          <h2>{title}</h2>
        </div>
        <div className="transport">
          <span>{session?.tempo_bpm ?? "–"} BPM</span>
          <span>step {playhead + 1}</span>
          <button
            type="button"
            className={observableTransport.playing ? "playing" : ""}
            onClick={toggleTransport}
          >
            {observableTransport.playing ? "Pause" : "Play"}
          </button>
        </div>
      </header>
      <p className="transport-note">
        Transport is a convergent observation for collaborators, not a claim of sample-accurate
        distributed clock sync. {presence.length} collaborator
        {presence.length === 1 ? "" : "s"} present. Your id: {userId}
      </p>
      <div className="step-ruler" aria-hidden="true">
        {Array.from({ length: session?.loop_steps ?? 16 }, (_, index) => (
          <span className={index === playhead ? "active" : ""} key={index}>
            {index + 1}
          </span>
        ))}
      </div>
      <div className="track-list">
        {tracks.map((track) => (
          <TrackLane key={track.id} trackId={track.id} name={track.name} color={track.color} />
        ))}
      </div>
    </section>
  );
}

"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
import { app } from "@/schema";
import { TrackLane } from "@/components/track-lane";
import { schedulePresenceHeartbeat } from "@/components/presence-heartbeat";
import { authorForSession } from "@/lib/identity";

export function SequencerSession({
  sessionId,
  author,
  issuer,
  profileId,
}: {
  sessionId: string;
  author: string;
  issuer: string;
  profileId: string;
}) {
  const db = useDb();
  const { data: sessions = [] } = useAll(app.sessions.where({ id: sessionId }));
  const { data: tracks = [] } = useAll(
    app.tracks.where({ session_id: sessionId }).orderBy("position", "asc"),
  );
  const { data: observations = [] } = useAll(
    app.transport_observations
      .where({ session_id: sessionId })
      .orderBy("observed_at", "desc")
      .limit(1),
  );
  const { data: presence = [] } = useAll(app.presence.where({ session_id: sessionId }));
  // Only createSession writes an owner membership, for the row creator. The
  // policy itself remains creator-bound and does not treat this role as transferable.
  const { data: creatorMembership = [] } = useAll(
    app.session_members.where({ session_id: sessionId, member_author: author, role: "owner" }),
  );
  const transport = observations[0];
  const session = sessions[0];
  const playhead = transport?.bar ?? 0;
  const title = session?.title ?? "Loading session…";
  const isCreator = creatorMembership.length > 0;
  const [memberUserId, setMemberUserId] = useState("");
  const [memberRole, setMemberRole] = useState<"editor" | "viewer">("editor");

  const observableTransport = useMemo(
    () => ({ playing: transport?.playing ?? false, bar: playhead }),
    [playhead, transport?.playing],
  );

  const writeHeartbeatRef = useRef<() => void>(() => {});
  const ownPresence = presence.find((value) => value.profile_id === profileId);
  writeHeartbeatRef.current = () => {
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

  useEffect(() => {
    return schedulePresenceHeartbeat(() => writeHeartbeatRef.current());
  }, [profileId, sessionId]);

  function toggleTransport() {
    db.insert(app.transport_observations, {
      session_id: sessionId,
      playing: !observableTransport.playing,
      bar: observableTransport.playing ? observableTransport.bar + 1 : observableTransport.bar,
      observed_at: new Date(),
    });
  }

  function addMember() {
    const invitedUserId = memberUserId.trim();
    if (!invitedUserId) return;
    db.insert(app.session_members, {
      session_id: sessionId,
      member_author: authorForSession(issuer, invitedUserId),
      role: memberRole,
    });
    setMemberUserId("");
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
        distributed clock sync. {presence.length} cached collaborator observation
        {presence.length === 1 ? "" : "s"}; observations may be stale. Your author: {author}
      </p>
      <p className="member-id">Your canonical author: {author}</p>
      {isCreator ? (
        <div className="member-controls">
          <label>
            Collaborator user ID from this auth provider
            <input value={memberUserId} onChange={(event) => setMemberUserId(event.target.value)} />
          </label>
          <label>
            Role
            <select
              value={memberRole}
              onChange={(event) => setMemberRole(event.target.value as typeof memberRole)}
            >
              <option value="editor">Editor</option>
              <option value="viewer">Viewer</option>
            </select>
          </label>
          <button type="button" onClick={addMember}>
            Add collaborator
          </button>
        </div>
      ) : null}
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

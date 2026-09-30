"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { PersistedWriteRejectedError } from "jazz-tools";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { Badge } from "@astryxdesign/core/Badge";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Card } from "@astryxdesign/core/Card";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { Heading } from "@astryxdesign/core/Heading";
import { HStack } from "@astryxdesign/core/HStack";
import { Spinner } from "@astryxdesign/core/Spinner";
import { app, type Track } from "@/schema";
import { ROLE_LABELS, strongestRole } from "@/lib/roles";
import { MAX_TRACKS } from "@/lib/instruments";
import { addTrack, stepRow } from "@/lib/session-setup";
import type { PlaybackTrack } from "@/lib/audio";
import type { ReportWrite } from "@/lib/report-write";
import type { TransportState } from "@/lib/transport";
import { schedulePresenceHeartbeat } from "@/components/presence-heartbeat";
import { PageColumn } from "@/components/page-column";
import { PresenceAvatars, usePresence } from "@/components/presence-avatars";
import { MembersDialog } from "@/components/members-dialog";
import { PatternBar } from "@/components/pattern-bar";
import { TrackLane } from "@/components/track-lane";
import { TrackSettingsDialog } from "@/components/track-settings-dialog";
import { TransportBar } from "@/components/transport-bar";
import { usePlayhead, useSequencerAudio } from "@/components/use-sequencer-audio";

export function SessionPage({ sessionId }: { sessionId: string }) {
  const db = useDb();
  const author = useSession()?.user.account;
  const { data: sessions = [], isLoading } = useAll(
    app.sessions.where({ id: sessionId }).select("*", "$createdBy"),
  );
  const { data: profiles = [] } = useAll(author ? app.profiles.where({ author }) : undefined);
  const { data: members = [] } = useAll(app.session_members.where({ session_id: sessionId }));
  const { data: tracks = [] } = useAll(
    app.tracks.where({ session_id: sessionId }).orderBy("position", "asc"),
  );
  const { data: patterns = [] } = useAll(
    app.patterns.where({ session_id: sessionId }).orderBy("position", "asc"),
  );
  const { data: observations = [] } = useAll(
    app.transport_observations
      .where({ session_id: sessionId })
      .orderBy("observed_at", "desc")
      .limit(1),
  );
  const presence = usePresence(sessionId);

  const session = sessions[0];
  const profileId = profiles[0]?.id;
  // `$createdBy` is immutable system metadata. Unlike a mutable `owner`
  // membership record, it remains the creator's administrative identity even
  // if that record is removed or its collaboration role changes.
  const isCreator = !!author && session?.$createdBy?.account === author;
  const role = strongestRole(
    members.filter((member) => member.member_author === author).map((member) => member.role),
  );
  const canEdit = role === "owner" || role === "editor";

  const observation = observations[0];
  const transport = useMemo<TransportState>(
    () => ({
      playing: observation?.playing ?? false,
      anchorStep: observation?.bar ?? 0,
      anchorAt: observation?.observed_at.getTime() ?? 0,
      tempo: observation?.tempo_bpm ?? session?.tempo_bpm ?? 120,
      patternId: observation?.pattern_id ?? undefined,
    }),
    [observation, session?.tempo_bpm],
  );
  const pattern = patterns.find((candidate) => candidate.id === transport.patternId) ?? patterns[0];
  const length = pattern?.length ?? 16;
  const playhead = usePlayhead(transport, length);

  // Audio reads the latest shared state on every scheduler tick.
  const stepsByTrack = useRef(new Map<string, boolean[]>());
  const onSteps = useCallback((trackId: string, enabled: boolean[]) => {
    stepsByTrack.current.set(trackId, enabled);
  }, []);
  const audio = useSequencerAudio(() => ({
    transport: { ...transport, patternId: pattern?.id },
    length,
    tracks: tracks.map<PlaybackTrack>((track) => ({
      instrument: track.instrument,
      volume: track.volume,
      muted: track.muted,
      solo: track.solo,
      steps: stepsByTrack.current.get(track.id) ?? [],
    })),
  }));

  // Presence heartbeats run on a timer, independent of subscription rerenders.
  const writeHeartbeatRef = useRef<() => void>(() => {});
  const ownPresence = presence.find((row) => row.profile_id === profileId);
  writeHeartbeatRef.current = () => {
    if (!profileId || !role) return;
    const heartbeat_at = new Date();
    if (ownPresence) db.update(app.presence, ownPresence.id, { heartbeat_at });
    else
      db.insert(app.presence, {
        session_id: sessionId,
        profile_id: profileId,
        cursor_step: 0,
        heartbeat_at,
      });
  };
  useEffect(() => schedulePresenceHeartbeat(() => writeHeartbeatRef.current()), [sessionId]);

  const [writeError, setWriteError] = useState<string | null>(null);
  const [isMembersOpen, setIsMembersOpen] = useState(false);
  const [settingsTrackId, setSettingsTrackId] = useState<string | null>(null);

  const reportWrite = useCallback<ReportWrite>(async (write, subject) => {
    setWriteError(null);
    try {
      await write;
    } catch (error) {
      // Local visibility remains optimistic. The receipt makes a server-side
      // permission rejection observable instead of silently looking like a
      // conflicting edit.
      setWriteError(
        error instanceof PersistedWriteRejectedError && error.code === "permission_denied"
          ? `${subject} was rejected by session permissions.`
          : `${subject} could not be confirmed. Check your connection and try again.`,
      );
    }
  }, []);

  const patternId = pattern?.id;
  const onToggleStep = useCallback(
    async (trackId: string, position: number, enabled: boolean) => {
      if (!patternId) return;
      const row = stepRow({ sessionId, trackId, patternId, position }, enabled);
      await reportWrite(
        db.upsert(app.steps, row.id, row.data).wait({ tier: "global" }),
        "Pad update",
      );
    },
    [db, patternId, reportWrite, sessionId],
  );
  const onUpdateTrack = useCallback(
    (trackId: string, change: Partial<Pick<Track, "muted" | "solo" | "volume">>) => {
      void reportWrite(
        db.update(app.tracks, trackId, change).wait({ tier: "global" }),
        "Track update",
      );
    },
    [db, reportWrite],
  );

  if (!session) {
    if (isLoading)
      return (
        <PageColumn>
          <Spinner label="Opening session…" />
        </PageColumn>
      );
    return (
      <PageColumn>
        <EmptyState
          headingLevel={1}
          title="Session not found"
          description="It may have been deleted, or you are not a member of it."
          actions={<Button label="Back to sessions" href="/dashboard" />}
        />
      </PageColumn>
    );
  }

  const settingsTrack = tracks.find((track) => track.id === settingsTrackId);

  return (
    <PageColumn>
      <HStack gap={3} justify="between" align="center" wrap="wrap">
        <HStack gap={3} align="center" wrap="wrap">
          <Heading level={1}>{session.title}</Heading>
          {role ? <Badge label={ROLE_LABELS[role]} variant={canEdit ? "info" : "neutral"} /> : null}
        </HStack>
        <HStack gap={3} align="center">
          <PresenceAvatars presence={presence} />
          <Button label="Members" onClick={() => setIsMembersOpen(true)} />
        </HStack>
      </HStack>

      {!canEdit ? (
        <Banner
          status="info"
          title="You're viewing this session"
          description="You can listen along, but only editors can change the pattern, mix and transport."
        />
      ) : null}
      {writeError ? (
        <div role="status">
          <Banner
            status="error"
            title={writeError}
            isDismissable
            onDismiss={() => setWriteError(null)}
          />
        </div>
      ) : null}

      <TransportBar
        sessionId={sessionId}
        transport={transport}
        patternId={pattern?.id}
        length={length}
        playhead={playhead}
        canEdit={canEdit}
        reportWrite={reportWrite}
        isSoundOn={audio.isSoundOn}
        onSoundChange={audio.setSound}
      />

      <Card padding={0}>
        <PatternBar
          sessionId={sessionId}
          patterns={patterns}
          current={pattern}
          transport={transport}
          canEdit={canEdit}
          reportWrite={reportWrite}
        />
        {pattern ? (
          <div className="sequencer-grid">
            <div className="sequencer-rows" style={{ "--steps": length } as React.CSSProperties}>
              <div className="step-ruler" aria-hidden="true">
                <span className="track-header" />
                {Array.from({ length }, (_, index) => (
                  <span key={index} data-current={index === playhead ? "" : undefined}>
                    {index % 4 === 0 ? index + 1 : ""}
                  </span>
                ))}
              </div>
              {tracks.map((track) => (
                <TrackLane
                  key={track.id}
                  track={track}
                  patternId={pattern.id}
                  length={length}
                  canEdit={canEdit}
                  onToggleStep={onToggleStep}
                  onUpdateTrack={onUpdateTrack}
                  onOpenSettings={setSettingsTrackId}
                  onSteps={onSteps}
                />
              ))}
              {playhead !== null && tracks.length > 0 ? (
                <div
                  className="playhead"
                  aria-hidden="true"
                  style={{ gridColumn: playhead + 2, gridRow: `2 / span ${tracks.length}` }}
                />
              ) : null}
            </div>
          </div>
        ) : null}
        {canEdit && tracks.length < MAX_TRACKS ? (
          <HStack padding={3}>
            <Button
              label="Add track"
              variant="ghost"
              onClick={() =>
                void reportWrite(
                  addTrack(db, sessionId, (tracks.at(-1)?.position ?? -1) + 1).wait({
                    tier: "global",
                  }),
                  "Adding a track",
                )
              }
            />
          </HStack>
        ) : null}
      </Card>

      <MembersDialog
        isOpen={isMembersOpen}
        onOpenChange={setIsMembersOpen}
        sessionId={sessionId}
        members={members}
        presence={presence}
        author={author ?? undefined}
        isCreator={isCreator}
        reportWrite={reportWrite}
      />
      {settingsTrack ? (
        <TrackSettingsDialog
          track={settingsTrack}
          isCreator={isCreator}
          onClose={() => setSettingsTrackId(null)}
          onPreview={audio.preview}
          reportWrite={reportWrite}
        />
      ) : null}
    </PageColumn>
  );
}

"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery } from "convex/react";
import { ConvexError } from "convex/values";
import { Badge } from "@astryxdesign/core/Badge";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Card } from "@astryxdesign/core/Card";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { Heading } from "@astryxdesign/core/Heading";
import { HStack } from "@astryxdesign/core/HStack";
import { Spinner } from "@astryxdesign/core/Spinner";
import { api } from "@/convex/_generated/api";
import type { Id, Track } from "@/lib/model";
import { ROLE_LABELS, strongestRole } from "@/lib/roles";
import { MAX_TRACKS } from "@/lib/instruments";
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
  const author = useQuery(api.users.viewer)?.id;
  const loadedSession = useQuery(api.sessions.get, { sessionId });
  const isLoading = loadedSession === undefined;
  const session = loadedSession ?? undefined;
  // The session's queries open once the URL's id names a session you can read.
  const id = session?._id;
  const args = id ? { sessionId: id } : "skip";
  const members = useQuery(api.members.bySession, args) ?? [];
  const isMember = members.some((member) => member.member_author === author);
  const memberArgs = id && isMember ? { sessionId: id } : "skip";
  const tracks = useQuery(api.tracks.bySession, memberArgs) ?? [];
  const patterns = useQuery(api.patterns.bySession, memberArgs) ?? [];
  const observation = useQuery(api.transport.latest, memberArgs) ?? undefined;
  const presence = usePresence(isMember ? id : undefined);
  const heartbeat = useMutation(api.presence.heartbeat);
  const setStep = useSetStep();
  const updateTrack = useMutation(api.tracks.update);
  const addTrack = useMutation(api.tracks.add);

  // The session's `creator` is fixed when it is created. Unlike a mutable
  // `owner` membership record, it remains the creator's administrative
  // identity even if that record is removed or its collaboration role changes.
  const isCreator = !!author && session?.creator === author;
  const role = strongestRole(
    members.filter((member) => member.member_author === author).map((member) => member.role),
  );
  const canEdit = role === "owner" || role === "editor";

  const transport = useMemo<TransportState>(
    () => ({
      playing: observation?.playing ?? false,
      anchorStep: observation?.bar ?? 0,
      anchorAt: observation?.observed_at ?? 0,
      tempo: observation?.tempo_bpm ?? session?.tempo_bpm ?? 120,
      patternId: observation?.pattern_id ?? undefined,
    }),
    [observation, session?.tempo_bpm],
  );
  const pattern = patterns.find((candidate) => candidate._id === transport.patternId) ?? patterns[0];
  const length = pattern?.length ?? 16;
  const playhead = usePlayhead(transport, length);

  // Audio reads the latest shared state on every scheduler tick.
  const stepsByTrack = useRef(new Map<string, boolean[]>());
  const onSteps = useCallback((trackId: string, enabled: boolean[]) => {
    stepsByTrack.current.set(trackId, enabled);
  }, []);
  const audio = useSequencerAudio(() => ({
    transport: { ...transport, patternId: pattern?._id },
    length,
    tracks: tracks.map<PlaybackTrack>((track) => ({
      instrument: track.instrument,
      volume: track.volume,
      muted: track.muted,
      solo: track.solo,
      steps: stepsByTrack.current.get(track._id) ?? [],
    })),
  }));

  // Presence heartbeats run on a timer, independent of subscription rerenders.
  const writeHeartbeatRef = useRef<() => void>(() => {});
  writeHeartbeatRef.current = () => {
    if (!id || !role) return;
    void heartbeat({ sessionId: id });
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
        error instanceof ConvexError && error.data === "permission_denied"
          ? `${subject} was rejected by session permissions.`
          : `${subject} could not be confirmed. Check your connection and try again.`,
      );
    }
  }, []);

  const patternId = pattern?._id;
  const onToggleStep = useCallback(
    async (trackId: string, position: number, enabled: boolean) => {
      if (!patternId) return;
      await reportWrite(
        setStep({ trackId: trackId as Id<"tracks">, patternId, position, enabled }),
        "Pad update",
      );
    },
    [setStep, patternId, reportWrite],
  );
  const onUpdateTrack = useCallback(
    (trackId: string, change: Partial<Pick<Track, "muted" | "solo" | "volume">>) => {
      void reportWrite(
        updateTrack({ trackId: trackId as Id<"tracks">, change }),
        "Track update",
      );
    },
    [updateTrack, reportWrite],
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

  const settingsTrack = tracks.find((track) => track._id === settingsTrackId);

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
        sessionId={session._id}
        transport={transport}
        patternId={pattern?._id}
        length={length}
        playhead={playhead}
        canEdit={canEdit}
        reportWrite={reportWrite}
        isSoundOn={audio.isSoundOn}
        onSoundChange={audio.setSound}
      />

      <Card padding={0}>
        <PatternBar
          sessionId={session._id}
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
                  key={track._id}
                  track={track}
                  patternId={pattern._id}
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
                  addTrack({
                    sessionId: session._id,
                    position: (tracks.at(-1)?.position ?? -1) + 1,
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
        sessionId={session._id}
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

/**
 * Pad writes. With NEXT_PUBLIC_CONVEX_OPTIMISTIC_PADS=1 (the harness's
 * "optimistic" variant) the lane shows the new state before the server
 * answers, the way Convex's counter example applies an optimistic update.
 * The default build waits for the server.
 */
function useSetStep() {
  const setStep = useMutation(api.steps.set);
  return useMemo(() => {
    if (process.env.NEXT_PUBLIC_CONVEX_OPTIMISTIC_PADS !== "1") return setStep;
    return setStep.withOptimisticUpdate((store, { trackId, patternId, position, enabled }) => {
      const lane = store.getQuery(api.steps.lane, { trackId, patternId });
      const template = lane?.[0];
      if (lane === undefined || template === undefined) return;
      const existing = lane.find((step) => step.position === position);
      store.setQuery(
        api.steps.lane,
        { trackId, patternId },
        existing
          ? lane.map((step) => (step === existing ? { ...step, enabled } : step))
          : [...lane, { ...template, _id: crypto.randomUUID() as Id<"steps">, position, enabled }],
      );
    });
  }, [setStep]);
}

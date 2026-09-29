"use client";

import { memo, useEffect, useState } from "react";
import { useAll } from "jazz-tools/react";
import { Button } from "@astryxdesign/core/Button";
import { HStack } from "@astryxdesign/core/HStack";
import { Slider } from "@astryxdesign/core/Slider";
import { Text } from "@astryxdesign/core/Text";
import { ToggleButton } from "@astryxdesign/core/ToggleButton";
import { app, type Step, type Track } from "@/schema";
import { STEPS_PER_BEAT, TRACK_COLORS, trackColor } from "@/lib/instruments";

export type TrackLaneProps = {
  track: Track;
  patternId: string;
  length: number;
  canEdit: boolean;
  onToggleStep: (step: Step) => void;
  onUpdateTrack: (
    trackId: string,
    change: Partial<Pick<Track, "muted" | "solo" | "volume">>,
  ) => void;
  onOpenSettings: (trackId: string) => void;
  onSteps: (trackId: string, enabled: boolean[]) => void;
};

/**
 * One instrument row: its header and its pads for the current pattern. Each
 * lane owns one ordered, parent-scoped subscription, so an edit to one pad
 * only re-renders the lane it belongs to.
 */
export const TrackLane = memo(function TrackLane({
  track,
  patternId,
  length,
  canEdit,
  onToggleStep,
  onUpdateTrack,
  onOpenSettings,
  onSteps,
}: TrackLaneProps) {
  const { data: steps = [] } = useAll(
    app.steps.where({ track_id: track.id, pattern_id: patternId }).orderBy("position", "asc"),
  );

  useEffect(() => {
    const enabled: boolean[] = [];
    for (const step of steps) enabled[step.position] = step.enabled;
    onSteps(track.id, enabled);
  }, [onSteps, steps, track.id]);

  const colorName = (TRACK_COLORS as readonly string[]).includes(track.color)
    ? track.color
    : trackColor(track.position);
  const byPosition = new Map(steps.map((step) => [step.position, step]));
  // The slider moves locally while dragging and writes once on release.
  const [draftVolume, setDraftVolume] = useState<number | null>(null);

  return (
    <div
      className="track-lane"
      role="group"
      aria-label={track.name}
      style={
        { "--track-color": `var(--color-data-categorical-${colorName})` } as React.CSSProperties
      }
    >
      <div className="track-header">
        <span className="track-swatch" aria-hidden="true" />
        <div className="track-name">
          {canEdit ? (
            <Button
              variant="ghost"
              size="sm"
              label={track.name}
              tooltip={`${track.name} settings`}
              onClick={() => onOpenSettings(track.id)}
            />
          ) : (
            <Text type="label" maxLines={1}>
              {track.name}
            </Text>
          )}
        </div>
        <HStack gap={1} align="center">
          <ToggleButton
            size="sm"
            label={`Mute ${track.name}`}
            tooltip={`Mute ${track.name}`}
            isPressed={track.muted}
            isDisabled={!canEdit}
            onPressedChange={(muted) => onUpdateTrack(track.id, { muted })}
          >
            M
          </ToggleButton>
          <ToggleButton
            size="sm"
            label={`Solo ${track.name}`}
            tooltip={`Solo ${track.name}`}
            isPressed={track.solo}
            isDisabled={!canEdit}
            onPressedChange={(solo) => onUpdateTrack(track.id, { solo })}
          >
            S
          </ToggleButton>
        </HStack>
        <div className="track-volume">
          <Slider
            label={`${track.name} volume`}
            isLabelHidden
            value={draftVolume ?? track.volume}
            min={0}
            max={100}
            isDisabled={!canEdit}
            onChange={(volume: number) => setDraftVolume(volume)}
            onChangeEnd={(volume: number) => {
              setDraftVolume(null);
              onUpdateTrack(track.id, { volume });
            }}
            width="100%"
          />
        </div>
      </div>
      {Array.from({ length }, (_, position) => {
        const step = byPosition.get(position);
        return (
          <button
            key={step?.id ?? `missing-${position}`}
            type="button"
            className="pad"
            aria-label={`${track.name}, step ${position + 1}`}
            aria-pressed={step?.enabled ?? false}
            data-beat-start={position % STEPS_PER_BEAT === 0 ? "" : undefined}
            disabled={!canEdit || !step}
            onClick={() => step && onToggleStep(step)}
          />
        );
      })}
    </div>
  );
});

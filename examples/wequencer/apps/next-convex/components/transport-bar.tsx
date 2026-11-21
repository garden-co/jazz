"use client";

import { useMutation } from "convex/react";
import { Button } from "@astryxdesign/core/Button";
import { HStack } from "@astryxdesign/core/HStack";
import { NumberInput } from "@astryxdesign/core/NumberInput";
import { Text } from "@astryxdesign/core/Text";
import { ToggleButton } from "@astryxdesign/core/ToggleButton";
import { api } from "@/convex/_generated/api";
import type { Id } from "@/lib/model";
import { MAX_TEMPO, MIN_TEMPO } from "@/lib/instruments";
import type { ReportWrite } from "@/lib/report-write";
import {
  retime,
  startPlayback,
  stopPlayback,
  type TransportState,
  type TransportWrite,
} from "@/lib/transport";

/**
 * Play, stop and tempo are shared: each press appends a transport
 * observation, and every bandmate's playhead follows the newest one.
 */
export function TransportBar({
  sessionId,
  transport,
  patternId,
  length,
  playhead,
  canEdit,
  reportWrite,
  isSoundOn,
  onSoundChange,
}: {
  sessionId: Id<"sessions">;
  transport: TransportState;
  patternId: string | undefined;
  length: number;
  playhead: number | null;
  canEdit: boolean;
  reportWrite: ReportWrite;
  isSoundOn: boolean;
  onSoundChange: (on: boolean) => void;
}) {
  const observe = useMutation(api.transport.observe);
  const current = { ...transport, patternId };

  function write(observation: TransportWrite) {
    void reportWrite(
      observe({
        sessionId,
        ...observation,
        observed_at: observation.observed_at.getTime(),
        pattern_id: observation.pattern_id as Id<"patterns"> | undefined,
      }),
      "Transport change",
    );
  }

  function togglePlay() {
    if (transport.playing) {
      write(stopPlayback(current, Date.now()));
    } else {
      // Pressing Play is a gesture, so this bandmate's sound can start too.
      onSoundChange(true);
      write(startPlayback(current, Date.now()));
    }
  }

  return (
    <HStack gap={3} align="center" wrap="wrap">
      <Button
        variant="primary"
        label={transport.playing ? "Stop" : "Play"}
        isDisabled={!canEdit}
        onClick={togglePlay}
      />
      <NumberInput
        label="Tempo"
        isLabelHidden
        units="BPM"
        value={transport.tempo}
        min={MIN_TEMPO}
        max={MAX_TEMPO}
        isIntegerOnly
        hasNumberSteppers
        isReadOnly={!canEdit}
        width={148}
        onChange={(tempo) => {
          if (tempo !== transport.tempo) write(retime(current, Date.now(), { tempo }));
        }}
      />
      <Text hasTabularNumbers data-testid="transport-position" color="secondary">
        {playhead === null ? "Stopped" : `Step ${playhead + 1} of ${length}`}
      </Text>
      <ToggleButton
        label={isSoundOn ? "Sound on" : "Sound off"}
        tooltip={isSoundOn ? "Mute this device" : "Hear the band on this device"}
        isPressed={isSoundOn}
        onPressedChange={onSoundChange}
      >
        {isSoundOn ? "Sound on" : "Sound off"}
      </ToggleButton>
    </HStack>
  );
}

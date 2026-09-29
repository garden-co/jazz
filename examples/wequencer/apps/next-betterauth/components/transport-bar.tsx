"use client";

import { useDb } from "jazz-tools/react";
import { Button } from "@astryxdesign/core/Button";
import { HStack } from "@astryxdesign/core/HStack";
import { NumberInput } from "@astryxdesign/core/NumberInput";
import { Text } from "@astryxdesign/core/Text";
import { ToggleButton } from "@astryxdesign/core/ToggleButton";
import { app } from "@/schema";
import { MAX_TEMPO, MIN_TEMPO } from "@/lib/instruments";
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
  isSoundOn,
  onSoundChange,
}: {
  sessionId: string;
  transport: TransportState;
  patternId: string | undefined;
  length: number;
  playhead: number | null;
  canEdit: boolean;
  isSoundOn: boolean;
  onSoundChange: (on: boolean) => void;
}) {
  const db = useDb();
  const current = { ...transport, patternId };

  function write(observation: TransportWrite) {
    db.insert(app.transport_observations, { session_id: sessionId, ...observation });
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

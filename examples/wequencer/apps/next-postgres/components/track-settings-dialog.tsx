"use client";

import { useState } from "react";
import { useDb } from "@/components/data-provider";
import { Button } from "@astryxdesign/core/Button";
import { Dialog, DialogHeader } from "@astryxdesign/core/Dialog";
import { HStack } from "@astryxdesign/core/HStack";
import { Selector } from "@astryxdesign/core/Selector";
import { TextInput } from "@astryxdesign/core/TextInput";
import { VStack } from "@astryxdesign/core/VStack";
import { mutators } from "@/mutators";
import type { Instrument, Track } from "@/schema";
import { INSTRUMENTS } from "@/lib/instruments";
import { volumeToGain } from "@/lib/audio";
import { confirmed, type ReportWrite } from "@/lib/report-write";
import { removeTrack } from "@/lib/session-setup";

export function TrackSettingsDialog({
  track,
  isCreator,
  onClose,
  onPreview,
  reportWrite,
}: {
  track: Track;
  isCreator: boolean;
  onClose: () => void;
  onPreview: (instrument: Instrument, level: number) => void;
  reportWrite: ReportWrite;
}) {
  const db = useDb();
  const [name, setName] = useState(track.name);

  function update(change: Partial<Pick<Track, "name" | "instrument">>) {
    void reportWrite(
      confirmed(db.mutate(mutators.tracks.update({ id: track.id, ...change }))),
      "Track update",
    );
  }

  async function remove() {
    void reportWrite(confirmed(removeTrack(db, track.id)), "Removing the track");
    onClose();
  }

  function commitName() {
    const trimmed = name.trim();
    if (trimmed && trimmed !== track.name) update({ name: trimmed });
  }

  return (
    <Dialog isOpen onOpenChange={(open) => !open && onClose()} width={420}>
      <DialogHeader title={`${track.name} settings`} onOpenChange={(open) => !open && onClose()} />
      <VStack gap={4} padding={4}>
        <TextInput
          label="Name"
          value={name}
          onChange={setName}
          onBlur={commitName}
          onEnter={commitName}
        />
        <HStack gap={2} align="end">
          <Selector
            label="Instrument"
            options={INSTRUMENTS}
            value={track.instrument}
            onChange={(value) => {
              update({ instrument: value as Instrument });
              onPreview(value as Instrument, volumeToGain(track.volume));
            }}
            width="100%"
          />
          <Button
            label="Preview"
            onClick={() => onPreview(track.instrument, volumeToGain(track.volume))}
          />
        </HStack>
        <HStack gap={2} justify={isCreator ? "between" : "end"}>
          {isCreator ? (
            <Button label="Remove track" variant="destructive" clickAction={remove} />
          ) : null}
          <Button
            label="Done"
            variant="primary"
            onClick={() => {
              commitName();
              onClose();
            }}
          />
        </HStack>
      </VStack>
    </Dialog>
  );
}

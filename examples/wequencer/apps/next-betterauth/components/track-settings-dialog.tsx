"use client";

import { useState } from "react";
import { useDb } from "jazz-tools/react";
import { Button } from "@astryxdesign/core/Button";
import { Dialog, DialogHeader } from "@astryxdesign/core/Dialog";
import { HStack } from "@astryxdesign/core/HStack";
import { Selector } from "@astryxdesign/core/Selector";
import { TextInput } from "@astryxdesign/core/TextInput";
import { VStack } from "@astryxdesign/core/VStack";
import { app, type Instrument, type Track } from "@/schema";
import { INSTRUMENTS } from "@/lib/instruments";
import { volumeToGain } from "@/lib/audio";
import type { ReportWrite } from "@/lib/report-write";
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
      db.update(app.tracks, track.id, change).wait({ tier: "global" }),
      "Track update",
    );
  }

  async function remove() {
    const result = await removeTrack(db, track.id);
    void reportWrite(result.wait({ tier: "global" }), "Removing the track");
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

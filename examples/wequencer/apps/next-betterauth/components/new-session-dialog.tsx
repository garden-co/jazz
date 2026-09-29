"use client";

import { useState, type FormEvent } from "react";
import { useDb } from "jazz-tools/react";
import { Button } from "@astryxdesign/core/Button";
import { Dialog, DialogHeader } from "@astryxdesign/core/Dialog";
import { HStack } from "@astryxdesign/core/HStack";
import { NumberInput } from "@astryxdesign/core/NumberInput";
import { SegmentedControl, SegmentedControlItem } from "@astryxdesign/core/SegmentedControl";
import { Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import { VStack } from "@astryxdesign/core/VStack";
import { MAX_TEMPO, MIN_TEMPO, PATTERN_LENGTHS } from "@/lib/instruments";
import { createSession } from "@/lib/session-setup";

const TRACK_COUNTS = [4, 8, 16] as const;

export function NewSessionDialog({
  author,
  isOpen,
  onOpenChange,
  onCreated,
}: {
  author: string;
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
  onCreated: (sessionId: string) => void;
}) {
  const db = useDb();
  const [title, setTitle] = useState("Late-night rehearsal");
  const [tempo, setTempo] = useState(124);
  const [trackCount, setTrackCount] = useState(8);
  const [length, setLength] = useState(16);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const sessionId = await createSession(db, author, {
      title: title.trim() || "Untitled session",
      tempo,
      trackCount,
      length,
    });
    onOpenChange(false);
    onCreated(sessionId);
  }

  return (
    <Dialog isOpen={isOpen} onOpenChange={onOpenChange} purpose="form" width={440}>
      <DialogHeader title="New session" onOpenChange={onOpenChange} />
      <form onSubmit={(event) => void submit(event)}>
        <VStack gap={4} padding={4}>
          <TextInput label="Title" value={title} onChange={setTitle} isRequired />
          <NumberInput
            label="Tempo"
            units="BPM"
            value={tempo}
            onChange={setTempo}
            min={MIN_TEMPO}
            max={MAX_TEMPO}
            isIntegerOnly
          />
          <VStack gap={1}>
            <Text type="label">Tracks</Text>
            <SegmentedControl
              label="Tracks"
              value={String(trackCount)}
              onChange={(value) => setTrackCount(Number(value))}
              layout="fill"
            >
              {TRACK_COUNTS.map((count) => (
                <SegmentedControlItem key={count} value={String(count)} label={String(count)} />
              ))}
            </SegmentedControl>
          </VStack>
          <VStack gap={1}>
            <Text type="label">Steps per pattern</Text>
            <SegmentedControl
              label="Steps per pattern"
              value={String(length)}
              onChange={(value) => setLength(Number(value))}
              layout="fill"
            >
              {PATTERN_LENGTHS.map((count) => (
                <SegmentedControlItem key={count} value={String(count)} label={String(count)} />
              ))}
            </SegmentedControl>
          </VStack>
          <HStack gap={2} justify="end">
            <Button label="Cancel" variant="ghost" onClick={() => onOpenChange(false)} />
            <Button label="Create session" variant="primary" type="submit" />
          </HStack>
        </VStack>
      </form>
    </Dialog>
  );
}

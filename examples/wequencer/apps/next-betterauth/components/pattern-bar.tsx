"use client";

import { useDb } from "jazz-tools/react";
import { Button } from "@astryxdesign/core/Button";
import { HStack } from "@astryxdesign/core/HStack";
import { SegmentedControl, SegmentedControlItem } from "@astryxdesign/core/SegmentedControl";
import { Tab, TabList } from "@astryxdesign/core/TabList";
import { app, type Pattern, type Track } from "@/schema";
import { PATTERN_LENGTHS } from "@/lib/instruments";
import { addPattern } from "@/lib/session-setup";
import { retime, type TransportState } from "@/lib/transport";

const MAX_PATTERNS = 16;

/**
 * The session's pattern list. Which pattern plays is part of the shared
 * transport, so choosing one switches it for the whole band.
 */
export function PatternBar({
  sessionId,
  patterns,
  current,
  transport,
  tracks,
  canEdit,
}: {
  sessionId: string;
  patterns: Pattern[];
  current: Pattern | undefined;
  transport: TransportState;
  tracks: Track[];
  canEdit: boolean;
}) {
  const db = useDb();

  function select(patternId: string) {
    if (!canEdit || patternId === current?.id) return;
    db.insert(app.transport_observations, {
      session_id: sessionId,
      ...retime({ ...transport, patternId: current?.id }, Date.now(), { patternId }),
    });
  }

  function add() {
    const position = (patterns.at(-1)?.position ?? -1) + 1;
    const patternId = addPattern(
      db,
      sessionId,
      position,
      current?.length ?? 16,
      tracks.map((track) => ({ id: track.id, instrument: track.instrument })),
    );
    select(patternId);
  }

  return (
    <HStack
      gap={3}
      padding={3}
      align="center"
      justify="between"
      wrap="wrap"
      className="pattern-bar"
    >
      <HStack gap={2} align="center" wrap="wrap">
        {current ? (
          <TabList value={current.id} onChange={select} size="sm">
            {patterns.map((pattern) =>
              canEdit || pattern.id === current.id ? (
                <Tab key={pattern.id} value={pattern.id} label={pattern.name} />
              ) : null,
            )}
          </TabList>
        ) : null}
        {canEdit && patterns.length < MAX_PATTERNS ? (
          <Button label="Add pattern" variant="ghost" size="sm" onClick={add} />
        ) : null}
      </HStack>
      {current ? (
        <SegmentedControl
          label="Steps"
          size="sm"
          value={String(current.length)}
          isDisabled={!canEdit}
          onChange={(value) => db.update(app.patterns, current.id, { length: Number(value) })}
        >
          {PATTERN_LENGTHS.map((count) => (
            <SegmentedControlItem key={count} value={String(count)} label={`${count} steps`} />
          ))}
        </SegmentedControl>
      ) : null}
    </HStack>
  );
}

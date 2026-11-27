"use client";

import { useMutation } from "convex/react";
import { Button } from "@astryxdesign/core/Button";
import { HStack } from "@astryxdesign/core/HStack";
import { SegmentedControl, SegmentedControlItem } from "@astryxdesign/core/SegmentedControl";
import { Tab, TabList } from "@astryxdesign/core/TabList";
import { api } from "@/convex/_generated/api";
import type { Id, Pattern } from "@/lib/model";
import type { ReportWrite } from "@/lib/report-write";
import { PATTERN_LENGTHS } from "@/lib/instruments";
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
  canEdit,
  reportWrite,
}: {
  sessionId: Id<"sessions">;
  patterns: Pattern[];
  current: Pattern | undefined;
  transport: TransportState;
  canEdit: boolean;
  reportWrite: ReportWrite;
}) {
  const observe = useMutation(api.transport.observe);
  const addPattern = useMutation(api.patterns.add);
  const setLength = useMutation(api.patterns.setLength);

  function select(patternId: string) {
    if (!canEdit || patternId === current?._id) return;
    const observation = retime({ ...transport, patternId: current?._id }, Date.now(), { patternId });
    void reportWrite(
      observe({
        sessionId,
        ...observation,
        observed_at: observation.observed_at.getTime(),
        pattern_id: observation.pattern_id as Id<"patterns"> | undefined,
      }),
      "Switching pattern",
    );
  }

  async function add() {
    const position = (patterns.at(-1)?.position ?? -1) + 1;
    const pattern = addPattern({ sessionId, position, length: current?.length ?? 16 });
    void reportWrite(pattern, "Adding a pattern");
    select(await pattern);
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
          <TabList value={current._id} onChange={select} size="sm">
            {patterns.map((pattern) =>
              canEdit || pattern._id === current._id ? (
                <Tab key={pattern._id} value={pattern._id} label={pattern.name} />
              ) : null,
            )}
          </TabList>
        ) : null}
        {canEdit && patterns.length < MAX_PATTERNS ? (
          <Button label="Add pattern" variant="ghost" size="sm" onClick={() => void add()} />
        ) : null}
      </HStack>
      {current ? (
        <SegmentedControl
          label="Steps"
          size="sm"
          value={String(current.length)}
          isDisabled={!canEdit}
          onChange={(value) =>
            void reportWrite(
              setLength({ patternId: current._id, length: Number(value) }),
              "Changing the pattern length",
            )
          }
        >
          {PATTERN_LENGTHS.map((count) => (
            <SegmentedControlItem key={count} value={String(count)} label={`${count} steps`} />
          ))}
        </SegmentedControl>
      ) : null}
    </HStack>
  );
}

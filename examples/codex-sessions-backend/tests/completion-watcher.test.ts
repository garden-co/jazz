import { describe, expect, it } from "vitest";
import type { CodexSessionProjection } from "../src/store.js";
import {
  collectRecentCompletionEvents,
  trackEmittedId,
} from "../src/completion-watcher.js";

function buildProjection(turnCompletedAt: Record<string, Date>): CodexSessionProjection {
  const turns = Object.entries(turnCompletedAt).map(([turnId, completedAt], index) => ({
    turnId,
    sequence: index + 1,
    status: "completed",
    completedAt,
    updatedAt: completedAt,
    assistantMessage: `${turnId} finished`,
  }));

  return {
    sessionId: "019d0000-0000-7000-8000-000000000099",
    rolloutPath: "/tmp/rollout.jsonl",
    cwd: "/tmp/demo",
    status: "completed",
    createdAt: new Date("2026-04-08T12:00:00.000Z"),
    updatedAt: turns.at(-1)?.completedAt ?? new Date("2026-04-08T12:00:00.000Z"),
    turns,
  };
}

describe("completion watcher helpers", () => {
  it("skips already-emitted completions even when the timestamp still clears the mtime cutoff", () => {
    const previousMtime = Date.now() - 5_000;
    const projection = buildProjection({
      "turn-duplicate": new Date(previousMtime + 1_000),
      "turn-fresh": new Date(previousMtime + 2_000),
      "turn-stale": new Date(previousMtime - 2_000),
    });
    const emittedIds = new Set(["019d0000-0000-7000-8000-000000000099-turn-duplicate"]);

    const events = collectRecentCompletionEvents({
      projection,
      previousMtime,
      bootstrapCutoff: previousMtime - 10_000,
      emittedIds,
    });

    expect(events.map((event) => event.turnId)).toEqual(["turn-fresh"]);
  });

  it("bounds emitted-id memory while retaining the newest ids", () => {
    const ids = new Set<string>();
    const order: string[] = [];

    for (let index = 0; index < 513; index += 1) {
      trackEmittedId(ids, order, `completion-${index}`);
    }

    expect(order).toHaveLength(320);
    expect(order[0]).toBe("completion-193");
    expect(order.at(-1)).toBe("completion-512");
    expect(ids.has("completion-0")).toBe(false);
    expect(ids.has("completion-512")).toBe(true);
  });
});

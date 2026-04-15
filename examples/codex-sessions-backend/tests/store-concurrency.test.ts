import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { buildSessionProjectionFromRollout, createCodexSessionStore, type CodexSessionStore } from "../src/index.js";

describe("codex session store concurrency", () => {
  let tempDir: string;
  let dataPath: string;
  let rolloutPath: string;
  let store: CodexSessionStore;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "codex-store-concurrency-"));
    dataPath = join(tempDir, "codex-sessions.db");
    rolloutPath = join(
      tempDir,
      "rollout-2026-04-08T12-00-00-019d0000-0000-7000-8000-000000000002.jsonl",
    );
    store = createCodexSessionStore({
      appId: "codex-session-store-concurrency-test",
      dataPath,
    });
  });

  afterEach(async () => {
    await store.shutdown();
    await rm(tempDir, { recursive: true, force: true });
  });

  it("coalesces concurrent projection writes for the same session", async () => {
    const rolloutText = [
      JSON.stringify({
        timestamp: "2026-04-08T12:00:00.000Z",
        type: "session_meta",
        payload: {
          id: "019d0000-0000-7000-8000-000000000002",
          timestamp: "2026-04-08T12:00:00.000Z",
          cwd: "/tmp/demo-concurrency",
          source: "codex",
        },
      }),
      JSON.stringify({
        timestamp: "2026-04-08T12:00:01.000Z",
        type: "event_msg",
        payload: {
          type: "task_started",
          turn_id: "turn-1",
          started_at: 1775649601,
        },
      }),
    ].join("\n");
    await writeFile(rolloutPath, rolloutText);

    const built = buildSessionProjectionFromRollout(rolloutPath, rolloutText);
    expect(built).not.toBeNull();
    const projection = built!.projection;

    await Promise.all(
      Array.from({ length: 5 }, (_, index) =>
        store.replaceSessionProjection(projection, {
          sourceId: rolloutPath,
          absolutePath: rolloutPath,
          sessionId: projection.sessionId,
          lineCount: built!.lineCount,
          syncedAt: new Date(1775649601_000 + index),
        }),
      ),
    );

    const summary = await store.getSessionSummary(projection.sessionId);
    expect(summary).not.toBeNull();
    expect(summary!.turns).toHaveLength(1);
    expect(summary!.turns[0]?.turn_id).toBe("turn-1");
    expect(summary!.presence?.current_turn_id).toBe("turn-1");
  });
});

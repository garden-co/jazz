import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createCodexSessionStore, syncCodexRollouts, type CodexSessionStore } from "../src/index.js";

describe("codex rollout projector ordering", () => {
  let tempDir: string;
  let codexHome: string;
  let dataPath: string;
  let store: CodexSessionStore;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "codex-projector-order-"));
    codexHome = join(tempDir, ".codex");
    dataPath = join(tempDir, "codex-sessions.db");
    store = createCodexSessionStore({
      appId: "codex-projector-order-test",
      dataPath,
    });
    await mkdir(join(codexHome, "sessions/2026/04/08"), { recursive: true });
  });

  afterEach(async () => {
    await store.shutdown();
    await rm(tempDir, { recursive: true, force: true });
  });

  it("syncs the newest rollout first so active sessions surface quickly", async () => {
    const olderPath = join(
      codexHome,
      "sessions/2026/04/08/rollout-2026-04-08T12-00-00-019d0000-0000-7000-8000-000000000001.jsonl",
    );
    const newerPath = join(
      codexHome,
      "sessions/2026/04/08/rollout-2026-04-08T12-10-00-019d0000-0000-7000-8000-000000000002.jsonl",
    );

    await writeFile(
      olderPath,
      [
        JSON.stringify({
          timestamp: "2026-04-08T12:00:00.000Z",
          type: "session_meta",
          payload: {
            id: "019d0000-0000-7000-8000-000000000001",
            timestamp: "2026-04-08T12:00:00.000Z",
            cwd: "/tmp/older",
            source: "codex",
          },
        }),
        JSON.stringify({
          timestamp: "2026-04-08T12:00:01.000Z",
          type: "event_msg",
          payload: {
            type: "task_complete",
            turn_id: "turn-older",
            last_agent_message: "Older session finished.",
            completed_at: 1775649601,
            duration_ms: 1000,
          },
        }),
      ].join("\n"),
    );

    await writeFile(
      newerPath,
      [
        JSON.stringify({
          timestamp: "2026-04-08T12:10:00.000Z",
          type: "session_meta",
          payload: {
            id: "019d0000-0000-7000-8000-000000000002",
            timestamp: "2026-04-08T12:10:00.000Z",
            cwd: "/tmp/newer",
            source: "codex",
          },
        }),
        JSON.stringify({
          timestamp: "2026-04-08T12:10:01.000Z",
          type: "event_msg",
          payload: {
            type: "task_started",
            turn_id: "turn-newer",
            started_at: 1775650201,
          },
        }),
      ].join("\n"),
    );

    const syncedSessionIds: string[] = [];
    await syncCodexRollouts({
      codexHome,
      store,
      onProjectionSynced: ({ projection }) => {
        syncedSessionIds.push(projection.sessionId);
      },
    });

    expect(syncedSessionIds).toEqual([
      "019d0000-0000-7000-8000-000000000002",
      "019d0000-0000-7000-8000-000000000001",
    ]);
  });
});

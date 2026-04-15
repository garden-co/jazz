import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  createCodexSessionStore,
  syncCodexRollouts,
  type CodexSessionStore,
} from "../src/index.js";

describe("codex completion events", () => {
  let tempDir: string;
  let codexHome: string;
  let rolloutPath: string;
  let store: CodexSessionStore;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "codex-sessions-completions-"));
    codexHome = join(tempDir, ".codex");
    const rolloutDir = join(codexHome, "sessions/2026/04/08");
    rolloutPath = join(
      rolloutDir,
      "rollout-2026-04-08T12-00-00-019d0000-0000-7000-8000-000000000002.jsonl",
    );
    await mkdir(rolloutDir, { recursive: true });
    store = createCodexSessionStore({
      appId: "codex-sessions-completions-test",
      dataPath: join(tempDir, "codex-sessions.db"),
    });
  });

  afterEach(async () => {
    await store.shutdown();
    await rm(tempDir, { recursive: true, force: true });
  });

  it("lists completed turns as Lin-ready completion events", async () => {
    await writeFile(
      rolloutPath,
      [
        JSON.stringify({
          timestamp: "2026-04-08T12:00:00.000Z",
          type: "session_meta",
          payload: {
            id: "019d0000-0000-7000-8000-000000000002",
            timestamp: "2026-04-08T12:00:00.000Z",
            cwd: "/tmp/demo-two",
            source: "codex",
          },
        }),
        JSON.stringify({
          timestamp: "2026-04-08T12:00:01.000Z",
          type: "event_msg",
          payload: {
            type: "task_started",
            turn_id: "turn-2",
            started_at: 1775649601,
          },
        }),
        JSON.stringify({
          timestamp: "2026-04-08T12:00:02.000Z",
          type: "event_msg",
          payload: {
            type: "user_message",
            message: "Ship the completion stream",
          },
        }),
        JSON.stringify({
          timestamp: "2026-04-08T12:00:03.000Z",
          type: "event_msg",
          payload: {
            type: "task_complete",
            turn_id: "turn-2",
            last_agent_message: "Completion stream is live.",
            completed_at: 1775649603,
            duration_ms: 2000,
          },
        }),
      ].join("\n"),
    );

    await syncCodexRollouts({ codexHome, store });
    const completions = await store.listCompletionEvents();

    expect(completions).toHaveLength(1);
    expect(completions[0]).toMatchObject({
      id: "019d0000-0000-7000-8000-000000000002-turn-2",
      sessionId: "019d0000-0000-7000-8000-000000000002",
      turnId: "turn-2",
      projectPath: "/tmp/demo-two",
      projectName: "demo-two",
      source: "codex",
      summary: "Completion stream is live.",
      status: "completed",
    });
    expect(completions[0]?.completedAt.toISOString()).toBe("2026-04-08T12:00:03.000Z");
  });

  it("filters completion events by completion timestamp", async () => {
    await writeFile(
      rolloutPath,
      [
        JSON.stringify({
          timestamp: "2026-04-08T12:00:00.000Z",
          type: "session_meta",
          payload: {
            id: "019d0000-0000-7000-8000-000000000002",
            timestamp: "2026-04-08T12:00:00.000Z",
            cwd: "/tmp/demo-two",
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
        JSON.stringify({
          timestamp: "2026-04-08T12:00:02.000Z",
          type: "event_msg",
          payload: {
            type: "task_complete",
            turn_id: "turn-1",
            last_agent_message: "First completion.",
            completed_at: 1775649602,
            duration_ms: 1000,
          },
        }),
        JSON.stringify({
          timestamp: "2026-04-08T12:00:04.000Z",
          type: "event_msg",
          payload: {
            type: "task_started",
            turn_id: "turn-2",
            started_at: 1775649604,
          },
        }),
        JSON.stringify({
          timestamp: "2026-04-08T12:00:05.000Z",
          type: "event_msg",
          payload: {
            type: "task_complete",
            turn_id: "turn-2",
            last_agent_message: "Second completion.",
            completed_at: 1775649605,
            duration_ms: 1000,
          },
        }),
      ].join("\n"),
    );

    await syncCodexRollouts({ codexHome, store });
    const completions = await store.listCompletionEvents({
      completedAfter: "2026-04-08T12:00:03.000Z",
    });

    expect(completions).toHaveLength(1);
    expect(completions[0]?.turnId).toBe("turn-2");
    expect(completions[0]?.summary).toBe("Second completion.");
  });
});

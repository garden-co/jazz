import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  createCodexSessionStore,
  syncCodexSessionRollout,
  syncCodexRollouts,
  syncRecentSessionsForProjectRoot,
  syncSessionsByPrefix,
  type CodexSessionStore,
} from "../src/index.js";

describe("codex session projector", () => {
  let tempDir: string;
  let codexHome: string;
  let rolloutPath: string;
  let store: CodexSessionStore;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "codex-sessions-projector-"));
    codexHome = join(tempDir, ".codex");
    const rolloutDir = join(codexHome, "sessions/2026/04/07");
    rolloutPath = join(
      rolloutDir,
      "rollout-2026-04-07T10-00-00-019d0000-0000-7000-8000-000000000001.jsonl",
    );
    await mkdir(rolloutDir, { recursive: true });
    store = createCodexSessionStore({
      appId: "codex-sessions-projector-test",
      dataPath: join(tempDir, "codex-sessions.db"),
    });
  });

  afterEach(async () => {
    await store.shutdown();
    await rm(tempDir, { recursive: true, force: true });
  });

  it("projects partial and final assistant text from rollout updates", async () => {
    await writeRollout([
      {
        timestamp: "2026-04-07T10:00:00.000Z",
        type: "session_meta",
        payload: {
          id: "019d0000-0000-7000-8000-000000000001",
          timestamp: "2026-04-07T10:00:00.000Z",
          cwd: "/tmp/demo",
          originator: "codex-tui",
          cli_version: "0.0.0",
          source: "cli",
          model_provider: "openai",
          git: {
            branch: "main",
          },
        },
      },
      {
        timestamp: "2026-04-07T10:00:00.500Z",
        type: "turn_context",
        payload: {
          turn_id: "turn-1",
          cwd: "/tmp/demo",
          repo_root: "/tmp/demo",
          model: "gpt-5.4",
          effort: "high",
        },
      },
      {
        timestamp: "2026-04-07T10:00:01.000Z",
        type: "event_msg",
        payload: {
          type: "task_started",
          turn_id: "turn-1",
          started_at: 1775556001,
        },
      },
      {
        timestamp: "2026-04-07T10:00:02.000Z",
        type: "event_msg",
        payload: {
          type: "user_message",
          message: "Mirror this session into Jazz",
        },
      },
      {
        timestamp: "2026-04-07T10:00:03.000Z",
        type: "event_msg",
        payload: {
          type: "agent_message_content_delta",
          turn_id: "turn-1",
          item_id: "item-1",
          delta: "Working",
        },
      },
      {
        timestamp: "2026-04-07T10:00:03.500Z",
        type: "event_msg",
        payload: {
          type: "agent_message_content_delta",
          turn_id: "turn-1",
          item_id: "item-1",
          delta: " on it",
        },
      },
    ]);

    await syncCodexRollouts({ codexHome, store });
    let summary = await store.getSessionSummary("019d0000-0000-7000-8000-000000000001");

    expect(summary?.session.status).toBe("in_progress");
    expect(summary?.session.git_branch).toBe("main");
    expect(summary?.session.model_name).toBe("gpt-5.4");
    expect(summary?.session.reasoning_effort).toBe("high");
    expect(summary?.session.latest_user_message).toBe("Mirror this session into Jazz");
    expect(summary?.session.latest_assistant_partial).toBe("Working on it");
    expect(summary?.turns).toHaveLength(1);
    expect(summary?.turns[0]?.assistant_partial).toBe("Working on it");
    expect(summary?.turns[0]?.assistant_message).toBeUndefined();

    await appendRollout([
      {
        timestamp: "2026-04-07T10:00:04.000Z",
        type: "event_msg",
        payload: {
          type: "agent_message",
          message: "Working on it",
          phase: "commentary",
        },
      },
      {
        timestamp: "2026-04-07T10:00:05.000Z",
        type: "event_msg",
        payload: {
          type: "task_complete",
          turn_id: "turn-1",
          last_agent_message: "Synced into Jazz.",
          completed_at: 1775556005,
          duration_ms: 4000,
        },
      },
    ]);

    await syncCodexRollouts({ codexHome, store });
    summary = await store.getSessionSummary("019d0000-0000-7000-8000-000000000001");

    expect(summary?.session.status).toBe("completed");
    expect(summary?.session.latest_assistant_partial).toBeUndefined();
    expect(summary?.session.latest_assistant_message).toBe("Working on it\n\nSynced into Jazz.");
    expect(summary?.turns[0]?.assistant_message).toBe("Working on it\n\nSynced into Jazz.");
    expect(summary?.turns[0]?.assistant_partial).toBeUndefined();
    expect(summary?.turns[0]?.status).toBe("completed");
    expect(summary?.syncState?.absolute_path).toBe(rolloutPath);
    expect(summary?.syncState?.line_count).toBe(8);
  });

  it("syncs a single session without scanning every rollout body", async () => {
    await writeRollout([
      {
        timestamp: "2026-04-07T10:00:00.000Z",
        type: "session_meta",
        payload: {
          id: "019d0000-0000-7000-8000-000000000001",
          timestamp: "2026-04-07T10:00:00.000Z",
          cwd: "/tmp/demo",
          originator: "codex-tui",
          cli_version: "0.0.0",
          source: "cli",
        },
      },
      {
        timestamp: "2026-04-07T10:00:00.500Z",
        type: "turn_context",
        payload: {
          turn_id: "turn-1",
          cwd: "/tmp/demo",
        },
      },
      {
        timestamp: "2026-04-07T10:00:02.000Z",
        type: "event_msg",
        payload: {
          type: "user_message",
          message: "Mirror only one session into Jazz",
        },
      },
    ]);

    const result = await syncCodexSessionRollout({
      codexHome,
      store,
      sessionId: "019d0000-0000-7000-8000-000000000001",
    });
    const summary = await store.getSessionSummary("019d0000-0000-7000-8000-000000000001");

    expect(result.found).toBe(true);
    expect(result.synced).toBe(1);
    expect(summary?.session.latest_user_message).toBe("Mirror only one session into Jazz");
    expect(summary?.syncState?.absolute_path).toBe(rolloutPath);
  });

  it("syncs only the newest sessions for a requested project during a cold lookup", async () => {
    const targetProjectRoot = "/tmp/target-project";
    const otherProjectRoot = "/tmp/other-project";

    await writeRolloutAt(
      "2026/04/05",
      "rollout-2026-04-05T09-00-00-019d0000-0000-7000-8000-000000000101.jsonl",
      createRolloutLines({
        sessionId: "019d0000-0000-7000-8000-000000000101",
        cwd: targetProjectRoot,
        message: "old target session",
        timestamp: "2026-04-05T09:00:00.000Z",
      }),
    );
    await writeRolloutAt(
      "2026/04/06",
      "rollout-2026-04-06T09-00-00-019d0000-0000-7000-8000-000000000102.jsonl",
      createRolloutLines({
        sessionId: "019d0000-0000-7000-8000-000000000102",
        cwd: otherProjectRoot,
        message: "other session newer than the old target one",
        timestamp: "2026-04-06T09:00:00.000Z",
      }),
    );
    await writeRolloutAt(
      "2026/04/07",
      "rollout-2026-04-07T09-00-00-019d0000-0000-7000-8000-000000000103.jsonl",
      createRolloutLines({
        sessionId: "019d0000-0000-7000-8000-000000000103",
        cwd: targetProjectRoot,
        message: "newest target session",
        timestamp: "2026-04-07T09:00:00.000Z",
      }),
    );
    await writeRolloutAt(
      "2026/04/08",
      "rollout-2026-04-08T09-00-00-019d0000-0000-7000-8000-000000000104.jsonl",
      createRolloutLines({
        sessionId: "019d0000-0000-7000-8000-000000000104",
        cwd: otherProjectRoot,
        message: "newest unrelated session",
        timestamp: "2026-04-08T09:00:00.000Z",
      }),
    );

    const result = await syncRecentSessionsForProjectRoot({
      codexHome,
      store,
      projectRoot: targetProjectRoot,
      limit: 2,
    });

    const targetSessions = await store.listSessionsForProjectRoot(targetProjectRoot, 10);
    const otherSessions = await store.listSessionsForProjectRoot(otherProjectRoot, 10);

    expect(result).toEqual({ scanned: 4, matched: 2, synced: 2 });
    expect(targetSessions.map((session) => session.session_id)).toEqual([
      "019d0000-0000-7000-8000-000000000103",
      "019d0000-0000-7000-8000-000000000101",
    ]);
    expect(otherSessions).toEqual([]);
  });

  it("syncs only prefix-matching sessions during a cold id lookup", async () => {
    await writeRolloutAt(
      "2026/04/05",
      "rollout-2026-04-05T09-00-00-019d0000-0000-7000-8000-000000000111.jsonl",
      createRolloutLines({
        sessionId: "019d0000-0000-7000-8000-000000000111",
        cwd: "/tmp/demo",
        message: "prefix match older",
        timestamp: "2026-04-05T09:00:00.000Z",
      }),
    );
    await writeRolloutAt(
      "2026/04/06",
      "rollout-2026-04-06T09-00-00-019d1111-0000-7000-8000-000000000222.jsonl",
      createRolloutLines({
        sessionId: "019d1111-0000-7000-8000-000000000222",
        cwd: "/tmp/demo",
        message: "non matching session",
        timestamp: "2026-04-06T09:00:00.000Z",
      }),
    );
    await writeRolloutAt(
      "2026/04/07",
      "rollout-2026-04-07T09-00-00-019d0000-0000-7000-8000-000000000333.jsonl",
      createRolloutLines({
        sessionId: "019d0000-0000-7000-8000-000000000333",
        cwd: "/tmp/demo",
        message: "prefix match newer",
        timestamp: "2026-04-07T09:00:00.000Z",
      }),
    );

    const result = await syncSessionsByPrefix({
      codexHome,
      store,
      prefix: "019d0000",
      limit: 2,
    });

    const sessions = await store.listSessions(10);
    expect(result).toEqual({ scanned: 3, matched: 2, synced: 2 });
    expect(sessions.map((session) => session.session_id)).toEqual([
      "019d0000-0000-7000-8000-000000000333",
      "019d0000-0000-7000-8000-000000000111",
    ]);
  });

  it("drops rolled-back turn summaries from session previews", async () => {
    await writeRollout([
      {
        timestamp: "2026-04-07T10:00:00.000Z",
        type: "session_meta",
        payload: {
          id: "019d0000-0000-7000-8000-000000000001",
          timestamp: "2026-04-07T10:00:00.000Z",
          cwd: "/tmp/demo",
          source: "cli",
        },
      },
      {
        timestamp: "2026-04-07T10:00:01.000Z",
        type: "event_msg",
        payload: {
          type: "task_started",
          turn_id: "turn-1",
          started_at: 1775556001,
        },
      },
      {
        timestamp: "2026-04-07T10:00:02.000Z",
        type: "event_msg",
        payload: {
          type: "user_message",
          message: "First prompt",
        },
      },
      {
        timestamp: "2026-04-07T10:00:03.000Z",
        type: "event_msg",
        payload: {
          type: "task_complete",
          turn_id: "turn-1",
          last_agent_message: "First answer",
          completed_at: 1775556003,
        },
      },
      {
        timestamp: "2026-04-07T10:00:04.000Z",
        type: "turn_context",
        payload: {
          turn_id: "turn-2",
          cwd: "/tmp/demo",
        },
      },
      {
        timestamp: "2026-04-07T10:00:05.000Z",
        type: "event_msg",
        payload: {
          type: "task_started",
          turn_id: "turn-2",
          started_at: 1775556005,
        },
      },
      {
        timestamp: "2026-04-07T10:00:06.000Z",
        type: "event_msg",
        payload: {
          type: "user_message",
          message: "Rolled back prompt",
        },
      },
      {
        timestamp: "2026-04-07T10:00:07.000Z",
        type: "event_msg",
        payload: {
          type: "thread_rolled_back",
          num_turns: 1,
        },
      },
    ]);

    await syncCodexRollouts({ codexHome, store });
    const summary = await store.getSessionSummary("019d0000-0000-7000-8000-000000000001");

    expect(summary?.turns).toHaveLength(1);
    expect(summary?.session.first_user_message).toBe("First prompt");
    expect(summary?.session.latest_user_message).toBe("First prompt");
    expect(summary?.session.latest_preview).toBe("First answer");
    expect(summary?.session.last_user_at?.toISOString()).toBe("2026-04-07T10:00:02.000Z");
  });

  async function writeRollout(lines: unknown[]): Promise<void> {
    await writeFile(rolloutPath, lines.map((line) => JSON.stringify(line)).join("\n"));
  }

  async function writeRolloutAt(
    dayPath: string,
    fileName: string,
    lines: unknown[],
  ): Promise<void> {
    const nestedDir = join(codexHome, "sessions", dayPath);
    await mkdir(nestedDir, { recursive: true });
    await writeFile(join(nestedDir, fileName), lines.map((line) => JSON.stringify(line)).join("\n"));
  }

  async function appendRollout(lines: unknown[]): Promise<void> {
    const existing = await readFile(rolloutPath, "utf8");
    const suffix = lines.map((line) => JSON.stringify(line)).join("\n");
    await writeFile(rolloutPath, `${existing}\n${suffix}`);
  }

  function createRolloutLines(options: {
    sessionId: string;
    cwd: string;
    message: string;
    timestamp: string;
  }): unknown[] {
    return [
      {
        timestamp: options.timestamp,
        type: "session_meta",
        payload: {
          id: options.sessionId,
          timestamp: options.timestamp,
          cwd: options.cwd,
          originator: "codex-tui",
          cli_version: "0.0.0",
          source: "cli",
        },
      },
      {
        timestamp: options.timestamp,
        type: "turn_context",
        payload: {
          turn_id: `${options.sessionId}-turn-1`,
          cwd: options.cwd,
        },
      },
      {
        timestamp: options.timestamp,
        type: "event_msg",
        payload: {
          type: "user_message",
          message: options.message,
        },
      },
    ];
  }
});

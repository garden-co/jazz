import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createJazzContext } from "jazz-tools/backend";
import { app } from "../schema/app.js";
import { createCodexSessionStore, type CodexSessionStore } from "../src/index.js";

describe("CodexSessionStore", () => {
  let tempDir: string;
  let store: CodexSessionStore;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "codex-session-store-"));
    store = createCodexSessionStore({
      appId: "codex-session-store-test",
      dataPath: join(tempDir, "codex-sessions.db"),
    });
  });

  afterEach(async () => {
    await store.shutdown();
    await rm(tempDir, { recursive: true, force: true });
  });

  it("upserts turns by turn id and keeps stable session summary fields", async () => {
    await store.replaceSessionProjection(
      {
        sessionId: "session-1",
        rolloutPath: "/tmp/session-1.jsonl",
        cwd: "/Users/nikitavoloboev/code/demo",
        status: "in_progress",
        createdAt: "2026-04-08T12:00:00.000Z",
        updatedAt: "2026-04-08T12:00:10.000Z",
        latestUserMessage: "Scan this repo",
        latestAssistantPartial: "Working on it",
        turns: [
          {
            turnId: "turn-1",
            sequence: 1,
            status: "in_progress",
            userMessage: "Scan this repo",
            assistantPartial: "Working on it",
            updatedAt: "2026-04-08T12:00:10.000Z",
          },
        ],
      },
      {
        sourceId: "session-1",
        absolutePath: "/tmp/session-1.jsonl",
        lineCount: 4,
        syncedAt: "2026-04-08T12:00:10.000Z",
      },
    );

    const initialSummary = await store.getSessionSummary("session-1");
    const firstTurnId = initialSummary?.turns[0]?.id;

    await store.replaceSessionProjection(
      {
        sessionId: "session-1",
        rolloutPath: "/tmp/session-1.jsonl",
        cwd: "/Users/nikitavoloboev/code/demo",
        status: "completed",
        createdAt: "2026-04-08T12:00:00.000Z",
        updatedAt: "2026-04-08T12:00:20.000Z",
        latestUserMessage: "Scan this repo",
        latestAssistantMessage: "Finished scan",
        turns: [
          {
            turnId: "turn-1",
            sequence: 1,
            status: "completed",
            userMessage: "Scan this repo",
            assistantMessage: "Finished scan",
            completedAt: "2026-04-08T12:00:20.000Z",
            updatedAt: "2026-04-08T12:00:20.000Z",
          },
        ],
      },
      {
        sourceId: "session-1",
        absolutePath: "/tmp/session-1.jsonl",
        lineCount: 8,
        syncedAt: "2026-04-08T12:00:20.000Z",
      },
    );

    const updatedSummary = await store.getSessionSummary("session-1");

    expect(updatedSummary?.session.project_root).toBe("/Users/nikitavoloboev/code/demo");
    expect(updatedSummary?.session.latest_preview).toBe("Finished scan");
    expect(updatedSummary?.session.latest_activity_at.toISOString()).toBe(
      "2026-04-08T12:00:20.000Z",
    );
    expect(updatedSummary?.session.last_completion_at?.toISOString()).toBe(
      "2026-04-08T12:00:20.000Z",
    );
    expect(updatedSummary?.turns).toHaveLength(1);
    expect(updatedSummary?.turns[0]?.id).toBe(firstTurnId);
    expect(updatedSummary?.turns[0]?.assistant_partial).toBeUndefined();
    expect(updatedSummary?.turns[0]?.assistant_message).toBe("Finished scan");
    expect(initialSummary?.presence?.state).toBe("streaming");
    expect(initialSummary?.presence?.current_turn_id).toBe("turn-1");
    expect(updatedSummary?.presence?.state).toBe("completed");
    expect(updatedSummary?.presence?.current_turn_id).toBeUndefined();
    expect(updatedSummary?.presence?.last_completion_at?.toISOString()).toBe(
      "2026-04-08T12:00:20.000Z",
    );
  });

  it("records terminal activity into session presence without rollout projection", async () => {
    const summary = await store.recordTerminalPresence({
      terminalSessionId: "designer-v2-wterm",
      sessionId: "session-terminal",
      turnId: "turn-terminal",
      cwd: "/Users/nikitavoloboev/repos/openai/codex",
      projectRoot: "/Users/nikitavoloboev/repos/openai/codex",
      repoRoot: "/Users/nikitavoloboev/repos/openai/codex",
      state: "running",
      active: true,
      activityPath: "/tmp/designer-v2-wterm.json",
      updatedAtMs: Date.parse("2026-04-08T12:05:10.000Z"),
      startedAtMs: Date.parse("2026-04-08T12:05:00.000Z"),
      pid: 1234,
      runtimeHost: "designer-dev",
    });

    expect(summary.session.session_id).toBe("session-terminal");
    expect(summary.session.status).toBe("in_progress");
    expect(summary.turns).toHaveLength(1);
    expect(summary.turns[0]?.turn_id).toBe("turn-terminal");
    expect(summary.turns[0]?.status).toBe("in_progress");
    expect(summary.presence?.state).toBe("running");
    expect(summary.presence?.current_turn_id).toBe("turn-terminal");
    expect(summary.presence?.runtime_pid).toBe(1234);
    expect(summary.presence?.runtime_host).toBe("designer-dev");
    expect(summary.syncState?.absolute_path).toBe("/tmp/designer-v2-wterm.json");

    const active = await store.listActiveSessionSummaries({
      projectRoot: "/Users/nikitavoloboev/repos/openai/codex",
      limit: 10,
    });
    expect(active.map((entry) => entry.presence.session_id)).toContain("session-terminal");
  });

  it("records stream events as durable idempotent session deltas", async () => {
    const event = await store.recordCodexStreamEvent({
      sessionId: "session-stream",
      turnId: "turn-stream",
      sequence: 1,
      eventKind: "assistant_delta",
      eventType: "message_delta",
      sourceHost: "op1",
      sourcePath: "/root/.codex/sessions/session-stream.jsonl",
      textDelta: "hel",
      payloadJson: { delta: "hel" },
      schemaHash: "schema-hash-1",
      createdAt: "2026-05-02T12:00:00.000Z",
      observedAt: "2026-05-02T12:00:00.010Z",
    });

    const updated = await store.recordCodexStreamEvent({
      eventId: event.event_id,
      sessionId: "session-stream",
      turnId: "turn-stream",
      sequence: 1,
      eventKind: "assistant_delta",
      eventType: "message_delta",
      sourceHost: "op1",
      sourcePath: "/root/.codex/sessions/session-stream.jsonl",
      textDelta: "hello",
      payloadJson: { delta: "hello" },
      schemaHash: "schema-hash-1",
      createdAt: "2026-05-02T12:00:00.000Z",
      observedAt: "2026-05-02T12:00:00.020Z",
    });
    await store.recordCodexStreamEvent({
      sessionId: "session-stream",
      turnId: "turn-other",
      sequence: 2,
      eventKind: "reasoning_delta",
      eventType: "reasoning_delta",
      sourceHost: "op1",
      textDelta: "next",
      schemaHash: "schema-hash-1",
      createdAt: "2026-05-02T12:00:01.000Z",
      observedAt: "2026-05-02T12:00:01.010Z",
    });

    const events = await store.listCodexStreamEvents({
      sessionId: "session-stream",
      turnId: "turn-stream",
      afterSequence: 0,
      limit: 10,
    });
    const laterEvents = await store.listCodexStreamEvents({
      sessionId: "session-stream",
      afterSequence: 1,
      limit: 10,
    });

    expect(updated.id).toBe(event.id);
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      event_id: event.event_id,
      session_id: "session-stream",
      turn_id: "turn-stream",
      sequence: 1,
      event_kind: "assistant_delta",
      event_type: "message_delta",
      source_host: "op1",
      text_delta: "hello",
      schema_hash: "schema-hash-1",
    });
    expect(events[0]?.observed_at.toISOString()).toBe("2026-05-02T12:00:00.020Z");
    expect(laterEvents).toHaveLength(1);
    expect(laterEvents[0]).toMatchObject({
      session_id: "session-stream",
      turn_id: "turn-other",
      sequence: 2,
      text_delta: "next",
    });
  });

  it("records project context entries for prompt-time cache materialization", async () => {
    const first = await store.recordProjectContext({
      projectRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
      provider: "codex",
      sessionId: "019df3bb",
      turnId: "turn-1",
      summary: "Initial Designer context handoff",
      body: "The work is about project-scoped context sync.",
      updatedAt: "2026-05-04T12:00:00.000Z",
    });
    await store.recordProjectContext({
      projectRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
      provider: "claude",
      sessionId: "claude-1",
      turnId: "turn-2",
      summary: "Claude reviewer context",
      updatedAt: "2026-05-04T12:02:00.000Z",
    });
    await store.recordProjectContext({
      projectRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
      provider: "codex",
      sessionId: "019df3bb",
      turnId: "turn-1",
      summary: "Updated Designer context handoff",
      body: "End-of-turn writes are durable; prompt-time reads use a cache.",
      updatedAt: "2026-05-04T12:03:00.000Z",
    });
    await store.recordProjectContext({
      projectRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
      provider: "cursor",
      sessionId: "cursor-1",
      summary: "Archived context",
      status: "archived",
      updatedAt: "2026-05-04T12:04:00.000Z",
    });
    await store.recordProjectContext({
      projectRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
      provider: "codex",
      sessionId: "expired",
      summary: "Expired context",
      updatedAt: "2026-05-04T12:05:00.000Z",
      expiresAt: "2026-05-04T12:06:00.000Z",
    });
    await store.recordProjectContext({
      projectRoot: "/Users/nikitavoloboev/repos/openai/codex",
      provider: "codex",
      sessionId: "other-project",
      summary: "Other project context",
      updatedAt: "2026-05-04T12:06:00.000Z",
    });

    const entries = await store.listProjectContextEntries({
      projectRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
      limit: 10,
    });
    const replacement = entries.find((entry) => entry.provider === "codex");

    expect(entries.map((entry) => entry.summary)).toEqual([
      "Updated Designer context handoff",
      "Claude reviewer context",
    ]);
    expect(replacement?.id).toBe(first.id);
    expect(replacement?.context_id).toBe(first.context_id);
    expect(replacement?.body).toBe(
      "End-of-turn writes are durable; prompt-time reads use a cache.",
    );

    const allEntries = await store.listProjectContextEntries({
      projectRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
      includeInactive: true,
      includeExpired: true,
      limit: 10,
    });
    expect(allEntries.map((entry) => entry.summary)).toEqual([
      "Expired context",
      "Archived context",
      "Updated Designer context handoff",
      "Claude reviewer context",
    ]);
  });

  it("uses deterministic Jazz row ids for project context entries", async () => {
    const first = await store.recordProjectContext({
      contextId: "designer:codex:turn-1",
      projectRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
      provider: "codex",
      sessionId: "019df3bb",
      turnId: "turn-1",
      summary: "Initial Designer handoff",
      updatedAt: "2026-05-04T12:00:00.000Z",
    });
    const second = await store.recordProjectContext({
      contextId: "designer:codex:turn-1",
      projectRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
      provider: "codex",
      sessionId: "019df4ed",
      turnId: "turn-review",
      summary: "Reviewed Designer handoff",
      updatedAt: "2026-05-04T12:05:00.000Z",
    });

    const entries = await store.listProjectContextEntries({
      projectRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
      limit: 10,
    });

    expect(first.id).toBe("91b36ae1-48d8-5eb4-b779-155475370aa4");
    expect(second.id).toBe(first.id);
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({
      id: first.id,
      context_id: "designer:codex:turn-1",
      session_id: "019df4ed",
      summary: "Reviewed Designer handoff",
    });
  });

  it("creates a native Jazz run binding for every projected codex session", async () => {
    await store.replaceSessionProjection(
      {
        sessionId: "session-native",
        rolloutPath: "/tmp/session-native.jsonl",
        cwd: "/Users/nikitavoloboev/repos/openai/codex",
        projectRoot: "/Users/nikitavoloboev/repos/openai/codex",
        repoRoot: "/Users/nikitavoloboev/repos/openai/codex",
        gitBranch: "j",
        modelName: "gpt-5.4",
        reasoningEffort: "xhigh",
        status: "completed",
        createdAt: "2026-04-08T12:15:00.000Z",
        updatedAt: "2026-04-08T12:16:00.000Z",
        latestActivityAt: "2026-04-08T12:16:00.000Z",
        latestUserMessage: "inspect native codex storage",
        latestAssistantMessage: "recorded native session metadata",
        turns: [
          {
            turnId: "turn-native",
            sequence: 1,
            status: "completed",
            userMessage: "inspect native codex storage",
            assistantMessage: "recorded native session metadata",
            completedAt: "2026-04-08T12:16:00.000Z",
            updatedAt: "2026-04-08T12:16:00.000Z",
          },
        ],
      },
      {
        sourceId: "session-native",
        absolutePath: "/tmp/session-native.jsonl",
        lineCount: 8,
        syncedAt: "2026-04-08T12:16:00.000Z",
      },
    );

    const runs = await store.listJAgentRunsForSession("session-native", { limit: 10 });
    expect(runs).toHaveLength(1);
    expect(runs[0]?.run_id).toBe("native-session:session-native");
    expect(runs[0]?.definition_id).toBe("native:codex-session");
    expect(runs[0]?.trigger_source).toBe("native-codex-session");
    expect(runs[0]?.initiator_session_id).toBe("session-native");
    expect(runs[0]?.requested_model).toBe("gpt-5.4");
    expect(runs[0]?.requested_reasoning_effort).toBe("xhigh");
    expect(runs[0]?.status).toBe("completed");
  });

  it("restores a missing native Jazz run binding when session-bound runs are queried", async () => {
    const dataPath = join(tempDir, "codex-sessions.db");

    await store.replaceSessionProjection(
      {
        sessionId: "session-native-restore",
        rolloutPath: "/tmp/session-native-restore.jsonl",
        cwd: "/Users/nikitavoloboev/repos/openai/codex",
        projectRoot: "/Users/nikitavoloboev/repos/openai/codex",
        repoRoot: "/Users/nikitavoloboev/repos/openai/codex",
        gitBranch: "j",
        modelName: "gpt-5.4",
        reasoningEffort: "high",
        status: "completed",
        createdAt: "2026-04-08T12:17:00.000Z",
        updatedAt: "2026-04-08T12:18:00.000Z",
        latestActivityAt: "2026-04-08T12:18:00.000Z",
        latestUserMessage: "restore native session binding",
        latestAssistantMessage: "binding restored",
        turns: [
          {
            turnId: "turn-native-restore",
            sequence: 1,
            status: "completed",
            userMessage: "restore native session binding",
            assistantMessage: "binding restored",
            completedAt: "2026-04-08T12:18:00.000Z",
            updatedAt: "2026-04-08T12:18:00.000Z",
          },
        ],
      },
      {
        sourceId: "session-native-restore",
        absolutePath: "/tmp/session-native-restore.jsonl",
        lineCount: 8,
        syncedAt: "2026-04-08T12:18:00.000Z",
      },
    );

    await store.shutdown();

    const adminContext = createJazzContext({
      appId: "codex-session-store-test",
      app,
      permissions: {},
      driver: { type: "persistent", dataPath },
      env: "dev",
      userBranch: "main",
      tier: "edge",
    });

    try {
      const db = adminContext.db(app);
      const run = await db.one(
        app.j_agent_runs.where({ run_id: "native-session:session-native-restore" }),
      );
      const binding = await db.one(
        app.j_agent_session_bindings.where({
          binding_id: "native-session:session-native-restore:primary-session",
        }),
      );
      expect(run).not.toBeNull();
      expect(binding).not.toBeNull();
      await db.delete(app.j_agent_session_bindings, binding!.id).wait({ tier: "edge" });
      await db.delete(app.j_agent_runs, run!.id).wait({ tier: "edge" });
    } finally {
      await adminContext.shutdown();
    }

    store = createCodexSessionStore({
      appId: "codex-session-store-test",
      dataPath,
    });

    const restoredRuns = await store.listJAgentRunsForSession("session-native-restore", {
      limit: 10,
    });
    expect(restoredRuns).toHaveLength(1);
    expect(restoredRuns[0]?.run_id).toBe("native-session:session-native-restore");
    expect(restoredRuns[0]?.trigger_source).toBe("native-codex-session");
  });

  it("lists active session summaries from presence rows ordered by the latest observed event", async () => {
    await store.replaceSessionProjection(
      {
        sessionId: "session-recent",
        rolloutPath: "/tmp/session-recent.jsonl",
        cwd: "/Users/nikitavoloboev/code/demo",
        projectRoot: "/Users/nikitavoloboev/code/demo",
        status: "in_progress",
        createdAt: "2026-04-08T12:00:00.000Z",
        updatedAt: "2026-04-08T12:10:00.000Z",
        latestAssistantPartial: "Still streaming",
        turns: [
          {
            turnId: "turn-recent",
            sequence: 1,
            status: "in_progress",
            assistantPartial: "Still streaming",
            updatedAt: "2026-04-08T12:10:00.000Z",
          },
        ],
      },
      {
        sourceId: "session-recent",
        absolutePath: "/tmp/session-recent.jsonl",
        lineCount: 8,
        syncedAt: "2026-04-08T12:10:00.000Z",
      },
    );

    await store.replaceSessionProjection(
      {
        sessionId: "session-older",
        rolloutPath: "/tmp/session-older.jsonl",
        cwd: "/Users/nikitavoloboev/code/demo",
        projectRoot: "/Users/nikitavoloboev/code/demo",
        status: "pending",
        createdAt: "2026-04-08T12:00:00.000Z",
        updatedAt: "2026-04-08T12:06:00.000Z",
        latestUserMessage: "Queue more work",
        turns: [
          {
            turnId: "turn-older",
            sequence: 1,
            status: "pending",
            userMessage: "Queue more work",
            updatedAt: "2026-04-08T12:06:00.000Z",
          },
        ],
      },
      {
        sourceId: "session-older",
        absolutePath: "/tmp/session-older.jsonl",
        lineCount: 6,
        syncedAt: "2026-04-08T12:06:00.000Z",
      },
    );

    await store.replaceSessionProjection(
      {
        sessionId: "session-done",
        rolloutPath: "/tmp/session-done.jsonl",
        cwd: "/Users/nikitavoloboev/code/demo",
        projectRoot: "/Users/nikitavoloboev/code/demo",
        status: "completed",
        createdAt: "2026-04-08T12:00:00.000Z",
        updatedAt: "2026-04-08T12:20:00.000Z",
        latestAssistantMessage: "All finished",
        turns: [
          {
            turnId: "turn-done",
            sequence: 1,
            status: "completed",
            assistantMessage: "All finished",
            completedAt: "2026-04-08T12:20:00.000Z",
            updatedAt: "2026-04-08T12:20:00.000Z",
          },
        ],
      },
      {
        sourceId: "session-done",
        absolutePath: "/tmp/session-done.jsonl",
        lineCount: 9,
        syncedAt: "2026-04-08T12:20:00.000Z",
      },
    );

    const activeSummaries = await store.listActiveSessionSummaries({
      projectRoot: "/Users/nikitavoloboev/code/demo",
    });

    expect(activeSummaries.map((summary) => summary.session.session_id)).toEqual([
      "session-recent",
      "session-older",
    ]);
    expect(activeSummaries.map((summary) => summary.presence.state)).toEqual([
      "streaming",
      "starting",
    ]);
    expect(activeSummaries[0]?.currentTurn?.turn_id).toBe("turn-recent");
  });

  it("backfills missing presence rows directly from stored Jazz session data", async () => {
    const dataPath = join(tempDir, "codex-sessions.db");

    await store.replaceSessionProjection(
      {
        sessionId: "session-upgrade",
        rolloutPath: "/tmp/session-upgrade.jsonl",
        cwd: "/Users/nikitavoloboev/code/demo",
        projectRoot: "/Users/nikitavoloboev/code/demo",
        status: "in_progress",
        createdAt: "2026-04-08T12:00:00.000Z",
        updatedAt: "2026-04-08T12:03:00.000Z",
        latestAssistantPartial: "Finishing the backfill",
        turns: [
          {
            turnId: "turn-upgrade",
            sequence: 1,
            status: "in_progress",
            assistantPartial: "Finishing the backfill",
            updatedAt: "2026-04-08T12:03:00.000Z",
          },
        ],
      },
      {
        sourceId: "session-upgrade",
        absolutePath: "/tmp/session-upgrade.jsonl",
        lineCount: 7,
        syncedAt: "2026-04-08T12:03:00.000Z",
      },
    );

    await store.shutdown();

    const adminContext = createJazzContext({
      appId: "codex-session-store-test",
      app,
      permissions: {},
      driver: { type: "persistent", dataPath },
      env: "dev",
      userBranch: "main",
      tier: "edge",
    });

    try {
      const db = adminContext.db(app);
      const presence = await db.one(
        app.codex_session_presence.where({ session_id: "session-upgrade" }),
      );
      expect(presence).not.toBeNull();
      await db.delete(app.codex_session_presence, presence!.id).wait({ tier: "edge" });
    } finally {
      await adminContext.shutdown();
    }

    store = createCodexSessionStore({
      appId: "codex-session-store-test",
      dataPath,
    });

    expect(
      await store.listActiveSessionSummaries({ projectRoot: "/Users/nikitavoloboev/code/demo" }),
    ).toEqual([]);

    const result = await store.backfillSessionPresence({
      projectRoot: "/Users/nikitavoloboev/code/demo",
    });
    const activeSummaries = await store.listActiveSessionSummaries({
      projectRoot: "/Users/nikitavoloboev/code/demo",
    });

    expect(result).toEqual({ scanned: 1, synced: 1 });
    expect(activeSummaries).toHaveLength(1);
    expect(activeSummaries[0]?.session.session_id).toBe("session-upgrade");
    expect(activeSummaries[0]?.presence.state).toBe("streaming");
    expect(activeSummaries[0]?.currentTurn?.turn_id).toBe("turn-upgrade");
  });

  it("records j-agent runs that bind back to projected Codex sessions", async () => {
    await store.replaceSessionProjection(
      {
        sessionId: "session-parent",
        rolloutPath: "/tmp/session-parent.jsonl",
        cwd: "/Users/nikitavoloboev/code/demo",
        status: "completed",
        createdAt: "2026-04-08T12:30:00.000Z",
        updatedAt: "2026-04-08T12:30:10.000Z",
        latestUserMessage: "Create a repo capsule",
        latestAssistantMessage: "Starting worker session",
        turns: [
          {
            turnId: "turn-parent",
            sequence: 1,
            status: "completed",
            userMessage: "Create a repo capsule",
            assistantMessage: "Starting worker session",
            completedAt: "2026-04-08T12:30:10.000Z",
            updatedAt: "2026-04-08T12:30:10.000Z",
          },
        ],
      },
      {
        sourceId: "session-parent",
        absolutePath: "/tmp/session-parent.jsonl",
        lineCount: 5,
        syncedAt: "2026-04-08T12:30:10.000Z",
      },
    );

    await store.replaceSessionProjection(
      {
        sessionId: "session-worker",
        rolloutPath: "/tmp/session-worker.jsonl",
        cwd: "/Users/nikitavoloboev/code/demo",
        status: "completed",
        createdAt: "2026-04-08T12:31:00.000Z",
        updatedAt: "2026-04-08T12:31:30.000Z",
        latestUserMessage: "Scan the repository structure",
        latestAssistantMessage: "Repo capsule written",
        turns: [
          {
            turnId: "turn-worker",
            sequence: 1,
            status: "completed",
            userMessage: "Scan the repository structure",
            assistantMessage: "Repo capsule written",
            completedAt: "2026-04-08T12:31:30.000Z",
            updatedAt: "2026-04-08T12:31:30.000Z",
          },
        ],
      },
      {
        sourceId: "session-worker",
        absolutePath: "/tmp/session-worker.jsonl",
        lineCount: 6,
        syncedAt: "2026-04-08T12:31:30.000Z",
      },
    );

    await store.upsertJAgentDefinition({
      definitionId: "repo-capsule",
      name: "repo-capsule",
      version: "v1",
      sourceKind: "barnum_ts",
      entrypoint: "barnum/workflows/repo-capsule.ts",
      metadataJson: { owner: "j" },
    });
    await store.recordJAgentRunStarted({
      runId: "run-1",
      definitionId: "repo-capsule",
      status: "running",
      projectRoot: "/Users/nikitavoloboev/code/demo",
      repoRoot: "/Users/nikitavoloboev/code/demo",
      cwd: "/Users/nikitavoloboev/code/demo",
      triggerSource: "j-inline-agent",
      parentSessionId: "session-parent",
      parentTurnId: "turn-parent",
      initiatorSessionId: "session-parent",
      requestedRole: "scan",
      requestedModel: "gpt-5",
      requestedReasoningEffort: "high",
      forkTurns: 2,
      currentStepKey: "spawn-worker",
      inputJson: { task: "build a repo capsule" },
      startedAt: "2026-04-08T12:31:00.000Z",
      updatedAt: "2026-04-08T12:31:00.000Z",
    });
    await store.recordJAgentStepStarted({
      runId: "run-1",
      stepId: "step-1",
      sequence: 1,
      stepKey: "spawn-worker",
      stepKind: "spawnChildSession",
      status: "running",
      inputJson: { requestedRole: "scan" },
      startedAt: "2026-04-08T12:31:01.000Z",
      updatedAt: "2026-04-08T12:31:01.000Z",
    });
    await store.recordJAgentAttemptStarted({
      runId: "run-1",
      stepId: "step-1",
      attemptId: "attempt-1",
      attempt: 1,
      status: "running",
      codexSessionId: "session-worker",
      codexTurnId: "turn-worker",
      forkTurns: 2,
      modelName: "gpt-5",
      reasoningEffort: "high",
      startedAt: "2026-04-08T12:31:02.000Z",
    });
    await store.recordJAgentWaitStarted({
      runId: "run-1",
      stepId: "step-1",
      waitId: "wait-1",
      waitKind: "session_turn_completion",
      targetSessionId: "session-worker",
      targetTurnId: "turn-worker",
      resumeConditionJson: { status: "completed" },
      startedAt: "2026-04-08T12:31:03.000Z",
    });
    await store.bindJAgentSession({
      runId: "run-1",
      codexSessionId: "session-parent",
      bindingRole: "parent",
      createdAt: "2026-04-08T12:31:03.000Z",
    });
    await store.bindJAgentSession({
      runId: "run-1",
      codexSessionId: "session-worker",
      bindingRole: "worker",
      parentSessionId: "session-parent",
      createdAt: "2026-04-08T12:31:04.000Z",
    });
    await store.recordJAgentArtifact({
      runId: "run-1",
      stepId: "step-1",
      artifactId: "artifact-1",
      kind: "repo_capsule",
      path: "/tmp/repo-capsule.md",
      textPreview: "Repo capsule written",
      metadataJson: { bytes: 1280 },
      createdAt: "2026-04-08T12:31:30.000Z",
    });

    const activeRuns = await store.listActiveJAgentRuns({
      projectRoot: "/Users/nikitavoloboev/code/demo",
    });

    expect(activeRuns).toHaveLength(1);
    expect(activeRuns[0]?.run_id).toBe("run-1");

    await store.recordJAgentWaitResolved({
      runId: "run-1",
      waitId: "wait-1",
      status: "resolved",
      resumedAt: "2026-04-08T12:31:31.000Z",
    });
    await store.recordJAgentAttemptCompleted({
      runId: "run-1",
      stepId: "step-1",
      attemptId: "attempt-1",
      status: "completed",
      completedAt: "2026-04-08T12:31:31.000Z",
    });
    await store.recordJAgentStepCompleted({
      runId: "run-1",
      stepId: "step-1",
      status: "completed",
      outputJson: { artifactId: "artifact-1" },
      completedAt: "2026-04-08T12:31:32.000Z",
      updatedAt: "2026-04-08T12:31:32.000Z",
    });
    await store.recordJAgentRunCompleted({
      runId: "run-1",
      status: "completed",
      outputJson: { artifactId: "artifact-1" },
      completedAt: "2026-04-08T12:31:33.000Z",
      updatedAt: "2026-04-08T12:31:33.000Z",
    });

    const summary = await store.getJAgentRunSummary("run-1");

    expect(summary?.definition.definition_id).toBe("repo-capsule");
    expect(summary?.run.parent_session_id).toBe("session-parent");
    expect(summary?.run.requested_role).toBe("scan");
    expect(summary?.steps).toHaveLength(1);
    expect(summary?.attempts).toHaveLength(1);
    expect(summary?.waits).toHaveLength(1);
    expect(summary?.sessionBindings).toHaveLength(2);
    expect(summary?.artifacts).toHaveLength(1);
    expect(summary?.boundSessions.map((session) => session.session_id)).toEqual([
      "session-parent",
      "session-worker",
    ]);

    const completedRuns = await store.listJAgentRunsForSession("session-worker");
    const activeAfterCompletion = await store.listActiveJAgentRuns();

    expect(completedRuns).toHaveLength(2);
    expect(completedRuns[0]?.run_id).toBe("run-1");
    expect(completedRuns[1]?.run_id).toBe("native-session:session-worker");
    expect(activeAfterCompletion).toHaveLength(0);
  });

  it("restores a missing native binding and respects active-run filtering before limit", async () => {
    await store.replaceSessionProjection(
      {
        sessionId: "session-native-binding-only",
        rolloutPath: "/tmp/session-native-binding-only.jsonl",
        cwd: "/Users/nikitavoloboev/repos/openai/codex",
        projectRoot: "/Users/nikitavoloboev/repos/openai/codex",
        status: "completed",
        createdAt: "2026-04-08T13:00:00.000Z",
        updatedAt: "2026-04-08T13:01:00.000Z",
        latestActivityAt: "2026-04-08T13:01:00.000Z",
        latestAssistantMessage: "native run exists",
        turns: [
          {
            turnId: "turn-native-binding-only",
            sequence: 1,
            status: "completed",
            assistantMessage: "native run exists",
            completedAt: "2026-04-08T13:01:00.000Z",
            updatedAt: "2026-04-08T13:01:00.000Z",
          },
        ],
      },
      {
        sourceId: "session-native-binding-only",
        absolutePath: "/tmp/session-native-binding-only.jsonl",
        lineCount: 5,
        syncedAt: "2026-04-08T13:01:00.000Z",
      },
    );

    const dataPath = join(tempDir, "codex-sessions.db");
    await store.shutdown();

    const adminContext = createJazzContext({
      appId: "codex-session-store-test",
      app,
      permissions: {},
      driver: { type: "persistent", dataPath },
      env: "dev",
      userBranch: "main",
      tier: "edge",
    });

    try {
      const db = adminContext.db(app);
      const binding = await db.one(
        app.j_agent_session_bindings.where({
          binding_id: "native-session:session-native-binding-only:primary-session",
        }),
      );
      expect(binding).not.toBeNull();
      await db.delete(app.j_agent_session_bindings, binding!.id).wait({ tier: "edge" });
    } finally {
      await adminContext.shutdown();
    }

    store = createCodexSessionStore({
      appId: "codex-session-store-test",
      dataPath,
    });

    const restoredRuns = await store.listJAgentRunsForSession("session-native-binding-only", {
      limit: 10,
    });
    expect(restoredRuns.map((run) => run.run_id)).toContain(
      "native-session:session-native-binding-only",
    );

    await store.upsertJAgentDefinition({
      definitionId: "repo-capsule",
      name: "repo-capsule",
      version: "v1",
      sourceKind: "barnum_ts",
      entrypoint: "barnum/workflows/repo-capsule.ts",
    });
    await store.recordJAgentRunStarted({
      runId: "run-completed-newer",
      definitionId: "repo-capsule",
      projectRoot: "/Users/nikitavoloboev/repos/openai/codex",
      status: "running",
      startedAt: "2026-04-08T13:10:00.000Z",
      updatedAt: "2026-04-08T13:10:00.000Z",
    });
    await store.recordJAgentRunCompleted({
      runId: "run-completed-newer",
      status: "completed",
      completedAt: "2026-04-08T13:11:00.000Z",
      updatedAt: "2026-04-08T13:11:00.000Z",
    });
    await store.recordJAgentRunStarted({
      runId: "run-other-project-newest",
      definitionId: "repo-capsule",
      projectRoot: "/Users/nikitavoloboev/repos/openai/other",
      status: "running",
      startedAt: "2026-04-08T13:12:00.000Z",
      updatedAt: "2026-04-08T13:12:00.000Z",
    });
    await store.recordJAgentRunStarted({
      runId: "run-target-active",
      definitionId: "repo-capsule",
      projectRoot: "/Users/nikitavoloboev/repos/openai/codex",
      status: "running",
      startedAt: "2026-04-08T13:09:00.000Z",
      updatedAt: "2026-04-08T13:09:00.000Z",
    });

    const activeRuns = await store.listActiveJAgentRuns({
      projectRoot: "/Users/nikitavoloboev/repos/openai/codex",
      limit: 1,
    });

    expect(activeRuns).toHaveLength(1);
    expect(activeRuns[0]?.run_id).toBe("run-target-active");
  });
});

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { AgentDataStore, createAgentDataStore } from "../src/index.js";

describe("AgentDataStore", () => {
  let tempDir: string;
  let store: AgentDataStore;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "agent-data-store-"));
    store = createAgentDataStore({
      appId: "agent-data-store-test",
      dataPath: join(tempDir, "agent-data.db"),
    });
  });

  afterEach(async () => {
    await store.shutdown();
    await rm(tempDir, { recursive: true, force: true });
  });

  it("records a run summary across the core operational tables", async () => {
    await store.recordRunStarted({
      runId: "run-1",
      agentId: "plan",
      threadId: "thread-1",
      turnId: "turn-1",
      cwd: "/Users/nikitavoloboev/run",
      repoRoot: "/Users/nikitavoloboev/run",
      requestSummary: "Summarize active plans",
      status: "running",
    });
    await store.recordItemStarted({
      runId: "run-1",
      itemId: "item-1",
      itemKind: "agentMessage",
      sequence: 1,
      phase: "commentary",
      status: "running",
    });
    await store.appendSemanticEvent({
      runId: "run-1",
      itemId: "item-1",
      eventId: "semantic-1",
      eventType: "workspace_snapshot",
      summaryText: "captured baseline state",
      payloadJson: { dirty_path_count: 4 },
    });
    await store.appendWireEvent({
      runId: "run-1",
      eventId: "wire-1",
      direction: "client_to_daemon",
      connectionId: 7,
      method: "turn/start",
      payloadJson: { query: "Summarize active plans" },
    });
    await store.recordArtifact({
      runId: "run-1",
      artifactId: "artifact-1",
      artifactKind: "report",
      absolutePath: "/tmp/report.md",
      title: "Run report",
    });
    await store.recordWorkspaceSnapshot({
      runId: "run-1",
      snapshotId: "workspace-1",
      repoRoot: "/Users/nikitavoloboev/run",
      branch: "main",
      headCommit: "abc123",
      dirtyPathCount: 4,
      snapshotJson: { files: ["flow.toml"] },
    });
    await store.updateAgentState({
      agentId: "plan",
      snapshotId: "state-1",
      stateVersion: 1,
      status: "idle",
      stateJson: { last_query: "Summarize active plans" },
    });
    await store.recordMemoryLink({
      runId: "run-1",
      itemId: "item-1",
      linkId: "memory-1",
      memoryScope: "repo-scoped",
      memoryRef: "helixir://memory-1",
      queryText: "active plans",
    });
    await store.recordSourceFile({
      runId: "run-1",
      sourceFileId: "source-1",
      fileKind: "events-jsonl",
      absolutePath: "/tmp/run-1.events.jsonl",
    });
    await store.recordItemCompleted({
      runId: "run-1",
      itemId: "item-1",
      status: "completed",
      summaryJson: { delivered: true },
    });
    await store.recordRunCompleted({
      runId: "run-1",
      status: "completed",
    });

    const recentRuns = await store.listRecentRuns();
    const activeRuns = await store.listActiveRuns();
    const summary = await store.getRunSummary("run-1");

    expect(recentRuns).toHaveLength(1);
    expect(recentRuns[0]?.agent_id).toBe("plan");
    expect(recentRuns[0]?.status).toBe("completed");
    expect(activeRuns).toHaveLength(0);
    expect(summary?.run.run_id).toBe("run-1");
    expect(summary?.items).toHaveLength(1);
    expect(summary?.semanticEvents).toHaveLength(1);
    expect(summary?.wireEvents).toHaveLength(1);
    expect(summary?.artifacts).toHaveLength(1);
    expect(summary?.workspaceSnapshots).toHaveLength(1);
    expect(summary?.memoryLinks).toHaveLength(1);
    expect(summary?.sourceFiles).toHaveLength(1);
    expect(summary?.latestAgentState?.status).toBe("idle");
  });

  it("upserts agents, runs, and items by their external ids", async () => {
    await store.recordRunStarted({
      runId: "run-2",
      agentId: "plan",
      requestSummary: "initial summary",
      status: "running",
      agent: {
        lane: "planning",
      },
    });
    await store.recordRunStarted({
      runId: "run-2",
      agentId: "plan",
      requestSummary: "updated summary",
      status: "waiting",
      agent: {
        lane: "planning",
        promptSurface: "/plan",
      },
    });
    await store.recordItemStarted({
      runId: "run-2",
      itemId: "item-2",
      itemKind: "commandExecution",
      sequence: 1,
      status: "running",
    });
    await store.recordItemStarted({
      runId: "run-2",
      itemId: "item-2",
      itemKind: "commandExecution",
      sequence: 2,
      phase: "commentary",
      status: "waiting",
    });

    const recentRuns = await store.listRecentRuns();
    const summary = await store.getRunSummary("run-2");

    expect(recentRuns).toHaveLength(1);
    expect(recentRuns[0]?.request_summary).toBe("updated summary");
    expect(recentRuns[0]?.status).toBe("waiting");
    expect(summary?.items).toHaveLength(1);
    expect(summary?.items[0]?.sequence).toBe(2);
    expect(summary?.items[0]?.phase).toBe("commentary");
  });

  it("upserts and lists task records in focus order", async () => {
    await store.upsertTaskRecord({
      taskId: "d-002",
      context: "designer",
      title:
        "Merge PR #3296 and clean up the rest of the open Designer PR stack",
      status: "active",
      priority: "P0",
      placement: "now",
      focusRank: 2,
      project: "prom/designer",
      branch: "review/nikiv-designer-build123d-monaco-editor",
      pr: "https://github.com/fl2024008/prometheus/pull/3296",
    });
    await store.upsertTaskRecord({
      taskId: "d-001",
      context: "designer",
      title: "Get the entire Designer stack reviewable and mergeable",
      status: "active",
      priority: "P0",
      placement: "now",
      focusRank: 1,
      project: "prom/designer",
      annotationsJson: [
        "- 2026-04-08: Migrated from ~/do/now.md and promoted as the top active Designer task",
      ],
    });

    const tasks = await store.listTaskRecords({
      context: "designer",
      statuses: ["active"],
    });
    const task = await store.getTaskRecord("d-002");

    expect(tasks.map((item) => item.task_id)).toEqual(["d-001", "d-002"]);
    expect(task?.branch).toBe("review/nikiv-designer-build123d-monaco-editor");
    expect(task?.pr).toBe("https://github.com/fl2024008/prometheus/pull/3296");
  });

  it("records daemon log sources, chunks, events, checkpoints, and summaries", async () => {
    const source = await store.recordDaemonLogSource({
      sourceId: "flow:sync:stderr",
      manager: "flow",
      daemonName: "sync",
      stream: "stderr",
      hostId: "workstation",
      logPath: "/Users/nikitavoloboev/.config/flow-state/daemons/sync/stderr.log",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      retentionClass: "normal",
      status: "active",
      createdAt: "2026-04-24T10:00:00.000Z",
      updatedAt: "2026-04-24T10:00:00.000Z",
    });
    const chunk = await store.recordDaemonLogChunk({
      chunkId: "chunk-1",
      sourceId: source.source_id,
      fileFingerprint: "dev:inode:size:mtime",
      startOffset: 0,
      endOffset: 128,
      firstLineNo: 1,
      lastLineNo: 2,
      lineCount: 2,
      byteCount: 128,
      sha256: "abc123",
      bodyPreview: "warn: slow sync",
      ingestedAt: "2026-04-24T10:01:00.000Z",
    });
    await store.recordDaemonLogEvent({
      eventId: "event-1",
      sourceId: source.source_id,
      chunkId: chunk.chunk_id,
      seq: 1,
      lineNo: 2,
      at: "2026-04-24T10:00:30.000Z",
      level: "warn",
      message: "sync took longer than expected",
      fieldsJson: { durationMs: 1250 },
      conversationHash: "conv-hash",
      traceId: "trace-1",
    });
    const checkpoint = await store.recordDaemonLogCheckpoint({
      sourceId: source.source_id,
      fileFingerprint: "dev:inode:size:mtime",
      offset: 128,
      lineNo: 2,
      lastChunkId: chunk.chunk_id,
      lastEventId: "event-1",
      lastSeenAt: "2026-04-24T10:01:00.000Z",
      updatedAt: "2026-04-24T10:01:00.000Z",
    });
    const summary = await store.recordDaemonLogSummary({
      summaryId: "summary-1",
      sourceId: source.source_id,
      windowStart: "2026-04-24T10:00:00.000Z",
      windowEnd: "2026-04-24T10:05:00.000Z",
      levelCountsJson: { warn: 1 },
      errorCount: 0,
      warningCount: 1,
      summaryText: "one warning",
    });

    const sources = await store.listDaemonLogSources({ manager: "flow" });
    const events = await store.listDaemonLogEvents({
      conversationHash: "conv-hash",
    });
    const summaries = await store.listDaemonLogSummaries({
      daemonName: "sync",
    });

    expect(sources.map((item) => item.source_id)).toEqual([
      "flow:sync:stderr",
    ]);
    expect(events.map((item) => item.event_id)).toEqual(["event-1"]);
    expect(events[0]?.repo_root).toBe("/Users/nikitavoloboev/code/prom");
    expect(checkpoint.checkpoint_id).toBe(source.source_id);
    expect(summary.summary_id).toBe("summary-1");
    expect(summaries).toHaveLength(1);
  });

  it("records cursor review operations and hides processed entries by default", async () => {
    const operation = await store.recordCursorReviewOperation({
      operationId: "cursor-op-1",
      operationType: "delete-branch-path",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      workspaceRoot: "/Users/nikitavoloboev/code/prom",
      bookmark: "review/nikiv-ai-proxy-opus-4-7-thinking",
      relPath: "tests",
      note: "remove failing branch tests",
      sourceSessionId: "cursor:session-1",
      sourceChatKind: "cursor",
    });

    const pending = await store.listCursorReviewOperations({
      repoRoot: "/Users/nikitavoloboev/code/prom",
    });

    expect(operation.operationId).toBe("cursor-op-1");
    expect(operation.relPath).toBe("tests");
    expect(pending).toHaveLength(1);
    expect(pending[0]?.bookmark).toBe(
      "review/nikiv-ai-proxy-opus-4-7-thinking",
    );
    expect(pending[0]?.latestResult).toBeUndefined();

    const result = await store.recordCursorReviewResult({
      operationId: "cursor-op-1",
      status: "completed",
      clientId: "flow-window-1",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      message: "Flow opened the diff",
    });

    const filtered = await store.listCursorReviewOperations({
      repoRoot: "/Users/nikitavoloboev/code/prom",
    });
    const withProcessed = await store.listCursorReviewOperations({
      repoRoot: "/Users/nikitavoloboev/code/prom",
      includeProcessed: true,
    });

    expect(result.operationId).toBe("cursor-op-1");
    expect(filtered).toEqual([]);
    expect(withProcessed).toHaveLength(1);
    expect(withProcessed[0]?.latestResult?.status).toBe("completed");
    expect(withProcessed[0]?.latestResult?.message).toBe(
      "Flow opened the diff",
    );
  });

  it("records and lists latest branch file review states", async () => {
    await store.recordBranchFileReviewState({
      eventId: "branch-file-review-1",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      workspaceRoot: "/Users/nikitavoloboev/code/prom",
      bookmark: "review/nikiv-designer-telemetry-pr1-main",
      relPath: "ide/designer/src/telemetry/log.ts",
      status: "needs-work",
      note: "event names are too noisy",
      sourceSessionId: "cursor:session-1",
      sourceChatKind: "cursor",
    });
    await store.recordBranchFileReviewState({
      eventId: "branch-file-review-2",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      workspaceRoot: "/Users/nikitavoloboev/code/prom",
      bookmark: "review/nikiv-designer-telemetry-pr1-main",
      relPath: "ide/designer/src/telemetry/log.ts",
      status: "happy",
      note: "looks good now",
      sourceSessionId: "codex:session-2",
      sourceChatKind: "codex",
    });
    await store.recordBranchFileReviewState({
      eventId: "branch-file-review-3",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      workspaceRoot: "/Users/nikitavoloboev/code/prom",
      bookmark: "review/nikiv-designer-telemetry-pr1-main",
      relPath: "ide/designer/src/telemetry/buffer.ts",
      status: "cleared",
    });

    const states = await store.listBranchFileReviewStates({
      repoRoot: "/Users/nikitavoloboev/code/prom",
      bookmark: "review/nikiv-designer-telemetry-pr1-main",
    });
    const withCleared = await store.listBranchFileReviewStates({
      repoRoot: "/Users/nikitavoloboev/code/prom",
      bookmark: "review/nikiv-designer-telemetry-pr1-main",
      includeCleared: true,
    });

    expect(states).toHaveLength(1);
    expect(states[0]?.relPath).toBe("ide/designer/src/telemetry/log.ts");
    expect(states[0]?.status).toBe("happy");
    expect(states[0]?.note).toBe("looks good now");
    expect(withCleared).toHaveLength(2);
    expect(
      withCleared.find(
        (item) => item.relPath === "ide/designer/src/telemetry/buffer.ts",
      )?.status,
    ).toBe("cleared");
  });
});

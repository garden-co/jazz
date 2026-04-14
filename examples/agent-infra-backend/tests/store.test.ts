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
      title: "Merge PR #3296 and clean up the rest of the open Designer PR stack",
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
});

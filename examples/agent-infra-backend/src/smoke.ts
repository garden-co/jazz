import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAgentDataStore } from "./index.js";

async function main(): Promise<void> {
  const tempDir = await mkdtemp(join(tmpdir(), "agent-infra-backend-"));
  const dataPath = join(tempDir, "agent-data.db");
  const store = createAgentDataStore({
    appId: "agent-infra-backend-smoke",
    dataPath,
  });

  try {
    await store.recordRunStarted({
      runId: "run-smoke-1",
      agentId: "plan",
      threadId: "thread-smoke-1",
      turnId: "turn-smoke-1",
      cwd: "/Users/nikitavoloboev/run",
      repoRoot: "/Users/nikitavoloboev/run",
      requestSummary: "Summarize active plans",
      status: "running",
    });
    await store.recordItemStarted({
      runId: "run-smoke-1",
      itemId: "item-smoke-1",
      itemKind: "agentMessage",
      sequence: 1,
      phase: "commentary",
      status: "running",
    });
    await store.appendSemanticEvent({
      runId: "run-smoke-1",
      itemId: "item-smoke-1",
      eventType: "workspace_snapshot",
      summaryText: "Captured initial dirty worktree state",
      payloadJson: { repo_root: "/Users/nikitavoloboev/run", dirty_path_count: 4 },
    });
    await store.appendWireEvent({
      runId: "run-smoke-1",
      direction: "client_to_daemon",
      connectionId: 1,
      method: "turn/start",
      payloadJson: { request_id: "smoke-request-1" },
    });
    await store.recordArtifact({
      runId: "run-smoke-1",
      artifactKind: "plan",
      absolutePath: "/Users/nikitavoloboev/docs/plan/26/example-plan.md",
      title: "Example plan artifact",
    });
    await store.recordWorkspaceSnapshot({
      runId: "run-smoke-1",
      repoRoot: "/Users/nikitavoloboev/run",
      branch: "main",
      headCommit: "abc123",
      dirtyPathCount: 4,
      snapshotJson: { dirty_paths: ["flow.toml", "scripts/plan-agent-context.py"] },
    });
    await store.updateAgentState({
      agentId: "plan",
      status: "idle",
      stateVersion: 1,
      stateJson: { last_query: "which plans are active right now?" },
    });
    await store.recordMemoryLink({
      runId: "run-smoke-1",
      itemId: "item-smoke-1",
      memoryScope: "repo-scoped",
      memoryRef: "helixir://memory/run-smoke-1",
      queryText: "active plans",
    });
    await store.recordSourceFile({
      runId: "run-smoke-1",
      fileKind: "events-jsonl",
      absolutePath: "/Users/nikitavoloboev/run/.ai/internal/agent-runs/plan/20260326T223725Z.events.jsonl",
    });
    await store.recordItemCompleted({
      runId: "run-smoke-1",
      itemId: "item-smoke-1",
      status: "completed",
      summaryJson: { delivered: true },
    });
    await store.recordRunCompleted({
      runId: "run-smoke-1",
      status: "completed",
    });

    const summary = await store.getRunSummary("run-smoke-1");
    console.log(JSON.stringify(summary, null, 2));
  } finally {
    await store.shutdown();
    await rm(tempDir, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

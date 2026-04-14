import { spawnSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));

describe("agent-infra backend CLI run commands", () => {
  let tempDir: string;
  let dataPath: string;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "agent-infra-cli-"));
    dataPath = join(tempDir, "agent-infra.db");
  });

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true });
  });

  it("records and summarizes a prep-workflow run through JSON CLI commands", () => {
    const run = runCliJson("record-run-started", {
      runId: "prep-run-1",
      agentId: "designer-prep-implementation",
      threadId: "thread-1",
      turnId: "turn-1",
      cwd: "/Users/nikitavoloboev/work/review/demo/ide/designer",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      requestSummary: "Prepare typed-cad phase 0 implementation",
      status: "running",
      contextJson: {
        taskId: "d-003",
        phaseId: "0",
        planPath:
          "/Users/nikitavoloboev/docs/plan/8/designer-typed-cad-3d-preview-on-build123d-fast-path-plan.md",
      },
      sourceTracePath: "/tmp/prep-run-1.trace.jsonl",
      agent: {
        lane: "designer",
        promptSurface: ":designer prep-implementation",
      },
    });
    expect(run.runId).toBe("prep-run-1");
    expect(run.agentId).toBe("designer-prep-implementation");
    expect(run.status).toBe("running");

    const activeRuns = runCliJson("list-active-runs");
    expect(activeRuns).toHaveLength(1);
    expect(activeRuns[0]?.runId).toBe("prep-run-1");

    const item = runCliJson("record-item-started", {
      runId: "prep-run-1",
      itemId: "gather-facts",
      itemKind: "workflowStage",
      sequence: 2,
      phase: "prep",
      status: "running",
      summaryJson: { source: "designer preflight + triage" },
    });
    expect(item.itemId).toBe("gather-facts");
    expect(item.status).toBe("running");

    const artifact = runCliJson("record-artifact", {
      runId: "prep-run-1",
      artifactId: "artifact-1",
      artifactKind: "implementation-packet",
      absolutePath: "/tmp/designer-prep/packet.json",
      title: "Phase 0 packet",
      checksum: "sha256:test",
    });
    expect(artifact.artifactId).toBe("artifact-1");

    const snapshot = runCliJson("record-workspace-snapshot", {
      runId: "prep-run-1",
      snapshotId: "snapshot-1",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      branch: "review/nikiv-designer-typed-cad-planning-next",
      headCommit: "abc123",
      dirtyPathCount: 8,
      snapshotJson: { workspaceStatus: "feature-only" },
    });
    expect(snapshot.snapshotId).toBe("snapshot-1");

    runCliJson("record-item-completed", {
      runId: "prep-run-1",
      itemId: "gather-facts",
      status: "completed",
      summaryJson: { classifiedAs: "prep_commit_required" },
    });
    runCliJson("record-run-completed", {
      runId: "prep-run-1",
      status: "completed",
    });

    const summary = runCliJson("get-run-summary", undefined, [
      "--run-id",
      "prep-run-1",
    ]);
    expect(summary.run.runId).toBe("prep-run-1");
    expect(summary.run.status).toBe("completed");
    expect(summary.items).toHaveLength(1);
    expect(summary.items[0]?.itemId).toBe("gather-facts");
    expect(summary.artifacts[0]?.artifactId).toBe("artifact-1");
    expect(summary.workspaceSnapshots[0]?.snapshotId).toBe("snapshot-1");

    const recentRuns = runCliJson("list-recent-runs", undefined, ["--limit", "5"]);
    expect(recentRuns).toHaveLength(1);
    expect(recentRuns[0]?.runId).toBe("prep-run-1");

    const activeAfter = runCliJson("list-active-runs");
    expect(activeAfter).toEqual([]);
  });

  function runCliJson(command: string, input?: unknown, extraArgs: string[] = []): any {
    const result = spawnSync(
      "pnpm",
      ["exec", "tsx", "src/cli.ts", command, "--data-path", dataPath, ...extraArgs],
      {
        cwd: packageRoot,
        input: input === undefined ? undefined : JSON.stringify(input),
        encoding: "utf8",
      },
    );

    if (result.status !== 0) {
      throw new Error(result.stderr || result.stdout || `CLI command ${command} failed`);
    }

    return JSON.parse(result.stdout.trim());
  }
});

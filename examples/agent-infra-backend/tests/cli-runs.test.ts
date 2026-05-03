import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, rm, stat } from "node:fs/promises";
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

  it("resolves a legacy directory data path to the nested SQLite store", async () => {
    await mkdir(dataPath, { recursive: true });

    const tasks = runCliJson("list-tasks");

    expect(tasks).toEqual([]);
    await expect(stat(join(dataPath, "agent-infra.sqlite"))).resolves.toEqual(
      expect.objectContaining({
        size: expect.any(Number),
      }),
    );
  });

  it("prints remote store config from flags and env without exposing secrets", () => {
    const config = runCliJson(
      "config",
      undefined,
      [
        "--server-url",
        "https://sync.example.test",
        "--server-path-prefix",
        "/apps/run-agent-infra",
        "--backend-secret",
        "backend-secret",
        "--admin-secret",
        "admin-secret",
        "--tier",
        "global",
      ],
      {
        PROM_DB_JAZZ_APP_ID: "agent-infra-remote",
        PROM_DB_JAZZ_ENV: "remote-autonomy",
        PROM_DB_JAZZ_USER_BRANCH: "linux-server",
      },
    );

    expect(config).toEqual({
      dataPath,
      appId: "agent-infra-remote",
      env: "remote-autonomy",
      userBranch: "linux-server",
      serverUrl: "https://sync.example.test",
      serverPathPrefix: "/apps/run-agent-infra",
      hasBackendSecret: true,
      hasAdminSecret: true,
      tier: "global",
    });
    expect(JSON.stringify(config)).not.toContain("backend-secret");
    expect(JSON.stringify(config)).not.toContain("admin-secret");
  });

  it("propagates remote store config through serve-json child commands", () => {
    const result = spawnSync(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "serve-json",
        "--data-path",
        dataPath,
        "--server-url",
        "https://sync.example.test",
        "--tier",
        "global",
      ],
      {
        cwd: packageRoot,
        input: `${JSON.stringify({ command: "config" })}\n`,
        encoding: "utf8",
        env: cliEnv({
          PROM_DB_JAZZ_APP_ID: "agent-infra-remote",
          PROM_DB_JAZZ_ENV: "remote-autonomy",
        }),
      },
    );

    if (result.status !== 0) {
      throw new Error(
        result.stderr || result.stdout || "serve-json config command failed",
      );
    }

    const line = JSON.parse(result.stdout.trim());
    expect(line).toEqual({
      ok: true,
      result: {
        dataPath,
        appId: "agent-infra-remote",
        env: "remote-autonomy",
        userBranch: "main",
        serverUrl: "https://sync.example.test",
        serverPathPrefix: null,
        hasBackendSecret: false,
        hasAdminSecret: false,
        tier: "global",
      },
    });
  });

  it(
    "records and summarizes a prep-workflow run through JSON CLI commands",
    { timeout: 20_000 },
    () => {
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

      const recentRuns = runCliJson("list-recent-runs", undefined, [
        "--limit",
        "5",
      ]);
      expect(recentRuns).toHaveLength(1);
      expect(recentRuns[0]?.runId).toBe("prep-run-1");

      const activeAfter = runCliJson("list-active-runs");
      expect(activeAfter).toEqual([]);
    },
  );

  it(
    "records and lists cursor review operations through the CLI",
    { timeout: 20_000 },
    () => {
      const operation = runCliJson("record-cursor-review-op", {
        operationId: "cursor-op-2",
        operationType: "delete-branch-path",
        repoRoot: "/Users/nikitavoloboev/code/prom",
        workspaceRoot: "/Users/nikitavoloboev/code/prom",
        bookmark: "review/nikiv-ai-proxy-opus-4-7-thinking",
        relPath: "tests",
        note: "remove failing branch tests",
        sourceSessionId: "cursor:session-2",
        sourceChatKind: "cursor",
      });

      expect(operation.operationId).toBe("cursor-op-2");
      expect(operation.operationType).toBe("delete-branch-path");
      expect(operation.relPath).toBe("tests");

      const pending = runCliJson("list-cursor-review-ops", undefined, [
        "--repo-root",
        "/Users/nikitavoloboev/code/prom",
      ]);
      expect(pending).toHaveLength(1);
      expect(pending[0]?.bookmark).toBe(
        "review/nikiv-ai-proxy-opus-4-7-thinking",
      );

      const result = runCliJson("record-cursor-review-result", {
        operationId: "cursor-op-2",
        status: "completed",
        clientId: "flow-window-cli",
        repoRoot: "/Users/nikitavoloboev/code/prom",
        message: "opened in fresh chat",
      });
      expect(result.operationId).toBe("cursor-op-2");

      const filtered = runCliJson("list-cursor-review-ops", undefined, [
        "--repo-root",
        "/Users/nikitavoloboev/code/prom",
      ]);
      expect(filtered).toEqual([]);

      const withProcessed = runCliJson("list-cursor-review-ops", undefined, [
        "--repo-root",
        "/Users/nikitavoloboev/code/prom",
        "--include-processed",
      ]);
      expect(withProcessed).toHaveLength(1);
      expect(withProcessed[0]?.latestResult?.status).toBe("completed");
    },
  );

  it(
    "records and lists branch file review states through the CLI",
    { timeout: 20_000 },
    () => {
      const state = runCliJson("record-branch-file-review-state", {
        eventId: "branch-file-review-cli-1",
        repoRoot: "/Users/nikitavoloboev/code/prom",
        workspaceRoot: "/Users/nikitavoloboev/code/prom",
        bookmark: "review/nikiv-designer-telemetry-pr1-main",
        relPath: "ide/designer/src/telemetry/log.ts",
        status: "needs-work",
        note: "buffering still breaks down",
        sourceSessionId: "cursor:session-2",
        sourceChatKind: "cursor",
      });

      expect(state.status).toBe("needs-work");
      expect(state.relPath).toBe("ide/designer/src/telemetry/log.ts");

      runCliJson("record-branch-file-review-state", {
        eventId: "branch-file-review-cli-2",
        repoRoot: "/Users/nikitavoloboev/code/prom",
        workspaceRoot: "/Users/nikitavoloboev/code/prom",
        bookmark: "review/nikiv-designer-telemetry-pr1-main",
        relPath: "ide/designer/src/telemetry/log.ts",
        status: "happy",
        note: "good enough to move on",
      });

      runCliJson("record-branch-file-review-state", {
        eventId: "branch-file-review-cli-3",
        repoRoot: "/Users/nikitavoloboev/code/prom",
        workspaceRoot: "/Users/nikitavoloboev/code/prom",
        bookmark: "review/nikiv-designer-telemetry-pr1-main",
        relPath: "ide/designer/src/telemetry/buffer.ts",
        status: "cleared",
      });

      const states = runCliJson("list-branch-file-review-states", undefined, [
        "--repo-root",
        "/Users/nikitavoloboev/code/prom",
        "--bookmark",
        "review/nikiv-designer-telemetry-pr1-main",
      ]);
      expect(states).toHaveLength(1);
      expect(states[0]?.status).toBe("happy");

      const withCleared = runCliJson(
        "list-branch-file-review-states",
        undefined,
        [
          "--repo-root",
          "/Users/nikitavoloboev/code/prom",
          "--bookmark",
          "review/nikiv-designer-telemetry-pr1-main",
          "--include-cleared",
        ],
      );
      expect(withCleared).toHaveLength(2);
      expect(
        withCleared.find(
          (item: any) =>
            item.relPath === "ide/designer/src/telemetry/buffer.ts",
        )?.status,
      ).toBe("cleared");
    },
  );

  function runCliJson(
    command: string,
    input?: unknown,
    extraArgs: string[] = [],
    extraEnv: NodeJS.ProcessEnv = {},
  ): any {
    const result = spawnSync(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        command,
        "--data-path",
        dataPath,
        ...extraArgs,
      ],
      {
        cwd: packageRoot,
        input: input === undefined ? undefined : JSON.stringify(input),
        encoding: "utf8",
        env: cliEnv(extraEnv),
      },
    );

    if (result.status !== 0) {
      throw new Error(
        result.stderr || result.stdout || `CLI command ${command} failed`,
      );
    }

    return JSON.parse(result.stdout.trim());
  }

  function cliEnv(extraEnv: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv {
    return {
      ...process.env,
      PROM_DB_JAZZ_APP_ID: "",
      PROM_DB_JAZZ_ENV: "",
      PROM_DB_JAZZ_USER_BRANCH: "",
      PROM_DB_JAZZ_SERVER_URL: "",
      PROM_DB_JAZZ_SERVER_PATH_PREFIX: "",
      PROM_DB_JAZZ_BACKEND_SECRET: "",
      PROM_DB_JAZZ_ADMIN_SECRET: "",
      PROM_DB_JAZZ_TIER: "",
      FLOW_AGENT_INFRA_JAZZ_APP_ID: "",
      FLOW_AGENT_INFRA_JAZZ_ENV: "",
      FLOW_AGENT_INFRA_JAZZ_USER_BRANCH: "",
      FLOW_AGENT_INFRA_JAZZ_SERVER_URL: "",
      FLOW_AGENT_INFRA_JAZZ_SERVER_PATH_PREFIX: "",
      FLOW_AGENT_INFRA_JAZZ_BACKEND_SECRET: "",
      FLOW_AGENT_INFRA_JAZZ_ADMIN_SECRET: "",
      FLOW_AGENT_INFRA_JAZZ_TIER: "",
      J_AGENT_INFRA_JAZZ_APP_ID: "",
      J_AGENT_INFRA_JAZZ_ENV: "",
      J_AGENT_INFRA_JAZZ_USER_BRANCH: "",
      J_AGENT_INFRA_JAZZ_SERVER_URL: "",
      J_AGENT_INFRA_JAZZ_SERVER_PATH_PREFIX: "",
      J_AGENT_INFRA_JAZZ_BACKEND_SECRET: "",
      J_AGENT_INFRA_JAZZ_ADMIN_SECRET: "",
      J_AGENT_INFRA_JAZZ_TIER: "",
      REMOTE_AUTONOMY_AGENT_APP_ID: "",
      REMOTE_AUTONOMY_ENV: "",
      REMOTE_AUTONOMY_USER_BRANCH: "",
      REMOTE_AUTONOMY_SYNC_SERVER_URL: "",
      REMOTE_AUTONOMY_SYNC_SERVER_PATH_PREFIX: "",
      REMOTE_AUTONOMY_BACKEND_SECRET: "",
      REMOTE_AUTONOMY_ADMIN_SECRET: "",
      ...extraEnv,
    };
  }
});

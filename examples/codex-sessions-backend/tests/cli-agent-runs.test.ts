import { spawnSync } from "node:child_process";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createCodexSessionStore, type CodexSessionStore } from "../src/index.js";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));

describe("codex-sessions CLI agent-run commands", () => {
  let tempDir: string;
  let dataPath: string;
  let store: CodexSessionStore | null;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "codex-session-cli-"));
    dataPath = join(tempDir, "codex-sessions.db");
    store = createCodexSessionStore({
      appId: "codex-session-cli-test",
      dataPath,
    });

    await store.replaceSessionProjection(
      {
        sessionId: "session-parent",
        rolloutPath: "/tmp/session-parent.jsonl",
        cwd: "/Users/nikitavoloboev/repos/openai/codex",
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

    await store.shutdown();
    store = null;
  });

  afterEach(async () => {
    if (store) {
      await store.shutdown();
      store = null;
    }
    await rm(tempDir, { recursive: true, force: true });
  });

  it("drives a full j-agent lifecycle through JSON CLI commands", () => {
    const definition = runCliJson("upsert-definition", {
      definitionId: "repo-capsule",
      name: "repo-capsule",
      version: "v1",
      sourceKind: "barnum_ts",
      entrypoint: "barnum/workflows/repo-capsule.ts",
      metadataJson: { owner: "j" },
    });
    expect(definition.definitionId).toBe("repo-capsule");

    const startedRun = runCliJson("record-run-started", {
      runId: "run-1",
      definitionId: "repo-capsule",
      status: "running",
      projectRoot: "/Users/nikitavoloboev/repos/openai/codex",
      repoRoot: "/Users/nikitavoloboev/repos/openai/codex",
      cwd: "/Users/nikitavoloboev/repos/openai/codex",
      triggerSource: "j-inline-agent",
      parentSessionId: "session-parent",
      parentTurnId: "turn-parent",
      initiatorSessionId: "session-parent",
      requestedRole: "scan",
      requestedModel: "gpt-5.4",
      requestedReasoningEffort: "high",
      forkTurns: 2,
      currentStepKey: "spawn-worker",
      inputJson: { task: "build a repo capsule" },
      startedAt: "2026-04-08T12:31:00.000Z",
      updatedAt: "2026-04-08T12:31:00.000Z",
    });
    expect(startedRun.runId).toBe("run-1");
    expect(startedRun.parentSessionId).toBe("session-parent");

    const activeBefore = runCliJson("list-active-runs", undefined, [
      "--project-root",
      "/Users/nikitavoloboev/repos/openai/codex",
      "--limit",
      "10",
    ]);
    expect(activeBefore).toHaveLength(1);
    expect(activeBefore[0]?.runId).toBe("run-1");

    runCliJson("record-step-started", {
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

    const attempt = runCliJson("record-attempt-started", {
      runId: "run-1",
      stepId: "step-1",
      attemptId: "attempt-1",
      attempt: 1,
      status: "running",
      codexSessionId: "session-parent",
      codexTurnId: "turn-parent",
      forkTurns: 2,
      modelName: "gpt-5.4",
      reasoningEffort: "high",
      startedAt: "2026-04-08T12:31:02.000Z",
    });
    expect(attempt.attemptId).toBe("attempt-1");

    const wait = runCliJson("record-wait-started", {
      runId: "run-1",
      stepId: "step-1",
      waitId: "wait-1",
      waitKind: "session_turn_completion",
      targetSessionId: "session-parent",
      targetTurnId: "turn-parent",
      resumeConditionJson: { status: "completed" },
      startedAt: "2026-04-08T12:31:03.000Z",
    });
    expect(wait.waitId).toBe("wait-1");

    const binding = runCliJson("bind-session", {
      runId: "run-1",
      codexSessionId: "session-parent",
      bindingRole: "parent",
      createdAt: "2026-04-08T12:31:04.000Z",
    });
    expect(binding.codexSessionId).toBe("session-parent");

    const artifact = runCliJson("record-artifact", {
      runId: "run-1",
      stepId: "step-1",
      artifactId: "artifact-1",
      kind: "repo_capsule",
      path: "/tmp/repo-capsule.md",
      textPreview: "Repo capsule written",
      metadataJson: { bytes: 1280 },
      createdAt: "2026-04-08T12:31:30.000Z",
    });
    expect(artifact.artifactId).toBe("artifact-1");

    const runsForSession = runCliJson("list-runs-for-session", undefined, [
      "--session-id",
      "session-parent",
      "--limit",
      "10",
    ]);
    expect(runsForSession).toHaveLength(2);
    expect(runsForSession[0]?.runId).toBe("run-1");
    expect(runsForSession[1]?.runId).toBe("native-session:session-parent");

    runCliJson("record-attempt-completed", {
      runId: "run-1",
      stepId: "step-1",
      attemptId: "attempt-1",
      status: "completed",
      completedAt: "2026-04-08T12:31:31.000Z",
    });
    runCliJson("record-step-completed", {
      runId: "run-1",
      stepId: "step-1",
      status: "completed",
      outputJson: { artifactId: "artifact-1" },
      completedAt: "2026-04-08T12:31:32.000Z",
      updatedAt: "2026-04-08T12:31:32.000Z",
    });
    runCliJson("resolve-wait", {
      runId: "run-1",
      waitId: "wait-1",
      status: "resolved",
      resumedAt: "2026-04-08T12:31:33.000Z",
    });
    runCliJson("record-run-completed", {
      runId: "run-1",
      status: "completed",
      outputJson: { artifactId: "artifact-1" },
      completedAt: "2026-04-08T12:31:34.000Z",
      updatedAt: "2026-04-08T12:31:34.000Z",
    });

    const summary = runCliJson("get-run-summary", undefined, ["--run-id", "run-1"]);
    expect(summary.run.status).toBe("completed");
    expect(summary.steps).toHaveLength(1);
    expect(summary.attempts).toHaveLength(1);
    expect(summary.waits[0]?.status).toBe("resolved");
    expect(summary.sessionBindings[0]?.bindingRole).toBe("parent");
    expect(summary.artifacts[0]?.artifactId).toBe("artifact-1");
    expect(summary.boundSessions[0]?.id).toBe("session-parent");

    const activeAfter = runCliJson("list-active-runs", undefined, [
      "--project-root",
      "/Users/nikitavoloboev/repos/openai/codex",
      "--limit",
      "10",
    ]);
    expect(activeAfter).toEqual([]);
  }, 30_000);

  it("syncs one session through the CLI", async () => {
    const codexHome = join(tempDir, ".codex");
    const rolloutDir = join(codexHome, "sessions/2026/04/08");
    const rolloutPath = join(
      rolloutDir,
      "rollout-2026-04-08T12-45-00-019d0000-0000-7000-8000-000000000002.jsonl",
    );
    await mkdir(rolloutDir, { recursive: true });
    await writeFile(
      rolloutPath,
      [
        {
          timestamp: "2026-04-08T12:45:00.000Z",
          type: "session_meta",
          payload: {
            id: "019d0000-0000-7000-8000-000000000002",
            timestamp: "2026-04-08T12:45:00.000Z",
            cwd: "/Users/nikitavoloboev/repos/openai/codex",
            originator: "codex-cli",
            cli_version: "0.0.0",
            source: "cli",
          },
        },
        {
          timestamp: "2026-04-08T12:45:01.000Z",
          type: "turn_context",
          payload: {
            turn_id: "turn-2",
            cwd: "/Users/nikitavoloboev/repos/openai/codex",
          },
        },
        {
          timestamp: "2026-04-08T12:45:02.000Z",
          type: "event_msg",
          payload: {
            type: "user_message",
            message: "Sync just this session",
          },
        },
      ].map((line) => JSON.stringify(line)).join("\n"),
    );

    const result = runCliJson("sync-session", undefined, [
      "--codex-home",
      codexHome,
      "--session-id",
      "019d0000-0000-7000-8000-000000000002",
    ]);
    const session = runCliJson("get-session", undefined, [
      "--session-id",
      "019d0000-0000-7000-8000-000000000002",
    ]);

    expect(result.found).toBe(true);
    expect(result.synced).toBe(1);
    expect(session.id).toBe("019d0000-0000-7000-8000-000000000002");
    expect(session.preview).toBe("Sync just this session");
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

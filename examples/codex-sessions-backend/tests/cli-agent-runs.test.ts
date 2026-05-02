import { spawn, spawnSync } from "node:child_process";
import { appendFile, mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
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
  }, 60_000);

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
  }, 15_000);

  it("lists raw rollout events from the source file with stable cursors", async () => {
    const codexHome = join(tempDir, ".codex");
    const rolloutDir = join(codexHome, "sessions/2026/04/08");
    const rolloutPath = join(
      rolloutDir,
      "rollout-2026-04-08T12-55-00-019d0000-0000-7000-8000-000000000004.jsonl",
    );
    await mkdir(rolloutDir, { recursive: true });
    await writeFile(
      rolloutPath,
      [
        {
          timestamp: "2026-04-08T12:55:00.000Z",
          type: "session_meta",
          payload: {
            id: "019d0000-0000-7000-8000-000000000004",
            timestamp: "2026-04-08T12:55:00.000Z",
            cwd: "/Users/nikitavoloboev/repos/openai/codex",
            originator: "codex-cli",
            cli_version: "0.0.0",
            source: "cli",
          },
        },
        {
          timestamp: "2026-04-08T12:55:01.000Z",
          type: "event_msg",
          payload: {
            type: "agent_message_content_delta",
            turn_id: "turn-live",
            delta: "streamed live",
          },
        },
        {
          timestamp: "2026-04-08T12:55:02.000Z",
          type: "event_msg",
          payload: {
            type: "task_complete",
            turn_id: "turn-live",
            last_agent_message: "done",
          },
        },
      ].map((line) => JSON.stringify(line)).join("\n"),
    );

    const firstTwo = runCliJson("list-rollout-events", undefined, [
      "--codex-home",
      codexHome,
      "--session-id",
      "019d0000-0000-7000-8000-000000000004",
      "--limit",
      "2",
    ]);
    expect(firstTwo).toHaveLength(2);
    expect(firstTwo[0]).toMatchObject({
      absolutePath: rolloutPath,
      lineNumber: 1,
      recordType: "session_meta",
      sessionId: "019d0000-0000-7000-8000-000000000004",
    });
    expect(firstTwo[1]).toMatchObject({
      lineNumber: 2,
      recordType: "event_msg",
      eventType: "agent_message_content_delta",
      turnId: "turn-live",
    });
    expect(firstTwo[1].byteOffset).toBeGreaterThan(firstTwo[0].byteOffset);

    const afterCursor = runCliJson("list-rollout-events", undefined, [
      "--absolute-path",
      rolloutPath,
      "--after-line-number",
      "2",
    ]);
    expect(afterCursor).toHaveLength(1);
    expect(afterCursor[0]).toMatchObject({
      lineNumber: 3,
      eventType: "task_complete",
      turnId: "turn-live",
    });
  }, 15_000);

  it("records and lists durable stream events through the CLI", () => {
    const recorded = runCliJson("record-event", {
      sessionId: "session-parent",
      turnId: "turn-parent",
      sequence: 1,
      eventKind: "agent_message",
      eventType: "thread/tail/frame",
      sourceHost: "op1",
      textDelta: "hello",
      schemaHash: "schema-hash-cli",
      createdAt: "2026-05-02T12:00:00.000Z",
      observedAt: "2026-05-02T12:00:00.010Z",
    });
    expect(recorded).toMatchObject({
      sessionId: "session-parent",
      turnId: "turn-parent",
      sequence: 1,
      eventKind: "agent_message",
      textDelta: "hello",
      schemaHash: "schema-hash-cli",
    });

    const events = runCliJson("list-stream-events", undefined, [
      "--session-id",
      "session-parent",
      "--after-sequence",
      "0",
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      eventId: recorded.eventId,
      sessionId: "session-parent",
      turnId: "turn-parent",
      sequence: 1,
      textDelta: "hello",
    });

    const appServerRecorded = runCliJson("record-event", {
      session_id: "thread-from-app-server",
      turn_id: "turn-from-app-server",
      sequence: 4,
      event_kind: "agentMessage",
      event_type: "thread/tail/frame",
      source_id: "codex-app-server:test",
      source_host: "linux-test",
      source_path: "/srv/codex/openai/codex",
      text_delta: "from app-server",
      payload_json: { delta: "from app-server" },
      raw_json: { method: "thread/tail/frame" },
      schema_hash: "schema-hash-snake",
      created_at: "2026-05-02T12:00:01.000Z",
      observed_at: "2026-05-02T12:00:01.010Z",
    });
    expect(appServerRecorded).toMatchObject({
      sessionId: "thread-from-app-server",
      turnId: "turn-from-app-server",
      sequence: 4,
      eventKind: "agentMessage",
      eventType: "thread/tail/frame",
      sourceId: "codex-app-server:test",
      sourceHost: "linux-test",
      sourcePath: "/srv/codex/openai/codex",
      textDelta: "from app-server",
      schemaHash: "schema-hash-snake",
    });

    runCliJson("record-event", {
      sessionId: "session-parent",
      turnId: "turn-parent",
      sequence: 2,
      eventKind: "agent_message",
      eventType: "thread/tail/frame",
      textDelta: "older stream frame",
      payloadJson: { large: "x".repeat(10_000) },
      rawJson: { large: "x".repeat(10_000) },
      createdAt: "2026-05-02T12:00:02.000Z",
      observedAt: "2026-05-02T12:00:02.010Z",
    });
    runCliJson("record-event", {
      sessionId: "session-parent",
      turnId: "turn-parent",
      sequence: 3,
      eventKind: "agent_message",
      eventType: "thread/tail/frame",
      textDelta: "latest stream frame",
      payloadJson: { large: "x".repeat(10_000) },
      rawJson: { large: "x".repeat(10_000) },
      createdAt: "2026-05-02T12:00:03.000Z",
      observedAt: "2026-05-02T12:00:03.010Z",
    });
    const latest = runCliJson("list-stream-events", undefined, [
      "--session-id",
      "session-parent",
      "--latest",
      "true",
      "--limit",
      "1",
      "--include-payload",
      "false",
    ]);
    expect(latest).toEqual([
      expect.objectContaining({
        sessionId: "session-parent",
        sequence: 3,
        textDelta: "latest stream frame",
      }),
    ]);
    expect(latest[0].payloadJson).toBeUndefined();
    expect(latest[0].rawJson).toBeUndefined();
  }, 15_000);

  it("replicates appended rollout events into the durable stream table", async () => {
    const codexHome = join(tempDir, ".codex");
    const rolloutDir = join(codexHome, "sessions/2026/05/02");
    const rolloutPath = join(
      rolloutDir,
      "rollout-2026-05-02T20-00-00-019d0000-0000-7000-8000-000000000005.jsonl",
    );
    await mkdir(rolloutDir, { recursive: true });
    await writeFile(
      rolloutPath,
      `${JSON.stringify({
        timestamp: "2026-05-02T20:00:00.000Z",
        type: "session_meta",
        payload: {
          id: "019d0000-0000-7000-8000-000000000005",
          timestamp: "2026-05-02T20:00:00.000Z",
          cwd: "/srv/codex/openai/codex",
        },
      })}\n`,
    );

    const child = spawn(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "replicate-rollout-events",
        "--data-path",
        dataPath,
        "--absolute-path",
        rolloutPath,
        "--follow",
        "true",
        "--idle-timeout-ms",
        "500",
        "--poll-interval-ms",
        "25",
        "--source-host",
        "linux-test",
      ],
      {
        cwd: packageRoot,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );

    let stdout = "";
    let stderr = "";
    let appended = false;
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdout += chunk;
      if (!appended && stdout.includes("\"sequence\":1")) {
        appended = true;
        void appendFile(
          rolloutPath,
          `${JSON.stringify({
            timestamp: "2026-05-02T20:00:01.000Z",
            type: "event_msg",
            payload: {
              type: "agent_message_content_delta",
              turn_id: "turn-remote",
              delta: "remote live delta",
            },
          })}\n`,
        );
      }
    });
    child.stderr.on("data", (chunk: string) => {
      stderr += chunk;
    });

    await new Promise<void>((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code) => {
        if (code === 0) {
          resolve();
          return;
        }
        reject(new Error(stderr || stdout || `replicate-rollout-events exited ${code}`));
      });
    });

    const events = runCliJson("list-stream-events", undefined, [
      "--session-id",
      "019d0000-0000-7000-8000-000000000005",
    ]);
    expect(events).toHaveLength(2);
    expect(events[0].payloadJson).toBeUndefined();
    expect(events[0].rawJson).toBeUndefined();
    expect(events[1]).toMatchObject({
      sequence: 2,
      eventKind: "event_msg",
      eventType: "agent_message_content_delta",
      turnId: "turn-remote",
      sourceHost: "linux-test",
      textDelta: "remote live delta",
    });
  }, 20_000);

  it("hydrates a missing session on demand through get-session", async () => {
    const codexHome = join(tempDir, ".codex");
    const rolloutDir = join(codexHome, "sessions/2026/04/08");
    const rolloutPath = join(
      rolloutDir,
      "rollout-2026-04-08T12-50-00-019d0000-0000-7000-8000-000000000003.jsonl",
    );
    await mkdir(rolloutDir, { recursive: true });
    await writeFile(
      rolloutPath,
      [
        {
          timestamp: "2026-04-08T12:50:00.000Z",
          type: "session_meta",
          payload: {
            id: "019d0000-0000-7000-8000-000000000003",
            timestamp: "2026-04-08T12:50:00.000Z",
            cwd: "/Users/nikitavoloboev/repos/openai/codex",
            originator: "codex-cli",
            cli_version: "0.0.0",
            source: "cli",
          },
        },
        {
          timestamp: "2026-04-08T12:50:01.000Z",
          type: "turn_context",
          payload: {
            turn_id: "turn-3",
            cwd: "/Users/nikitavoloboev/repos/openai/codex",
          },
        },
        {
          timestamp: "2026-04-08T12:50:02.000Z",
          type: "event_msg",
          payload: {
            type: "user_message",
            message: "Hydrate this session on demand",
          },
        },
      ].map((line) => JSON.stringify(line)).join("\n"),
    );

    const session = runCliJson("get-session", undefined, [
      "--codex-home",
      codexHome,
      "--session-id",
      "019d0000-0000-7000-8000-000000000003",
    ]);

    expect(session.id).toBe("019d0000-0000-7000-8000-000000000003");
    expect(session.preview).toBe("Hydrate this session on demand");
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

import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import { existsSync } from "node:fs";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { createConnection } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createCodexSessionStore, syncCodexRollouts, type CodexSessionStore } from "../src/index.js";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));

describe("codex completion session service", () => {
  let tempDir: string;
  let dataPath: string;
  let socketPath: string;
  let codexHome: string;
  let rolloutPath: string;
  let store: CodexSessionStore | null;
  let serviceProcess: ChildProcessWithoutNullStreams | null;
  let serviceStderr = "";

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "csvc-"));
    dataPath = join(tempDir, "codex-sessions.db");
    socketPath = join(tempDir, "c.sock");
    codexHome = join(tempDir, ".codex");
    const rolloutDir = join(codexHome, "sessions/2026/04/08");
    rolloutPath = join(
      rolloutDir,
      "rollout-2026-04-08T12-00-00-019d0000-0000-7000-8000-000000000002.jsonl",
    );
    await mkdir(rolloutDir, { recursive: true });
    store = createCodexSessionStore({
      appId: "codex-session-completion-service-test",
      dataPath,
    });
    serviceProcess = null;
    serviceStderr = "";
  });

  afterEach(async () => {
    if (serviceProcess && serviceProcess.exitCode == null && !serviceProcess.killed) {
      serviceProcess.kill("SIGTERM");
      await once(serviceProcess, "exit");
    }
    serviceProcess = null;
    if (store) {
      await store.shutdown();
      store = null;
    }
    await rm(tempDir, { recursive: true, force: true });
  });

  it("lists completion events over the local session socket", async () => {
    await writeFile(
      rolloutPath,
      [
        JSON.stringify({
          timestamp: "2026-04-08T12:00:00.000Z",
          type: "session_meta",
          payload: {
            id: "019d0000-0000-7000-8000-000000000002",
            timestamp: "2026-04-08T12:00:00.000Z",
            cwd: "/tmp/demo-three",
            source: "codex",
          },
        }),
        JSON.stringify({
          timestamp: "2026-04-08T12:00:01.000Z",
          type: "event_msg",
          payload: {
            type: "task_started",
            turn_id: "turn-7",
            started_at: 1775649601,
          },
        }),
        JSON.stringify({
          timestamp: "2026-04-08T12:00:02.000Z",
          type: "event_msg",
          payload: {
            type: "task_complete",
            turn_id: "turn-7",
            last_agent_message: "Session socket completion is live.",
            completed_at: 1775649602,
            duration_ms: 1000,
          },
        }),
      ].join("\n"),
    );

    await syncCodexRollouts({ codexHome, store: store! });
    await store!.shutdown();
    store = null;

    serviceProcess = spawn(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "serve",
        "--data-path",
        dataPath,
        "--socket-path",
        socketPath,
        "--codex-home",
        codexHome,
        "--watch-rollouts",
        "false",
      ],
      {
        cwd: packageRoot,
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    const process = serviceProcess;
    process.stderr.setEncoding("utf8");
    process.stderr.on("data", (chunk: string) => {
      serviceStderr += chunk;
    });

    await waitForSocket(socketPath, process, () => serviceStderr);

    const response = await sendSocketRequest(socketPath, {
      id: "req-1",
      method: "list-completions",
      completedAfter: "2026-04-08T12:00:01.500Z",
      limit: 10,
    });

    expect(response).toMatchObject({
      id: "req-1",
      ok: true,
    });
    expect(response.result).toEqual([
      {
        id: "019d0000-0000-7000-8000-000000000002-turn-7",
        source: "codex",
        sessionId: "019d0000-0000-7000-8000-000000000002",
        turnId: "turn-7",
        projectPath: "/tmp/demo-three",
        projectName: "demo-three",
        summary: "Session socket completion is live.",
        status: "completed",
        timestamp: "2026-04-08T12:00:02.000Z",
        completedAt: "2026-04-08T12:00:02.000Z",
        updatedAt: "2026-04-08T12:00:02.000Z",
      },
    ]);
  }, 30_000);

  it("answers health without opening the Jazz store when rollout watchers are disabled", async () => {
    const coldDataPath = join(tempDir, "cold-health.db");
    serviceProcess = spawn(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "serve",
        "--data-path",
        coldDataPath,
        "--socket-path",
        socketPath,
        "--codex-home",
        codexHome,
        "--watch-rollouts",
        "false",
        "--watch-stream-rollouts",
        "false",
        "--warm-stream-store",
        "false",
        "--poll-interval-ms",
        "50",
      ],
      {
        cwd: packageRoot,
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    const process = serviceProcess;
    process.stderr.setEncoding("utf8");
    process.stderr.on("data", (chunk: string) => {
      serviceStderr += chunk;
    });

    await waitForSocket(socketPath, process, () => serviceStderr);

    const health = await sendSocketRequest(socketPath, {
      id: "health-no-watchers",
      method: "health",
    });
    expect(health).toMatchObject({
      id: "health-no-watchers",
      ok: true,
      result: {
        status: "ok",
        watchRollouts: false,
        watchStreamRollouts: false,
      },
    });
    expect(existsSync(coldDataPath)).toBe(false);
  }, 30_000);

  it("automatically watches rollout projections and stream events by default", async () => {
    serviceProcess = spawn(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "serve",
        "--data-path",
        dataPath,
        "--socket-path",
        socketPath,
        "--codex-home",
        codexHome,
        "--poll-interval-ms",
        "50",
      ],
      {
        cwd: packageRoot,
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    const process = serviceProcess;
    process.stderr.setEncoding("utf8");
    process.stderr.on("data", (chunk: string) => {
      serviceStderr += chunk;
    });

    await waitForSocket(socketPath, process, () => serviceStderr);

    const health = await sendSocketRequest(socketPath, {
      id: "health-default-watchers",
      method: "health",
    });
    expect(health).toMatchObject({
      id: "health-default-watchers",
      ok: true,
      result: {
        status: "ok",
        watchRollouts: true,
        watchStreamRollouts: true,
      },
    });
  }, 30_000);

  it("records stream events over the socket without waiting for the sync server", async () => {
    serviceProcess = spawn(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "serve",
        "--data-path",
        dataPath,
        "--socket-path",
        socketPath,
        "--codex-home",
        codexHome,
        "--watch-rollouts",
        "false",
        "--server-url",
        "https://203.0.113.1",
        "--tier",
        "edge",
      ],
      {
        cwd: packageRoot,
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    const process = serviceProcess;
    process.stderr.setEncoding("utf8");
    process.stderr.on("data", (chunk: string) => {
      serviceStderr += chunk;
    });

    await waitForSocket(socketPath, process, () => serviceStderr);
    const startedAt = Date.now();
    const response = await Promise.race([
      sendSocketRequest(socketPath, {
        id: "stream-record",
        method: "record-event",
        payload: {
          session_id: "socket-stream-session",
          turn_id: "socket-stream-turn",
          sequence: 1,
          event_kind: "agentMessage",
          event_type: "thread/tail/frame",
          source_id: "codex-app-server:test",
          text_delta: "socket persisted",
          created_at: "2026-05-02T22:00:00.000Z",
          observed_at: "2026-05-02T22:00:00.010Z",
        },
      }),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("record-event timed out")), 1_000),
      ),
    ]);
    expect(Date.now() - startedAt).toBeLessThan(1_000);
    expect(response).toMatchObject({
      id: "stream-record",
      ok: true,
      result: {
        sessionId: "socket-stream-session",
        turnId: "socket-stream-turn",
        sequence: 1,
        eventKind: "agentMessage",
        textDelta: "socket persisted",
      },
    });

    const listed = await sendSocketRequest(socketPath, {
      id: "stream-list",
      method: "list-stream-events",
      sessionId: "socket-stream-session",
      limit: 5,
    });
    expect(listed).toMatchObject({ id: "stream-list", ok: true });
    expect(listed.result).toMatchObject([
      {
        sessionId: "socket-stream-session",
        turnId: "socket-stream-turn",
        sequence: 1,
        textDelta: "socket persisted",
      },
    ]);
    expect(existsSync(join(tempDir, "codex-sessions.stream.db"))).toBe(true);
  }, 30_000);

  it("replicates recent rollout stream events into the stream sidecar while serving", async () => {
    const today = new Date();
    const sessionId = "019d0000-0000-7000-8000-000000000092";
    const todayRolloutDir = join(
      codexHome,
      "sessions",
      String(today.getFullYear()),
      String(today.getMonth() + 1).padStart(2, "0"),
      String(today.getDate()).padStart(2, "0"),
    );
    const liveRolloutPath = join(
      todayRolloutDir,
      `rollout-2026-05-02T12-00-00-${sessionId}.jsonl`,
    );
    await mkdir(todayRolloutDir, { recursive: true });

    serviceProcess = spawn(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "serve",
        "--data-path",
        dataPath,
        "--socket-path",
        socketPath,
        "--codex-home",
        codexHome,
        "--watch-rollouts",
        "false",
        "--watch-stream-rollouts",
        "true",
        "--poll-interval-ms",
        "50",
      ],
      {
        cwd: packageRoot,
        env: {
          ...globalThis.process.env,
          FLOW_CODEX_SESSION_STREAM_WATCH_BOOTSTRAP_MODE: "backfill",
        },
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    const process = serviceProcess;
    process.stderr.setEncoding("utf8");
    process.stderr.on("data", (chunk: string) => {
      serviceStderr += chunk;
    });

    await waitForSocket(socketPath, process, () => serviceStderr);

    const now = new Date().toISOString();
    await writeFile(
      liveRolloutPath,
      [
        JSON.stringify({
          timestamp: now,
          type: "session_meta",
          payload: {
            id: sessionId,
            timestamp: now,
            cwd: "/tmp/stream-sidecar-rollout",
            source: "codex",
          },
        }),
        JSON.stringify({
          timestamp: now,
          type: "event_msg",
          payload: {
            type: "agent_message_delta",
            turn_id: "turn-stream-sidecar",
            delta: "rollout sidecar",
          },
        }),
      ].join("\n"),
    );

    const events = await waitForStreamEvents(socketPath, sessionId, 2);
    expect(events).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          sessionId,
          sequence: 2,
          eventKind: "event_msg",
          eventType: "agent_message_delta",
          turnId: "turn-stream-sidecar",
          textDelta: "rollout sidecar",
        }),
      ]),
    );
    expect(existsSync(join(tempDir, "codex-sessions.stream.db"))).toBe(true);
  }, 30_000);

  it("resumes stream rollout replication from the latest recorded Jazz2 sequence", async () => {
    const today = new Date();
    const sessionId = "019d0000-0000-7000-8000-000000000093";
    const todayRolloutDir = join(
      codexHome,
      "sessions",
      String(today.getFullYear()),
      String(today.getMonth() + 1).padStart(2, "0"),
      String(today.getDate()).padStart(2, "0"),
    );
    const liveRolloutPath = join(
      todayRolloutDir,
      `rollout-2026-05-02T12-00-00-${sessionId}.jsonl`,
    );
    await mkdir(todayRolloutDir, { recursive: true });
    const now = new Date().toISOString();
    await writeFile(
      liveRolloutPath,
      [
        JSON.stringify({
          timestamp: now,
          type: "session_meta",
          payload: {
            id: sessionId,
            timestamp: now,
            cwd: "/tmp/stream-resume-rollout",
            source: "codex",
          },
        }),
        JSON.stringify({
          timestamp: now,
          type: "event_msg",
          payload: {
            type: "agent_message_delta",
            turn_id: "turn-stream-resume",
            delta: "missed while worker was down",
          },
        }),
        JSON.stringify({
          timestamp: now,
          type: "event_msg",
          payload: {
            type: "agent_message_delta",
            turn_id: "turn-stream-resume",
            delta: "caught after restart",
          },
        }),
      ].join("\n"),
    );

    const streamStore = createCodexSessionStore({
      dataPath: join(tempDir, "codex-sessions.stream.db"),
    });
    await streamStore.recordCodexStreamEvent({
      eventId: "seeded-session-meta",
      sessionId,
      sequence: 1,
      eventKind: "session_meta",
      eventType: "session_meta",
      sourceId: liveRolloutPath,
      sourcePath: liveRolloutPath,
      schemaHash: "schema-hash-1",
      createdAt: now,
      observedAt: now,
    });
    await streamStore.shutdown();

    serviceProcess = spawn(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "serve",
        "--data-path",
        dataPath,
        "--socket-path",
        socketPath,
        "--codex-home",
        codexHome,
        "--watch-rollouts",
        "false",
        "--watch-stream-rollouts",
        "true",
        "--poll-interval-ms",
        "50",
      ],
      {
        cwd: packageRoot,
        env: {
          ...globalThis.process.env,
          FLOW_CODEX_SESSION_STREAM_WATCH_BOOTSTRAP_MODE: "tail",
        },
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    const process = serviceProcess;
    process.stderr.setEncoding("utf8");
    process.stderr.on("data", (chunk: string) => {
      serviceStderr += chunk;
    });

    await waitForSocket(socketPath, process, () => serviceStderr);

    const events = await waitForStreamEvents(socketPath, sessionId, 3);
    expect(events).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          sessionId,
          sequence: 2,
          eventKind: "event_msg",
          eventType: "agent_message_delta",
          textDelta: "missed while worker was down",
        }),
        expect.objectContaining({
          sessionId,
          sequence: 3,
          eventKind: "event_msg",
          eventType: "agent_message_delta",
          textDelta: "caught after restart",
        }),
      ]),
    );
  }, 30_000);

  it("captures a new rollout file in tail bootstrap mode from its first line", async () => {
    const today = new Date();
    const sessionId = "019d0000-0000-7000-8000-000000000094";
    const todayRolloutDir = join(
      codexHome,
      "sessions",
      String(today.getFullYear()),
      String(today.getMonth() + 1).padStart(2, "0"),
      String(today.getDate()).padStart(2, "0"),
    );
    const liveRolloutPath = join(
      todayRolloutDir,
      `rollout-2026-05-02T12-00-00-${sessionId}.jsonl`,
    );
    await mkdir(todayRolloutDir, { recursive: true });

    serviceProcess = spawn(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "serve",
        "--data-path",
        dataPath,
        "--socket-path",
        socketPath,
        "--codex-home",
        codexHome,
        "--watch-rollouts",
        "false",
        "--watch-stream-rollouts",
        "true",
        "--poll-interval-ms",
        "50",
      ],
      {
        cwd: packageRoot,
        env: {
          ...globalThis.process.env,
          FLOW_CODEX_SESSION_STREAM_WATCH_BOOTSTRAP_MODE: "tail",
        },
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    const process = serviceProcess;
    process.stderr.setEncoding("utf8");
    process.stderr.on("data", (chunk: string) => {
      serviceStderr += chunk;
    });

    await waitForSocket(socketPath, process, () => serviceStderr);

    const now = new Date().toISOString();
    await writeFile(
      liveRolloutPath,
      [
        JSON.stringify({
          timestamp: now,
          type: "session_meta",
          payload: {
            id: sessionId,
            timestamp: now,
            cwd: "/tmp/stream-tail-new-rollout",
            source: "codex",
          },
        }),
        JSON.stringify({
          timestamp: now,
          type: "event_msg",
          payload: {
            type: "agent_message_delta",
            turn_id: "turn-stream-tail-new",
            delta: "tail mode first write",
          },
        }),
      ].join("\n"),
    );

    const events = await waitForStreamEvents(socketPath, sessionId, 2);
    expect(events).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          sessionId,
          sequence: 1,
          eventKind: "session_meta",
        }),
        expect.objectContaining({
          sessionId,
          sequence: 2,
          eventKind: "event_msg",
          eventType: "agent_message_delta",
          textDelta: "tail mode first write",
        }),
      ]),
    );
  }, 30_000);

  it("streams completion events over the local session socket when the legacy file watcher is enabled", async () => {
    const today = new Date();
    const todayRolloutDir = join(
      codexHome,
      "sessions",
      String(today.getFullYear()),
      String(today.getMonth() + 1).padStart(2, "0"),
      String(today.getDate()).padStart(2, "0"),
    );
    const liveRolloutPath = join(
      todayRolloutDir,
      "rollout-2026-04-08T12-00-00-019d0000-0000-7000-8000-000000000002.jsonl",
    );
    await mkdir(todayRolloutDir, { recursive: true });

    serviceProcess = spawn(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "serve",
        "--data-path",
        dataPath,
        "--socket-path",
        socketPath,
        "--codex-home",
        codexHome,
        "--watch-rollouts",
        "true",
        "--poll-interval-ms",
        "50",
      ],
      {
        cwd: packageRoot,
        env: {
          ...globalThis.process.env,
          FLOW_CODEX_SESSION_FILE_COMPLETION_WATCH: "1",
        },
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    const process = serviceProcess;
    process.stderr.setEncoding("utf8");
    process.stderr.on("data", (chunk: string) => {
      serviceStderr += chunk;
    });

    await waitForSocket(socketPath, process, () => serviceStderr);

    const eventPromise = waitForCompletionStreamEvent(socketPath, {
      id: "watch-1",
      method: "watch-completions",
      completedAfter: "1970-01-01T00:00:00.000Z",
      limit: 10,
    });

    const startedAt = new Date();
    const completedAt = new Date(startedAt.getTime() + 1000);
    await writeFile(
      liveRolloutPath,
      [
        JSON.stringify({
          timestamp: startedAt.toISOString(),
          type: "session_meta",
          payload: {
            id: "019d0000-0000-7000-8000-000000000002",
            timestamp: startedAt.toISOString(),
            cwd: "/tmp/demo-stream",
            source: "codex",
          },
        }),
        JSON.stringify({
          timestamp: startedAt.toISOString(),
          type: "event_msg",
          payload: {
            type: "task_started",
            turn_id: "turn-8",
            started_at: Math.floor(startedAt.getTime() / 1000),
          },
        }),
        JSON.stringify({
          timestamp: completedAt.toISOString(),
          type: "event_msg",
          payload: {
            type: "task_complete",
            turn_id: "turn-8",
            last_agent_message: "Streamed completion is live.",
            completed_at: Math.floor(completedAt.getTime() / 1000),
            duration_ms: 1000,
          },
        }),
      ].join("\n"),
    );

    const event = await eventPromise;
    expect(event).toEqual({
      id: "019d0000-0000-7000-8000-000000000002-turn-8",
      source: "codex",
      sessionId: "019d0000-0000-7000-8000-000000000002",
      turnId: "turn-8",
      projectPath: "/tmp/demo-stream",
      projectName: "demo-stream",
      summary: "Streamed completion is live.",
      status: "completed",
      timestamp: new Date(Math.floor(completedAt.getTime() / 1000) * 1000).toISOString(),
      completedAt: new Date(Math.floor(completedAt.getTime() / 1000) * 1000).toISOString(),
      updatedAt: completedAt.toISOString(),
    });
  }, 30_000);

  it("publishes a completion event when j sync-session updates the store", async () => {
    serviceProcess = spawn(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "serve",
        "--data-path",
        dataPath,
        "--socket-path",
        socketPath,
        "--codex-home",
        codexHome,
        "--watch-rollouts",
        "false",
        "--poll-interval-ms",
        "50",
      ],
      {
        cwd: packageRoot,
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    const process = serviceProcess;
    process.stderr.setEncoding("utf8");
    process.stderr.on("data", (chunk: string) => {
      serviceStderr += chunk;
    });

    await waitForSocket(socketPath, process, () => serviceStderr);

    const eventPromise = waitForCompletionStreamEvent(socketPath, {
      id: "watch-2",
      method: "watch-completions",
      completedAfter: "2026-04-08T12:00:01.500Z",
      limit: 10,
    });

    await writeFile(
      rolloutPath,
      [
        JSON.stringify({
          timestamp: "2026-04-08T12:00:00.000Z",
          type: "session_meta",
          payload: {
            id: "019d0000-0000-7000-8000-000000000002",
            timestamp: "2026-04-08T12:00:00.000Z",
            cwd: "/tmp/demo-sync-session",
            source: "codex",
          },
        }),
        JSON.stringify({
          timestamp: "2026-04-08T12:00:01.000Z",
          type: "event_msg",
          payload: {
            type: "task_started",
            turn_id: "turn-9",
            started_at: 1775649601,
          },
        }),
        JSON.stringify({
          timestamp: "2026-04-08T12:00:02.000Z",
          type: "event_msg",
          payload: {
            type: "task_complete",
            turn_id: "turn-9",
            last_agent_message: "sync-session published this completion.",
            completed_at: 1775649602,
            duration_ms: 1000,
          },
        }),
      ].join("\n"),
    );

    const syncResponse = await sendSocketRequest(socketPath, {
      id: "sync-1",
      method: "sync-session",
      sessionId: "019d0000-0000-7000-8000-000000000002",
    });
    expect(syncResponse).toMatchObject({
      id: "sync-1",
      ok: true,
      result: {
        found: true,
      },
    });

    const event = await eventPromise;
    expect(event).toEqual({
      id: "019d0000-0000-7000-8000-000000000002-turn-9",
      source: "codex",
      sessionId: "019d0000-0000-7000-8000-000000000002",
      turnId: "turn-9",
      projectPath: "/tmp/demo-sync-session",
      projectName: "demo-sync-session",
      summary: "sync-session published this completion.",
      status: "completed",
      timestamp: "2026-04-08T12:00:02.000Z",
      completedAt: "2026-04-08T12:00:02.000Z",
      updatedAt: "2026-04-08T12:00:02.000Z",
    });
  }, 30_000);

  it("lists newly started active sessions even when older active sessions are already cached", async () => {
    const olderStartedAt = new Date(Date.now() - 60_000);
    const newerStartedAt = new Date(Date.now() - 15_000);
    const olderRolloutPath = join(
      join(codexHome, "sessions/2026/04/08"),
      "rollout-2026-04-08T11-00-00-019d0000-0000-7000-8000-000000000011.jsonl",
    );
    await writeFile(
      olderRolloutPath,
      [
        JSON.stringify({
          timestamp: olderStartedAt.toISOString(),
          type: "session_meta",
          payload: {
            id: "019d0000-0000-7000-8000-000000000011",
            timestamp: olderStartedAt.toISOString(),
            cwd: "/tmp/older-active",
            source: "codex",
          },
        }),
        JSON.stringify({
          timestamp: new Date(olderStartedAt.getTime() + 1_000).toISOString(),
          type: "event_msg",
          payload: {
            type: "task_started",
            turn_id: "turn-older-active",
            started_at: Math.floor(olderStartedAt.getTime() / 1000) + 1,
          },
        }),
      ].join("\n"),
    );

    await syncCodexRollouts({ codexHome, store: store! });
    await store!.shutdown();
    store = null;

    serviceProcess = spawn(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "serve",
        "--data-path",
        dataPath,
        "--socket-path",
        socketPath,
        "--codex-home",
        codexHome,
        "--watch-rollouts",
        "false",
      ],
      {
        cwd: packageRoot,
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    const process = serviceProcess;
    process.stderr.setEncoding("utf8");
    process.stderr.on("data", (chunk: string) => {
      serviceStderr += chunk;
    });

    await waitForSocket(socketPath, process, () => serviceStderr);

    const newerRolloutPath = join(
      join(codexHome, "sessions/2026/04/08"),
      "rollout-2026-04-08T11-05-00-019d0000-0000-7000-8000-000000000012.jsonl",
    );
    await writeFile(
      newerRolloutPath,
      [
        JSON.stringify({
          timestamp: newerStartedAt.toISOString(),
          type: "session_meta",
          payload: {
            id: "019d0000-0000-7000-8000-000000000012",
            timestamp: newerStartedAt.toISOString(),
            cwd: "/tmp/newer-active",
            source: "codex",
          },
        }),
        JSON.stringify({
          timestamp: new Date(newerStartedAt.getTime() + 1_000).toISOString(),
          type: "event_msg",
          payload: {
            type: "task_started",
            turn_id: "turn-newer-active",
            started_at: Math.floor(newerStartedAt.getTime() / 1000) + 1,
          },
        }),
      ].join("\n"),
    );

    const response = await sendSocketRequest(socketPath, {
      id: "req-active-latest",
      method: "list-active-sessions",
      limit: 10,
    });

    expect(response).toMatchObject({
      id: "req-active-latest",
      ok: true,
    });
    expect(response.result).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: "019d0000-0000-7000-8000-000000000012",
          cwd: "/tmp/newer-active",
        }),
      ]),
    );
  }, 30_000);

  it("rejects a duplicate serve startup without breaking the first socket", async () => {
    serviceProcess = spawn(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "serve",
        "--data-path",
        dataPath,
        "--socket-path",
        socketPath,
        "--codex-home",
        codexHome,
        "--watch-rollouts",
        "false",
      ],
      {
        cwd: packageRoot,
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    const process = serviceProcess;
    process.stderr.setEncoding("utf8");
    process.stderr.on("data", (chunk: string) => {
      serviceStderr += chunk;
    });

    await waitForSocket(socketPath, process, () => serviceStderr);
    expect(existsSync(`${socketPath}.lock`)).toBe(true);

    let secondStderr = "";
    const secondProcess = spawn(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "serve",
        "--data-path",
        dataPath,
        "--socket-path",
        socketPath,
        "--codex-home",
        codexHome,
        "--watch-rollouts",
        "false",
      ],
      {
        cwd: packageRoot,
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    secondProcess.stderr.setEncoding("utf8");
    secondProcess.stderr.on("data", (chunk: string) => {
      secondStderr += chunk;
    });

    await once(secondProcess, "exit");
    expect(secondProcess.exitCode).not.toBe(0);
    expect(secondStderr).toContain("session service already running");

    const health = await sendSocketRequest(socketPath, {
      id: "health-1",
      method: "health",
    });
    expect(health).toMatchObject({
      id: "health-1",
      ok: true,
      result: {
        status: "ok",
      },
    });
  }, 30_000);

  it("reclaims a stale lock from a live pid when no socket ever comes up", async () => {
    await writeFile(
      `${socketPath}.lock`,
      `${JSON.stringify({
        pid: process.pid,
        socketPath,
        startedAt: "2026-01-01T00:00:00.000Z",
      })}\n`,
    );

    serviceProcess = spawn(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "serve",
        "--data-path",
        dataPath,
        "--socket-path",
        socketPath,
        "--codex-home",
        codexHome,
        "--watch-rollouts",
        "false",
      ],
      {
        cwd: packageRoot,
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    const serviceChild = serviceProcess;
    serviceChild.stderr.setEncoding("utf8");
    serviceChild.stderr.on("data", (chunk: string) => {
      serviceStderr += chunk;
    });

    await waitForSocket(socketPath, serviceChild, () => serviceStderr);
    expect(serviceStderr).toContain("reclaiming stale Jazz2 session service lock");

    const health = await sendSocketRequest(socketPath, {
      id: "health-stale",
      method: "health",
    });
    expect(health).toMatchObject({
      id: "health-stale",
      ok: true,
      result: {
        status: "ok",
      },
    });
  }, 30_000);
});

async function waitForSocket(
  socketPath: string,
  process: ChildProcessWithoutNullStreams,
  stderr: () => string,
): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (existsSync(socketPath)) {
      return;
    }
    if (process.exitCode != null) {
      throw new Error(stderr() || `session service exited with code ${process.exitCode}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`timed out waiting for session socket at ${socketPath}\n${stderr()}`);
}

async function sendSocketRequest(
  socketPath: string,
  request: Record<string, unknown>,
): Promise<{ id?: string; ok: boolean; result?: unknown; error?: string }> {
  return await new Promise((resolve, reject) => {
    const socket = createConnection({ path: socketPath });
    let buffer = "";
    let settled = false;

    const finish = (callback: () => void) => {
      if (settled) {
        return;
      }
      settled = true;
      callback();
    };

    socket.setEncoding("utf8");
    socket.once("error", (error) => {
      finish(() => reject(error));
    });
    socket.on("data", (chunk: string) => {
      buffer += chunk;
      const newlineIndex = buffer.indexOf("\n");
      if (newlineIndex === -1) {
        return;
      }
      const rawLine = buffer.slice(0, newlineIndex).trim();
      finish(() => {
        socket.end();
        try {
          resolve(JSON.parse(rawLine) as { id?: string; ok: boolean; result?: unknown; error?: string });
        } catch (error) {
          reject(error);
        }
      });
    });
    socket.once("connect", () => {
      socket.write(`${JSON.stringify(request)}\n`);
    });
  });
}

async function waitForCompletionStreamEvent(
  socketPath: string,
  request: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  return await new Promise((resolve, reject) => {
    const socket = createConnection({ path: socketPath });
    let buffer = "";
    let subscribed = false;
    let settled = false;

    const finish = (callback: () => void) => {
      if (settled) {
        return;
      }
      settled = true;
      socket.end();
      callback();
    };

    socket.setEncoding("utf8");
    socket.once("error", (error) => {
      finish(() => reject(error));
    });
    socket.on("data", (chunk: string) => {
      buffer += chunk;
      while (true) {
        const newlineIndex = buffer.indexOf("\n");
        if (newlineIndex === -1) {
          return;
        }
        const rawLine = buffer.slice(0, newlineIndex).trim();
        buffer = buffer.slice(newlineIndex + 1);
        if (!rawLine) {
          continue;
        }

        try {
          const parsed = JSON.parse(rawLine) as {
            ok: boolean;
            result?: { status?: string };
            event?: Record<string, unknown>;
            error?: string;
          };
          if (!parsed.ok) {
            finish(() => reject(new Error(parsed.error ?? "stream request failed")));
            return;
          }
          if (!subscribed) {
            subscribed = parsed.result?.status === "subscribed";
            continue;
          }
          if (parsed.event) {
            finish(() => resolve(parsed.event!));
            return;
          }
        } catch (error) {
          finish(() => reject(error));
          return;
        }
      }
    });
    socket.once("connect", () => {
      socket.write(`${JSON.stringify(request)}\n`);
    });
  });
}

async function waitForStreamEvents(
  socketPath: string,
  sessionId: string,
  minCount: number,
): Promise<Record<string, unknown>[]> {
  const deadline = Date.now() + 10_000;
  let lastResponse: { id?: string; ok: boolean; result?: unknown; error?: string } | null = null;
  while (Date.now() < deadline) {
    lastResponse = await sendSocketRequest(socketPath, {
      id: "wait-stream-events",
      method: "list-stream-events",
      sessionId,
      limit: 20,
    });
    if (!lastResponse.ok) {
      throw new Error(lastResponse.error ?? "list-stream-events failed");
    }
    const events = Array.isArray(lastResponse.result)
      ? lastResponse.result as Record<string, unknown>[]
      : [];
    if (events.length >= minCount) {
      return events;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`timed out waiting for stream events: ${JSON.stringify(lastResponse)}`);
}

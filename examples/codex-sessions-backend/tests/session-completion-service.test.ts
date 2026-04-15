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

  it("streams completion events over the local session socket", async () => {
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
            cwd: "/tmp/demo-stream",
            source: "codex",
          },
        }),
        JSON.stringify({
          timestamp: "2026-04-08T12:00:01.000Z",
          type: "event_msg",
          payload: {
            type: "task_started",
            turn_id: "turn-8",
            started_at: 1775649601,
          },
        }),
        JSON.stringify({
          timestamp: "2026-04-08T12:00:02.000Z",
          type: "event_msg",
          payload: {
            type: "task_complete",
            turn_id: "turn-8",
            last_agent_message: "Streamed completion is live.",
            completed_at: 1775649602,
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
      timestamp: "2026-04-08T12:00:02.000Z",
      completedAt: "2026-04-08T12:00:02.000Z",
      updatedAt: "2026-04-08T12:00:02.000Z",
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

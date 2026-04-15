import { spawnSync } from "node:child_process";
import { mkdtemp, mkdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createCodexSessionStore, type CodexSessionStore } from "../src/index.js";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const projectRoot = "/Users/nikitavoloboev/repos/openai/codex";

describe("codex session lookup CLI", () => {
  let tempDir: string;
  let dataPath: string;
  let store: CodexSessionStore | null;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "codex-session-lookup-"));
    dataPath = join(tempDir, "codex-sessions.db");
    store = createCodexSessionStore({
      appId: "codex-session-lookup-test",
      dataPath,
    });

    await store.replaceSessionProjection(
      {
        sessionId: "019d58b5-a54b-75f1-8c62-3679667476d9",
        rolloutPath: "/tmp/session-guide.jsonl",
        cwd: projectRoot,
        projectRoot,
        gitBranch: "j",
        modelName: "gpt-5.4",
        reasoningEffort: "xhigh",
        status: "completed",
        createdAt: "2026-04-08T12:00:00.000Z",
        updatedAt: "2026-04-08T12:05:00.000Z",
        latestActivityAt: "2026-04-08T12:05:00.000Z",
        lastAssistantAt: "2026-04-08T12:04:00.000Z",
        latestUserMessage:
          "read /Users/nikitavoloboev/docs/zed/j-k-l-shell-command-guide.md is this valid use of our fork",
        latestAssistantMessage: "Checked the guide and documented the fork workflow.",
        turns: [
          {
            turnId: "turn-guide",
            sequence: 1,
            status: "completed",
            userMessage:
              "read /Users/nikitavoloboev/docs/zed/j-k-l-shell-command-guide.md is this valid use of our fork",
            assistantMessage: "Checked the guide and documented the fork workflow.",
            completedAt: "2026-04-08T12:04:00.000Z",
            updatedAt: "2026-04-08T12:04:00.000Z",
          },
        ],
      },
      {
        sourceId: "session-guide",
        absolutePath: "/tmp/session-guide.jsonl",
        lineCount: 12,
        syncedAt: "2026-04-08T12:05:00.000Z",
      },
    );

    await store.replaceSessionProjection(
      {
        sessionId: "019d6c8a-6073-7923-a85c-b26edb20e7b5",
        rolloutPath: "/tmp/session-other.jsonl",
        cwd: projectRoot,
        projectRoot,
        gitBranch: "main",
        modelName: "gpt-5.4",
        reasoningEffort: "xhigh",
        status: "completed",
        createdAt: "2026-04-08T12:00:00.000Z",
        updatedAt: "2026-04-08T12:04:30.000Z",
        latestActivityAt: "2026-04-08T12:04:30.000Z",
        lastAssistantAt: "2026-04-08T12:04:30.000Z",
        latestUserMessage: "explain fb3dcfde1 from this repo",
        latestAssistantMessage: "Explained the commit internals.",
        turns: [
          {
            turnId: "turn-other",
            sequence: 1,
            status: "completed",
            userMessage: "explain fb3dcfde1 from this repo",
            assistantMessage: "Explained the commit internals.",
            completedAt: "2026-04-08T12:04:30.000Z",
            updatedAt: "2026-04-08T12:04:30.000Z",
          },
        ],
      },
      {
        sourceId: "session-other",
        absolutePath: "/tmp/session-other.jsonl",
        lineCount: 8,
        syncedAt: "2026-04-08T12:04:30.000Z",
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

  it("sorts lookup rows by last message time and fuzzy-matches multi-token queries", () => {
    const listed = runCliJson("list-sessions", ["--project-root", projectRoot, "--limit", "10"]);
    expect(listed.map((row: any) => row.id)).toEqual([
      "019d6c8a-6073-7923-a85c-b26edb20e7b5",
      "019d58b5-a54b-75f1-8c62-3679667476d9",
    ]);
    expect(listed[1]?.updatedAt).toBe(
      Math.floor(Date.parse("2026-04-08T12:04:00.000Z") / 1000),
    );

    const searched = runCliJson("search-sessions", [
      "--project-root",
      projectRoot,
      "--query",
      "guide valid fork",
      "--limit",
      "5",
    ]);
    expect(searched).toHaveLength(1);
    expect(searched[0]?.id).toBe("019d58b5-a54b-75f1-8c62-3679667476d9");
  });

  it("searches globally when project root is omitted", async () => {
    const otherProjectRoot = "/Users/nikitavoloboev/work/review-codex-experiment";
    const writer = createCodexSessionStore({
      appId: "codex-session-lookup-test",
      dataPath,
    });

    await writer.replaceSessionProjection(
      {
        sessionId: "019daaaa-a54b-75f1-8c62-3679667476d9",
        rolloutPath: "/tmp/session-global.jsonl",
        cwd: otherProjectRoot,
        projectRoot: otherProjectRoot,
        gitBranch: "feature/native-providers",
        modelName: "gpt-5.4",
        reasoningEffort: "high",
        status: "completed",
        createdAt: "2026-04-08T13:00:00.000Z",
        updatedAt: "2026-04-08T13:05:00.000Z",
        latestActivityAt: "2026-04-08T13:05:00.000Z",
        latestUserMessage: "wire anthropic and gemini native endpoints in the ai proxy",
        latestAssistantMessage: "documented the native provider translation plan",
        turns: [
          {
            turnId: "turn-global",
            sequence: 1,
            status: "completed",
            userMessage: "wire anthropic and gemini native endpoints in the ai proxy",
            assistantMessage: "documented the native provider translation plan",
            completedAt: "2026-04-08T13:05:00.000Z",
            updatedAt: "2026-04-08T13:05:00.000Z",
          },
        ],
      },
      {
        sourceId: "session-global",
        absolutePath: "/tmp/session-global.jsonl",
        lineCount: 10,
        syncedAt: "2026-04-08T13:05:00.000Z",
      },
    );

    await writer.shutdown();

    const searched = runCliJson("search-sessions", [
      "--query",
      "anthropic gemini native proxy",
      "--limit",
      "5",
    ]);

    expect(searched[0]?.id).toBe("019daaaa-a54b-75f1-8c62-3679667476d9");
  });

  it(
    "searches across the full project history instead of only the newest 200 sessions",
    async () => {
      const writer = createCodexSessionStore({
        appId: "codex-session-lookup-test",
        dataPath,
      });

      for (let index = 0; index < 220; index += 1) {
        await writer.replaceSessionProjection(
          {
            sessionId: `019dffff-0000-7000-8000-${index.toString().padStart(12, "0")}`,
            rolloutPath: `/tmp/session-bulk-${index}.jsonl`,
            cwd: projectRoot,
            projectRoot,
            gitBranch: "main",
            modelName: "gpt-5.4",
            reasoningEffort: "xhigh",
            status: "completed",
            createdAt: `2026-04-08T10:${(index % 60).toString().padStart(2, "0")}:00.000Z`,
            updatedAt: `2026-04-08T10:${(index % 60).toString().padStart(2, "0")}:30.000Z`,
            latestActivityAt: `2026-04-08T10:${(index % 60).toString().padStart(2, "0")}:30.000Z`,
            latestUserMessage: `generic filler lookup ${index}`,
            latestAssistantMessage: `generic filler answer ${index}`,
            turns: [
              {
                turnId: `turn-bulk-${index}`,
                sequence: 1,
                status: "completed",
                userMessage: `generic filler lookup ${index}`,
                assistantMessage: `generic filler answer ${index}`,
                completedAt: `2026-04-08T10:${(index % 60).toString().padStart(2, "0")}:30.000Z`,
                updatedAt: `2026-04-08T10:${(index % 60).toString().padStart(2, "0")}:30.000Z`,
              },
            ],
          },
          {
            sourceId: `session-bulk-${index}`,
            absolutePath: `/tmp/session-bulk-${index}.jsonl`,
            lineCount: 8,
            syncedAt: `2026-04-08T10:${(index % 60).toString().padStart(2, "0")}:30.000Z`,
          },
        );
      }

      await writer.replaceSessionProjection(
        {
          sessionId: "019dffff-dead-7000-8000-000000000999",
          rolloutPath: "/tmp/session-deep-history.jsonl",
          cwd: projectRoot,
          projectRoot,
          gitBranch: "j",
          modelName: "gpt-5.4",
          reasoningEffort: "xhigh",
          status: "completed",
          createdAt: "2026-04-05T01:00:00.000Z",
          updatedAt: "2026-04-05T01:01:00.000Z",
          latestActivityAt: "2026-04-05T01:01:00.000Z",
          latestUserMessage: "deep archive thread about zebra quartz rollout",
          latestAssistantMessage: "documented the zebra quartz rollout notes",
          turns: [
            {
              turnId: "turn-deep-history",
              sequence: 1,
              status: "completed",
              userMessage: "deep archive thread about zebra quartz rollout",
              assistantMessage: "documented the zebra quartz rollout notes",
              completedAt: "2026-04-05T01:01:00.000Z",
              updatedAt: "2026-04-05T01:01:00.000Z",
            },
          ],
        },
        {
          sourceId: "session-deep-history",
          absolutePath: "/tmp/session-deep-history.jsonl",
          lineCount: 8,
          syncedAt: "2026-04-05T01:01:00.000Z",
        },
      );

      await writer.shutdown();

      const searched = runCliJson("search-sessions", [
        "--project-root",
        projectRoot,
        "--query",
        "zebra quartz rollout",
        "--limit",
        "5",
      ]);

      expect(searched[0]?.id).toBe("019dffff-dead-7000-8000-000000000999");
    },
    15_000,
  );

  it("lists only active sessions using presence rows ordered by recent activity", async () => {
    const writer = createCodexSessionStore({
      appId: "codex-session-lookup-test",
      dataPath,
    });

    await writer.replaceSessionProjection(
      {
        sessionId: "019dffff-feed-7000-8000-000000000001",
        rolloutPath: "/tmp/session-active-newer.jsonl",
        cwd: projectRoot,
        projectRoot,
        gitBranch: "j",
        modelName: "gpt-5.4",
        reasoningEffort: "xhigh",
        status: "in_progress",
        createdAt: "2026-04-08T12:00:00.000Z",
        updatedAt: "2026-04-08T12:06:00.000Z",
        latestAssistantPartial: "working through the regression",
        turns: [
          {
            turnId: "turn-active-newer",
            sequence: 1,
            status: "in_progress",
            assistantPartial: "working through the regression",
            updatedAt: "2026-04-08T12:06:00.000Z",
          },
        ],
      },
      {
        sourceId: "session-active-newer",
        absolutePath: "/tmp/session-active-newer.jsonl",
        lineCount: 8,
        syncedAt: "2026-04-08T12:06:00.000Z",
      },
    );

    await writer.replaceSessionProjection(
      {
        sessionId: "019dffff-feed-7000-8000-000000000002",
        rolloutPath: "/tmp/session-active-older.jsonl",
        cwd: projectRoot,
        projectRoot,
        gitBranch: "j",
        modelName: "gpt-5.4",
        reasoningEffort: "xhigh",
        status: "pending",
        createdAt: "2026-04-08T12:00:00.000Z",
        updatedAt: "2026-04-08T12:05:00.000Z",
        latestUserMessage: "check the picker regression",
        turns: [
          {
            turnId: "turn-active-older",
            sequence: 1,
            status: "pending",
            userMessage: "check the picker regression",
            updatedAt: "2026-04-08T12:05:00.000Z",
          },
        ],
      },
      {
        sourceId: "session-active-older",
        absolutePath: "/tmp/session-active-older.jsonl",
        lineCount: 6,
        syncedAt: "2026-04-08T12:05:00.000Z",
      },
    );

    await writer.replaceSessionProjection(
      {
        sessionId: "019dffff-feed-7000-8000-000000000003",
        rolloutPath: "/tmp/session-active-complete.jsonl",
        cwd: projectRoot,
        projectRoot,
        gitBranch: "j",
        modelName: "gpt-5.4",
        reasoningEffort: "xhigh",
        status: "completed",
        createdAt: "2026-04-08T12:00:00.000Z",
        updatedAt: "2026-04-08T12:07:00.000Z",
        latestAssistantMessage: "done",
        turns: [
          {
            turnId: "turn-active-complete",
            sequence: 1,
            status: "completed",
            assistantMessage: "done",
            completedAt: "2026-04-08T12:07:00.000Z",
            updatedAt: "2026-04-08T12:07:00.000Z",
          },
        ],
      },
      {
        sourceId: "session-active-complete",
        absolutePath: "/tmp/session-active-complete.jsonl",
        lineCount: 8,
        syncedAt: "2026-04-08T12:07:00.000Z",
      },
    );

    await writer.shutdown();

    const active = runCliJson("list-active-sessions", [
      "--project-root",
      projectRoot,
      "--limit",
      "10",
    ]);

    expect(active.map((row: any) => row.id)).toEqual([
      "019dffff-feed-7000-8000-000000000001",
      "019dffff-feed-7000-8000-000000000002",
    ]);
  });

  it("omits stale active sessions whose presence has not been refreshed recently", async () => {
    const writer = createCodexSessionStore({
      appId: "codex-session-lookup-test",
      dataPath,
    });
    const freshUpdatedAt = new Date(Date.now() - 60_000).toISOString();
    const staleUpdatedAt = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString();

    await writer.replaceSessionProjection(
      {
        sessionId: "019dffff-feed-7000-8000-000000000101",
        rolloutPath: "/tmp/session-active-fresh.jsonl",
        cwd: projectRoot,
        projectRoot,
        gitBranch: "j",
        modelName: "gpt-5.4",
        reasoningEffort: "xhigh",
        status: "in_progress",
        createdAt: freshUpdatedAt,
        updatedAt: freshUpdatedAt,
        latestActivityAt: freshUpdatedAt,
        latestAssistantPartial: "still working",
        turns: [
          {
            turnId: "turn-active-fresh",
            sequence: 1,
            status: "in_progress",
            assistantPartial: "still working",
            updatedAt: freshUpdatedAt,
          },
        ],
      },
      {
        sourceId: "session-active-fresh",
        absolutePath: "/tmp/session-active-fresh.jsonl",
        lineCount: 8,
        syncedAt: freshUpdatedAt,
      },
    );

    await writer.replaceSessionProjection(
      {
        sessionId: "019dffff-feed-7000-8000-000000000102",
        rolloutPath: "/tmp/session-active-stale.jsonl",
        cwd: projectRoot,
        projectRoot,
        gitBranch: "j",
        modelName: "gpt-5.4",
        reasoningEffort: "xhigh",
        status: "in_progress",
        createdAt: staleUpdatedAt,
        updatedAt: staleUpdatedAt,
        latestActivityAt: staleUpdatedAt,
        latestAssistantPartial: "old partial",
        turns: [
          {
            turnId: "turn-active-stale",
            sequence: 1,
            status: "in_progress",
            assistantPartial: "old partial",
            updatedAt: staleUpdatedAt,
          },
        ],
      },
      {
        sourceId: "session-active-stale",
        absolutePath: "/tmp/session-active-stale.jsonl",
        lineCount: 8,
        syncedAt: staleUpdatedAt,
      },
    );

    await writer.shutdown();

    const active = runCliJson(
      "list-active-sessions",
      ["--project-root", projectRoot, "--limit", "10"],
      undefined,
      { CODEX_ACTIVE_SESSION_MAX_AGE_MS: String(15 * 60 * 1000) },
    );

    expect(active.map((row: any) => row.id)).toEqual([
      "019dffff-feed-7000-8000-000000000101",
    ]);
  });

  it("cold-loads recent project sessions from rollout files when the Jazz store is empty", async () => {
    const coldDataPath = join(tempDir, "cold-codex-sessions.db");
    const coldCodexHome = join(tempDir, "cold-codex-home");
    const rolloutDir = join(coldCodexHome, "sessions/2026/04/09");
    const recentRolloutPath = join(
      rolloutDir,
      "rollout-2026-04-09T12-00-00-019dffff-feed-7000-8000-000000000201.jsonl",
    );
    const olderRolloutPath = join(
      rolloutDir,
      "rollout-2026-04-09T11-00-00-019dffff-feed-7000-8000-000000000202.jsonl",
    );

    await mkdir(rolloutDir, { recursive: true });
    await writeFile(
      recentRolloutPath,
      createRolloutText({
        sessionId: "019dffff-feed-7000-8000-000000000201",
        timestamp: "2026-04-09T12:00:00.000Z",
        cwd: projectRoot,
        message: "recent cold-start session",
      }),
    );
    await writeFile(
      olderRolloutPath,
      createRolloutText({
        sessionId: "019dffff-feed-7000-8000-000000000202",
        timestamp: "2026-04-09T11:00:00.000Z",
        cwd: projectRoot,
        message: "older cold-start session",
      }),
    );

    const listed = runCliJson(
      "list-sessions",
      ["--project-root", projectRoot, "--limit", "10", "--codex-home", coldCodexHome],
      coldDataPath,
    );

    expect(listed.map((row: any) => row.id)).toEqual([
      "019dffff-feed-7000-8000-000000000201",
      "019dffff-feed-7000-8000-000000000202",
    ]);
  });

  it("cold-loads id-prefix matches from rollout files when the Jazz store is empty", async () => {
    const coldDataPath = join(tempDir, "cold-prefix-codex-sessions.db");
    const coldCodexHome = join(tempDir, "cold-prefix-codex-home");
    const rolloutDir = join(coldCodexHome, "sessions/2026/04/10");

    await mkdir(rolloutDir, { recursive: true });
    await writeFile(
      join(
        rolloutDir,
        "rollout-2026-04-10T12-00-00-019dffff-feed-7000-8000-000000000211.jsonl",
      ),
      createRolloutText({
        sessionId: "019dffff-feed-7000-8000-000000000211",
        timestamp: "2026-04-10T12:00:00.000Z",
        cwd: projectRoot,
        message: "recent prefix session",
      }),
    );
    await writeFile(
      join(
        rolloutDir,
        "rollout-2026-04-10T11-00-00-019d1111-feed-7000-8000-000000000212.jsonl",
      ),
      createRolloutText({
        sessionId: "019d1111-feed-7000-8000-000000000212",
        timestamp: "2026-04-10T11:00:00.000Z",
        cwd: projectRoot,
        message: "non matching prefix session",
      }),
    );

    const listed = runCliJson(
      "search-prefix-sessions",
      ["--prefix", "019dffff-feed", "--limit", "5", "--codex-home", coldCodexHome],
      coldDataPath,
    );

    expect(listed.map((row: any) => row.id)).toEqual([
      "019dffff-feed-7000-8000-000000000211",
    ]);
  });

  it("redirects legacy directory-shaped data paths to a sibling sqlite file", async () => {
    const legacyDataPath = join(tempDir, "legacy", "codex-sessions.db");
    const fallbackPath = join(tempDir, "legacy", "codex-sessions.sqlite");
    const coldCodexHome = join(tempDir, "legacy-codex-home");
    const rolloutDir = join(coldCodexHome, "sessions/2026/04/10");

    await mkdir(legacyDataPath, { recursive: true });
    await mkdir(rolloutDir, { recursive: true });
    await writeFile(
      join(
        rolloutDir,
        "rollout-2026-04-10T12-00-00-019dffff-feed-7000-8000-000000000299.jsonl",
      ),
      createRolloutText({
        sessionId: "019dffff-feed-7000-8000-000000000299",
        timestamp: "2026-04-10T12:00:00.000Z",
        cwd: projectRoot,
        message: "legacy directory fallback session",
      }),
    );

    const result = spawnSync(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        "list-sessions",
        "--project-root",
        projectRoot,
        "--limit",
        "5",
        "--codex-home",
        coldCodexHome,
      ],
      {
        cwd: packageRoot,
        encoding: "utf8",
        env: {
          ...process.env,
          CODEX_ACTIVE_SESSION_MAX_AGE_MS: "315360000000",
          FLOW_CODEX_JAZZ_DATA_PATH: legacyDataPath,
        },
      },
    );

    expect(result.status).toBe(0);
    expect(result.stderr).toContain(`using ${fallbackPath} instead`);
    expect(JSON.parse(result.stdout.trim()).map((row: any) => row.id)).toEqual([
      "019dffff-feed-7000-8000-000000000299",
    ]);
    const fallbackStat = await stat(fallbackPath);
    expect(fallbackStat.isFile()).toBe(true);
  });

  function runCliJson(
    command: string,
    extraArgs: string[] = [],
    dataPathOverride?: string,
    envOverrides?: Record<string, string>,
  ): any {
    const result = spawnSync(
      "pnpm",
      [
        "exec",
        "tsx",
        "src/cli.ts",
        command,
        "--data-path",
        dataPathOverride ?? dataPath,
        ...extraArgs,
      ],
      {
        cwd: packageRoot,
        encoding: "utf8",
        env: {
          ...process.env,
          CODEX_ACTIVE_SESSION_MAX_AGE_MS: "315360000000",
          ...envOverrides,
        },
      },
    );

    if (result.status !== 0) {
      throw new Error(result.stderr || result.stdout || `CLI command ${command} failed`);
    }

    return JSON.parse(result.stdout.trim());
  }

  function createRolloutText(options: {
    sessionId: string;
    timestamp: string;
    cwd: string;
    message: string;
  }): string {
    return [
      {
        timestamp: options.timestamp,
        type: "session_meta",
        payload: {
          id: options.sessionId,
          timestamp: options.timestamp,
          cwd: options.cwd,
          originator: "codex-tui",
          cli_version: "0.0.0",
          source: "cli",
        },
      },
      {
        timestamp: options.timestamp,
        type: "turn_context",
        payload: {
          turn_id: `${options.sessionId}-turn-1`,
          cwd: options.cwd,
          model: "gpt-5.4",
          effort: "high",
        },
      },
      {
        timestamp: options.timestamp,
        type: "event_msg",
        payload: {
          type: "user_message",
          message: options.message,
        },
      },
      {
        timestamp: options.timestamp,
        type: "event_msg",
        payload: {
          type: "task_complete",
          turn_id: `${options.sessionId}-turn-1`,
          last_agent_message: `Completed ${options.message}`,
          completed_at: Math.floor(Date.parse(options.timestamp) / 1000),
          duration_ms: 1000,
        },
      },
    ]
      .map((line) => JSON.stringify(line))
      .join("\n");
  }
});

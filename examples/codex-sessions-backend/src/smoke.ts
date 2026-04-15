import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { syncCodexRollouts } from "./projector.js";
import { createCodexSessionStore } from "./store.js";

async function main(): Promise<void> {
  const tempDir = await mkdtemp(join(tmpdir(), "codex-sessions-smoke-"));
  const codexHome = join(tempDir, ".codex");
  const rolloutDir = join(codexHome, "sessions/2026/04/07");
  const rolloutPath = join(
    rolloutDir,
    "rollout-2026-04-07T10-00-00-019d0000-0000-7000-8000-000000000001.jsonl",
  );
  const dataPath = join(tempDir, "codex-sessions.db");
  await mkdir(rolloutDir, { recursive: true });
  await writeFile(
    rolloutPath,
    [
      JSON.stringify({
        timestamp: "2026-04-07T10:00:00.000Z",
        type: "session_meta",
        payload: {
          id: "019d0000-0000-7000-8000-000000000001",
          timestamp: "2026-04-07T10:00:00.000Z",
          cwd: "/tmp/demo",
          originator: "codex-tui",
          cli_version: "0.0.0",
          source: "cli",
          model_provider: "openai",
        },
      }),
      JSON.stringify({
        timestamp: "2026-04-07T10:00:01.000Z",
        type: "event_msg",
        payload: {
          type: "task_started",
          turn_id: "turn-1",
          started_at: 1775556001,
        },
      }),
      JSON.stringify({
        timestamp: "2026-04-07T10:00:02.000Z",
        type: "event_msg",
        payload: {
          type: "user_message",
          message: "Mirror this session into Jazz",
        },
      }),
      JSON.stringify({
        timestamp: "2026-04-07T10:00:03.000Z",
        type: "event_msg",
        payload: {
          type: "agent_message_content_delta",
          turn_id: "turn-1",
          item_id: "item-1",
          delta: "Working on it",
        },
      }),
    ].join("\n"),
  );

  const store = createCodexSessionStore({
    appId: "codex-sessions-smoke",
    dataPath,
  });

  try {
    const result = await syncCodexRollouts({ codexHome, store });
    const sessions = await store.listSessions();
    console.log(JSON.stringify({ result, sessions }, null, 2));
  } finally {
    await store.shutdown();
    await rm(tempDir, { recursive: true, force: true });
  }
}

void main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});

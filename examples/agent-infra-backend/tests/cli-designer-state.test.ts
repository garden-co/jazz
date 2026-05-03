import { spawnSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));

describe("agent-infra Designer state CLI", () => {
  let tempDir: string;
  let dataPath: string;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "agent-infra-designer-state-cli-"));
    dataPath = join(tempDir, "agent-infra.db");
  });

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true });
  });

  it(
    "records an object-backed Codex conversation and telemetry event",
    { timeout: 30_000 },
    () => {
      const transcript = runCliJson("record-designer-object-ref", {
        objectRefId: "obj-transcript",
        provider: "oci",
        uri: "oci://designer-codex/conversations/session/transcript.jsonl",
        objectKind: "codex.transcript",
      });
      const turnPayload = runCliJson("record-designer-object-ref", {
        objectRefId: "obj-turn",
        provider: "oci",
        uri: "oci://designer-codex/conversations/session/turns/0001.json",
        objectKind: "codex.turn",
      });
      const telemetryPayload = runCliJson("record-designer-object-ref", {
        objectRefId: "obj-telemetry",
        provider: "oci",
        uri: "oci://designer-telemetry/events/0001.json",
        objectKind: "designer.telemetry.event",
      });

      const conversation = runCliJson("record-designer-codex-conversation", {
        conversationId: "conversation-1",
        provider: "codex",
        providerSessionId: "019dec01-6eaa-7650-986f-f41ab49a59fd",
        workspaceKey: "rubiks-cube",
        transcriptObjectRefId: transcript.objectRefId,
      });
      runCliJson("record-designer-codex-turn", {
        conversationId: conversation.conversationId,
        sequence: 1,
        turnKind: "assistant",
        role: "assistant",
        actorKind: "agent",
        summaryText: "Object-backed response payload.",
        payloadObjectRefId: turnPayload.objectRefId,
      });
      runCliJson("record-designer-telemetry-event", {
        telemetryEventId: "telemetry-1",
        conversationId: conversation.conversationId,
        eventType: "designer.agent_prompt_completed",
        pane: "chat",
        sequence: 1,
        payloadObjectRefId: telemetryPayload.objectRefId,
      });

      const summary = runCliJson(
        "get-designer-codex-conversation-summary",
        undefined,
        ["--conversation-id", conversation.conversationId],
      );
      expect(summary.transcriptObject.objectRefId).toBe("obj-transcript");
      expect(summary.turns[0]?.payloadObjectRefId).toBe("obj-turn");
      expect(summary.telemetryEvents[0]?.payloadObjectRefId).toBe(
        "obj-telemetry",
      );
    },
  );

  function runCliJson(
    command: string,
    input?: unknown,
    extraArgs: string[] = [],
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
      },
    );

    if (result.status !== 0) {
      throw new Error(
        result.stderr || result.stdout || `CLI command ${command} failed`,
      );
    }

    return JSON.parse(result.stdout.trim());
  }
});

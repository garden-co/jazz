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

  it(
    "records Designer agent contracts and object-backed live commits",
    { timeout: 30_000 },
    () => {
      const patchObject = runCliJson("record-designer-object-ref", {
        objectRefId: "obj-live-commit-patch",
        provider: "oci",
        uri: "oci://designer-commits/prom/live/01f4d1e.patch",
        contentType: "text/x-diff",
        objectKind: "vcs.commit.patch",
      });
      const manifestObject = runCliJson("record-designer-object-ref", {
        objectRefId: "obj-live-commit-manifest",
        provider: "oci",
        uri: "oci://designer-commits/prom/live/01f4d1e.json",
        contentType: "application/json",
        objectKind: "vcs.commit.manifest",
      });
      const agent = runCliJson("record-designer-agent", {
        agentId: "agent.remote-codex.designer",
        agentKind: "codex",
        provider: "openai-codex",
        displayName: "Remote Codex Designer",
        model: "gpt-5.5",
        defaultContextJson: {
          repoRoot: "/Users/nikitavoloboev/code/prom",
          workspaceRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
        },
      });

      runCliJson("record-designer-agent-tool", {
        toolId: "agent.remote-codex.designer:tool:apply_patch",
        agentId: agent.agentId,
        toolName: "apply_patch",
        toolKind: "workspace.edit",
        scopeJson: {
          allowedPathPrefixes: ["ide/designer"],
        },
      });
      runCliJson("record-designer-agent-context", {
        contextId: "agent.remote-codex.designer:context:prom-live",
        agentId: agent.agentId,
        contextKind: "workflow",
        sourceKind: "jazz.row",
        inlineContextJson: {
          branch: "live",
          invariant: "all committed changes are reflected into Jazz2",
        },
        priority: 10,
      });
      const commit = runCliJson("record-designer-live-commit", {
        commitId: "01f4d1ea1cea8f331c1691a3312c6df1043db08b",
        repoRoot: "/Users/nikitavoloboev/code/prom",
        workspaceRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
        branch: "live",
        bookmark: "nikiv-live",
        subject: "fix(designer): harden remote codex chat replay",
        traceRef:
          "codex:1_eyJzIjoiMDE5ZGViMGEtZDE5Yi03ZDkyLTgxZGQtNzY2MTJkMDc2ZDRjIiwidCI6MX0",
        sourceSessionId: "codex:019deb0a-d19b-7d92-81dd-76612d076d4c",
        sourceTurnOrdinal: 1,
        agentId: agent.agentId,
        changedPathsJson: ["ide/designer/src/v2/chat/HarnessChatTile.tsx"],
        patchObjectRefId: patchObject.objectRefId,
        manifestObjectRefId: manifestObject.objectRefId,
        committedAt: "2026-05-03T22:53:03Z",
        reflectedAt: "2026-05-03T22:54:34Z",
      });

      const commits = runCliJson("list-designer-live-commits", undefined, [
        "--repo-root",
        "/Users/nikitavoloboev/code/prom",
        "--branch",
        "live",
        "--source-session-id",
        "codex:019deb0a-d19b-7d92-81dd-76612d076d4c",
      ]);
      const summary = runCliJson(
        "get-designer-live-commit-summary",
        undefined,
        ["--commit-id", commit.commitId],
      );
      const tools = runCliJson("list-designer-agent-tools", undefined, [
        "--agent-id",
        agent.agentId,
      ]);
      const contexts = runCliJson("list-designer-agent-contexts", undefined, [
        "--agent-id",
        agent.agentId,
      ]);

      expect(commits.map((item: any) => item.commitId)).toEqual([
        "01f4d1ea1cea8f331c1691a3312c6df1043db08b",
      ]);
      expect(summary.patchObject.uri).toBe(
        "oci://designer-commits/prom/live/01f4d1e.patch",
      );
      expect(summary.agent.displayName).toBe("Remote Codex Designer");
      expect(tools[0]?.toolName).toBe("apply_patch");
      expect(contexts[0]?.contextKind).toBe("workflow");
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

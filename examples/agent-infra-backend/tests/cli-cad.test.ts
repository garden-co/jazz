import { spawnSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { startLocalJazzServer } from "jazz-tools/testing";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));

describe("agent-infra Designer CAD CLI", () => {
  let tempDir: string;
  let dataPath: string;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "agent-infra-cad-cli-"));
    dataPath = join(tempDir, "agent-infra.db");
  });

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true });
  });

  it(
    "records a collaborative build123d session through JSON CLI commands",
    { timeout: 30_000 },
    () => {
      const workspace = runCliJson("record-designer-cad-workspace", {
        workspaceId: "workspace-build123d-collab",
        workspaceKey: "prom-designer-build123d",
        title: "Prom Designer build123d collaboration",
        repoRoot: "/Users/nikitavoloboev/code/prom",
        workspaceRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
        metadataJson: {
          occtRepo: "/Users/nikitavoloboev/repos/Open-Cascade-SAS/OCCT",
          build123dRepo: "/Users/nikitavoloboev/repos/gumyr/build123d",
        },
      });
      expect(workspace.workspaceId).toBe("workspace-build123d-collab");

      const document = runCliJson("record-designer-cad-document", {
        workspaceId: workspace.workspaceId,
        documentId: "doc-openclaw-bracket",
        filePath: "workspace/openclaw-bracket.build123d.py",
        sourceHash: "sha256:source-0",
      });
      expect(document.language).toBe("build123d-python");

      const session = runCliJson("record-designer-cad-session", {
        cadSessionId: "cad-session-openclaw-1",
        workspaceId: workspace.workspaceId,
        documentId: document.documentId,
        codexSessionId: "codex:019deb0a-d19b-7d92-81dd-76612d076d4c",
        openedBy: "alice",
        metadataJson: {
          collaboratorSession:
            "codex:019defcc-a8da-76d0-942e-b0dbaff55f86",
        },
      });
      expect(session.status).toBe("active");

      const operation = runCliJson("record-designer-cad-operation", {
        operationId: "op-add-mounting-hole",
        cadSessionId: session.cadSessionId,
        actorKind: "agent",
        actorId: "codex:019defcc-a8da-76d0-942e-b0dbaff55f86",
        operationKind: "source.patch",
        status: "validated",
        operationJson: {
          filePath: "workspace/openclaw-bracket.build123d.py",
          intent: "add parametric mounting hole",
        },
      });
      expect(operation.operationId).toBe("op-add-mounting-hole");

      runCliJson("record-designer-cad-source-edit", {
        editId: "edit-add-mounting-hole",
        operationId: operation.operationId,
        sequence: 1,
        filePath: "workspace/openclaw-bracket.build123d.py",
        rangeJson: { startLine: 18, startColumn: 1, endLine: 18, endColumn: 1 },
        textPreview: "hole = Hole(radius=3)",
        status: "planned",
      });

      runCliJson("record-designer-cad-event", {
        eventId: "event-operation-validated",
        cadSessionId: session.cadSessionId,
        sequence: 1,
        eventKind: "operation.validated",
        actorKind: "agent",
        actorId: "codex:019defcc-a8da-76d0-942e-b0dbaff55f86",
        operationId: operation.operationId,
        payloadJson: { ok: true },
      });

      const summary = runCliJson("get-designer-cad-session-summary", undefined, [
        "--cad-session-id",
        session.cadSessionId,
      ]);
      expect(summary.workspace.workspaceKey).toBe("prom-designer-build123d");
      expect(summary.document.filePath).toBe(
        "workspace/openclaw-bracket.build123d.py",
      );
      expect(summary.session.codexSessionId).toBe(
        "codex:019deb0a-d19b-7d92-81dd-76612d076d4c",
      );
      expect(summary.operations.map((item: any) => item.operationId)).toEqual([
        "op-add-mounting-hole",
      ]);
      expect(summary.sourceEdits.map((item: any) => item.editId)).toEqual([
        "edit-add-mounting-hole",
      ]);
      expect(summary.events.map((item: any) => item.eventKind)).toEqual([
        "operation.validated",
      ]);
    },
  );

  it("publishes the agent-infra schema to a remote Jazz server", async () => {
    const appId = "00000000-0000-0000-0000-000000000123";
    const adminSecret = "agent-infra-admin-secret";
    const backendSecret = "agent-infra-backend-secret";
    const server = await startLocalJazzServer({
      appId,
      adminSecret,
      backendSecret,
    });

    try {
      const result = runCliJson("publish-schema", undefined, [
        "--app-id",
        appId,
        "--server-url",
        server.url,
        "--admin-secret",
        adminSecret,
      ]);

      expect(result.appId).toBe(appId);
      expect(result.serverUrl).toBe(server.url);
      expect(result.hash).toEqual(expect.any(String));
      expect(result.objectId).toEqual(expect.any(String));
    } finally {
      await server.stop();
    }
  }, 45_000);

  function runCliJson(
    command: string,
    input?: unknown,
    extraArgs: string[] = [],
  ): any {
    return runCliJsonAt(dataPath, command, input, extraArgs);
  }

  function runCliJsonAt(
    selectedDataPath: string,
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
        selectedDataPath,
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

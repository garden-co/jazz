import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { AgentDataStore, createAgentDataStore } from "../src/index.js";

describe("AgentDataStore", () => {
  let tempDir: string;
  let store: AgentDataStore;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "agent-data-store-"));
    store = createAgentDataStore({
      appId: "agent-data-store-test",
      dataPath: join(tempDir, "agent-data.db"),
    });
  });

  afterEach(async () => {
    await store.shutdown();
    await rm(tempDir, { recursive: true, force: true });
  });

  it("records a run summary across the core operational tables", async () => {
    await store.recordRunStarted({
      runId: "run-1",
      agentId: "plan",
      threadId: "thread-1",
      turnId: "turn-1",
      cwd: "/Users/nikitavoloboev/run",
      repoRoot: "/Users/nikitavoloboev/run",
      requestSummary: "Summarize active plans",
      status: "running",
    });
    await store.recordItemStarted({
      runId: "run-1",
      itemId: "item-1",
      itemKind: "agentMessage",
      sequence: 1,
      phase: "commentary",
      status: "running",
    });
    await store.appendSemanticEvent({
      runId: "run-1",
      itemId: "item-1",
      eventId: "semantic-1",
      eventType: "workspace_snapshot",
      summaryText: "captured baseline state",
      payloadJson: { dirty_path_count: 4 },
    });
    await store.appendWireEvent({
      runId: "run-1",
      eventId: "wire-1",
      direction: "client_to_daemon",
      connectionId: 7,
      method: "turn/start",
      payloadJson: { query: "Summarize active plans" },
    });
    await store.recordArtifact({
      runId: "run-1",
      artifactId: "artifact-1",
      artifactKind: "report",
      absolutePath: "/tmp/report.md",
      title: "Run report",
    });
    await store.recordWorkspaceSnapshot({
      runId: "run-1",
      snapshotId: "workspace-1",
      repoRoot: "/Users/nikitavoloboev/run",
      branch: "main",
      headCommit: "abc123",
      dirtyPathCount: 4,
      snapshotJson: { files: ["flow.toml"] },
    });
    await store.updateAgentState({
      agentId: "plan",
      snapshotId: "state-1",
      stateVersion: 1,
      status: "idle",
      stateJson: { last_query: "Summarize active plans" },
    });
    await store.recordMemoryLink({
      runId: "run-1",
      itemId: "item-1",
      linkId: "memory-1",
      memoryScope: "repo-scoped",
      memoryRef: "helixir://memory-1",
      queryText: "active plans",
    });
    await store.recordSourceFile({
      runId: "run-1",
      sourceFileId: "source-1",
      fileKind: "events-jsonl",
      absolutePath: "/tmp/run-1.events.jsonl",
    });
    await store.recordItemCompleted({
      runId: "run-1",
      itemId: "item-1",
      status: "completed",
      summaryJson: { delivered: true },
    });
    await store.recordRunCompleted({
      runId: "run-1",
      status: "completed",
    });

    const recentRuns = await store.listRecentRuns();
    const activeRuns = await store.listActiveRuns();
    const summary = await store.getRunSummary("run-1");

    expect(recentRuns).toHaveLength(1);
    expect(recentRuns[0]?.agent_id).toBe("plan");
    expect(recentRuns[0]?.status).toBe("completed");
    expect(activeRuns).toHaveLength(0);
    expect(summary?.run.run_id).toBe("run-1");
    expect(summary?.items).toHaveLength(1);
    expect(summary?.semanticEvents).toHaveLength(1);
    expect(summary?.wireEvents).toHaveLength(1);
    expect(summary?.artifacts).toHaveLength(1);
    expect(summary?.workspaceSnapshots).toHaveLength(1);
    expect(summary?.memoryLinks).toHaveLength(1);
    expect(summary?.sourceFiles).toHaveLength(1);
    expect(summary?.latestAgentState?.status).toBe("idle");
  });

  it("upserts agents, runs, and items by their external ids", async () => {
    await store.recordRunStarted({
      runId: "run-2",
      agentId: "plan",
      requestSummary: "initial summary",
      status: "running",
      agent: {
        lane: "planning",
      },
    });
    await store.recordRunStarted({
      runId: "run-2",
      agentId: "plan",
      requestSummary: "updated summary",
      status: "waiting",
      agent: {
        lane: "planning",
        promptSurface: "/plan",
      },
    });
    await store.recordItemStarted({
      runId: "run-2",
      itemId: "item-2",
      itemKind: "commandExecution",
      sequence: 1,
      status: "running",
    });
    await store.recordItemStarted({
      runId: "run-2",
      itemId: "item-2",
      itemKind: "commandExecution",
      sequence: 2,
      phase: "commentary",
      status: "waiting",
    });

    const recentRuns = await store.listRecentRuns();
    const summary = await store.getRunSummary("run-2");

    expect(recentRuns).toHaveLength(1);
    expect(recentRuns[0]?.request_summary).toBe("updated summary");
    expect(recentRuns[0]?.status).toBe("waiting");
    expect(summary?.items).toHaveLength(1);
    expect(summary?.items[0]?.sequence).toBe(2);
    expect(summary?.items[0]?.phase).toBe("commentary");
  });

  it("upserts and lists task records in focus order", async () => {
    await store.upsertTaskRecord({
      taskId: "d-002",
      context: "designer",
      title:
        "Merge PR #3296 and clean up the rest of the open Designer PR stack",
      status: "active",
      priority: "P0",
      placement: "now",
      focusRank: 2,
      project: "prom/designer",
      branch: "review/nikiv-designer-build123d-monaco-editor",
      pr: "https://github.com/fl2024008/prometheus/pull/3296",
    });
    await store.upsertTaskRecord({
      taskId: "d-001",
      context: "designer",
      title: "Get the entire Designer stack reviewable and mergeable",
      status: "active",
      priority: "P0",
      placement: "now",
      focusRank: 1,
      project: "prom/designer",
      annotationsJson: [
        "- 2026-04-08: Migrated from ~/do/now.md and promoted as the top active Designer task",
      ],
    });

    const tasks = await store.listTaskRecords({
      context: "designer",
      statuses: ["active"],
    });
    const task = await store.getTaskRecord("d-002");

    expect(tasks.map((item) => item.task_id)).toEqual(["d-001", "d-002"]);
    expect(task?.branch).toBe("review/nikiv-designer-build123d-monaco-editor");
    expect(task?.pr).toBe("https://github.com/fl2024008/prometheus/pull/3296");
  });

  it("records Designer CAD sessions as a steerable operation stream", async () => {
    await store.recordDesignerCadWorkspace({
      workspaceId: "workspace-build123d",
      workspaceKey: "prom-designer",
      title: "Prom Designer CAD",
      repoRoot: "~/code/prom",
      workspaceRoot: "~/work/prom-agent/ide/designer",
      metadataJson: { branch: "shiva/designer/cad/e2e-parity-clean-port" },
    });
    const document = await store.recordDesignerCadDocument({
      workspaceId: "workspace-build123d",
      documentId: "doc-torus",
      filePath: "test-workspace/torus.build123d.py",
      sourceHash: "sha256:source-1",
    });
    const cadSession = await store.recordDesignerCadSession({
      cadSessionId: "cad-session-1",
      workspaceId: "workspace-build123d",
      documentId: document.document_id,
      codexSessionId: "codex:019dea80-dee6-7c93-b8f9-15cbd75246dc",
      openedBy: "alice",
      latestProjectionId: "projection-1",
    });
    const toolSession = await store.recordDesignerCadToolSession({
      toolSessionId: "tool-press-pull-1",
      cadSessionId: cadSession.cad_session_id,
      toolKind: "press-pull",
      actorKind: "human",
      actorId: "alice",
      inputJson: { target: "sketch_0" },
    });
    await store.upsertDesignerCadSceneNode({
      nodeId: "node-sketch-0",
      cadSessionId: cadSession.cad_session_id,
      projectionId: "projection-1",
      kind: "sketch",
      label: "sketch_0",
      path: "sketches/sketch_0",
      stableRef: "build123d:sketch:0",
      sourceSpanJson: { startLine: 12, endLine: 21 },
    });
    await store.upsertDesignerCadSelection({
      cadSessionId: cadSession.cad_session_id,
      actorKind: "human",
      actorId: "alice",
      targetKind: "scene-node",
      targetId: "node-sketch-0",
      nodeId: "node-sketch-0",
      selectionJson: { faceIndices: [0] },
    });
    const operation = await store.recordDesignerCadOperation({
      operationId: "op-extrude-sketch-0",
      cadSessionId: cadSession.cad_session_id,
      toolSessionId: toolSession.tool_session_id,
      actorKind: "human",
      actorId: "alice",
      operationKind: "addFeature",
      status: "validated",
      operationJson: {
        kind: "addFeature",
        feature: { kind: "extrude", params: { amount: 10 } },
      },
      validationJson: { ok: true },
    });
    await store.recordDesignerCadSourceEdit({
      editId: "edit-extrude-sketch-0",
      operationId: operation.operation_id,
      sequence: 1,
      filePath: "test-workspace/torus.build123d.py",
      rangeJson: { startLine: 38, startColumn: 1, endLine: 38, endColumn: 1 },
      textPreview: "extrude_sketch_0 = extrude(sketch_0, amount=10.0)",
      status: "planned",
    });
    const preview = await store.recordDesignerCadPreviewHandle({
      previewId: "preview-extrude-sketch-0",
      cadSessionId: cadSession.cad_session_id,
      toolSessionId: toolSession.tool_session_id,
      operationId: operation.operation_id,
      previewKind: "press-pull-sketch",
      targetJson: { sketchVarName: "sketch_0" },
      handleRef: "docp-preview:handle-1",
    });
    await store.recordDesignerCadPreviewUpdate({
      updateId: "preview-update-1",
      previewId: preview.preview_id,
      sequence: 1,
      paramsJson: { amount: 10 },
      meshRefJson: { artifact: "mesh://preview-1" },
    });
    await store.recordDesignerCadWidget({
      workspaceId: "workspace-build123d",
      widgetKey: "cad/press-pull",
      title: "Press Pull",
      sourceKind: "designer-widget",
      sourcePath: "~/.designer/widgets/cad/press-pull",
      version: "1",
      manifestJson: { tools: ["press-pull"] },
    });
    await store.recordDesignerCadSteer({
      steerId: "steer-1",
      cadSessionId: cadSession.cad_session_id,
      actorKind: "human",
      actorId: "alice",
      targetAgentId: "clanka-cad",
      messageText: "Use the current preview and keep the source edit via CodeBridge.",
      contextJson: { selectedNodeId: "node-sketch-0" },
    });
    await store.recordDesignerCadEvent({
      eventId: "event-1",
      cadSessionId: cadSession.cad_session_id,
      sequence: 1,
      eventKind: "tool.started",
      actorKind: "human",
      actorId: "alice",
      toolSessionId: toolSession.tool_session_id,
      payloadJson: { toolKind: "press-pull" },
    });
    await store.recordDesignerCadEvent({
      eventId: "event-2",
      cadSessionId: cadSession.cad_session_id,
      sequence: 2,
      eventKind: "operation.validated",
      actorKind: "system",
      operationId: operation.operation_id,
      payloadJson: { ok: true },
    });

    const eventsAfterOne = await store.listDesignerCadEvents({
      cadSessionId: cadSession.cad_session_id,
      afterSequence: 1,
    });
    const operations = await store.listDesignerCadOperations({
      cadSessionId: cadSession.cad_session_id,
      status: "validated",
    });
    const summary = await store.getDesignerCadSessionSummary(
      cadSession.cad_session_id,
    );

    expect(eventsAfterOne.map((event) => event.event_id)).toEqual(["event-2"]);
    expect(operations.map((item) => item.operation_id)).toEqual([
      "op-extrude-sketch-0",
    ]);
    expect(summary?.workspace.workspace_key).toBe("prom-designer");
    expect(summary?.document.file_path).toBe("test-workspace/torus.build123d.py");
    expect(summary?.events.map((event) => event.sequence)).toEqual([1, 2]);
    expect(summary?.sceneNodes[0]?.stable_ref).toBe("build123d:sketch:0");
    expect(summary?.sourceEdits[0]?.operation_id).toBe("op-extrude-sketch-0");
    expect(summary?.previewUpdates[0]?.mesh_ref_json).toEqual({
      artifact: "mesh://preview-1",
    });
    expect(summary?.widgets[0]?.widget_key).toBe("cad/press-pull");
    expect(summary?.steers[0]?.target_agent_id).toBe("clanka-cad");
  });

  it("records Designer Codex conversations with object-backed payloads and telemetry", async () => {
    const transcriptObject = await store.recordDesignerObjectRef({
      objectRefId: "obj-transcript-019dec01",
      provider: "oci",
      uri: "oci://designer-codex/conversations/019dec01/transcript.jsonl",
      bucket: "designer-codex",
      key: "conversations/019dec01/transcript.jsonl",
      digestSha256: "sha256:transcript",
      byteSize: 4096,
      contentType: "application/jsonl",
      objectKind: "codex.transcript",
      metadataJson: {
        sourceSession: "codex:019dec01-6eaa-7650-986f-f41ab49a59fd",
      },
    });
    const turnPayloadObject = await store.recordDesignerObjectRef({
      objectRefId: "obj-turn-1",
      provider: "oci",
      uri: "oci://designer-codex/conversations/019dec01/turns/0001.json",
      digestSha256: "sha256:turn-1",
      contentType: "application/json",
      objectKind: "codex.turn",
    });
    const telemetryPayloadObject = await store.recordDesignerObjectRef({
      objectRefId: "obj-telemetry-1",
      provider: "oci",
      uri: "oci://designer-telemetry/events/usage/0001.json",
      digestSha256: "sha256:telemetry-1",
      contentType: "application/json",
      objectKind: "designer.telemetry.event",
    });

    const conversation = await store.recordDesignerCodexConversation({
      conversationId: "designer-codex-019dec01",
      provider: "codex",
      providerSessionId: "019dec01-6eaa-7650-986f-f41ab49a59fd",
      threadId: "remote-codex-thread-1",
      workspaceId: "designer-workspace-rubiks",
      workspaceKey: "rubiks-cube",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      workspaceRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
      model: "gpt-5.5",
      status: "running",
      transcriptObjectRefId: transcriptObject.object_ref_id,
      latestEventSequence: 1,
      metadataJson: {
        remoteCwd: "/Users/nikitavoloboev/work/codex-launch-rsync-fa10aa66",
      },
    });
    await store.recordDesignerCodexTurn({
      turnId: "designer-codex-019dec01:1",
      conversationId: conversation.conversation_id,
      sequence: 1,
      turnKind: "user",
      role: "user",
      actorKind: "human",
      actorId: "alice",
      summaryText: "Make a Rubik's Cube collaboratively.",
      payloadObjectRefId: turnPayloadObject.object_ref_id,
      status: "completed",
      tokenCountsJson: { input: 42 },
    });
    await store.recordDesignerTelemetryEvent({
      telemetryEventId: "usage-event-1",
      sessionId: "designer-session-1",
      workspaceId: "designer-workspace-rubiks",
      conversationId: conversation.conversation_id,
      eventType: "designer.agent_prompt_sent",
      pane: "chat",
      sequence: 1,
      summaryText: "prompt sent",
      payloadObjectRefId: telemetryPayloadObject.object_ref_id,
      propertiesJson: {
        upload_receipts: [
          {
            backend: "s3",
            uri: "s3://designer-telemetry/events/usage/0001.json",
          },
        ],
      },
    });

    const turns = await store.listDesignerCodexTurns({
      conversationId: conversation.conversation_id,
    });
    const telemetry = await store.listDesignerTelemetryEvents({
      conversationId: conversation.conversation_id,
    });
    const summary = await store.getDesignerCodexConversationSummary(
      conversation.conversation_id,
    );

    expect(turns.map((turn) => turn.payload_object_ref_id)).toEqual([
      "obj-turn-1",
    ]);
    expect(telemetry.map((event) => event.payload_object_ref_id)).toEqual([
      "obj-telemetry-1",
    ]);
    expect(summary?.transcriptObject.uri).toBe(
      "oci://designer-codex/conversations/019dec01/transcript.jsonl",
    );
    expect(summary?.turns[0]?.summary_text).toContain("Rubik");
    expect(summary?.telemetryEvents[0]?.event_type).toBe(
      "designer.agent_prompt_sent",
    );
  });

  it("records Designer agents and object-backed live commits", async () => {
    const patchObject = await store.recordDesignerObjectRef({
      objectRefId: "obj-commit-01f4d1e-patch",
      provider: "oci",
      uri: "oci://designer-commits/prom/live/01f4d1e.patch",
      contentType: "text/x-diff",
      objectKind: "vcs.commit.patch",
    });
    const manifestObject = await store.recordDesignerObjectRef({
      objectRefId: "obj-commit-01f4d1e-manifest",
      provider: "oci",
      uri: "oci://designer-commits/prom/live/01f4d1e.json",
      contentType: "application/json",
      objectKind: "vcs.commit.manifest",
    });
    const agent = await store.recordDesignerAgent({
      agentId: "agent.remote-codex.designer",
      agentKind: "codex",
      provider: "openai-codex",
      displayName: "Remote Codex Designer",
      model: "gpt-5.5",
      defaultContextJson: {
        repoRoot: "/Users/nikitavoloboev/code/prom",
        workspaceRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
      },
      status: "active",
    });
    await store.recordDesignerAgentTool({
      toolId: "agent.remote-codex.designer:tool:apply_patch",
      agentId: agent.agent_id,
      toolName: "apply_patch",
      toolKind: "workspace.edit",
      scopeJson: {
        repoRoot: "/Users/nikitavoloboev/code/prom",
        allowedPathPrefixes: ["ide/designer"],
      },
    });
    await store.recordDesignerAgentContext({
      contextId: "agent.remote-codex.designer:context:prom-live",
      agentId: agent.agent_id,
      contextKind: "workflow",
      sourceKind: "jazz.row",
      inlineContextJson: {
        branch: "live",
        invariant: "all committed changes are reflected into Jazz2",
      },
      priority: 10,
    });

    const commit = await store.recordDesignerLiveCommit({
      commitId: "01f4d1ea1cea8f331c1691a3312c6df1043db08b",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      workspaceRoot: "/Users/nikitavoloboev/code/prom/ide/designer",
      branch: "live",
      bookmark: "nikiv-live",
      subject: "fix(designer): harden remote codex chat replay",
      body:
        "Prevent replayed pending Remote Codex turns from resurrecting an already committed assistant answer as a stream draft.",
      traceRef:
        "codex:1_eyJzIjoiMDE5ZGViMGEtZDE5Yi03ZDkyLTgxZGQtNzY2MTJkMDc2ZDRjIiwidCI6MX0",
      sourceSessionId: "codex:019deb0a-d19b-7d92-81dd-76612d076d4c",
      sourceTurnOrdinal: 1,
      agentId: agent.agent_id,
      courierRunId: "live-commit-courier:6e5b4fe709dd9c25",
      changedPathsJson: [
        "ide/designer/src/v2/chat/HarnessChatTile.tsx",
        "ide/designer/src/v2/chat/harness-trace.ts",
      ],
      patchObjectRefId: patchObject.object_ref_id,
      manifestObjectRefId: manifestObject.object_ref_id,
      status: "reflected",
      committedAt: "2026-05-03T22:53:03Z",
      reflectedAt: "2026-05-03T22:54:34Z",
    });

    const commits = await store.listDesignerLiveCommits({
      repoRoot: "/Users/nikitavoloboev/code/prom",
      branch: "live",
      sourceSessionId: "codex:019deb0a-d19b-7d92-81dd-76612d076d4c",
    });
    const summary = await store.getDesignerLiveCommitSummary(commit.commit_id);
    const tools = await store.listDesignerAgentTools({ agentId: agent.agent_id });
    const contexts = await store.listDesignerAgentContexts({
      agentId: agent.agent_id,
    });

    expect(commits.map((item) => item.commit_id)).toEqual([
      "01f4d1ea1cea8f331c1691a3312c6df1043db08b",
    ]);
    expect(summary?.patchObject?.uri).toBe(
      "oci://designer-commits/prom/live/01f4d1e.patch",
    );
    expect(summary?.agent?.display_name).toBe("Remote Codex Designer");
    expect(tools[0]?.tool_name).toBe("apply_patch");
    expect(contexts[0]?.context_kind).toBe("workflow");
  });

  it("guards Designer CAD stream replay and relation consistency", async () => {
    await store.recordDesignerCadWorkspace({
      workspaceId: "workspace-a",
      workspaceKey: "workspace-a",
    });
    await store.recordDesignerCadWorkspace({
      workspaceId: "workspace-b",
      workspaceKey: "workspace-b",
    });
    await store.recordDesignerCadDocument({
      workspaceId: "workspace-a",
      documentId: "doc-a",
      filePath: "model.build123d.py",
    });

    await expect(
      store.recordDesignerCadSession({
        cadSessionId: "cad-session-mismatch",
        workspaceId: "workspace-b",
        documentId: "doc-a",
      }),
    ).rejects.toThrow(/not in workspace/);

    await store.recordDesignerCadSession({
      cadSessionId: "cad-session-pagination",
      workspaceId: "workspace-a",
      documentId: "doc-a",
    });
    await expect(
      store.recordDesignerCadOperation({
        operationId: "op-missing-tool",
        cadSessionId: "cad-session-pagination",
        toolSessionId: "missing-tool",
        actorKind: "agent",
        operationKind: "addFeature",
        operationJson: { kind: "addFeature" },
      }),
    ).rejects.toThrow(/tool session/);

    for (let sequence = 1; sequence <= 401; sequence += 1) {
      await store.recordDesignerCadEvent({
        cadSessionId: "cad-session-pagination",
        sequence,
        eventKind: "stream.delta",
        actorKind: "agent",
        sourceEventId: `source-${sequence}`,
        payloadJson: { sequence },
      });
    }
    const replayed = await store.recordDesignerCadEvent({
      cadSessionId: "cad-session-pagination",
      sequence: 401,
      eventKind: "stream.delta",
      actorKind: "agent",
      sourceEventId: "source-401",
      payloadJson: { sequence: 401, replayed: true },
    });

    const afterFourHundred = await store.listDesignerCadEvents({
      cadSessionId: "cad-session-pagination",
      afterSequence: 400,
    });
    const summary = await store.getDesignerCadSessionSummary(
      "cad-session-pagination",
    );

    expect(replayed.event_id).toBe("cad-session-pagination:source-401");
    expect(afterFourHundred.map((event) => event.sequence)).toEqual([401]);
    expect(afterFourHundred[0]?.payload_json).toEqual({
      sequence: 401,
      replayed: true,
    });
    expect(summary?.events).toHaveLength(401);
  }, 30_000);

  it("records daemon log sources, chunks, events, checkpoints, and summaries", async () => {
    const source = await store.recordDaemonLogSource({
      sourceId: "flow:sync:stderr",
      manager: "flow",
      daemonName: "sync",
      stream: "stderr",
      hostId: "workstation",
      logPath: "/Users/nikitavoloboev/.config/flow-state/daemons/sync/stderr.log",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      retentionClass: "normal",
      status: "active",
      createdAt: "2026-04-24T10:00:00.000Z",
      updatedAt: "2026-04-24T10:00:00.000Z",
    });
    const chunk = await store.recordDaemonLogChunk({
      chunkId: "chunk-1",
      sourceId: source.source_id,
      fileFingerprint: "dev:inode:size:mtime",
      startOffset: 0,
      endOffset: 128,
      firstLineNo: 1,
      lastLineNo: 2,
      lineCount: 2,
      byteCount: 128,
      sha256: "abc123",
      bodyPreview: "warn: slow sync",
      ingestedAt: "2026-04-24T10:01:00.000Z",
    });
    await store.recordDaemonLogEvent({
      eventId: "event-1",
      sourceId: source.source_id,
      chunkId: chunk.chunk_id,
      seq: 1,
      lineNo: 2,
      at: "2026-04-24T10:00:30.000Z",
      level: "warn",
      message: "sync took longer than expected",
      fieldsJson: { durationMs: 1250 },
      conversationHash: "conv-hash",
      traceId: "trace-1",
    });
    const checkpoint = await store.recordDaemonLogCheckpoint({
      sourceId: source.source_id,
      fileFingerprint: "dev:inode:size:mtime",
      offset: 128,
      lineNo: 2,
      lastChunkId: chunk.chunk_id,
      lastEventId: "event-1",
      lastSeenAt: "2026-04-24T10:01:00.000Z",
      updatedAt: "2026-04-24T10:01:00.000Z",
    });
    const summary = await store.recordDaemonLogSummary({
      summaryId: "summary-1",
      sourceId: source.source_id,
      windowStart: "2026-04-24T10:00:00.000Z",
      windowEnd: "2026-04-24T10:05:00.000Z",
      levelCountsJson: { warn: 1 },
      errorCount: 0,
      warningCount: 1,
      summaryText: "one warning",
    });

    const sources = await store.listDaemonLogSources({ manager: "flow" });
    const events = await store.listDaemonLogEvents({
      conversationHash: "conv-hash",
    });
    const summaries = await store.listDaemonLogSummaries({
      daemonName: "sync",
    });

    expect(sources.map((item) => item.source_id)).toEqual([
      "flow:sync:stderr",
    ]);
    expect(events.map((item) => item.event_id)).toEqual(["event-1"]);
    expect(events[0]?.repo_root).toBe("/Users/nikitavoloboev/code/prom");
    expect(checkpoint.checkpoint_id).toBe(source.source_id);
    expect(summary.summary_id).toBe("summary-1");
    expect(summaries).toHaveLength(1);
  });

  it("records cursor review operations and hides processed entries by default", async () => {
    const operation = await store.recordCursorReviewOperation({
      operationId: "cursor-op-1",
      operationType: "delete-branch-path",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      workspaceRoot: "/Users/nikitavoloboev/code/prom",
      bookmark: "review/nikiv-ai-proxy-opus-4-7-thinking",
      relPath: "tests",
      note: "remove failing branch tests",
      sourceSessionId: "cursor:session-1",
      sourceChatKind: "cursor",
    });

    const pending = await store.listCursorReviewOperations({
      repoRoot: "/Users/nikitavoloboev/code/prom",
    });

    expect(operation.operationId).toBe("cursor-op-1");
    expect(operation.relPath).toBe("tests");
    expect(pending).toHaveLength(1);
    expect(pending[0]?.bookmark).toBe(
      "review/nikiv-ai-proxy-opus-4-7-thinking",
    );
    expect(pending[0]?.latestResult).toBeUndefined();

    const result = await store.recordCursorReviewResult({
      operationId: "cursor-op-1",
      status: "completed",
      clientId: "flow-window-1",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      message: "Flow opened the diff",
    });

    const filtered = await store.listCursorReviewOperations({
      repoRoot: "/Users/nikitavoloboev/code/prom",
    });
    const withProcessed = await store.listCursorReviewOperations({
      repoRoot: "/Users/nikitavoloboev/code/prom",
      includeProcessed: true,
    });

    expect(result.operationId).toBe("cursor-op-1");
    expect(filtered).toEqual([]);
    expect(withProcessed).toHaveLength(1);
    expect(withProcessed[0]?.latestResult?.status).toBe("completed");
    expect(withProcessed[0]?.latestResult?.message).toBe(
      "Flow opened the diff",
    );
  });

  it("records and lists latest branch file review states", async () => {
    await store.recordBranchFileReviewState({
      eventId: "branch-file-review-1",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      workspaceRoot: "/Users/nikitavoloboev/code/prom",
      bookmark: "review/nikiv-designer-telemetry-pr1-main",
      relPath: "ide/designer/src/telemetry/log.ts",
      status: "needs-work",
      note: "event names are too noisy",
      sourceSessionId: "cursor:session-1",
      sourceChatKind: "cursor",
    });
    await store.recordBranchFileReviewState({
      eventId: "branch-file-review-2",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      workspaceRoot: "/Users/nikitavoloboev/code/prom",
      bookmark: "review/nikiv-designer-telemetry-pr1-main",
      relPath: "ide/designer/src/telemetry/log.ts",
      status: "happy",
      note: "looks good now",
      sourceSessionId: "codex:session-2",
      sourceChatKind: "codex",
    });
    await store.recordBranchFileReviewState({
      eventId: "branch-file-review-3",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      workspaceRoot: "/Users/nikitavoloboev/code/prom",
      bookmark: "review/nikiv-designer-telemetry-pr1-main",
      relPath: "ide/designer/src/telemetry/buffer.ts",
      status: "cleared",
    });

    const states = await store.listBranchFileReviewStates({
      repoRoot: "/Users/nikitavoloboev/code/prom",
      bookmark: "review/nikiv-designer-telemetry-pr1-main",
    });
    const withCleared = await store.listBranchFileReviewStates({
      repoRoot: "/Users/nikitavoloboev/code/prom",
      bookmark: "review/nikiv-designer-telemetry-pr1-main",
      includeCleared: true,
    });

    expect(states).toHaveLength(1);
    expect(states[0]?.relPath).toBe("ide/designer/src/telemetry/log.ts");
    expect(states[0]?.status).toBe("happy");
    expect(states[0]?.note).toBe("looks good now");
    expect(withCleared).toHaveLength(2);
    expect(
      withCleared.find(
        (item) => item.relPath === "ide/designer/src/telemetry/buffer.ts",
      )?.status,
    ).toBe("cleared");
  });
});

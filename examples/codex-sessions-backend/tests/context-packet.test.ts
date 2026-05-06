import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  buildProjectContextPacket,
  getProjectContextPacketSection,
} from "../src/context-packet.js";
import { createCodexSessionStore, type CodexSessionStore } from "../src/index.js";

describe("project context packets", () => {
  let tempDir: string;
  let store: CodexSessionStore;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "codex-context-packet-"));
    store = createCodexSessionStore({
      appId: "codex-context-packet-test",
      dataPath: join(tempDir, "codex-sessions.db"),
    });
  });

  afterEach(async () => {
    await store.shutdown();
    await rm(tempDir, { recursive: true, force: true });
  });

  it("builds a PageIndex-style structure with bounded section expansion and cursors", async () => {
    await store.replaceSessionProjection(
      {
        sessionId: "session-active",
        rolloutPath: "/tmp/session-active.jsonl",
        cwd: "/Users/nikitavoloboev/code/prom",
        projectRoot: "/Users/nikitavoloboev/code/prom",
        repoRoot: "/Users/nikitavoloboev/code/prom",
        status: "in_progress",
        createdAt: "2026-05-06T10:00:00.000Z",
        updatedAt: "2026-05-06T10:06:00.000Z",
        latestUserMessage: "build indexed context packets",
        latestAssistantPartial: "Reading the Jazz2 session store",
        turns: [
          {
            turnId: "turn-active",
            sequence: 1,
            status: "in_progress",
            userMessage: "build indexed context packets",
            assistantPartial: "Reading the Jazz2 session store",
            updatedAt: "2026-05-06T10:06:00.000Z",
          },
        ],
      },
      {
        sourceId: "session-active",
        absolutePath: "/tmp/session-active.jsonl",
        lineCount: 12,
        syncedAt: "2026-05-06T10:06:00.000Z",
      },
    );
    await store.replaceSessionProjection(
      {
        sessionId: "session-complete",
        rolloutPath: "/tmp/session-complete.jsonl",
        cwd: "/Users/nikitavoloboev/code/prom",
        projectRoot: "/Users/nikitavoloboev/code/prom",
        repoRoot: "/Users/nikitavoloboev/code/prom",
        status: "completed",
        createdAt: "2026-05-06T09:50:00.000Z",
        updatedAt: "2026-05-06T10:02:00.000Z",
        latestUserMessage: "review PageIndex",
        latestAssistantMessage: "PageIndex uses tree search over document structure.",
        turns: [
          {
            turnId: "turn-complete",
            sequence: 1,
            status: "completed",
            userMessage: "review PageIndex",
            assistantMessage: "PageIndex uses tree search over document structure.",
            completedAt: "2026-05-06T10:02:00.000Z",
            updatedAt: "2026-05-06T10:02:00.000Z",
          },
        ],
      },
      {
        sourceId: "session-complete",
        absolutePath: "/tmp/session-complete.jsonl",
        lineCount: 20,
        syncedAt: "2026-05-06T10:02:00.000Z",
      },
    );
    await store.recordCodexStreamEvent({
      sessionId: "session-active",
      turnId: "turn-active",
      sequence: 13,
      eventKind: "assistant_delta",
      eventType: "message_delta",
      textDelta: "new delta",
      sourcePath: "/tmp/session-active.jsonl",
      observedAt: "2026-05-06T10:06:30.000Z",
    });
    await store.recordProjectContext({
      projectRoot: "/Users/nikitavoloboev/code/prom",
      provider: "codex",
      sessionId: "session-complete",
      turnId: "turn-complete",
      sourceKind: "decision",
      summary: "Use PageIndex-style tree retrieval before exact section expansion.",
      body: "Jazz2 should keep offsets and evidence refs; PageIndex should inspire structure-first lookup.",
      updatedAt: "2026-05-06T10:03:00.000Z",
      metadataJson: { paths: ["examples/codex-sessions-backend/src/context-packet.ts"] },
    });

    const packet = await buildProjectContextPacket(store, {
      projectRoot: "/Users/nikitavoloboev/code/prom",
      since: "1h",
      now: "2026-05-06T10:07:00.000Z",
      includeSectionBodies: false,
    });

    expect(packet.document.structure.map((node) => node.title)).toEqual([
      "Active Sessions",
      "Recent Turns",
      "Project Context",
      "Stream Cursors",
    ]);
    expect(packet.sections).toBeUndefined();
    expect(packet.cursors.stream).toEqual([
      {
        sessionId: "session-active",
        afterSequence: 13,
        eventId: expect.any(String),
        observedAt: "2026-05-06T10:06:30.000Z",
      },
    ]);

    const activeSection = await getProjectContextPacketSection(store, {
      projectRoot: "/Users/nikitavoloboev/code/prom",
      nodeId: "0001",
      since: "1h",
      now: "2026-05-06T10:07:00.000Z",
    });
    expect(activeSection?.items).toEqual([
      expect.objectContaining({
        kind: "active-session",
        title: "session-active",
        summary: "build indexed context packets",
        refs: ["codex:session-active", "rollout:/tmp/session-active.jsonl#line=12"],
      }),
    ]);
    expect(activeSection?.body).toContain("session-active");

    const fullPacket = await buildProjectContextPacket(store, {
      projectRoot: "/Users/nikitavoloboev/code/prom",
      since: "1h",
      now: "2026-05-06T10:07:00.000Z",
      includeSectionBodies: true,
    });
    expect(fullPacket.sections?.find((section) => section.nodeId === "0003")?.body).toContain(
      "PageIndex-style tree retrieval",
    );
  });
});

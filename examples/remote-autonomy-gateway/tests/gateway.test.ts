import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  createRemoteAutonomyGateway,
  type RemoteAutonomyGateway,
} from "../src/app.js";

describe("remote autonomy gateway", () => {
  let tempDir: string;
  let gateway: RemoteAutonomyGateway;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "remote-autonomy-gateway-"));
    gateway = createRemoteAutonomyGateway({
      agentDataPath: join(tempDir, "agent-infra.db"),
      codexDataPath: join(tempDir, "codex-sessions.db"),
      syncServerUrl: "https://jazz2.example.test",
      syncServerAppId: "test-app-id",
      hostId: "mac-test",
      connectStoresToSyncServer: false,
      syncServerProbe: async () => ({
        ok: true,
        status: "healthy",
        latencyMs: 3,
      }),
    });
  });

  afterEach(async () => {
    await gateway.close();
    await rm(tempDir, { recursive: true, force: true });
  });

  it("reports bootstrap and sync-server health without requiring a live server", async () => {
    const response = await requestJson("GET", "/health");

    expect(response).toMatchObject({
      ok: true,
      service: "remote-autonomy-gateway",
      hostId: "mac-test",
      syncServer: {
        url: "https://jazz2.example.test",
        appId: "test-app-id",
        ok: true,
        status: "healthy",
      },
    });

    const bootstrap = await requestJson("GET", "/v1/bootstrap");
    expect(bootstrap).toMatchObject({
      ok: true,
      endpoints: {
        health: "/health",
        state: "/v1/state",
        codexPresence: "/v1/codex/presence",
        codexStreamEvents: "/v1/codex/stream-events",
        syncJobs: "/v1/sync/jobs",
      },
      syncServer: {
        url: "https://jazz2.example.test",
        appId: "test-app-id",
      },
    });
  });

  it("records Codex terminal presence into Jazz2 and exposes active sessions", async () => {
    const recorded = await requestJson("POST", "/v1/codex/presence", {
      terminalSessionId: "terminal-1",
      sessionId: "codex-session-1",
      turnId: "turn-1",
      cwd: "/srv/codex/openai/codex",
      projectRoot: "/srv/codex/openai/codex",
      repoRoot: "/srv/codex/openai/codex",
      state: "running",
      runtimeHost: "gpu-a",
      pid: 991,
    });

    expect(recorded).toMatchObject({
      ok: true,
      session: {
        session_id: "codex-session-1",
        cwd: "/srv/codex/openai/codex",
      },
      presence: {
        session_id: "codex-session-1",
        state: "running",
      },
    });

    const sessions = await requestJson("GET", "/v1/codex/sessions?active=1");
    expect(sessions.sessions).toHaveLength(1);
    expect(sessions.sessions[0]).toMatchObject({
      sessionId: "codex-session-1",
      state: "running",
      cwd: "/srv/codex/openai/codex",
    });
  });

  it("records Codex stream events for remote tail replication", async () => {
    const recorded = await requestJson("POST", "/v1/codex/stream-events", {
      sessionId: "codex-session-1",
      turnId: "turn-1",
      sequence: 7,
      eventKind: "event_msg",
      eventType: "agent_message_content_delta",
      sourceId: "rollout:/srv/.codex/session.jsonl",
      sourceHost: "gpu-a",
      sourcePath: "/srv/.codex/session.jsonl",
      textDelta: "live from linux",
      payloadJson: { delta: "live from linux" },
      rawJson: "{\"type\":\"event_msg\"}",
      schemaHash: "schema-hash-test",
      createdAt: "2026-05-02T20:00:00.000Z",
      observedAt: "2026-05-02T20:00:00.010Z",
    });

    expect(recorded).toMatchObject({
      ok: true,
      event: {
        sessionId: "codex-session-1",
        turnId: "turn-1",
        sequence: 7,
        eventKind: "event_msg",
        eventType: "agent_message_content_delta",
        sourceHost: "gpu-a",
        textDelta: "live from linux",
        schemaHash: "schema-hash-test",
      },
    });

    const listed = await requestJson(
      "GET",
      "/v1/codex/stream-events?sessionId=codex-session-1&afterSequence=6",
    );
    expect(listed.events).toHaveLength(1);
    expect(listed.events[0]).toMatchObject({
      eventId: recorded.event.eventId,
      sessionId: "codex-session-1",
      sequence: 7,
      textDelta: "live from linux",
    });
  });

  it("creates idempotent sync jobs, claims them, and records completion receipts", async () => {
    const created = await requestJson("POST", "/v1/sync/jobs", {
      kind: "rsync-mirror",
      repoRoot: "/srv/codex/openai/codex",
      workspaceRoot: "/srv/codex/openai/codex",
      sourceSession: "codex-session-1",
      dedupeKey: "rsync:/srv/codex/openai/codex:/Users/nikitavoloboev/repos/openai/codex",
      payloadJson: {
        sourcePath: "/srv/codex/openai/codex",
        targetPath: "/Users/nikitavoloboev/repos/openai/codex",
        transport: "rsync",
      },
    });
    const duplicate = await requestJson("POST", "/v1/sync/jobs", {
      kind: "rsync-mirror",
      dedupeKey: "rsync:/srv/codex/openai/codex:/Users/nikitavoloboev/repos/openai/codex",
    });

    expect(created.job.status).toBe("queued");
    expect(duplicate.job.jobId).toBe(created.job.jobId);

    const claimed = await requestJson(
      "POST",
      `/v1/sync/jobs/${created.job.jobId}/claim`,
      {
        claimedBy: "server-worker-gpu-a",
      },
    );
    expect(claimed.job).toMatchObject({
      jobId: created.job.jobId,
      status: "claimed",
      claimedBy: "server-worker-gpu-a",
    });

    const receipt = await requestJson("POST", "/v1/sync/receipts", {
      jobId: created.job.jobId,
      status: "completed",
      transport: "rsync",
      sourcePath: "/srv/codex/openai/codex",
      targetPath: "/Users/nikitavoloboev/repos/openai/codex",
      checksum: "sha256:abc",
      bytes: 42,
    });
    expect(receipt).toMatchObject({
      ok: true,
      receipt: {
        jobId: created.job.jobId,
        status: "completed",
        transport: "rsync",
      },
      job: {
        status: "completed",
      },
    });

    const jobs = await requestJson("GET", "/v1/sync/jobs?includeFinished=1");
    expect(jobs.jobs).toHaveLength(1);
    expect(jobs.jobs[0]).toMatchObject({
      jobId: created.job.jobId,
      status: "completed",
    });
  });

  it("records remote workspace claims and includes them in state", async () => {
    const claim = await requestJson("POST", "/v1/claims", {
      scope: "repo:/srv/codex/openai/codex",
      owner: "server-worker-gpu-a",
      ownerSession: "codex-session-1",
      mode: "exclusive-write",
      repoRoot: "/srv/codex/openai/codex",
      workspaceRoot: "/srv/codex/openai/codex",
      note: "server owns remote Codex workspace while Mac mirrors it",
    });

    expect(claim.claim).toMatchObject({
      scope: "repo:/srv/codex/openai/codex",
      owner: "server-worker-gpu-a",
      status: "active",
    });

    const state = await requestJson("GET", "/v1/state");
    expect(state.claims).toHaveLength(1);
    expect(state.claims[0]).toMatchObject({
      scope: "repo:/srv/codex/openai/codex",
      status: "active",
    });
  });

  async function requestJson(method: string, path: string, body?: unknown) {
    const response = await gateway.app.handle(
      new Request(`http://remote-autonomy.test${path}`, {
        method,
        headers: body === undefined ? undefined : { "content-type": "application/json" },
        body: body === undefined ? undefined : JSON.stringify(body),
      }),
    );
    const json = await response.json();
    expect(response.status).toBeGreaterThanOrEqual(200);
    expect(response.status).toBeLessThan(300);
    return json;
  }
});

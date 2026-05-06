import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { hostname, tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createRemoteAutonomyGateway, type RemoteAutonomyGateway } from "../src/app.js";
import { runRemoteAutonomyWorkerOnce, type CommandRunner } from "../src/worker.js";

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
      localSpacesRoot: join(tempDir, ".designer", "spaces"),
      remoteSpacesRoot: "/users/nikiv/.designer/spaces",
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
        executorTraces: "/v1/executor/traces",
        syncJobs: "/v1/sync/jobs",
        spaces: "/v1/spaces",
      },
      syncServer: {
        url: "https://jazz2.example.test",
        appId: "test-app-id",
      },
    });
  });

  it("defaults hostId to the machine hostname, not an unknown placeholder", async () => {
    await gateway.close();
    gateway = createRemoteAutonomyGateway({
      agentDataPath: join(tempDir, "hostname-agent-infra.db"),
      codexDataPath: join(tempDir, "hostname-codex-sessions.db"),
      syncServerUrl: "https://jazz2.example.test",
      syncServerAppId: "test-app-id",
      localSpacesRoot: join(tempDir, ".designer", "spaces"),
      remoteSpacesRoot: "/users/nikiv/.designer/spaces",
      connectStoresToSyncServer: false,
      syncServerProbe: async () => ({
        ok: true,
        status: "healthy",
        latencyMs: 3,
      }),
    });

    const response = await requestJson("GET", "/health");

    expect(response.hostId).toBe(hostname());
    expect(response.hostId).not.toBe("unknown-host");
  });

  it("records executor traces as durable semantic events", async () => {
    const recorded = await requestJson("POST", "/v1/executor/traces", {
      schemaVersion: 1,
      kind: "nullclaw.worker.result",
      executor: "nullclaw",
      eventType: "nullclaw_worker_trace",
      trace_id: "trace-123",
      run_id: "run-123",
      task_id: "step-1",
      session_key: "agent:coder:worker:run-123",
      status: "ok",
      thread_events: [{ type: "tool_summary", total: 1, failed: 0 }],
    });

    expect(recorded).toMatchObject({
      ok: true,
      traceId: "trace-123",
      event: {
        eventId: "nullclaw_worker_trace:nullclaw:trace-123",
        eventType: "nullclaw_worker_trace",
        payloadJson: {
          traceId: "trace-123",
          executor: "nullclaw",
          status: "ok",
          hostId: "mac-test",
        },
      },
    });

    const duplicate = await requestJson("POST", "/v1/executor/traces", {
      executor: "nullclaw",
      eventType: "nullclaw_worker_trace",
      traceId: "trace-123",
      status: "skipped",
    });
    expect(duplicate.event.eventId).toBe(recorded.event.eventId);
    expect(duplicate.event.payloadJson.status).toBe("skipped");

    const listed = await requestJson(
      "GET",
      "/v1/executor/traces?eventType=nullclaw_worker_trace&executor=nullclaw&traceId=trace-123",
    );
    expect(listed.events).toHaveLength(1);
    expect(listed.events[0]).toMatchObject({
      eventId: "nullclaw_worker_trace:nullclaw:trace-123",
      eventType: "nullclaw_worker_trace",
      payloadJson: {
        traceId: "trace-123",
        status: "skipped",
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
      rawJson: '{"type":"event_msg"}',
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

    const claimed = await requestJson("POST", `/v1/sync/jobs/${created.job.jobId}/claim`, {
      claimedBy: "server-worker-gpu-a",
    });
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

  it("registers Designer spaces with local mirrors, remote roots, and object storage prefixes", async () => {
    const registered = await requestJson("POST", "/v1/spaces", {
      slug: "designer-starter-project",
      title: "Designer Starter Project",
      localPath: "/Users/nikitavoloboev/code/prom/ide/designer/starter-project",
      remotePath: "/users/nikiv/code/prom/ide/designer/starter-project",
      ownerSession: "codex-session-1",
    });

    expect(registered).toMatchObject({
      ok: true,
      space: {
        slug: "designer-starter-project",
        title: "Designer Starter Project",
        localPath: "/Users/nikitavoloboev/code/prom/ide/designer/starter-project",
        remotePath: "/users/nikiv/code/prom/ide/designer/starter-project",
        objectStoragePrefix: "nikiv/designer/designer-starter-project",
        objectStorageUri: "oci://us-sanjose-1/x-sanjose/nikiv/designer/designer-starter-project/",
        objectStorage: {
          provider: "oci",
          region: "us-sanjose-1",
          bucket: "x-sanjose",
          prefix: "nikiv/designer/designer-starter-project",
          uri: "oci://us-sanjose-1/x-sanjose/nikiv/designer/designer-starter-project/",
        },
      },
      job: {
        kind: "space-rsync-mirror",
        status: "queued",
        workspaceRoot: "/users/nikiv/code/prom/ide/designer/starter-project",
        dedupeKey: "space-rsync-mirror:designer-starter-project",
        payloadJson: {
          sourcePath: "/users/nikiv/code/prom/ide/designer/starter-project",
          targetPath: "/Users/nikitavoloboev/code/prom/ide/designer/starter-project",
          transport: "rsync",
        },
      },
      claim: {
        claimId: "designer-space:designer-starter-project",
        scope: "space:designer-starter-project",
        ownerSession: "codex-session-1",
        workspaceRoot: "/users/nikiv/code/prom/ide/designer/starter-project",
      },
    });

    const listed = await requestJson("GET", "/v1/spaces");
    expect(listed.spaces).toEqual([
      expect.objectContaining({
        slug: "designer-starter-project",
        objectStorageUri: "oci://us-sanjose-1/x-sanjose/nikiv/designer/designer-starter-project/",
        objectStorage: {
          provider: "oci",
          region: "us-sanjose-1",
          bucket: "x-sanjose",
          prefix: "nikiv/designer/designer-starter-project",
          uri: "oci://us-sanjose-1/x-sanjose/nikiv/designer/designer-starter-project/",
        },
      }),
    ]);
  });

  it("defaults Designer space paths from configured roots", async () => {
    const registered = await requestJson("POST", "/v1/spaces", {
      slug: "bay-bridge-clock",
    });

    expect(registered).toMatchObject({
      ok: true,
      space: {
        slug: "bay-bridge-clock",
        title: "bay-bridge-clock",
        localPath: join(tempDir, ".designer", "spaces", "bay-bridge-clock", "work"),
        remotePath: "/users/nikiv/.designer/spaces/bay-bridge-clock/work",
        objectStoragePrefix: "nikiv/designer/bay-bridge-clock",
        objectStorageUri: "oci://us-sanjose-1/x-sanjose/nikiv/designer/bay-bridge-clock/",
        objectStorage: {
          provider: "oci",
          region: "us-sanjose-1",
          bucket: "x-sanjose",
          prefix: "nikiv/designer/bay-bridge-clock",
          uri: "oci://us-sanjose-1/x-sanjose/nikiv/designer/bay-bridge-clock/",
        },
        syncKind: "space-rsync-mirror",
      },
      job: {
        repoRoot: "/users/nikiv/.designer/spaces/bay-bridge-clock/work",
        workspaceRoot: "/users/nikiv/.designer/spaces/bay-bridge-clock/work",
        payloadJson: {
          sourcePath: "/users/nikiv/.designer/spaces/bay-bridge-clock/work",
          targetPath: join(tempDir, ".designer", "spaces", "bay-bridge-clock", "work"),
          transport: "rsync",
        },
      },
      claim: {
        scope: "space:bay-bridge-clock",
        mode: "sync-owner",
        owner: "mac-test",
      },
    });
  });

  it("uses configured Designer object storage settings", async () => {
    await gateway.close();
    gateway = createRemoteAutonomyGateway({
      agentDataPath: join(tempDir, "custom-agent-infra.db"),
      codexDataPath: join(tempDir, "custom-codex-sessions.db"),
      syncServerUrl: "https://jazz2.example.test",
      syncServerAppId: "test-app-id",
      hostId: "mac-test",
      localSpacesRoot: join(tempDir, ".designer", "spaces"),
      remoteSpacesRoot: "/users/nikiv/.designer/spaces",
      objectStorageRegion: "us-ashburn-1",
      objectStorageBucket: "designer-spaces-test",
      designerSpacesPrefix: "/custom/designer/spaces/",
      connectStoresToSyncServer: false,
      syncServerProbe: async () => ({
        ok: true,
        status: "healthy",
        latencyMs: 3,
      }),
    });

    const registered = await requestJson("POST", "/v1/spaces", {
      slug: "custom-space",
    });

    expect(registered.space).toMatchObject({
      objectStoragePrefix: "custom/designer/spaces/custom-space",
      objectStorageUri:
        "oci://us-ashburn-1/designer-spaces-test/custom/designer/spaces/custom-space/",
      objectStorage: {
        provider: "oci",
        region: "us-ashburn-1",
        bucket: "designer-spaces-test",
        prefix: "custom/designer/spaces/custom-space",
        uri: "oci://us-ashburn-1/designer-spaces-test/custom/designer/spaces/custom-space/",
      },
    });
  });

  it("does not list Designer spaces with inconsistent object storage descriptors", async () => {
    await requestJson("POST", "/v1/sync/jobs", {
      kind: "space-rsync-mirror",
      repoRoot: "/users/nikiv/.designer/spaces/bad-storage-space/work",
      workspaceRoot: "/users/nikiv/.designer/spaces/bad-storage-space/work",
      payloadJson: {
        space: {
          slug: "bad-storage-space",
          title: "Bad Storage Space",
          localPath: join(tempDir, ".designer", "spaces", "bad-storage-space", "work"),
          remotePath: "/users/nikiv/.designer/spaces/bad-storage-space/work",
          objectStorage: {
            provider: "oci",
            region: "us-sanjose-1",
            bucket: "x-sanjose",
            prefix: "nikiv/designer/bad-storage-space",
            uri: "oci://us-sanjose-1/x-sanjose/nikiv/designer/other-space/",
          },
        },
      },
    });

    const listed = await requestJson("GET", "/v1/spaces");
    expect(listed.spaces).toEqual([]);
  });

  it("rejects invalid Designer space slugs before recording jobs", async () => {
    const rejected = await requestJsonWithStatus("POST", "/v1/spaces", 400, {
      slug: "../not-a-space",
    });

    expect(rejected).toMatchObject({
      ok: false,
      error: "invalid Designer space slug ../not-a-space",
    });

    const jobs = await requestJson("GET", "/v1/sync/jobs?kind=space-rsync-mirror");
    expect(jobs.jobs).toHaveLength(0);
  });

  it("keeps Designer space registration idempotent by slug", async () => {
    const first = await requestJson("POST", "/v1/spaces", {
      slug: "shared-space",
      ownerSession: "codex-session-1",
    });
    const second = await requestJson("POST", "/v1/spaces", {
      slug: "shared-space",
      ownerSession: "codex-session-1",
    });

    expect(second.job.jobId).toBe(first.job.jobId);
    expect(second.claim.claimId).toBe(first.claim.claimId);

    const state = await requestJson("GET", "/v1/state");
    expect(state.spaces).toEqual([
      expect.objectContaining({
        slug: "shared-space",
      }),
    ]);
    expect(state.claims).toEqual([
      expect.objectContaining({
        claimId: "designer-space:shared-space",
        scope: "space:shared-space",
      }),
    ]);

    const limited = await requestJson("GET", "/v1/spaces?limit=1");
    expect(limited.spaces).toHaveLength(1);

    const negativeLimit = await requestJson("GET", "/v1/spaces?limit=-1");
    expect(negativeLimit.spaces).toEqual([]);
  });

  it("keeps the Designer space visible after a worker claims and completes its rsync job", async () => {
    const registered = await requestJson("POST", "/v1/spaces", {
      slug: "remote-cad-space",
      ownerSession: "codex-session-1",
    });

    const claimed = await requestJson("POST", `/v1/sync/jobs/${registered.job.jobId}/claim`, {
      claimedBy: "op1-rsync-worker",
    });
    expect(claimed.job).toMatchObject({
      jobId: registered.job.jobId,
      status: "claimed",
      claimedBy: "op1-rsync-worker",
      kind: "space-rsync-mirror",
    });

    const receipt = await requestJson("POST", "/v1/sync/receipts", {
      jobId: registered.job.jobId,
      status: "completed",
      transport: "rsync",
      sourcePath: registered.space.remotePath,
      targetPath: registered.space.localPath,
      checksum: "sha256:space",
      bytes: 4096,
      payloadJson: {
        slug: registered.space.slug,
      },
    });
    expect(receipt).toMatchObject({
      ok: true,
      receipt: {
        jobId: registered.job.jobId,
        status: "completed",
        transport: "rsync",
        sourcePath: "/users/nikiv/.designer/spaces/remote-cad-space/work",
        targetPath: join(tempDir, ".designer", "spaces", "remote-cad-space", "work"),
      },
      job: {
        status: "completed",
        resultJson: {
          status: "completed",
          transport: "rsync",
        },
      },
    });

    const listed = await requestJson("GET", "/v1/spaces");
    expect(listed.spaces).toEqual([
      expect.objectContaining({
        slug: "remote-cad-space",
        localPath: join(tempDir, ".designer", "spaces", "remote-cad-space", "work"),
        remotePath: "/users/nikiv/.designer/spaces/remote-cad-space/work",
      }),
    ]);
  });

  it("runs the server worker to claim and mirror queued Designer space sync jobs", async () => {
    const remotePath = join(tempDir, "server-spaces", "worker-sync-space", "work");
    const localPath = join(tempDir, "mac-mirrors", "worker-sync-space", "work");
    await mkdir(join(remotePath, "parts"), { recursive: true });
    await writeFile(join(remotePath, "parts", "bracket.build123d.py"), "from build123d import *\n");

    const registered = await requestJson("POST", "/v1/spaces", {
      slug: "worker-sync-space",
      remotePath,
      localPath,
    });

    const result = await runRemoteAutonomyWorkerOnce({
      gatewayUrl: "http://remote-autonomy.test",
      workerId: "op1-worker-test",
      fetchImpl: gatewayFetch(),
      commandRunner: rsyncCopyRunner(),
    });

    expect(result).toMatchObject({
      workerId: "op1-worker-test",
      processed: 1,
      completed: 1,
      failed: 0,
    });
    expect(await readFile(join(localPath, "parts", "bracket.build123d.py"), "utf8")).toBe(
      "from build123d import *\n",
    );

    const jobs = await requestJson("GET", "/v1/sync/jobs?includeFinished=1");
    expect(jobs.jobs).toEqual([
      expect.objectContaining({
        jobId: registered.job.jobId,
        status: "completed",
        resultJson: expect.objectContaining({
          status: "completed",
          transport: "rsync",
          sourcePath: remotePath,
          targetPath: localPath,
          payloadJson: expect.objectContaining({
            workerId: "op1-worker-test",
          }),
        }),
      }),
    ]);
  });

  it("records Designer space files with object refs and worker jobs", async () => {
    await requestJson("POST", "/v1/spaces", {
      slug: "remote-cad-space",
      title: "Remote CAD Space",
    });

    const recorded = await requestJson("POST", "/v1/spaces/remote-cad-space/files", {
      path: "parts/gear.build123d.py",
      contentHash: "sha256:gear-v1",
      sizeBytes: 4096,
      contentType: "text/x-python",
      revisionId: "rev-gear-1",
      writerId: "designer-user",
      sourceSession: "codex-session-1",
    });

    expect(recorded).toMatchObject({
      ok: true,
      file: {
        spaceSlug: "remote-cad-space",
        path: "parts/gear.build123d.py",
        localPath: join(
          tempDir,
          ".designer",
          "spaces",
          "remote-cad-space",
          "work",
          "parts",
          "gear.build123d.py",
        ),
        remotePath: "/users/nikiv/.designer/spaces/remote-cad-space/work/parts/gear.build123d.py",
        contentHash: "sha256:gear-v1",
        sizeBytes: 4096,
        contentType: "text/x-python",
        revisionId: "rev-gear-1",
        writerId: "designer-user",
        sourceSession: "codex-session-1",
        objectStorage: {
          provider: "oci",
          region: "us-sanjose-1",
          bucket: "x-sanjose",
          key: "nikiv/designer/remote-cad-space/files/parts/gear.build123d.py",
          uri: "oci://us-sanjose-1/x-sanjose/nikiv/designer/remote-cad-space/files/parts/gear.build123d.py",
        },
      },
      uploadJob: {
        kind: "space-file-object-upload",
        status: "queued",
        dedupeKey:
          "space-file-object-upload:remote-cad-space:parts/gear.build123d.py:sha256:gear-v1",
      },
      materializeJob: {
        kind: "space-file-materialize",
        status: "queued",
        dedupeKey:
          "space-file-materialize:remote-cad-space:parts/gear.build123d.py:sha256:gear-v1:local",
      },
    });

    const listed = await requestJson("GET", "/v1/spaces/remote-cad-space/files");
    expect(listed.files).toEqual([
      expect.objectContaining({
        path: "parts/gear.build123d.py",
        objectRefId: recorded.file.objectRefId,
        uploadJobId: recorded.uploadJob.jobId,
        materializeJobId: recorded.materializeJob.jobId,
      }),
    ]);

    const jobs = await requestJson("GET", "/v1/sync/jobs?kind=space-file-object-upload");
    expect(jobs.jobs).toEqual([
      expect.objectContaining({
        jobId: recorded.uploadJob.jobId,
        payloadJson: expect.objectContaining({
          file: expect.objectContaining({
            path: "parts/gear.build123d.py",
            contentHash: "sha256:gear-v1",
          }),
          objectStorage: expect.objectContaining({
            key: "nikiv/designer/remote-cad-space/files/parts/gear.build123d.py",
          }),
        }),
      }),
    ]);
  });

  it("runs the server worker to upload object bytes and materialize files from cache", async () => {
    const remotePath = join(tempDir, "server-spaces", "worker-object-space", "work");
    const localPath = join(tempDir, "mac-mirrors", "worker-object-space", "work");
    const sourcePath = join(tempDir, "uploads", "gear.build123d.py");
    const source = "from build123d import *\n\ngear = Box(2, 3, 5)\n";
    const contentHash = `sha256:${createHash("sha256").update(source).digest("hex")}`;
    await mkdir(dirname(sourcePath), { recursive: true });
    await mkdir(remotePath, { recursive: true });
    await writeFile(sourcePath, source);
    await requestJson("POST", "/v1/spaces", {
      slug: "worker-object-space",
      remotePath,
      localPath,
    });
    const recorded = await requestJson("POST", "/v1/spaces/worker-object-space/files", {
      path: "parts/gear.build123d.py",
      contentHash,
      sizeBytes: Buffer.byteLength(source),
      contentType: "text/x-python",
      materializeTarget: "remote",
      sourcePath,
    });

    const result = await runRemoteAutonomyWorkerOnce({
      gatewayUrl: "http://remote-autonomy.test",
      workerId: "op1-worker-test",
      fetchImpl: gatewayFetch(),
      commandRunner: rsyncCopyRunner(),
      localSpacesRoot: join(tempDir, ".designer", "spaces"),
    });

    expect(result.failed).toBe(0);
    expect(result.completed).toBe(3);
    expect(await readFile(join(remotePath, "parts", "gear.build123d.py"), "utf8")).toBe(source);

    const hydrated = await requestJson(
      "GET",
      "/v1/spaces/worker-object-space/files?includeContent=1",
    );
    expect(hydrated.files).toEqual([
      expect.objectContaining({
        objectRefId: recorded.file.objectRefId,
        contentHash,
        contentBase64: Buffer.from(source).toString("base64"),
      }),
    ]);

    const jobs = await requestJson("GET", "/v1/sync/jobs?includeFinished=1&limit=10");
    expect(jobs.jobs).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          jobId: recorded.uploadJob.jobId,
          status: "completed",
          resultJson: expect.objectContaining({
            transport: "object-cache",
            checksum: contentHash,
          }),
        }),
        expect.objectContaining({
          jobId: recorded.materializeJob.jobId,
          status: "completed",
          resultJson: expect.objectContaining({
            transport: "object-cache",
            checksum: contentHash,
            targetPath: join(remotePath, "parts", "gear.build123d.py"),
          }),
        }),
      ]),
    );
  });

  it("materializes inline Designer space file payloads to the remote filesystem", async () => {
    const remotePath = join(tempDir, "remote-spaces", "inline-cad-space");
    await requestJson("POST", "/v1/spaces", {
      slug: "inline-cad-space",
      title: "Inline CAD Space",
      remotePath,
    });

    const source = "from build123d import *\n\nbox = Box(1, 2, 3)\n";
    const contentHash = `sha256:${createHash("sha256").update(source).digest("hex")}`;
    const recorded = await requestJson("POST", "/v1/spaces/inline-cad-space/files", {
      path: "parts/box.build123d.py",
      contentHash,
      sizeBytes: Buffer.byteLength(source),
      contentType: "text/x-python",
      materializeTarget: "remote",
      contentBase64: Buffer.from(source).toString("base64"),
    });

    expect(await readFile(join(remotePath, "parts", "box.build123d.py"), "utf8")).toBe(source);
    expect(recorded).toMatchObject({
      ok: true,
      file: {
        path: "parts/box.build123d.py",
        contentHash,
        remotePath: join(remotePath, "parts", "box.build123d.py"),
        materializeTarget: "remote",
      },
      uploadJob: {
        kind: "space-file-object-upload",
        status: "completed",
        resultJson: {
          status: "completed",
          transport: "inline-object-cache",
          checksum: contentHash,
          bytes: Buffer.byteLength(source),
        },
      },
      materializeJob: {
        kind: "space-file-materialize",
        status: "completed",
        resultJson: {
          status: "completed",
          transport: "inline-file",
          targetPath: join(remotePath, "parts", "box.build123d.py"),
          checksum: contentHash,
          bytes: Buffer.byteLength(source),
        },
      },
    });
    const listed = await requestJson("GET", "/v1/spaces/inline-cad-space/files?includeContent=1");
    expect(listed.files).toEqual([
      expect.objectContaining({
        path: "parts/box.build123d.py",
        contentHash,
        contentBase64: Buffer.from(source).toString("base64"),
      }),
    ]);
  });

  it("serves latest inline Designer space bytes so two users hydrate the same remote file space", async () => {
    const remotePath = join(tempDir, "remote-spaces", "collab-cad-space");
    await requestJson("POST", "/v1/spaces", {
      slug: "collab-cad-space",
      title: "Collab CAD Space",
      remotePath,
    });

    const filePath = "parts/shared-gear.build123d.py";
    const aliceSource = "from build123d import *\n\ngear = Box(1, 2, 3)\n";
    const aliceHash = `sha256:${createHash("sha256").update(aliceSource).digest("hex")}`;
    await requestJson("POST", "/v1/spaces/collab-cad-space/files", {
      path: filePath,
      contentHash: aliceHash,
      sizeBytes: Buffer.byteLength(aliceSource),
      contentType: "text/x-python",
      materializeTarget: "remote",
      writerId: "alice",
      contentBase64: Buffer.from(aliceSource).toString("base64"),
    });

    const hydratedForBob = await requestJson(
      "GET",
      "/v1/spaces/collab-cad-space/files?includeContent=1",
    );
    expect(hydratedForBob.files).toEqual([
      expect.objectContaining({
        path: filePath,
        writerId: "alice",
        contentHash: aliceHash,
        contentBase64: Buffer.from(aliceSource).toString("base64"),
      }),
    ]);

    const bobSource = "from build123d import *\n\ngear = Box(5, 8, 13)\n";
    const bobHash = `sha256:${createHash("sha256").update(bobSource).digest("hex")}`;
    await requestJson("POST", "/v1/spaces/collab-cad-space/files", {
      path: filePath,
      contentHash: bobHash,
      sizeBytes: Buffer.byteLength(bobSource),
      contentType: "text/x-python",
      materializeTarget: "remote",
      writerId: "bob",
      contentBase64: Buffer.from(bobSource).toString("base64"),
    });

    const hydratedForAlice = await requestJson(
      "GET",
      "/v1/spaces/collab-cad-space/files?includeContent=1",
    );
    expect(hydratedForAlice.files).toEqual([
      expect.objectContaining({
        path: filePath,
        writerId: "bob",
        contentHash: bobHash,
        contentBase64: Buffer.from(bobSource).toString("base64"),
      }),
    ]);
    expect(await readFile(join(remotePath, "parts", "shared-gear.build123d.py"), "utf8")).toBe(
      bobSource,
    );
  });

  it("rejects unsafe Designer space file paths", async () => {
    await requestJson("POST", "/v1/spaces", {
      slug: "remote-cad-space",
    });

    const rejected = await requestJsonWithStatus("POST", "/v1/spaces/remote-cad-space/files", 400, {
      path: "../private-key",
      contentHash: "sha256:bad",
    });

    expect(rejected).toMatchObject({
      ok: false,
      error: "invalid Designer space file path ../private-key",
    });
  });

  it("queues explicit Designer space sync jobs for pull and push", async () => {
    await requestJson("POST", "/v1/spaces", {
      slug: "collab-cad-space",
    });

    const pull = await requestJson("POST", "/v1/spaces/collab-cad-space/sync", {
      direction: "pull",
      sourceSession: "codex-session-1",
    });
    const push = await requestJson("POST", "/v1/spaces/collab-cad-space/sync", {
      direction: "push",
      sourceSession: "codex-session-2",
    });

    expect(pull.job).toMatchObject({
      kind: "space-rsync-mirror",
      dedupeKey: "space-rsync-mirror:collab-cad-space:pull",
      payloadJson: {
        direction: "pull",
        sourcePath: "/users/nikiv/.designer/spaces/collab-cad-space/work",
        targetPath: join(tempDir, ".designer", "spaces", "collab-cad-space", "work"),
        transport: "rsync",
      },
    });
    expect(push.job).toMatchObject({
      kind: "space-rsync-mirror",
      dedupeKey: "space-rsync-mirror:collab-cad-space:push",
      payloadJson: {
        direction: "push",
        sourcePath: join(tempDir, ".designer", "spaces", "collab-cad-space", "work"),
        targetPath: "/users/nikiv/.designer/spaces/collab-cad-space/work",
        transport: "rsync",
      },
    });
  });

  it("ignores malformed legacy space jobs when listing spaces", async () => {
    await requestJson("POST", "/v1/sync/jobs", {
      kind: "space-rsync-mirror",
      payloadJson: {
        sourcePath: "/users/nikiv/.designer/spaces/missing-space-payload/work",
        targetPath: join(tempDir, ".designer", "spaces", "missing-space-payload", "work"),
        transport: "rsync",
      },
    });

    const listed = await requestJson("GET", "/v1/spaces");
    expect(listed.spaces).toEqual([]);
  });

  async function requestJson(method: string, path: string, body?: unknown) {
    const response = await request(method, path, body);
    const json = await response.json();
    expect(response.status).toBeGreaterThanOrEqual(200);
    expect(response.status).toBeLessThan(300);
    return json;
  }

  async function requestJsonWithStatus(
    method: string,
    path: string,
    status: number,
    body?: unknown,
  ) {
    const response = await request(method, path, body);
    const json = await response.json();
    expect(response.status).toBe(status);
    return json;
  }

  async function request(method: string, path: string, body?: unknown) {
    return gateway.app.handle(
      new Request(`http://remote-autonomy.test${path}`, {
        method,
        headers: body === undefined ? undefined : { "content-type": "application/json" },
        body: body === undefined ? undefined : JSON.stringify(body),
      }),
    );
  }

  function gatewayFetch(): typeof fetch {
    return ((input: RequestInfo | URL, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(input, init);
      return gateway.app.handle(request);
    }) as typeof fetch;
  }

  function rsyncCopyRunner(): CommandRunner {
    return async (command, args) => {
      expect(command).toBe("rsync");
      const source = args.at(-2)?.replace(/\/+$/, "");
      const target = args.at(-1)?.replace(/\/+$/, "");
      if (!source || !target) {
        return { exitCode: 2, stdout: "", stderr: "missing source or target" };
      }
      await rm(target, { recursive: true, force: true });
      await mkdir(target, { recursive: true });
      await cp(source, target, {
        recursive: true,
        force: true,
        errorOnExist: false,
      });
      return { exitCode: 0, stdout: "copied", stderr: "" };
    };
  }
});

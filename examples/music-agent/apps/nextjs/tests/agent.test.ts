import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import type { Db } from "jazz-tools";
import { startLocalJazzServer, type LocalJazzServerHandle } from "jazz-tools/testing";
import { app } from "../schema";
import permissions from "../permissions";

// The agent runs exactly as it does in the app: backend authority against a
// Jazz server that enforces the app's schema and permissions.
let server: LocalJazzServerHandle;
beforeAll(async () => {
  server = await startLocalJazzServer({ inMemory: true, schema: app, permissions });
  process.env.NEXT_PUBLIC_JAZZ_SERVER_URL = server.url;
  process.env.NEXT_PUBLIC_JAZZ_APP_ID = server.appId;
  process.env.BACKEND_SECRET = server.backendSecret;
  process.env.BETTER_AUTH_SECRET = "music-agent-test-better-auth-secret-0123456789";
  process.env.MUSIC_AGENT_PROVIDER = "scripted";
  process.env.SCRIPTED_AGENT_TOKEN_DELAY_MS = "0";
});
afterAll(async () => {
  await secondSession?.close();
  await server?.stop();
});

// A second server process: its own backend client, which sees the first
// one's writes only through the Jazz server.
let second: Db | undefined;
let secondSession: { close(): Promise<void> } | undefined;
async function secondBackend(): Promise<Db> {
  if (second) return second;
  const { createJazzSession } = await import("jazz-tools/backend");
  const { jazzEnv } = await import("../src/lib/jazz-env");
  const session = await createJazzSession({
    app,
    permissions,
    appId: server.appId,
    driver: { type: "memory" },
    serverUrl: server.url,
    initial: { backendSecret: server.backendSecret },
    env: jazzEnv,
  });
  secondSession = session;
  const client = session.getSnapshot().client;
  if (!client) throw new Error("second backend session is not ready");
  second = client.db;
  return second;
}

const load = async () => ({
  ...(await import("../src/agent/runner")),
  ...(await import("../src/server/bootstrap")),
  db: (await (await import("../src/lib/backend-jazz-client")).backendJazzClient()).db,
});

describe("MusicAgent server execution", () => {
  const accountId = randomUUID();
  let firstReply: string;

  test("bootstrap seeds a workspace once and the first reply streams in with tool calls", async () => {
    const { bootstrapWorkspace, runTurn, db } = await load();
    firstReply = (await bootstrapWorkspace(accountId, "auth-user-1", "Sam"))!;
    expect(firstReply).toBeDefined();
    expect(await bootstrapWorkspace(accountId, "auth-user-1", "Sam")).toBeUndefined();

    await runTurn(firstReply);

    const turn = await db.one(app.turns.where({ id: firstReply }), { tier: "remote" });
    expect(turn).toMatchObject({ status: "complete", provider: "Scripted agent" });
    expect(turn!.body).toContain("night-shift-single-rough-mix.wav");
    expect(turn!.body).toContain("The Green Room");
    const calls = await db.all(
      app.toolCalls.where({ turnId: firstReply }).orderBy("ordinal", "asc"),
    );
    expect(calls.map((call) => [call.name, call.status])).toEqual([
      ["find_venues", "complete"],
      ["check_calendar", "complete"],
    ]);
    expect(
      JSON.parse(calls[0]!.resultJson!).venues.every((v: { city: string }) => v.city === "Chicago"),
    ).toBe(true);
  });

  test("a reply whose server died is marked interrupted and resumes to the same answer", async () => {
    const { queueAssistantTurn, sweepStaleTurns, runTurn, db, STALE_AFTER_MS } = await load();
    const original = (await db.one(app.turns.where({ id: firstReply }), { tier: "remote" }))!;

    // A regenerated sibling whose server stopped partway: streaming, a stale
    // heartbeat and half a reply.
    const { turnId: sibling } = await queueAssistantTurn(
      db,
      original.conversationId,
      original.parentId!,
    );
    const partial = original.body.slice(0, 60);
    await db
      .update(app.turns, sibling, {
        status: "streaming",
        runnerId: "a-server-that-crashed",
        heartbeatAt: new Date(Date.now() - STALE_AFTER_MS - 1_000),
        body: partial,
      })
      .wait({ tier: "global" });

    expect(await sweepStaleTurns()).toBe(1);
    expect((await db.one(app.turns.where({ id: sibling }), { tier: "remote" }))!.status).toBe(
      "interrupted",
    );

    await runTurn(sibling);
    const resumed = (await db.one(app.turns.where({ id: sibling }), { tier: "remote" }))!;
    expect(resumed.status).toBe("complete");
    expect(resumed.body).toBe(original.body);

    const conversation = await db.one(app.conversations.where({ id: original.conversationId }));
    expect(conversation!.headTurnId).toBe(sibling);
  });

  test("a turn that is already running is not claimed twice", async () => {
    const { runTurn, db } = await load();
    const before = (await db.one(app.turns.where({ id: firstReply }), { tier: "remote" }))!;
    await runTurn(firstReply); // complete, so nothing to claim
    const after = (await db.one(app.turns.where({ id: firstReply }), { tier: "remote" }))!;
    expect(after.body).toBe(before.body);
  });

  test("two runners racing to claim one reply write it once", async () => {
    const { queueAssistantTurn, runTurn, db } = await load();
    const original = (await db.one(app.turns.where({ id: firstReply }), { tier: "remote" }))!;

    // Two runners in one process: they collide on the local runtime.
    const { turnId: local } = await queueAssistantTurn(
      db,
      original.conversationId,
      original.parentId!,
    );
    await Promise.all([runTurn(local), runTurn(local)]);

    // Two servers: the second backend client only learns of the other claim
    // from the authority.
    const other = await secondBackend();
    const { turnId: remote } = await queueAssistantTurn(
      db,
      original.conversationId,
      original.parentId!,
    );
    await other.one(app.turns.where({ id: remote }), { tier: "remote" });
    await Promise.all([runTurn(remote), runTurn(remote, other)]);

    for (const turnId of [local, remote]) {
      const reply = (await db.one(app.turns.where({ id: turnId }), { tier: "remote" }))!;
      expect(reply.status).toBe("complete");
      expect(reply.body).toBe(original.body);
      const calls = await db.all(app.toolCalls.where({ turnId }), { tier: "remote" });
      expect(calls.map((call) => call.name).sort()).toEqual(["check_calendar", "find_venues"]);
    }
  });

  test("an exclusive write that loses at the authority is a retryable conflict", async () => {
    const { db } = await load();
    const { isExclusiveConflict } = await import("../src/lib/write-errors");
    const other = await secondBackend();
    const turn = (await db.one(app.turns.where({ id: firstReply }), { tier: "remote" }))!;
    await other.one(app.turns.where({ id: turn.id }), { tier: "remote" });

    // Both read the turn, then both write it: only one of them can commit.
    let read = 0;
    let bothRead!: () => void;
    const barrier = new Promise<void>((resolve) => (bothRead = resolve));
    const results = await Promise.allSettled(
      [db, other].map(async (client) => {
        const write = await client.exclusiveTransaction(async (tx) => {
          await tx.one(app.turns.where({ id: turn.id }));
          if (++read === 2) bothRead();
          await barrier;
          tx.update(app.turns, turn.id, { heartbeatAt: new Date() });
        });
        await write.wait();
      }),
    );
    const failures = results.flatMap((result) =>
      result.status === "rejected" ? [result.reason] : [],
    );
    expect(failures).toHaveLength(1);
    expect(isExclusiveConflict(failures[0])).toBe(true);
  });

  test("a runner that loses its lease stops writing and never finishes the turn", async () => {
    const { queueAssistantTurn, runTurn, db, HEARTBEAT_MS } = await load();
    const original = (await db.one(app.turns.where({ id: firstReply }), { tier: "remote" }))!;
    const { turnId } = await queueAssistantTurn(db, original.conversationId, original.parentId!);
    const read = async () => (await db.one(app.turns.where({ id: turnId }), { tier: "remote" }))!;

    // Slow enough that the reply is still streaming well after the takeover.
    process.env.SCRIPTED_AGENT_TOKEN_DELAY_MS = "60";
    try {
      const run = runTurn(turnId);
      await expect
        .poll(async () => (await read()).body.length, { timeout: 30_000 })
        .toBeGreaterThan(0);

      // Another runner takes the turn over (as after a sweep and a resume
      // elsewhere), and notes the body as it stood at that moment.
      const { retryOnConflict } = await import("../src/lib/retry");
      const takeover = await retryOnConflict(async () => {
        const write = await db.exclusiveTransaction(async (tx) => {
          const current = (await tx.one(app.turns.where({ id: turnId })))!;
          tx.update(app.turns, turnId, { runnerId: "another-runner" });
          return current.body;
        });
        await write.wait();
        return write.value;
      });
      await run; // its next write is refused, and it stops

      const stopped = await read();
      expect(stopped.status).toBe("streaming");
      expect(stopped.runnerId).toBe("another-runner");
      // Not one splice landed after the takeover.
      expect(stopped.body).toBe(takeover);
      expect(stopped.body.length).toBeLessThan(original.body.length);

      await new Promise((resolve) => setTimeout(resolve, HEARTBEAT_MS));
      expect((await read()).body).toBe(takeover);
    } finally {
      process.env.SCRIPTED_AGENT_TOKEN_DELAY_MS = "0";
    }
  });
});

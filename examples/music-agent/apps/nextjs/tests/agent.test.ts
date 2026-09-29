import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
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
  const { backendJazzClient } = await import("../src/lib/backend-jazz-client");
  await (await backendJazzClient()).shutdown?.();
  await server?.stop();
});

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

    const turn = await db.one(app.turns.where({ id: firstReply }), { tier: "global" });
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
    const original = (await db.one(app.turns.where({ id: firstReply }), { tier: "global" }))!;

    // A regenerated sibling whose server stopped partway: streaming, a stale
    // heartbeat and half a reply.
    const sibling = queueAssistantTurn(db, original.conversationId, original.parentId!);
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
    expect((await db.one(app.turns.where({ id: sibling }), { tier: "global" }))!.status).toBe(
      "interrupted",
    );

    await runTurn(sibling);
    const resumed = (await db.one(app.turns.where({ id: sibling }), { tier: "global" }))!;
    expect(resumed.status).toBe("complete");
    expect(resumed.body).toBe(original.body);

    const conversation = await db.one(app.conversations.where({ id: original.conversationId }));
    expect(conversation!.headTurnId).toBe(sibling);
  });

  test("a turn that is already running is not claimed twice", async () => {
    const { runTurn, db } = await load();
    const before = (await db.one(app.turns.where({ id: firstReply }), { tier: "global" }))!;
    await runTurn(firstReply); // complete, so nothing to claim
    const after = (await db.one(app.turns.where({ id: firstReply }), { tier: "global" }))!;
    expect(after.body).toBe(before.body);
  });
});

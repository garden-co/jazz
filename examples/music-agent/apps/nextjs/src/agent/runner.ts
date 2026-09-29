import type { Db } from "jazz-tools";
import { app, type Turn } from "@/schema";
import { backendJazzClient } from "@/src/lib/backend-jazz-client";
import { agentLabel, agentProvider } from "./config";
import type { GenerateInput, HistoryTurn, TurnSink } from "./provider";
import { runTool, type ToolContext } from "./tools";

/** How often a running turn proves its server process is alive. */
export const HEARTBEAT_MS = 3_000;
/** A streaming turn whose heartbeat is older than this lost its process. */
export const STALE_AFTER_MS = 15_000;

declare global {
  var __musicAgentRunner: { id: string; active: Set<string>; sweeper?: NodeJS.Timeout } | undefined;
}

// One identity per server process. Next dev reloads modules, so keep it global.
const runner = (globalThis.__musicAgentRunner ??= { id: crypto.randomUUID(), active: new Set() });

/** Queue a new assistant reply under `parentId` and show it as the conversation's head. */
export function queueAssistantTurn(db: Db, conversationId: string, parentId: string): string {
  const { value: turn } = db.insert(app.turns, {
    conversationId,
    parentId,
    role: "assistant",
    body: "",
    status: "queued",
    provider: agentLabel(),
    // Queued counts as alive: the sweeper only interrupts it if no runner claims it.
    heartbeatAt: new Date(),
  });
  db.update(app.conversations, conversationId, { headTurnId: turn.id });
  return turn.id;
}

/**
 * Generate (or resume) one assistant turn. Every chunk lands in Jazz as it is
 * produced, so the reply streams to every open client and survives this
 * request: a closed tab comes back to the finished answer. If this process
 * dies, the heartbeat stops and the sweeper marks the turn interrupted.
 */
export async function runTurn(turnId: string): Promise<void> {
  const db = (await backendJazzClient()).db;
  const turn = await claim(db, turnId);
  if (!turn) return;

  runner.active.add(turnId);
  const heartbeat = setInterval(() => {
    db.update(app.turns, turnId, { heartbeatAt: new Date() });
  }, HEARTBEAT_MS);
  const body = new BodyWriter(db, turnId, turn.body.length);
  try {
    const provider = agentProvider();
    const input = await loadInput(db, turn);
    const sink = await turnSink(db, turn, input.tools, body, provider.resume === "replay");
    await provider.generate(
      provider.resume === "continue" && turn.body ? { ...input, partialReply: turn.body } : input,
      sink,
    );
    body.flush();
    await db
      .update(app.turns, turnId, {
        status: "complete",
        provider: provider.label,
        heartbeatAt: new Date(),
      })
      .wait({ tier: "global" });
  } catch (error) {
    body.flush();
    console.error(`MusicAgent turn ${turnId} failed`, error);
    await db
      .update(app.turns, turnId, {
        status: "failed",
        error: error instanceof Error ? error.message : String(error),
      })
      .wait({ tier: "global" });
  } finally {
    clearInterval(heartbeat);
    runner.active.delete(turnId);
  }
}

/** Take ownership of a queued or interrupted turn, exactly once across servers. */
async function claim(db: Db, turnId: string): Promise<Turn | null> {
  try {
    const write = await db.exclusiveTransaction(async (tx) => {
      const turn = await tx.one(app.turns.where({ id: turnId }));
      if (!turn || turn.role !== "assistant") return null;
      if (!["queued", "interrupted", "failed"].includes(turn.status)) return null;
      tx.update(app.turns, turnId, {
        status: "streaming",
        runnerId: runner.id,
        heartbeatAt: new Date(),
        error: null,
      });
      return turn;
    });
    await write.wait();
    return write.value;
  } catch (error) {
    // Another runner claimed it first.
    if (/exclusive_conflict|transaction_conflict/.test(String(error))) return null;
    throw error;
  }
}

/** The conversation path from its first turn to this reply's parent. */
async function loadInput(db: Db, turn: Turn): Promise<GenerateInput> {
  const conversation = await db.one(app.conversations.where({ id: turn.conversationId }));
  if (!conversation) throw new Error("conversation not found");
  const artist = await db.one(app.artists.where({ id: conversation.artistId }));
  const turns = await db.all(app.turns.where({ conversationId: turn.conversationId }));
  const attachments = await db.all(
    app.attachments
      .where({ conversationId: turn.conversationId })
      .select("turnId", "filename", "mediaType", "byteLength"),
  );
  const byId = new Map(turns.map((row) => [row.id, row]));
  const history: HistoryTurn[] = [];
  for (let id = turn.parentId; id; id = byId.get(id)?.parentId ?? null) {
    const row = byId.get(id);
    if (!row) break;
    history.unshift({
      role: row.role,
      text: row.body,
      attachments: attachments.filter((file) => file.turnId === row.id),
    });
  }
  return {
    artistName: artist?.name ?? "the artist",
    history,
    tools: {
      db,
      ownerAccount: conversation.ownerAccount,
      artistId: conversation.artistId,
      today: new Date().toISOString().slice(0, 10),
    },
  };
}

/**
 * Tool calls are rows of their own, written before and after each call, so
 * clients see "running" and then the result. A replayed turn reuses the
 * results it already has instead of calling the tool again, and skips the
 * prose it already wrote.
 */
async function turnSink(
  db: Db,
  turn: Turn,
  context: ToolContext,
  body: BodyWriter,
  replay: boolean,
): Promise<TurnSink> {
  const previous = await db.all(app.toolCalls.where({ turnId: turn.id }));
  let skip = replay ? turn.body.length : 0;
  let ordinal = replay ? 0 : previous.length;
  return {
    async text(chunk) {
      if (skip >= chunk.length) {
        skip -= chunk.length;
        return;
      }
      body.append(chunk.slice(skip));
      skip = 0;
    },
    async tool(name, input) {
      const position = ordinal++;
      const earlier = previous.find((call) => call.ordinal === position && call.name === name);
      if (earlier?.status === "complete" && earlier.resultJson)
        return JSON.parse(earlier.resultJson) as unknown;
      body.flush();
      const started = Date.now();
      const id =
        earlier?.id ??
        db.insert(app.toolCalls, {
          conversationId: turn.conversationId,
          turnId: turn.id,
          ordinal: position,
          name,
          argumentsJson: JSON.stringify(input),
          status: "running",
        }).value.id;
      try {
        const result = await runTool(context, name, input);
        db.update(app.toolCalls, id, {
          status: "complete",
          resultJson: JSON.stringify(result),
          durationMs: Date.now() - started,
        });
        return result;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        db.update(app.toolCalls, id, {
          status: "error",
          resultJson: JSON.stringify({ error: message }),
          durationMs: Date.now() - started,
        });
        throw error;
      }
    },
  };
}

/**
 * Appends prose to the turn's body. Tokens are batched briefly so a fast
 * stream becomes a few writes per second; each write is a page-relative
 * splice at the current end of the text, so no write resends the whole body.
 */
class BodyWriter {
  private pending = "";
  private timer: NodeJS.Timeout | undefined;

  constructor(
    private readonly db: Db,
    private readonly turnId: string,
    private length: number,
  ) {}

  append(text: string) {
    this.pending += text;
    if (this.pending.length >= 200) this.flush();
    else this.timer ??= setTimeout(() => this.flush(), 50);
  }

  flush() {
    clearTimeout(this.timer);
    this.timer = undefined;
    if (!this.pending) return;
    const insert = this.pending;
    this.pending = "";
    const end = this.length;
    this.db.update(
      app.turns,
      this.turnId,
      {},
      {
        applyDiffs: {
          body: { within: { from: end, to: end }, splices: [{ at: 0, delete: 0, insert }] },
        },
      },
    );
    this.length += insert.length;
  }
}

/**
 * Mark turns whose server process went away as interrupted, so clients can
 * offer to resume or retry them. Runs at server start and every few seconds.
 */
export async function sweepStaleTurns(now = Date.now()): Promise<number> {
  const db = (await backendJazzClient()).db;
  const open = await db.all(app.turns.where({ status: { in: ["queued", "streaming"] } }));
  let swept = 0;
  for (const turn of open) {
    if (runner.active.has(turn.id) || !isStale(turn, now)) continue;
    try {
      const write = await db.exclusiveTransaction(async (tx) => {
        const current = await tx.one(app.turns.where({ id: turn.id }));
        if (!current || !["queued", "streaming"].includes(current.status) || !isStale(current, now))
          return false;
        tx.update(app.turns, turn.id, { status: "interrupted" });
        return true;
      });
      await write.wait();
      if (write.value) swept++;
    } catch (error) {
      if (!/exclusive_conflict|transaction_conflict/.test(String(error))) throw error;
    }
  }
  return swept;
}

function isStale(turn: Turn, now: number) {
  return !turn.heartbeatAt || now - new Date(turn.heartbeatAt).getTime() > STALE_AFTER_MS;
}

export function startSweeper() {
  if (runner.sweeper) return;
  const sweep = () =>
    sweepStaleTurns().catch((error) => console.warn("MusicAgent sweep failed", error));
  void sweep();
  runner.sweeper = setInterval(sweep, 5_000);
  runner.sweeper.unref?.();
}

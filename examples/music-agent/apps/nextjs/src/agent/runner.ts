import "server-only";
import type { Db } from "jazz-tools";
import { app, type ToolCall, type Turn } from "@/schema";
import { backendJazzClient } from "@/src/lib/backend-jazz-client";
import { retryOnConflict } from "@/src/lib/retry";
import { isExclusiveConflict } from "@/src/lib/write-errors";
import { agentProvider } from "./config";
import type { GenerateInput, HistoryTurn, TurnSink } from "./provider";
import { runTool, type ToolContext } from "./tools";

/** How often a running turn renews its lease. */
export const HEARTBEAT_MS = 3_000;
/** A streaming turn whose heartbeat is older than this lost its process. */
export const STALE_AFTER_MS = 15_000;
/**
 * A runner writes only while its last confirmed renewal is younger than this.
 * It is well inside STALE_AFTER_MS, so no sweeper can have interrupted the
 * turn (and let another runner claim it) while this runner is still writing.
 */
const WRITABLE_FOR_MS = STALE_AFTER_MS / 2;

const CLAIMABLE: Turn["status"][] = ["queued", "interrupted", "failed"];

declare global {
  var __musicAgentRunner: { id: string; active: Set<string>; sweeper?: NodeJS.Timeout } | undefined;
}

// One identity per server process. Next dev reloads modules, so keep it global.
const runner = (globalThis.__musicAgentRunner ??= { id: crypto.randomUUID(), active: new Set() });

export type QueuedReply = { turnId: string; created: boolean };

/**
 * Queue an assistant reply under `parentId` and make it the conversation's
 * head, in one exclusive transaction. With `reuse`, a reply that already
 * exists under the same parent is returned instead, so a retried request
 * never queues a second answer.
 */
export async function queueAssistantTurn(
  db: Db,
  conversationId: string,
  parentId: string,
  { reuse = false }: { reuse?: boolean } = {},
): Promise<QueuedReply> {
  const provider = agentProvider();
  return retryOnConflict(async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      if (reuse) {
        const existing = await tx.one(
          app.turns.where({ conversationId, parentId, role: "assistant" }),
        );
        if (existing) return { turnId: existing.id, created: false };
      }
      const turnId = crypto.randomUUID();
      tx.insert(
        app.turns,
        {
          conversationId,
          parentId,
          role: "assistant",
          body: "",
          status: "queued",
          provider: provider.label,
          // Queued counts as alive: the sweeper only interrupts it if no runner claims it.
          heartbeatAt: new Date(),
        },
        { id: turnId },
      );
      tx.update(app.conversations, conversationId, { headTurnId: turnId });
      return { turnId, created: true };
    });
    await write.wait();
    return write.value;
  });
}

class LeaseLostError extends Error {
  override name = "LeaseLostError";
}

/**
 * The right to write one turn, held by one claim. Its id is stored as the
 * turn's `runnerId`; each claim gets a fresh id, so a resumed turn can never be
 * mistaken for an older run of the same process.
 */
class Lease {
  readonly id = `${runner.id}/${crypto.randomUUID()}`;
  private readonly controller = new AbortController();
  private confirmedAt = Date.now();
  renewing = false;

  get signal() {
    return this.controller.signal;
  }
  get lost() {
    return this.controller.signal.aborted;
  }
  /** Safe to write now: still ours, recently confirmed, and no renewal in flight. */
  get writable() {
    return !this.lost && !this.renewing && Date.now() - this.confirmedAt < WRITABLE_FOR_MS;
  }
  confirm(at: number) {
    this.confirmedAt = at;
  }
  lose(reason: string) {
    if (!this.lost) this.controller.abort(new LeaseLostError(reason));
  }
  assertHeld() {
    if (this.lost) throw this.signal.reason;
  }
  /** Wait until writing is safe; give up (and the lease) if it isn't within the stale window. */
  async ready() {
    const deadline = Date.now() + STALE_AFTER_MS;
    while (!this.writable) {
      this.assertHeld();
      if (Date.now() > deadline) this.lose("lease could not be renewed");
      await sleep(50);
    }
  }
}

/**
 * Generate (or resume) one assistant turn. Every chunk lands in Jazz as it is
 * produced, so the reply streams to every open client and survives this
 * request: a closed tab comes back to the finished answer. If this process
 * stalls or dies, the heartbeat stops, the sweeper marks the turn interrupted
 * and another runner may claim it; this runner then stops writing.
 */
export async function runTurn(turnId: string): Promise<void> {
  const db = (await backendJazzClient()).db;
  const lease = new Lease();
  const turn = await claim(db, turnId, lease);
  if (!turn) return;

  runner.active.add(turnId);
  const heartbeat = keepLease(db, turnId, lease);
  const body = new BodyWriter(db, turnId, turn.body.length, lease);
  try {
    let outcome: Partial<Turn> & Pick<Turn, "status">;
    try {
      const provider = agentProvider();
      const input = await loadInput(db, turn, lease.signal);
      const sink = turnSink(db, turn, input.tools, body, provider.resume === "replay", lease);
      await provider.generate(
        provider.resume === "continue" && turn.body ? { ...input, partialReply: turn.body } : input,
        sink,
      );
      outcome = { status: "complete", provider: provider.label };
    } catch (error) {
      if (lease.lost) {
        console.warn(`MusicAgent turn ${turnId}: ${String(lease.signal.reason)}; stopped writing`);
        return;
      }
      console.error(`MusicAgent turn ${turnId} failed`, error);
      outcome = { status: "failed", error: error instanceof Error ? error.message : String(error) };
    }
    await body.drain();
    await heartbeat.stop();
    await finish(db, turnId, lease, outcome);
  } finally {
    body.stop();
    await heartbeat.stop();
    runner.active.delete(turnId);
  }
}

/** Take ownership of a queued, interrupted or failed turn, exactly once across runners. */
async function claim(db: Db, turnId: string, lease: Lease): Promise<Turn | null> {
  return retryOnConflict(async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const turn = await tx.one(app.turns.where({ id: turnId }));
      if (!turn || turn.role !== "assistant" || !CLAIMABLE.includes(turn.status)) return null;
      tx.update(app.turns, turnId, {
        status: "streaming",
        runnerId: lease.id,
        heartbeatAt: new Date(),
        error: null,
      });
      return turn;
    });
    await write.wait();
    lease.confirm(Date.now());
    return write.value;
  });
}

/**
 * Renew the lease every HEARTBEAT_MS. Each renewal is an exclusive
 * transaction that only succeeds while this lease still owns a streaming
 * turn; once another runner owns it (or it was interrupted), generation is
 * aborted and nothing more is written.
 */
function keepLease(db: Db, turnId: string, lease: Lease) {
  let timer: NodeJS.Timeout | undefined;
  let inFlight: Promise<void> = Promise.resolve();
  let stopped = false;

  const renew = async () => {
    const at = Date.now();
    lease.renewing = true;
    try {
      const held = await retryOnConflict(async () => {
        const write = await db.exclusiveTransaction(async (tx) => {
          const current = await tx.one(app.turns.where({ id: turnId }));
          if (current?.runnerId !== lease.id || current.status !== "streaming") return false;
          tx.update(app.turns, turnId, { heartbeatAt: new Date(at) });
          return true;
        });
        await write.wait();
        return write.value;
      });
      if (held) lease.confirm(at);
      else lease.lose("another runner owns this turn");
    } catch (error) {
      // Not proof of loss: writes pause until a renewal succeeds or the lease expires.
      console.warn(`MusicAgent turn ${turnId}: lease renewal failed`, error);
    } finally {
      lease.renewing = false;
    }
  };
  const schedule = () => {
    if (stopped || lease.lost) return;
    timer = setTimeout(() => {
      inFlight = renew().finally(schedule);
    }, HEARTBEAT_MS);
  };
  schedule();

  return {
    async stop() {
      stopped = true;
      clearTimeout(timer);
      await inFlight;
    },
  };
}

/** Record how the turn ended, only if this lease still owns it. */
async function finish(
  db: Db,
  turnId: string,
  lease: Lease,
  outcome: Partial<Turn> & Pick<Turn, "status">,
) {
  if (lease.lost) return;
  const recorded = await retryOnConflict(async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const current = await tx.one(app.turns.where({ id: turnId }));
      if (current?.runnerId !== lease.id || current.status !== "streaming") return false;
      tx.update(app.turns, turnId, { ...outcome, heartbeatAt: new Date() });
      return true;
    });
    await write.wait();
    return write.value;
  });
  if (!recorded) lease.lose("another runner owns this turn");
}

/** The conversation path from its first turn to this reply's parent. */
async function loadInput(db: Db, turn: Turn, signal: AbortSignal): Promise<GenerateInput> {
  const conversation = await db.one(app.conversations.where({ id: turn.conversationId }));
  if (!conversation) throw new Error("conversation not found");
  // Backend authority bypasses policies, so scope every read to the owner explicitly.
  const owner = conversation.ownerAccount;
  const artist = await db.one(
    app.artists.where({ id: conversation.artistId, ownerAccount: owner }),
  );
  if (!artist) throw new Error("the conversation's artist is not in this workspace");
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
    artistName: artist.name,
    history,
    signal,
    tools: {
      db,
      ownerAccount: owner,
      artistId: artist.id,
      today: new Date().toISOString().slice(0, 10),
    },
  };
}

/**
 * Tool calls are rows of their own, written before and after each call, so
 * clients see "running" and then the result. A replayed turn reuses the
 * results it already has instead of calling the tool again, and skips the
 * prose it already wrote. Every call checks the lease first.
 */
function turnSink(
  db: Db,
  turn: Turn,
  context: ToolContext,
  body: BodyWriter,
  replay: boolean,
  lease: Lease,
): TurnSink {
  let previous: Promise<ToolCall[]> | undefined;
  let skip = replay ? turn.body.length : 0;
  let ordinal: number | undefined;
  return {
    async text(chunk) {
      lease.assertHeld();
      if (skip >= chunk.length) {
        skip -= chunk.length;
        return;
      }
      body.append(chunk.slice(skip));
      skip = 0;
    },
    async tool(name, input) {
      lease.assertHeld();
      const calls = await (previous ??= db.all(app.toolCalls.where({ turnId: turn.id })));
      ordinal ??= replay ? 0 : calls.length;
      const position = ordinal++;
      const earlier = calls.find((call) => call.ordinal === position && call.name === name);
      if (earlier?.status === "complete" && earlier.resultJson)
        return JSON.parse(earlier.resultJson) as unknown;
      await body.drain();
      await lease.ready();
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
      let update: { status: "complete" | "error"; resultJson: string };
      let failure: unknown;
      let result: unknown;
      try {
        result = await runTool(context, name, input);
        update = { status: "complete", resultJson: JSON.stringify(result) };
      } catch (error) {
        failure = error;
        const message = error instanceof Error ? error.message : String(error);
        update = { status: "error", resultJson: JSON.stringify({ error: message }) };
      }
      await lease.ready();
      db.update(app.toolCalls, id, { ...update, durationMs: Date.now() - started });
      if (failure) throw failure;
      return result;
    },
  };
}

/**
 * Appends prose to the turn's body. Tokens are batched briefly so a fast
 * stream becomes a few writes per second; each write is a page-relative
 * splice at the current end of the text, so no write resends the whole body.
 * A splice is only written while the lease is writable; otherwise it waits
 * for the next renewal, and it is dropped if the lease is lost.
 */
class BodyWriter {
  private pending = "";
  private timer: NodeJS.Timeout | undefined;

  constructor(
    private readonly db: Db,
    private readonly turnId: string,
    private length: number,
    private readonly lease: Lease,
  ) {}

  append(text: string) {
    this.pending += text;
    if (this.pending.length >= 200) this.flush();
    else this.timer ??= setTimeout(() => this.flush(), 50);
  }

  /** Write what is pending if the lease allows it now; returns whether nothing is left. */
  private flush(): boolean {
    clearTimeout(this.timer);
    this.timer = undefined;
    if (this.lease.lost) this.pending = "";
    if (!this.pending) return true;
    if (!this.lease.writable) {
      this.timer = setTimeout(() => this.flush(), 50);
      return false;
    }
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
    return true;
  }

  /** Write everything pending, waiting for the lease if needed. */
  async drain() {
    while (!this.flush()) await this.lease.ready().catch(() => undefined);
  }

  stop() {
    clearTimeout(this.timer);
    this.timer = undefined;
    this.pending = "";
  }
}

/**
 * Mark turns whose server process went away as interrupted, so clients can
 * offer to resume or retry them: streaming turns whose heartbeat stopped and
 * queued turns no runner claimed. Runs at server start and every few seconds.
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
      // A runner renewed or finished the turn meanwhile; the next sweep reads it again.
      if (!isExclusiveConflict(error)) throw error;
    }
  }
  return swept;
}

function isStale(turn: Turn, now: number) {
  return !turn.heartbeatAt || now - new Date(turn.heartbeatAt).getTime() > STALE_AFTER_MS;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function startSweeper() {
  if (runner.sweeper) return;
  const sweep = () =>
    sweepStaleTurns().catch((error) => console.warn("MusicAgent sweep failed", error));
  void sweep();
  runner.sweeper = setInterval(sweep, 5_000);
  runner.sweeper.unref?.();
}

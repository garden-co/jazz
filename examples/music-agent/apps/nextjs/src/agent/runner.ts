import "server-only";
import type { Db } from "jazz-tools";
import { app, type ToolCall, type Turn } from "@/schema";
import { backendJazzClient } from "@/src/lib/backend-jazz-client";
import { retryOnConflict } from "@/src/lib/retry";
import { isExclusiveConflict } from "@/src/lib/write-errors";
import { agentProvider } from "./config";
import type { GenerateInput, HistoryTurn, TurnSink } from "./provider";
import { runTool, type ToolContext } from "./tools";

/** How often a running turn renews its lease when nothing else was written. */
export const HEARTBEAT_MS = 3_000;
/** A streaming turn whose heartbeat is older than this lost its process. */
export const STALE_AFTER_MS = 15_000;

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

type Tx = Parameters<Parameters<Db["exclusiveTransaction"]>[0]>[0];

/**
 * The right to write one turn, held by one claim. Its id is stored as the
 * turn's `runnerId`; each claim gets a fresh id, so a resumed turn can never be
 * mistaken for an older run of the same process.
 *
 * Every write the holder makes (body appends, tool calls, heartbeats and the
 * final status) goes through `write`: one exclusive transaction that first
 * checks the turn is still streaming under this lease, and renews the
 * heartbeat with it. The authority rejects the transaction if the turn changed
 * meanwhile, so a runner that was swept and taken over can never land a write
 * after the takeover. Writes run one at a time, so the holder never races
 * itself.
 */
class Lease {
  readonly id = `${runner.id}/${crypto.randomUUID()}`;
  private readonly controller = new AbortController();
  private queue: Promise<unknown> = Promise.resolve();

  constructor(
    private readonly db: Db,
    private readonly turnId: string,
  ) {}

  get signal() {
    return this.controller.signal;
  }
  get lost() {
    return this.controller.signal.aborted;
  }
  lose(reason: string) {
    if (!this.lost) this.controller.abort(new LeaseLostError(reason));
  }
  assertHeld() {
    if (this.lost) throw this.signal.reason;
  }

  /** Run `change` in a transaction that only commits while this lease owns the turn. */
  write(change: (tx: Tx, turn: Turn) => void = () => {}): Promise<void> {
    const next = this.queue.then(async () => {
      this.assertHeld();
      const held = await retryOnConflict(async () => {
        const write = await this.db.exclusiveTransaction(async (tx) => {
          const turn = await tx.one(app.turns.where({ id: this.turnId }));
          if (turn?.runnerId !== this.id || turn.status !== "streaming") return false;
          change(tx, turn);
          tx.update(app.turns, this.turnId, { heartbeatAt: new Date() });
          return true;
        });
        await write.wait();
        return write.value;
      });
      if (!held) this.lose("another runner owns this turn");
      this.assertHeld();
    });
    // A failed write must not block the ones after it; each caller sees its own error.
    this.queue = next.catch(() => undefined);
    return next;
  }
}

/**
 * Generate (or resume) one assistant turn. Every chunk lands in Jazz as it is
 * produced, so the reply streams to every open client and survives this
 * request: a closed tab comes back to the finished answer. If this process
 * stalls or dies, the heartbeat stops, the sweeper marks the turn interrupted
 * and another runner may claim it; this runner then stops writing.
 */
export async function runTurn(turnId: string, client?: Db): Promise<void> {
  const db = client ?? (await backendJazzClient()).db;
  const lease = new Lease(db, turnId);
  const turn = await claim(db, turnId, lease);
  if (!turn) return;

  runner.active.add(turnId);
  const heartbeat = keepLease(turnId, lease);
  const body = new BodyWriter(lease);
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
      await body.drain();
      outcome = { status: "complete", provider: provider.label };
    } catch (error) {
      if (lease.lost) {
        console.warn(`MusicAgent turn ${turnId}: ${String(lease.signal.reason)}; stopped writing`);
        return;
      }
      console.error(`MusicAgent turn ${turnId} failed`, error);
      outcome = { status: "failed", error: error instanceof Error ? error.message : String(error) };
      // Keep what was written before the failure; resume continues from it.
      await body.drain().catch(() => undefined);
    }
    await heartbeat.stop();
    await finish(turnId, lease, outcome);
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
    return write.value;
  });
}

/**
 * Renew the lease every HEARTBEAT_MS. A renewal only commits while this lease
 * still owns a streaming turn; once another runner owns it (or it was
 * interrupted), generation is aborted and nothing more is written.
 */
function keepLease(turnId: string, lease: Lease) {
  let timer: NodeJS.Timeout | undefined;
  let inFlight: Promise<void> = Promise.resolve();
  let stopped = false;

  const renew = async () => {
    try {
      await lease.write();
    } catch (error) {
      // Not proof of loss: the next write or renewal checks again. If none gets
      // through for STALE_AFTER_MS, the sweeper interrupts the turn and every
      // later write from this runner is refused.
      if (!lease.lost) console.warn(`MusicAgent turn ${turnId}: lease renewal failed`, error);
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
async function finish(turnId: string, lease: Lease, outcome: Partial<Turn> & Pick<Turn, "status">) {
  if (lease.lost) return;
  await lease
    .write((tx) => tx.update(app.turns, turnId, outcome))
    .catch((error: unknown) => {
      if (!lease.lost) throw error;
    });
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
      const started = Date.now();
      const id = earlier?.id ?? crypto.randomUUID();
      if (!earlier)
        await lease.write((tx) =>
          tx.insert(
            app.toolCalls,
            {
              conversationId: turn.conversationId,
              turnId: turn.id,
              ordinal: position,
              name,
              argumentsJson: JSON.stringify(input),
              status: "running",
            },
            { id },
          ),
        );
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
      await lease.write((tx) =>
        tx.update(app.toolCalls, id, { ...update, durationMs: Date.now() - started }),
      );
      if (failure) throw failure;
      return result;
    },
  };
}

/**
 * Appends prose to the turn's body. Tokens are batched: while one append is
 * being written, the next batch collects, so a fast stream becomes a few
 * writes per second. Each append is a lease write, so it only lands while
 * this runner still owns the turn.
 *
 * A transaction can't take `applyDiffs`, so an append writes the whole body
 * (the stored text plus the batch) rather than a page-relative splice.
 */
class BodyWriter {
  private pending = "";
  private timer: NodeJS.Timeout | undefined;
  private writing: Promise<void> | undefined;
  private failure: unknown;

  constructor(private readonly lease: Lease) {}

  append(text: string) {
    if (this.failure) throw this.failure;
    this.pending += text;
    if (!this.writing) this.timer ??= setTimeout(() => this.start(), 50);
  }

  private start() {
    clearTimeout(this.timer);
    this.timer = undefined;
    this.writing ??= this.writeAll().finally(() => {
      this.writing = undefined;
      // Text that arrived as the last write settled starts the next one.
      if (this.pending) this.timer ??= setTimeout(() => this.start(), 50);
    });
    return this.writing;
  }

  private async writeAll() {
    try {
      while (this.pending) {
        const insert = this.pending;
        this.pending = "";
        await this.lease.write((tx, turn) =>
          tx.update(app.turns, turn.id, { body: turn.body + insert }),
        );
      }
    } catch (error) {
      this.failure ??= error;
      this.pending = "";
    }
  }

  /** Write everything pending; throws if an append was refused. */
  async drain() {
    while (this.pending || this.writing) await this.start();
    if (this.failure) throw this.failure;
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

export function startSweeper() {
  if (runner.sweeper) return;
  const sweep = () =>
    sweepStaleTurns().catch((error) => console.warn("MusicAgent sweep failed", error));
  void sweep();
  runner.sweeper = setInterval(sweep, 5_000);
  runner.sweeper.unref?.();
}

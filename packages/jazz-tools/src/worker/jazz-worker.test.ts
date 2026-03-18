/**
 * Tests for server-payload batching in the Jazz worker.
 *
 * RED: ServerPayloadBatcher does not exist yet — these tests fail at import
 * time until server-payload-batcher.ts is created and the class exported.
 *
 * The batcher is the piece that sits between the WASM outbox callback and
 * sendSyncPayloadBatch. It accumulates server-bound payloads within a
 * microtask boundary and flushes them as a single batch.
 */

import { describe, expect, it, vi } from "vitest";
import { ServerPayloadBatcher } from "./server-payload-batcher.js";

const playerPayload = (seq: number) =>
  JSON.stringify({
    ObjectUpdated: { object_id: `player-obj`, branch_name: "main", seq },
  });

describe("ServerPayloadBatcher", () => {
  it("batches 60 synchronous enqueues into a single sendBatch call after a microtask", async () => {
    // alice's game loop fires db.update 60 times in one tick — the batcher
    // must collapse them into exactly 1 HTTP POST
    //
    //  tick N:  enqueue×60 ──► [pending queue]
    //             microtask ──► sendBatch([p0…p59])   ← 1 call, not 60

    const sendBatch = vi.fn().mockResolvedValue(undefined);
    const batcher = new ServerPayloadBatcher(sendBatch);

    for (let i = 0; i < 60; i++) {
      batcher.enqueue(playerPayload(i));
    }

    // Nothing flushed synchronously
    expect(sendBatch).not.toHaveBeenCalled();

    await Promise.resolve(); // yield to microtask queue

    expect(sendBatch).toHaveBeenCalledTimes(1);
    expect(sendBatch.mock.calls[0]![0]).toHaveLength(60);
  });

  it("preserves payload order in the flushed batch", async () => {
    // bob's collected flag transitions false→true→false in one tick;
    // all three writes must reach the server in that exact order
    //
    //  enqueue(false) → enqueue(true) → enqueue(false)
    //  flush → sendBatch([false, true, false])  ← order preserved

    const received: string[][] = [];
    const batcher = new ServerPayloadBatcher(async (payloads) => {
      received.push([...payloads]);
    });

    const p1 = playerPayload(1);
    const p2 = playerPayload(2);
    const p3 = playerPayload(3);
    batcher.enqueue(p1);
    batcher.enqueue(p2);
    batcher.enqueue(p3);

    await Promise.resolve();

    expect(received).toHaveLength(1);
    expect(received[0]).toEqual([p1, p2, p3]);
  });

  it("second wave of enqueues after flush produces a separate batch", async () => {
    // two separate ticks must produce two separate POSTs — payloads from
    // tick N must not bleed into tick N+1's batch
    //
    //  tick N:  enqueue×2 → flush → sendBatch([p1, p2])
    //  tick N+1: enqueue×1 → flush → sendBatch([p3])

    const sendBatch = vi.fn().mockResolvedValue(undefined);
    const batcher = new ServerPayloadBatcher(sendBatch);

    batcher.enqueue(playerPayload(1));
    batcher.enqueue(playerPayload(2));

    await Promise.resolve();

    expect(sendBatch).toHaveBeenCalledTimes(1);
    expect(sendBatch.mock.calls[0]![0]).toHaveLength(2);

    batcher.enqueue(playerPayload(3));

    await Promise.resolve();

    expect(sendBatch).toHaveBeenCalledTimes(2);
    expect(sendBatch.mock.calls[1]![0]).toHaveLength(1);
  });

  it("does not flush when queue is empty", async () => {
    const sendBatch = vi.fn().mockResolvedValue(undefined);
    const _batcher = new ServerPayloadBatcher(sendBatch);

    // enqueue then drain immediately (simulates a flush with nothing pending)
    await Promise.resolve();

    expect(sendBatch).not.toHaveBeenCalled();
  });
});

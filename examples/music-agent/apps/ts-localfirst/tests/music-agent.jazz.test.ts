import { afterEach, describe, expect, test } from "vitest";
import { createDb, type Db } from "jazz-tools";
import { app } from "../schema.js";
import { DeterministicMusicAgent, JazzMusicStore, byteChunks } from "../src/music-agent.js";

let db: Db | undefined;

afterEach(async () => {
  await db?.shutdown();
  db = undefined;
});

function musicDbName(label: string): string {
  return `music-agent-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function openMusicStore(label: string): Promise<JazzMusicStore> {
  db = await createDb({
    appId: "music-agent-e2e",
    driver: { type: "persistent", dbName: musicDbName(label) },
  });
  return new JazzMusicStore(db);
}

/**
 * These assertions intentionally address only MusicStore and typed Db APIs.
 * A topology runner can reuse them unchanged for client/edge/core scenarios.
 */
describe("MusicAgent Jazz persistence E2E", () => {
  test("persists streamed turns, tool output, and attachments through the public app boundary", async () => {
    const store = await openMusicStore("streamed-records");
    const conversation = await store.createConversation("Late-night listening");
    const transcript = await new DeterministicMusicAgent(store).answer(
      conversation,
      "warm saxophone",
    );
    const attachment = await store.addAttachment(
      {
        turnId: transcript[0]!.id,
        filename: "clip.raw",
        mediaType: "audio/raw",
        byteLength: 6,
      },
      byteChunks([new TextEncoder().encode("ab"), new TextEncoder().encode("cdef")]),
    );

    expect(transcript.map((turn) => [turn.role, turn.ordinal, turn.body])).toEqual([
      ["user", 0, "warm saxophone"],
      [
        "assistant",
        1,
        "I found a focused listening path for warm saxophone. Starting with the live cut.",
      ],
      ["tool", 2, expect.stringContaining("music.search selected Midnight Practice")],
    ]);
    expect(await db!.all(app.tool_calls)).toHaveLength(1);
    expect(await db!.all(app.attachments)).toEqual([
      expect.objectContaining({ id: attachment, byte_length: 6, filename: "clip.raw" }),
    ]);
  });

  test("concurrent application writes retain every published turn exactly once", async () => {
    const store = await openMusicStore("concurrent-turns");
    const conversation = await store.createConversation("Parallel requests");

    await Promise.all(
      ["first request", "second request", "third request"].map((body, ordinal) =>
        store.addTurn({ conversationId: conversation, role: "assistant", ordinal, body }),
      ),
    );

    const transcript = await store.transcript(conversation);
    expect(transcript.map((turn) => [turn.ordinal, turn.body])).toEqual([
      [0, "first request"],
      [1, "second request"],
      [2, "third request"],
    ]);
    expect(new Set(transcript.map((turn) => turn.id)).size).toBe(3);
  });

  test("does not publish a partial attachment when its app stream is cancelled", async () => {
    const store = await openMusicStore("cancelled-attachment");
    const conversation = await store.createConversation("Interrupted upload");
    const turnId = await store.addTurn({
      conversationId: conversation,
      role: "user",
      ordinal: 0,
      body: "identify this clip",
    });

    async function* cancelledUpload(): AsyncIterable<Uint8Array> {
      yield new TextEncoder().encode("first chunk");
      throw new Error("upload cancelled");
    }

    await expect(
      store.addAttachment(
        {
          turnId,
          filename: "cancelled.raw",
          mediaType: "audio/raw",
          byteLength: 32,
        },
        cancelledUpload(),
      ),
    ).rejects.toThrow("upload cancelled");
    expect(await db!.all(app.attachments)).toEqual([]);
  });
});

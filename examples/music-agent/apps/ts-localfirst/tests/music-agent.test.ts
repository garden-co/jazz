import { describe, expect, test } from "vitest";
import { DeterministicMusicAgent, MemoryMusicStore, chunks } from "../src/music-agent.js";

describe("MusicAgent deterministic E2E", () => {
  test("streams one assistant turn, records its tool call, and preserves ordering", async () => {
    const store = new MemoryMusicStore();
    const conversation = await store.createConversation("Late-night listening");
    const transcript = await new DeterministicMusicAgent(store).answer(
      conversation,
      "warm saxophone",
    );

    expect(transcript.map((turn) => turn.role)).toEqual(["user", "assistant", "tool"]);
    expect(transcript[1]?.body).toContain("warm saxophone");
    expect(store.toolCallCount()).toBe(1);
  });

  test("materializes a streamed byte attachment and reads only its requested range", async () => {
    const store = new MemoryMusicStore();
    const conversation = await store.createConversation("Attachment");
    const turnId = await store.addTurn({
      conversationId: conversation,
      role: "user",
      ordinal: 0,
      body: "identify this clip",
    });
    const attachment = await store.addAttachment(
      { turnId, filename: "clip.raw", mediaType: "audio/raw", byteLength: 6 },
      chunks(["ab", "cdef"]),
    );

    expect(Array.from(await store.readAttachmentRange(attachment, 2, 5))).toEqual([99, 100, 101]);
  });
});

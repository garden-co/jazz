import { describe, expect, test } from "vitest";
import {
  ALBUM_TRACK_LIMIT,
  JazzRecordPlayerStore,
  PLAYLIST_WINDOW_LIMIT,
  PLAYLIST_WINDOW_OFFSET,
} from "../src/record-player.js";

describe("RecordPlayer scenario receipt", () => {
  test("creates an audio-bearing track with the public streaming mutation", async () => {
    let captured: unknown;
    const db = {
      insertStreaming: async (_table: unknown, data: unknown) => {
        captured = data;
        return { value: { id: "track-1" } };
      },
    };
    const audio = (async function* () {
      yield new Uint8Array([0x52, 0x50]);
    })();

    const id = await new JazzRecordPlayerStore(db as never).createTrackWithAudio(
      {
        albumId: "album-1",
        title: "Track one",
        ordinal: 1,
        durationMs: 180_000,
      },
      audio,
    );

    expect(id).toBe("track-1");
    expect(captured).toMatchObject({
      album_id: "album-1",
      audio_bytes: audio,
    });
  });

  test("declares bounded metadata and playlist query contracts", () => {
    expect(ALBUM_TRACK_LIMIT).toBe(32);
    expect(PLAYLIST_WINDOW_OFFSET).toBe(8);
    expect(PLAYLIST_WINDOW_LIMIT).toBe(16);
  });
});

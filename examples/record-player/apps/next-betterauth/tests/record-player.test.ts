import { describe, expect, test } from "vitest";
import { JazzRecordPlayerStore, RecordPlayerScenarioStore } from "../src/record-player.js";

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

  test("invites distinguish listener reads from editor playlist edits", () => {
    const store = new RecordPlayerScenarioStore();
    const listener = store.invite("road-tape", "listener", "listener");
    const editor = store.invite("road-tape", "editor", "editor");
    store.accept(listener);
    store.accept(editor);

    expect(store.canRead("road-tape", "listener", "owner")).toBe(true);
    expect(store.canEdit("road-tape", "listener", "owner")).toBe(false);
    expect(store.canEdit("road-tape", "editor", "owner")).toBe(true);
    expect(store.canRead("road-tape", "outsider", "owner")).toBe(false);
  });

  test("two clients retain ordered playlist entries across offline reconnect", () => {
    const store = new RecordPlayerScenarioStore();
    const owner = store.client("owner");
    const editor = store.client("editor");
    owner.addEntry("road-tape", "track-2", 2);
    editor.disconnect();
    editor.addEntry("road-tape", "track-1", 1);
    editor.reconnect();

    expect(store.window("road-tape", 16).map((entry) => entry.trackId)).toEqual([
      "track-1",
      "track-2",
    ]);
  });

  test("playlist windows are bounded and deterministically ordered", () => {
    const store = new RecordPlayerScenarioStore();
    const owner = store.client("owner");
    owner.addEntry("road-tape", "late", 3);
    owner.addEntry("road-tape", "first", 1);
    owner.addEntry("road-tape", "middle", 2);

    expect(store.window("road-tape", 2).map((entry) => entry.trackId)).toEqual(["first", "middle"]);
  });
});

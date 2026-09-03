import { describe, expect, test } from "vitest";
import { translateQuery } from "../../../../../packages/jazz-tools/src/runtime/query-adapter.js";
import { app } from "../schema.js";
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

  test("lowers metadata browsing without selecting streamed audio", async () => {
    let capturedQuery: { _build(): string } | undefined;
    const db = {
      all: async (query: { _build(): string }) => {
        capturedQuery = query;
        return [
          {
            id: "track-1",
            album_id: "album-1",
            title: "Metadata only",
            ordinal: 0,
            duration_ms: 1,
          },
        ];
      },
    };

    await expect(new JazzRecordPlayerStore(db as never).tracksForAlbum("album-1")).resolves.toEqual(
      [
        {
          id: "track-1",
          albumId: "album-1",
          title: "Metadata only",
          ordinal: 0,
          durationMs: 1,
        },
      ],
    );

    const runtimeQuery = JSON.parse(translateQuery(capturedQuery!._build(), app.wasmSchema));
    expect(runtimeQuery.table).toBe("tracks");
    // `id` is an implicit row key in the runtime query contract, so only
    // ordinary metadata columns appear in `select_columns`. The descriptor
    // shape also makes the requested full-value materialization explicit.
    expect(runtimeQuery.select_columns).toEqual([
      { column: "album_id", kind: "full" },
      { column: "title", kind: "full" },
      { column: "ordinal", kind: "full" },
      { column: "duration_ms", kind: "full" },
    ]);
    expect(
      runtimeQuery.select_columns.map((selection: { column: string }) => selection.column),
    ).not.toContain("audio_bytes");
    expect(runtimeQuery.order_by).toEqual([{ column: "ordinal", direction: "Asc" }]);
    expect(runtimeQuery.limit).toBe(ALBUM_TRACK_LIMIT);
  });

  test("lowers playlist browsing to the requested bounded window", async () => {
    let capturedQuery: { _build(): string } | undefined;
    let capturedOptions: unknown;
    const db = {
      all: async (query: { _build(): string }, options: unknown) => {
        capturedQuery = query;
        capturedOptions = options;
        return [];
      },
    };

    await expect(
      new JazzRecordPlayerStore(db as never).playlistWindow(
        "playlist-1",
        PLAYLIST_WINDOW_OFFSET,
        2,
      ),
    ).resolves.toEqual([]);

    expect(capturedOptions).toEqual({ tier: "remote-if-possible" });
    const runtimeQuery = JSON.parse(translateQuery(capturedQuery!._build(), app.wasmSchema));
    expect(runtimeQuery.table).toBe("playlist_entries");
    expect(runtimeQuery.conditions).toEqual([
      {
        Cmp: {
          left: { column: "playlist_id" },
          op: "Eq",
          right: { Literal: { type: "Uuid", value: "playlist-1" } },
        },
      },
    ]);
    expect(runtimeQuery.order_by).toEqual([{ column: "position", direction: "Asc" }]);
    expect(runtimeQuery.offset).toBe(PLAYLIST_WINDOW_OFFSET);
    expect(runtimeQuery.limit).toBe(2);
  });
});

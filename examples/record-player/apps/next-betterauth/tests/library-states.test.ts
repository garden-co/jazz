import { describe, expect, test } from "vitest";
import { albumSummary, albumTracksState, countLabel } from "../app/format.js";
import { DEMO_LIBRARY } from "../src/demo-audio.js";
import type { JazzRecordPlayerStore } from "../src/record-player.js";
import { seedDemoLibrary } from "../src/upload.js";

describe("album header copy", () => {
  test("pluralises track counts", () => {
    expect(countLabel(0, "track")).toBe("0 tracks");
    expect(countLabel(1, "track")).toBe("1 track");
    expect(countLabel(2, "track")).toBe("2 tracks");
  });

  test("summarises a loaded album with its count and length", () => {
    expect(albumSummary("ready", 1, 65_000)).toBe("1 track · 1:05");
    expect(albumSummary("empty", 0, 0)).toBe("0 tracks · 0:00");
  });

  test("does not report 0 tracks while the list loads or its tracks stream in", () => {
    expect(albumSummary("loading", 0, 0)).toBe("Loading tracks…");
    expect(albumSummary("receiving", 0, 0)).toBe("Uploading tracks…");
  });
});

describe("album track list state", () => {
  test("is loading until the first result arrives", () => {
    expect(albumTracksState(undefined, false)).toBe("loading");
    expect(albumTracksState(undefined, true)).toBe("loading");
  });

  test("an empty album this client is uploading into is receiving, not empty", () => {
    expect(albumTracksState([], true)).toBe("receiving");
    expect(albumTracksState([], false)).toBe("empty");
  });

  test("shows the rows it has, even mid-upload", () => {
    expect(albumTracksState([{}], true)).toBe("ready");
    expect(albumTracksState([{}], false)).toBe("ready");
  });
});

describe("demo library seeding", () => {
  test("marks each album as receiving until its last track is written", async () => {
    const events: string[] = [];
    let albums = 0;
    const store = {
      createAlbum: () => {
        const id = `album-${++albums}`;
        events.push(`create ${id}`);
        return id;
      },
      createTrackWithAudio: async (track: { albumId: string; ordinal: number }) => {
        events.push(`track ${track.albumId}#${track.ordinal}`);
        return `${track.albumId}-track-${track.ordinal}`;
      },
    } as unknown as JazzRecordPlayerStore;

    await seedDemoLibrary(
      store,
      () => {},
      (albumId, isReceiving) => events.push(`${isReceiving ? "start" : "stop"} ${albumId}`),
    );

    const expected = DEMO_LIBRARY.flatMap((album, index) => {
      const id = `album-${index + 1}`;
      return [
        `create ${id}`,
        `start ${id}`,
        ...album.tracks.map((_, ordinal) => `track ${id}#${ordinal + 1}`),
        `stop ${id}`,
      ];
    });
    expect(events).toEqual(expected);
  });

  test("clears the receiving mark when a track write fails", async () => {
    const events: string[] = [];
    const store = {
      createAlbum: () => "album-1",
      createTrackWithAudio: async () => {
        throw new Error("upload failed");
      },
    } as unknown as JazzRecordPlayerStore;

    await expect(
      seedDemoLibrary(
        store,
        () => {},
        (albumId, isReceiving) => events.push(`${isReceiving ? "start" : "stop"} ${albumId}`),
      ),
    ).rejects.toThrow("upload failed");
    expect(events).toEqual(["start album-1", "stop album-1"]);
  });
});

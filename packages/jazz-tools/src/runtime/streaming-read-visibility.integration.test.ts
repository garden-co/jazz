import { describe, expect, it } from "vitest";
import { createPolicyTestApp, testApp } from "../testing/index.js";

// Trimmed from examples/record-player: albums plus streamed track audio.
const app = testApp({
  tables: {
    albums: {
      columns: { title: "string", artist: "string" },
      relations: { tracksViaAlbum: { reverse: "tracks", relation: "album" } },
      indexes: ["title"],
    },
    tracks: {
      columns: { album_id: "uuid", title: "string", ordinal: "int", audio_bytes: "bytes?" },
      relations: { album: { rel: "albums", column: "album_id" } },
      indexes: ["album_id", "ordinal"],
    },
  },
});

const permissions = {
  tables: {
    albums: { read: "true", insert: "$createdBy.account = session.user" },
    tracks: { read: "true", insert: "$createdBy.account = session.user" },
  },
};

const listener = {
  issuer: "https://auth.record-player.example",
  user_id: "listener",
  account_id: "00000000-0000-4000-8000-00000000a11c",
  claims: {},
  authMode: "external" as const,
};

async function* audio(): AsyncGenerator<Uint8Array> {
  yield new Uint8Array([1, 2, 3]);
  yield new Uint8Array([4, 5, 6]);
}

// A backend client with a server URL reads at the Global tier by default. Its
// reads see its own writes because each write reaches the server before the
// read's query does. A streamed value holds later writes back while its chunks
// upload, so the query must wait behind them too.
describe("read-your-writes after a streaming insert (#3839)", () => {
  it("sees a plain insert made right after insertStreaming", async () => {
    const policyApp = await createPolicyTestApp(app, permissions, expect);
    try {
      const db = policyApp.as(listener);
      const first = db.insert(app.albums, { title: "First", artist: "A" }).value.id;
      await db.insertStreaming(app.tracks, {
        album_id: first,
        title: "Track",
        ordinal: 1,
        audio_bytes: audio(),
      });
      const id = db.insert(app.albums, { title: "Second", artist: "B" }).value.id;

      // The default tier, as the example app's position lookup uses it.
      await expect(db.all(app.albums.where({ id }))).resolves.toEqual([
        expect.objectContaining({ id, title: "Second" }),
      ]);
    } finally {
      await policyApp.shutdown();
    }
  }, 20_000);

  it("sees the streamed row itself right after insertStreaming", async () => {
    const policyApp = await createPolicyTestApp(app, permissions, expect);
    try {
      const db = policyApp.as(listener);
      const album = db.insert(app.albums, { title: "Album", artist: "A" }).value.id;
      const id = (
        await db.insertStreaming(app.tracks, {
          album_id: album,
          title: "Streamed",
          ordinal: 1,
          audio_bytes: audio(),
        })
      ).value.id;

      await expect(db.all(app.tracks.where({ id }))).resolves.toEqual([
        expect.objectContaining({ id, title: "Streamed" }),
      ]);
    } finally {
      await policyApp.shutdown();
    }
  }, 20_000);

  it("sees every album and track of an import that interleaves streamed and plain writes", async () => {
    const policyApp = await createPolicyTestApp(app, permissions, expect);
    try {
      const db = policyApp.as(listener);
      const albums: string[] = [];
      for (let album = 0; album < 3; album++) {
        const id = db.insert(app.albums, { title: `Album ${album}`, artist: "A" }).value.id;
        albums.push(id);
        for (let ordinal = 1; ordinal <= 2; ordinal++) {
          await db.insertStreaming(app.tracks, {
            album_id: id,
            title: `Track ${album}.${ordinal}`,
            ordinal,
            audio_bytes: audio(),
          });
        }
        // Each album's own lookup, as an import computes the next position.
        await expect(db.all(app.tracks.where({ album_id: id }))).resolves.toHaveLength(2);
      }

      await expect(db.all(app.albums)).resolves.toHaveLength(albums.length);
      await expect(db.all(app.tracks)).resolves.toHaveLength(albums.length * 2);
    } finally {
      await policyApp.shutdown();
    }
  }, 30_000);
});

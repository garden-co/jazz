import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createPolicyTestApp } from "../testing/index.js";

// Trimmed from examples/record-player: albums plus streamed track audio.
const app = s.defineApp({
  albums: s
    .table(
      { title: s.string(), artist: s.string() },
      { tracksViaAlbum: s.reverse("tracks", "album") },
    )
    .indexOnly(["title"]),
  tracks: s
    .table(
      {
        album_id: s.uuid(),
        title: s.string(),
        ordinal: s.int(),
        audio_bytes: s.bytes().optional(),
      },
      { album: s.rel("albums", "album_id") },
    )
    .indexOnly(["album_id", "ordinal"]),
});

const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.albums.allowRead.where({});
  policy.albums.allowInsert.where({ "$createdBy.account": session.user.account });
  policy.tracks.allowRead.where({});
  policy.tracks.allowInsert.where({ "$createdBy.account": session.user.account });
});

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
    const testApp = await createPolicyTestApp(app, permissions, expect);
    try {
      const db = testApp.as(listener);
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
      await testApp.shutdown();
    }
  }, 20_000);
});

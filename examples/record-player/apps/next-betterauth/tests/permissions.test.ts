import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createPolicyTestApp, type PolicyTestApp } from "jazz-tools/testing";
import { app } from "../schema";
import permissions from "../permissions";
import { DEMO_LIBRARY, synthesizeWav } from "../src/demo-audio";
import { AUDIO_WINDOW_BYTES, JazzRecordPlayerStore, positionBetween } from "../src/record-player";

let testApp: PolicyTestApp;

beforeEach(async () => {
  testApp = await createPolicyTestApp(app, permissions, expect);
});

afterEach(async () => {
  await testApp.shutdown();
});

describe("RecordPlayer Better Auth storage boundary", () => {
  it("keeps Better Auth rows unreadable and unwritable from an authenticated app session", async () => {
    await testApp.seed((db) =>
      db.insert(app.better_auth_user, {
        name: "Private account",
        email: "private@example.invalid",
        emailVerified: true,
        createdAt: new Date(0),
        updatedAt: new Date(0),
      }),
    );
    const browser = testApp.as({
      issuer: "https://auth.record-player.example",
      user_id: "listener",
      claims: {},
      authMode: "external",
    });

    expect(await browser.all(app.better_auth_user)).toEqual([]);
    await browser.expectDenied((db) =>
      db.insert(app.better_auth_user, {
        name: "Injected account",
        email: "attacker@example.invalid",
        emailVerified: true,
        createdAt: new Date(0),
        updatedAt: new Date(0),
      }),
    );
  });
});

const alice = {
  issuer: "https://auth.record-player.example",
  user_id: "alice",
  account_id: "00000000-0000-4000-8000-00000000a11c",
  claims: {},
  authMode: "external" as const,
};
const bob = {
  issuer: "https://auth.record-player.example",
  user_id: "bob",
  account_id: "00000000-0000-4000-8000-000000000b0b",
  claims: {},
  authMode: "external" as const,
};

describe("RecordPlayer library and playlists", () => {
  it("streams audio in and reads it back in byte ranges", async () => {
    const db = testApp.as(alice);
    const store = new JazzRecordPlayerStore(db);
    const albumId = store.createAlbum({ title: "Sine studies", artist: "The oscillators" });
    const wav = synthesizeWav(DEMO_LIBRARY[0]!.tracks[0]!);
    const trackId = await store.createTrackWithAudio(
      { albumId, title: "Morning tone", ordinal: 1, durationMs: 12_000 },
      (async function* () {
        for (let from = 0; from < wav.byteLength; from += 16 * 1024) {
          yield wav.slice(from, from + 16 * 1024);
        }
      })(),
      { mimeType: "audio/wav", byteLength: wav.byteLength },
    );

    expect(wav.byteLength).toBeLessThan(AUDIO_WINDOW_BYTES);
    const window = await store.readAudioRange(trackId, 1000, 9000);
    expect(window).toEqual(wav.slice(1000, 9000));
    const tail = await store.readAudioRange(trackId, wav.byteLength - 10, wav.byteLength);
    expect(tail).toEqual(wav.slice(wav.byteLength - 10));
    expect(await store.readAudio(trackId)).toEqual(wav);
  });

  // Writes and reads about 1 MiB, so it gets more time on a busy machine.
  it("reassembles a value larger than one playback window from range reads", async () => {
    const store = new JazzRecordPlayerStore(testApp.as(alice));
    const albumId = store.createAlbum({ title: "Long players", artist: "The windows" });
    const size = 2 * AUDIO_WINDOW_BYTES + 12_345;
    const bytes = new Uint8Array(size);
    for (let i = 0; i < size; i++) bytes[i] = (i * 31 + (i >> 9)) & 0xff;
    const trackId = await store.createTrackWithAudio(
      { albumId, title: "Three windows", ordinal: 1, durationMs: 1_000 },
      (async function* () {
        for (let from = 0; from < size; from += 64 * 1024)
          yield bytes.slice(from, from + 64 * 1024);
      })(),
      { mimeType: "application/octet-stream", byteLength: size },
    );

    const windows: Uint8Array[] = [];
    for (let from = 0; from < size; from += AUDIO_WINDOW_BYTES) {
      const window = await store.readAudioRange(
        trackId,
        from,
        Math.min(size, from + AUDIO_WINDOW_BYTES),
      );
      expect(window?.byteLength).toBe(Math.min(AUDIO_WINDOW_BYTES, size - from));
      windows.push(window!);
    }
    expect(windows).toHaveLength(3);
    const joined = new Uint8Array(size);
    let offset = 0;
    for (const window of windows) {
      joined.set(window, offset);
      offset += window.byteLength;
    }
    expect(joined).toEqual(bytes);
  }, 60_000);

  it("admits an invited editor only after they accept", async () => {
    const owner = new JazzRecordPlayerStore(testApp.as(alice));
    const bobDb = testApp.as(bob);
    const guest = new JazzRecordPlayerStore(bobDb);
    const albumId = owner.createAlbum({ title: "Night shift", artist: "Quiet carrier" });
    const trackId = await owner.createTrackWithAudio(
      { albumId, title: "Relay", ordinal: 1, durationMs: 1_000 },
      (async function* () {
        yield new Uint8Array([1, 2, 3]);
      })(),
      { mimeType: "audio/wav", byteLength: 3 },
    );
    const playlistId = owner.createPlaylist("Late night");
    owner.addToPlaylist(playlistId, trackId, positionBetween());
    const invitationId = await owner.invite(playlistId, bob.account_id, "editor");

    await bobDb.expectDenied((db) =>
      db.insert(app.playlist_entries, { playlist_id: playlistId, track_id: trackId, position: 2 }),
    );

    await guest.acceptInvitation(invitationId);
    const entry = bobDb.insert(app.playlist_entries, {
      playlist_id: playlistId,
      track_id: trackId,
      position: 2,
    });
    await entry.wait({ tier: "global" });
    await expect(
      bobDb.all(app.playlists.where({ id: playlistId }).select("name"), { tier: "remote" }),
    ).resolves.toMatchObject([{ name: "Late night" }]);
    // Renaming stays with the owner.
    await bobDb.expectDenied((db) => db.update(app.playlists, playlistId, { name: "Mine now" }));
  });
});

describe("fractional playlist positions", () => {
  it("places new entries between, before and after neighbours", () => {
    expect(positionBetween()).toBe(1);
    expect(positionBetween(undefined, 1)).toBe(0);
    expect(positionBetween(1, undefined)).toBe(2);
    expect(positionBetween(1, 2)).toBe(1.5);
  });
});

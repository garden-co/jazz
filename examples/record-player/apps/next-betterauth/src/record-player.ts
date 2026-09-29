import type { Db, StreamingValueSource } from "jazz-tools";
import { app } from "../schema";

export type InvitationRole = "listener" | "editor";

export type TrackMetadata = {
  id: string;
  albumId: string;
  title: string;
  ordinal: number;
  durationMs: number;
};

export type AudioMetadata = {
  /** Media type of the stored bytes, e.g. `audio/mpeg` or `audio/wav`. */
  mimeType?: string;
  /** Total stored bytes; lets playback plan range reads without reading the value. */
  byteLength?: number;
};

export type PlaylistEntry = {
  id: string;
  playlistId: string;
  trackId: string;
  position: number;
};

export const ALBUM_TRACK_LIMIT = 32;
export const PLAYLIST_WINDOW_OFFSET = 8;
export const PLAYLIST_WINDOW_LIMIT = 16;
/**
 * Size of one audio range read during playback. Until exact chunk demand lands
 * (#2090), every range read materialises the whole stored value before slicing
 * it, so windows are kept large to bound that repeated cost.
 */
export const AUDIO_WINDOW_BYTES = 512 * 1024;

/**
 * Fractional ordering: a new position strictly between two neighbours (either
 * may be missing at the ends of the list). Concurrent inserts at the same gap
 * pick the same number and then tie-break by entry id.
 */
export function positionBetween(before?: number, after?: number): number {
  if (before === undefined && after === undefined) return 1;
  if (before === undefined) return after! - 1;
  if (after === undefined) return before + 1;
  return (before + after) / 2;
}

/**
 * The application persistence boundary keeps library browsing independent of
 * audio payloads. Audio is written with the public streaming create API and
 * read back only in bounded byte ranges when a track is actually played.
 */
export class JazzRecordPlayerStore {
  constructor(private readonly db: Db) {}

  createAlbum(album: {
    title: string;
    artist: string;
    cover?: { bytes: Uint8Array; mimeType: string };
  }): string {
    return this.db.insert(app.albums, {
      title: album.title,
      artist: album.artist,
      cover_image: album.cover?.bytes,
      cover_mime: album.cover?.mimeType,
    }).value.id;
  }

  async createTrackWithAudio(
    track: Omit<TrackMetadata, "id">,
    audio: StreamingValueSource,
    metadata: AudioMetadata = {},
  ): Promise<string> {
    const write = await this.db.insertStreaming(app.tracks, {
      album_id: track.albumId,
      title: track.title,
      ordinal: track.ordinal,
      duration_ms: track.durationMs,
      audio_mime: metadata.mimeType,
      audio_byte_length: metadata.byteLength,
      audio_bytes: audio,
    });
    return write.value.id;
  }

  /** A bounded metadata-only catalogue read; this intentionally selects no audio bytes. */
  async tracksForAlbum(albumId: string): Promise<TrackMetadata[]> {
    const rows = await this.db.all(
      app.tracks
        .where({ album_id: albumId })
        .orderBy("ordinal", "asc")
        .limit(ALBUM_TRACK_LIMIT)
        .select("id", "album_id", "title", "ordinal", "duration_ms"),
    );
    return rows.map((row) => ({
      id: row.id,
      albumId: row.album_id,
      title: row.title,
      ordinal: row.ordinal,
      durationMs: row.duration_ms,
    }));
  }

  /**
   * Reads `[from, to)` of a track's audio and returns only that slice. Today
   * Jazz still materialises the whole value to cut the slice (#2090); callers
   * don't need to change when it reads just the requested chunks.
   */
  async readAudioRange(trackId: string, from: number, to: number): Promise<Uint8Array | null> {
    const [row] = await this.db.all(
      app.tracks.where({ id: trackId }).select({ audio_bytes: { from, to } }),
      { tier: "local-first-unless-empty" },
    );
    return row?.audio_bytes ?? null;
  }

  /** Whole-value read, for tracks whose byte length was not recorded. */
  async readAudio(trackId: string): Promise<Uint8Array | null> {
    const [row] = await this.db.all(app.tracks.where({ id: trackId }).select("audio_bytes"), {
      tier: "local-first-unless-empty",
    });
    return row?.audio_bytes ?? null;
  }

  /** Authority-relative while the server can answer; offsets use cached rows offline. */
  async playlistWindow(
    playlistId: string,
    offset = PLAYLIST_WINDOW_OFFSET,
    limit = PLAYLIST_WINDOW_LIMIT,
  ): Promise<PlaylistEntry[]> {
    const rows = await this.db.all(
      app.playlist_entries
        .where({ playlist_id: playlistId })
        .orderBy("position", "asc")
        .offset(offset)
        .limit(limit),
      { tier: "local-first-unless-empty" },
    );
    return rows.map((row) => ({
      id: row.id,
      playlistId: row.playlist_id,
      trackId: row.track_id,
      position: row.position,
    }));
  }

  createPlaylist(name: string): string {
    return this.db.insert(app.playlists, { name }).value.id;
  }

  renamePlaylist(playlistId: string, name: string): void {
    this.db.update(app.playlists, playlistId, { name });
  }

  addToPlaylist(playlistId: string, trackId: string, position: number): string {
    return this.db.insert(app.playlist_entries, {
      playlist_id: playlistId,
      track_id: trackId,
      position,
    }).value.id;
  }

  moveEntry(entryId: string, position: number): void {
    this.db.update(app.playlist_entries, entryId, { position });
  }

  removeEntry(entryId: string): void {
    this.db.delete(app.playlist_entries, entryId);
  }

  /** `user` is the invitee's enrolled Jazz account ID, not an auth-provider id. */
  async invite(playlistId: string, user: string, role: InvitationRole): Promise<string> {
    return this.db.insert(app.invitations, {
      playlist_id: playlistId,
      subject: user,
      role,
      status: "pending",
    }).value.id;
  }

  async acceptInvitation(invitationId: string): Promise<void> {
    await this.db.update(app.invitations, invitationId, { status: "accepted" });
  }

  revokeInvitation(invitationId: string): void {
    this.db.update(app.invitations, invitationId, { status: "revoked" });
  }
}

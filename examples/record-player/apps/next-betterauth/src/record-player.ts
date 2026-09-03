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
 * The application persistence boundary keeps library browsing independent of
 * audio payloads. `createTrackWithAudio` is the only payload operation: it
 * uses the public streaming create API and never reads the bytes back eagerly.
 */
export class JazzRecordPlayerStore {
  constructor(private readonly db: Db) {}

  async createTrackWithAudio(
    track: Omit<TrackMetadata, "id">,
    audio: StreamingValueSource,
  ): Promise<string> {
    const write = await this.db.insertStreaming(app.tracks, {
      album_id: track.albumId,
      title: track.title,
      ordinal: track.ordinal,
      duration_ms: track.durationMs,
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

  /** Authority-relative while online; offsets use cached rows while offline. */
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
      { tier: "remote-if-possible" },
    );
    return rows.map((row) => ({
      id: row.id,
      playlistId: row.playlist_id,
      trackId: row.track_id,
      position: row.position,
    }));
  }

  /** `user` is the invitee's canonical Jazz session user, not an auth-provider id. */
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
}

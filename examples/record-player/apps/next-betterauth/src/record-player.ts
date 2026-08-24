import type { Db, StreamingValueSource } from "jazz-tools";
import { app } from "../schema.js";

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
      app.tracks.where({ album_id: albumId }).orderBy("ordinal", "asc"),
    );
    return rows.map((row) => ({
      id: row.id,
      albumId: row.album_id,
      title: row.title,
      ordinal: row.ordinal,
      durationMs: row.duration_ms,
    }));
  }

  /** Bounded, indexed playlist-window query used by the playlist screen. */
  async playlistWindow(playlistId: string, limit = 32): Promise<PlaylistEntry[]> {
    const rows = await this.db.all(
      app.playlist_entries
        .where({ playlist_id: playlistId })
        .orderBy("position", "asc")
        .limit(limit),
    );
    return rows.map((row) => ({
      id: row.id,
      playlistId: row.playlist_id,
      trackId: row.track_id,
      position: row.position,
    }));
  }

  async invite(playlistId: string, subject: string, role: InvitationRole): Promise<string> {
    return this.db.insert(app.invitations, {
      playlist_id: playlistId,
      subject,
      role,
      status: "pending",
    }).value.id;
  }

  async acceptInvitation(invitationId: string): Promise<void> {
    await this.db.update(app.invitations, invitationId, { status: "accepted" });
  }
}

/**
 * A deterministic, framework-neutral topology receipt. It models the app's
 * product rule (unique entries sorted by position) and lets two clients queue
 * edits while disconnected. Transport authorization remains Jazz's job.
 */
export class RecordPlayerScenarioStore {
  private readonly entries = new Map<string, PlaylistEntry>();
  private readonly invitations = new Map<
    string,
    { playlistId: string; subject: string; role: InvitationRole; accepted: boolean }
  >();
  private nextId = 1;

  client(subject: string): ScenarioClient {
    return new ScenarioClient(this, subject);
  }

  invite(playlistId: string, subject: string, role: InvitationRole): string {
    const id = this.id("invite");
    this.invitations.set(id, { playlistId, subject, role, accepted: false });
    return id;
  }

  accept(invitationId: string): void {
    const invitation = this.invitations.get(invitationId);
    if (!invitation) throw new Error("unknown invitation");
    invitation.accepted = true;
  }

  canRead(playlistId: string, subject: string, owner: string): boolean {
    return (
      subject === owner ||
      [...this.invitations.values()].some(
        (invite) =>
          invite.playlistId === playlistId && invite.subject === subject && invite.accepted,
      )
    );
  }

  canEdit(playlistId: string, subject: string, owner: string): boolean {
    return (
      subject === owner ||
      [...this.invitations.values()].some(
        (invite) =>
          invite.playlistId === playlistId &&
          invite.subject === subject &&
          invite.accepted &&
          invite.role === "editor",
      )
    );
  }

  apply(entry: PlaylistEntry): void {
    this.entries.set(entry.id, entry);
  }

  window(playlistId: string, limit: number): PlaylistEntry[] {
    return [...this.entries.values()]
      .filter((entry) => entry.playlistId === playlistId)
      .sort((left, right) => left.position - right.position || left.id.localeCompare(right.id))
      .slice(0, limit);
  }

  id(kind: string): string {
    return `${kind}-${this.nextId++}`;
  }
}

export class ScenarioClient {
  private online = true;
  private readonly pending: PlaylistEntry[] = [];

  constructor(
    private readonly store: RecordPlayerScenarioStore,
    readonly subject: string,
  ) {}

  disconnect(): void {
    this.online = false;
  }

  reconnect(): void {
    this.online = true;
    for (const entry of this.pending.splice(0)) this.store.apply(entry);
  }

  addEntry(playlistId: string, trackId: string, position: number): string {
    const id = this.store.id("entry");
    const entry = { id, playlistId, trackId, position };
    if (this.online) this.store.apply(entry);
    else this.pending.push(entry);
    return id;
  }
}

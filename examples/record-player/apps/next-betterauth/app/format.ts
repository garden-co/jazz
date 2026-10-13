export function formatDuration(ms: number): string {
  const total = Math.max(0, Math.round(ms / 1000));
  const minutes = Math.floor(total / 60);
  const seconds = total % 60;
  return `${minutes}:${seconds.toString().padStart(2, "0")}`;
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

export function shortId(id: string): string {
  return id.slice(0, 8);
}

/** "1 track", "3 tracks". */
export function countLabel(count: number, singular: string, plural = `${singular}s`): string {
  return `${count} ${count === 1 ? singular : plural}`;
}

/**
 * What the track list can show. A streamed track only becomes visible once its
 * whole file is written, so an album this client is still uploading into has no
 * rows yet: that is "receiving", not "empty".
 */
export type AlbumTracksState = "loading" | "receiving" | "empty" | "ready";

export function albumTracksState(
  tracks: readonly unknown[] | undefined,
  isReceivingTracks: boolean,
): AlbumTracksState {
  if (tracks === undefined) return "loading";
  if (tracks.length > 0) return "ready";
  return isReceivingTracks ? "receiving" : "empty";
}

export function albumSummary(state: AlbumTracksState, count: number, totalMs: number): string {
  switch (state) {
    case "loading":
      return "Loading tracks…";
    case "receiving":
      return "Uploading tracks…";
    default:
      return `${countLabel(count, "track")} · ${formatDuration(totalMs)}`;
  }
}

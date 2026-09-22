"use client";

import { useEffect, useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
import { app } from "../schema";
import { PLAYLIST_WINDOW_LIMIT, PLAYLIST_WINDOW_OFFSET } from "../src/record-player";

/** The reachable metadata-first playlist surface; audio is never selected here. */
export function RecordPlayerClient({ playlistId }: { playlistId?: string }) {
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  // The Jazz provider is browser-owned. Do not invoke its hooks during Next's
  // static render; the live component mounts once the provider is available.
  return mounted ? <LiveRecordPlayer playlistId={playlistId} /> : null;
}

function LiveRecordPlayer({ playlistId }: { playlistId?: string }) {
  const db = useDb();
  const albums = useAll(app.albums.orderBy("title", "asc").limit(20));
  const entries = useAll(
    playlistId
      ? app.playlist_entries
          .where({ playlist_id: playlistId })
          .orderBy("position", "asc")
          .offset(PLAYLIST_WINDOW_OFFSET)
          .limit(PLAYLIST_WINDOW_LIMIT)
      : app.playlist_entries.where({ playlist_id: "" }).limit(0),
  );

  async function createPlaylist() {
    await db.insert(app.playlists, { name: "New playlist" });
  }

  return (
    <section>
      <button onClick={createPlaylist}>Create playlist</button>
      <h2>Albums</h2>
      <ul>
        {albums.data?.map((album) => (
          <li key={album.id}>{album.title}</li>
        ))}
      </ul>
      <h2>Playlist window</h2>
      <ol start={PLAYLIST_WINDOW_OFFSET + 1}>
        {entries.data?.map((entry) => (
          <li key={entry.id}>{entry.track_id}</li>
        ))}
      </ol>
    </section>
  );
}

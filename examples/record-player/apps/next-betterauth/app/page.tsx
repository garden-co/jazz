"use client";

import { RecordPlayerClient } from "./record-player-client";
import { RecordPlayerProvider } from "./record-player-provider";

export default function Home() {
  return (
    <main>
      <p className="eyebrow">Jazz example · Next.js + Better Auth variant</p>
      <div className="record" aria-hidden="true" />
      <h1>RecordPlayer</h1>
      <p>Albums, shared playlists, and streaming audio without losing the groove.</p>
      <ul>
        <li>CoverFlow-style album browsing</li>
        <li>Ordered, collaboratively edited playlists</li>
        <li>Partial audio buffering and durable playback position</li>
      </ul>
      <RecordPlayerProvider>
        <RecordPlayerClient />
      </RecordPlayerProvider>
    </main>
  );
}

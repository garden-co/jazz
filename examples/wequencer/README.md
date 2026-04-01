# Wequencer

Collaborative real-time music sequencer. Multiple users place beats on a shared 16-step grid and play in sync. Jazz handles state sync; Tone.js handles audio.

## Getting started

```bash
pnpm install
pnpm dev        # starts the Jazz server, pushes the schema, and opens Vite
```

To understand how the app uses Jazz, run the walkthrough:

```bash
npm run walkthrough
```

## Commands

```bash
npm run walkthrough        # Marp slideshow — Jazz patterns used in this app
npm run walkthrough:shots  # Re-capture screenshots for the slideshow
npm run test:e2e           # Playwright e2e tests
npm run test               # Vitest unit tests
npm run build              # Validate schema.ts + production build
```

## How it works

**State sync** is entirely handled by Jazz. Every beat, instrument change, BPM update, and participant rename is a synchronous local write (`db.insert`, `db.update`, `db.delete`). Jazz replicates the change to all connected peers in the background. The UI is driven by `QuerySubscription` reactive queries — no polling, no manual state management.

**Synchronized playback** is also scheduled through Jazz. All peers need to start their Tone.js transport at exactly the same moment. When a user hits play, a future timestamp (in server epoch time) is written to `jam.transport_start` via Jazz. All peers receive the update, convert it to their local clock using a measured offset, and schedule Tone.js to start at that instant. The offset is tracked by `ClockSync`, which connects to the Jazz WebSocket and computes a smoothed estimate of the difference between local `performance.now()` and server epoch time.

**Drift correction** runs every 500ms during playback. If the measured beat position drifts more than 10ms from the expected position for two consecutive checks, BPM is nudged up or down by up to 2% to pull the clocks back into alignment.

**Audio samples** are stored as `BYTEA` blobs in the instruments table. When an instrument is seeded or added, the MP3 is fetched and written to the database. All peers receive the binary data through Jazz sync and decode it locally — no separate asset server required.

## Schema

Defined in `schema.ts` using the Jazz typed schema DSL. Running `pnpm build` runs `jazz-tools validate` before the production build; the app imports the typed `app` export directly from that file.

- **instruments** — name, sound (BYTEA), display_order
- **jams** — created_at, transport_start (nullable), bpm, beat_count
- **beats** — jam (ref), instrument (ref), beat_index, placed_by
- **participants** — jam (ref), user_id, display_name

## HTTPS in development

The dev server uses a self-signed TLS certificate (`@vitejs/plugin-basic-ssl`), required because `crypto.subtle` (used by Jazz) is only available in secure contexts. Vite proxies Jazz server requests (`/sync`, `/events`, `/health`, `/auth`) through the same HTTPS origin to avoid mixed content issues. When accessing from another device on the same network, accept the self-signed certificate warning in the browser.

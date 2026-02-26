# Wequencer

Collaborative real-time music sequencer. Multiple users create rhythmic patterns together on a 16-beat grid. Jazz2 handles state synchronisation via a relational model, and Tone.js handles audio playback.

## Commands

```bash
pnpm install                    # Install dependencies
pnpm dev                        # Start jazz server + push schema + Vite dev server
pnpm build                      # Schema codegen + production build
pnpm test:e2e                   # Playwright E2E tests (spawns its own jazz server)
```

## Schema

The relational schema is defined in `schema/current.ts` using jazz2's `table()` and `col.*` DSL. Running `pnpm build` generates the typed client (`schema/app.ts`) and SQL files.

Tables:

- **instruments** — name, sound (BYTEA), display_order
- **jams** — created_at, transport_start (nullable, for playback sync)
- **beats** — jam (ref), instrument (ref), beat_index, placed_by
- **participants** — jam (ref), user_id, display_name

## How it works

`AudioProvider.svelte` orchestrates playback:

1. A `ClockSync` WebSocket connection estimates the offset between local and server time.
2. On play, a future server-epoch start time is computed and written to the jam's `transport_start`.
3. All peers read `transport_start`, convert to local time, and schedule Tone.js transport.
4. A drift correction loop adjusts BPM within +/-2% to keep peers aligned.

Audio samples (MP3) live in `public/`. On first run, `ensureInstrumentsSeeded()` fetches them and stores the data in the instruments table so all peers can load them via jazz sync.

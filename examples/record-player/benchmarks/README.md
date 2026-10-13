# RecordPlayer benchmark variant

This isolated native package duplicates RecordPlayer's metadata column names
and indexed ordered-playlist query shape. It measures a CoverFlow album window
and a bounded playlist window, and owns **large-value range reads**: scrubbing
to the middle of a 64 MiB streamed track (`record_player_scrub_track_64mb`,
formerly EpicDrop's `epic_drop_seek_64mb`, which also covers MusicAgent's former
`music_agent_attachment_seek_8mb`).

[metadata.ts](metadata.ts) owns the wall-clock descriptions and timing
boundaries for `benches/walltime.rs`, which CodSpeed runs on every
`benchmark`-labelled PR and nightly: opening the CoverFlow library, opening a
4,096-track playlist, adding a track to a live playlist window, and scrubbing
a 64 MiB track.

```sh
cargo test -p jazz-example-record-player-benchmark
cargo bench -p jazz-example-record-player-benchmark --bench walltime
```

# RecordPlayer learnings

- Metadata/query behavior is independently useful now. The adapter uses public
  `insertStreaming` for audio creation, but deliberately exposes no playback or
  byte-range read: typed `Db` range reads are still tracked by #1833/#1839/#1844.
- Accepted invitations grant listener reads; accepted editor invitations grant
  playlist-entry mutations. Only the playlist creator can issue, change, or
  revoke invitations, and only they can rename a playlist.
- Concurrent playlist additions converge by entry identity and position. A
  concurrent move of the _same_ entry is deliberately not given a product-level
  winner here; the UI must reconcile it after a specific move contract exists.
- The maintained package `test` gate covers unit/query lowering, the Better
  Auth provider lifecycle, permission denial, and test selection. The explicit
  `test:topology` gate covers invitation roles, two-client edits, streamed audio,
  and offline reconnect across browser/edge/core; it remains outside the
  aggregate gate only while #2091 tracks isolated browser artifact execution.

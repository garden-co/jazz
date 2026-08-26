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
- `tests/record-player.test.ts` is a bounded scenario receipt for metadata-first
  reads, streaming creation, invitation roles, two-client edits, and an offline
  reconnect flush. It is not a substitute for the pending browser/relay
  topology E2E that can exercise those APIs end-to-end.

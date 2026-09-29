# RecordPlayer learnings

- Audio is written with public `insertStreaming` and played back through typed
  large-value range selections (`select({ audio_bytes: { from, to } })`, #2088).
  Library browsing selects metadata only. Range reads still materialise the
  whole stored value before slicing (#2090, #3471); the player reads in 64 KiB
  windows regardless, so it gains from exact chunk demand without changes.
- Store the byte length (and media type) beside a streamed value. A range read
  must stay inside the value and the typed query API has no length-only
  projection, so the player plans its windows from `audio_byte_length`.
- A pending invitee cannot read the playlist's name, because the read policy
  admits accepted invitations only; the invitations UI says so rather than
  showing an id.
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

# RecordPlayer learnings

- Metadata/query behavior is independently useful now; audio bytes must not be
  faked as an eager browser value while large-value APIs are unfinished.
- Invite/revoke requires a real shared-playlist policy traversal and two
  authenticated clients before it can receive an app-facing E2E assertion.
- Playlist reordering needs an explicit concurrent-move product rule; do not
  claim a deterministic winner merely from presentation order.

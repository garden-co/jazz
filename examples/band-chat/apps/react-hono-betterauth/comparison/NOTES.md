# BandChat on Jazz: comparison notes

- Copied from jazz `examples/band-chat/apps/react-hono-betterauth` at `de1cc17c9a`, on published
  `jazz-tools@2.0.0-alpha.62`.
- `messages` declares `.compositeIndex(["roomId", "$createdAt"])`, the index a competent developer
  would declare for the room's newest page (the other ports declare theirs). alpha.62's engine
  supports it, but its TypeScript check rejected `$createdAt`; `patches/jazz-tools@2.0.0-alpha.62.patch`
  applies the one-line check from [jazz#5473](https://github.com/garden-co/jazz/pull/5473) until
  that ships. Measured with and without: no meaningful difference (open 1,268 vs 1,237 ms, thin
  cross-client 293 vs 277 ms), so the time is elsewhere (the shared UI's render dominates).
- Harness-only flags: `VITE_JAZZ_THIN=1`, `VITE_JAZZ_LOCAL_FIRST=now` (see harness.mjs).
- Seed: users sign up through Better Auth; rooms, the ask-to-join and admit flow, and messages are
  written as those users through Jazz (policies apply).

# Interceptor spike (web SW + RN loopback)

Type: `wayfinder:prototype`
Status: open
Assignee: (unclaimed)
Blocked by: (none)

## Question

Prove the two offline-read interceptors work as designed, and surface the
constraints they impose on the device file store and on `url()` — before
the store's home is decided.

Build the cheapest end-to-end slices (throwaway, via `/prototype`):

- **Web:** a minimal service worker intercepting `/files/*` that serves a
  "staged" body from OPFS (or Cache API) into a plain `<img>` and a
  `<video>`, plus fetch-through with cache write. Verify: OPFS readability
  from the SW context; **Range/206 handling** for video seeking through
  the SW; first-load fallthrough behavior (no controlling SW); cache
  write-through without double-buffering large bodies.
- **React Native:** a minimal loopback HTTP server (ideally in the
  existing Rust native module; a stub native lib is acceptable for the
  spike) serving bytes to `<Image>` and a video component. Verify:
  loopback bind + random port + secret path segment; cleartext-to-localhost
  on iOS (ATS) and Android (manifest exemption); **Range/seek** against
  the loopback; behavior when the app suspends mid-stream; whether the
  server can read bodies straight from the candidate store homes
  (filesystem dir vs blobs-in-KV).

Deliverable: linked prototype code plus a findings note stating the
constraints on the store choice (ticket
[B — device file store](B-staging-store.md)) and any `url()` shape
implications. Not a deliverable: production code — this is a spike; the
artifacts are throwaway by contract.

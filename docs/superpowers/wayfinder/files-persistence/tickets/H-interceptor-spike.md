# Interceptor spike (web SW + RN loopback)

Type: `wayfinder:prototype`
Status: closed (resolved 2026-07-10)
Assignee: guido (claimed 2026-07-10)
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

## Resolution (2026-07-10)

Both interceptors proven with executed throwaway prototypes
([assets + full findings](../prototypes/NOTES.md)):

- **Web SW:** interception of `<img>`-initiated `/files/*` loads works;
  OPFS is readable from the SW context (async handles); Range/206
  synthesis (explicit + suffix) works from stored bodies — video seeking
  is safe; first-load fallthrough confirmed (Blob preview stays);
  fetch-through + cache write-through work, with the requirement that the
  cache put be wrapped in `event.waitUntil`.
- **Loopback (RN model):** a ~120-line std-only Rust server binds
  loopback-only (LAN refused), enforces the secret path (403), and serves
  200/206 with correct Content-Range and nosniff straight off a
  filesystem directory via seek.

Constraints handed to [B — device file store](B-staging-store.md):
OPFS raw files are the natural browser home (both staging writes and SW
reads proven against them); a plain filesystem directory is strongly
favored on native/RN (seek-based Range vs whole-value KV reads); and the
**SW can only intercept same-origin in-scope URLs**, so web deployments
wanting SW offline must expose `/files/*` on the app's own origin
(proxy/CDN path-through) — recorded in the PRD as a deployment
requirement. On-device items (ATS/cleartext exemptions, Fresco/AVPlayer
vs 127.0.0.1, suspend mid-stream) remain for implementation.

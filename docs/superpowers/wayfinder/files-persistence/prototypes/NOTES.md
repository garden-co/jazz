# Interceptor spike — findings (2026-07-10)

PROTOTYPES — throwaway by contract. `web-sw/` (run:
`python3 -m http.server <port> --bind 127.0.0.1 -d web-sw/`, open
`/index.html`, results render in `#status`) and `rn-loopback/loopback.rs`
(run: `rustc -O loopback.rs -o /tmp/spike-loopback && /tmp/spike-loopback

<dir>`, exercise with curl). Both were executed and verified on 2026-07-10
(Chrome via agent-browser; curl against the Rust binary).

## Verified — web service worker

- **Interception works end to end:** an `<img src="/files/…">` whose path
  the origin cannot serve renders from the SW (Cache API body). ✓
- **OPFS is readable from inside the SW** via the async handle API
  (`navigator.storage.getDirectory` → `getFileHandle` → `getFile`);
  page-side write + SW-side read proven. ✓ (Sync access handles are
  dedicated-worker-only — not needed.)
- **Range/206 synthesis from a stored body works**, both explicit
  (`bytes=100-199` → `206`, `Content-Range: bytes 100-199/1048576`,
  payload bytes verified) and suffix (`bytes=-100`) forms — the mechanics
  `<video>` seeking depends on. ✓
- **First-load fallthrough confirmed:** the first page load is
  uncontrolled (requests hit the network); control begins after
  registration + reload. The Blob-in-hand preview stays necessary. ✓
- **Fetch-through + cache write-through work**, with one implementation
  requirement: the cache `put` must be wrapped in `event.waitUntil` —
  fire-and-forget writes race reads. ✓ (found by hitting the race)

## Verified — loopback server (RN model)

Std-only Rust, ~120 lines, no dependencies:

- Binds `127.0.0.1:0` (random port); the LAN address refuses connections —
  loopback-only confirmed. ✓
- Per-boot secret path segment: wrong secret → 403. ✓
- `200` full reads and `206` partial reads with correct `Content-Range`
  (explicit + suffix), payload bytes verified; `Accept-Ranges` and
  `X-Content-Type-Options: nosniff` served. ✓
- Reads bodies straight off a **plain filesystem directory** with
  seek-based Range — no buffering. ✓

## Constraints surfaced for ticket B (device file store)

1. **Browser store home:** OPFS raw files and Cache API both work as
   SW-readable homes. OPFS is the natural single home (staging writes and
   SW reads against the same files); Cache API remains a viable
   alternative for the cached class.
2. **Native/RN store home:** a plain filesystem directory is strongly
   favored over blobs-in-KV — the loopback server Range-serves via seek;
   a KV blob would force whole-value reads per request.
3. **`url()` / serving constraint (important):** a service worker can only
   intercept **same-origin, in-scope** requests. If web `url()` points at
   a different host (CDN/serving endpoint domain), `<img>` fetches bypass
   the app's SW entirely and offline breaks. Web deployments wanting SW
   offline must expose `/files/*` on the app's own origin (proxy or CDN
   path-through). This belongs in the PRD as a stated deployment
   requirement.
4. **SW implementation note:** cache write-through must use
   `event.waitUntil`.

## Not testable here (left for on-device implementation)

iOS ATS localhost exemption and Android `usesCleartextTraffic`/network-
security-config behavior; `<Image>`/video component behavior against
`127.0.0.1` (Fresco/AVPlayer); app-suspend mid-stream. Platform docs say
all are supported/expected; verify on device during implementation.

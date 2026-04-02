# Transport Protocol Upgrade: WebTransport + WebSocket Fallback

## Problem

Jazz's current transport is a hybrid of **SSE** (server-to-client push via `GET /events`) and **HTTP POST** (client-to-server sync via `POST /sync`). This architecture has a fundamental ordering flaw: outgoing updates are dispatched as independent async tasks, so a fast sequence of writes can reach the server in a different order than the writer produced them.

Concrete impact:

- **`subscription_reflects_final_state_after_rapid_bulk_updates`** (`subscribe_all_integration.rs`) is `#[ignore]`d — 500 rapid title overwrites reorder in flight. Alice writes title-001 → title-002 → title-003, server sees title-002 → title-001 → title-003. The subscriber's last delta disagrees with the snapshot query.
- **`single_client_operations_reach_server_in_causal_order`** (`policies_integration.rs`) is `#[ignore]`d — Alice transfers ownership then immediately writes a title update. If the server reorders these, the title update is incorrectly accepted because the ownership transfer hasn't landed yet. **Ordering bugs bypass policy enforcement.**
- The reliability idea doc (`todo/ideas/1_mvp/sync-protocol-reliability.md`) identifies six gaps, with outbound ordering as gap #1.

The root cause is architectural: HTTP POST is connectionless. Each request is independent — the server has no way to enforce arrival order across requests, and the client has no single ordered channel to serialize writes into. SSE is unidirectional (server→client only), so the client side is forced into a separate POST path.

On top of the ordering problem, **SSE is a UTF-8 text protocol**. Binary data must be base64-encoded before transmission, inflating payload size by ~33%. This rules out efficient binary wire formats (MessagePack, FlatBuffers, raw protobuf) for the server→client path — every byte must round-trip through base64 encode/decode. The current codebase works around this with a custom binary framing layer over SSE, but the fundamental UTF-8 constraint remains: any future move to a compact binary protocol for sync payloads is blocked by the transport.

A bidirectional, ordered, **binary-native** transport eliminates both the ordering bugs and the encoding overhead by design.

## Appetite

Big batch. This touches the transport layer across Rust server, TypeScript client, and WASM bindings. Multiple PRs, phased rollout.

## Solution

Replace SSE + HTTP POST with **WebSocket** (ships now) behind a **TransportProvider** abstraction that makes **WebTransport** a drop-in future upgrade. Both provide bidirectional ordered streams over a single connection — WebSocket solves the ordering bugs today, WebTransport adds performance later.

### Why WebTransport as primary

WebTransport ([W3C Working Draft](https://www.w3.org/TR/webtransport/#webtransport-stream)) runs over HTTP/3 / QUIC and provides:

1. **Reliable, in-order delivery per stream** — each stream is "a reliable in-order stream of bytes" ([spec](https://www.w3.org/TR/webtransport/#webtransport-stream)). Within a single bidirectional stream, write order = delivery order. This directly solves the ordering bug.
2. **Independent stream multiplexing** — multiple streams within one connection don't block each other. A lost packet on stream A doesn't stall stream B ([HTTP/3 in-order](https://http3-explained.haxx.se/en/the-protocol/feature-inorder)). This eliminates head-of-line blocking that TCP-based WebSocket suffers from.
3. **Lower latency** — QUIC 0-RTT connection establishment vs TCP+TLS 2-3 RTT. Reconnection after network switch is near-instant (connection migration).
4. **Datagram support** — unreliable, unordered datagrams available for latency-sensitive signals (heartbeats, presence) without consuming stream resources.
5. **Native browser API** — `new WebTransport(url)` with Streams API integration. Baseline browser support since March 2026 (~80.7% global coverage): Chrome 97+, Firefox 114+, Safari 26.4+, Edge 98+.

### Adoption reality check

WebTransport is appealing — it's fast, modern, and a genuine selling point ("Jazz syncs over WebTransport" sounds great in a landing page). The multiplexing, 0-RTT, and connection migration are real advantages for a sync engine.

But it may be **too early to depend on it**:

- **Infra complexity** — HTTP/3 server, NLB migration, server-side TLS cert management, Chrome's custom CA limitations for local dev. That's a lot of operational surface for a prototype.
- **Ecosystem maturity** — Rust libraries (`wtransport`, `web-transport`) are young. Production battle-testing is limited. The moq.dev team runs it in prod but calls TLS "a primary pain point."
- **The ordering fix doesn't require it.** WebSocket alone solves the core problem (bidirectional, ordered, binary-native). WebTransport adds performance benefits on top, but those aren't blocking us today.

**Pragmatic strategy: WebSocket first, WebTransport later.**

Ship WebSocket as the transport — it solves ordering, drops base64 overhead, works everywhere, trivial infra (axum built-in, ALB-compatible, mkcert for local dev). Design the `TransportProvider` abstraction from day one so WebTransport slots in later without touching sync logic. When the infra story stabilizes and we have real latency data showing WebSocket isn't enough, add WebTransport as an upgrade path.

This gives us:
1. **Immediate fix** for the ordering bugs blocking tests today
2. **Clean architecture** that doesn't lock out WebTransport
3. **Zero infra migration risk** — stays on ALB, no cert management changes
4. **A future selling point** — "Jazz supports WebTransport" when we're ready, not before

### Why WebSocket solves the problem

WebSocket provides the same bidirectional ordered stream semantics we need:

- **Single connection, ordered delivery** — writes go out in enqueue order, arrive in that order. Done.
- **Binary-native** — WebSocket binary frames, no base64, no UTF-8 constraint.
- **Universal support** — every browser, Node.js, React Native, Rust (`tokio-tungstenite`), every proxy/CDN/load balancer.
- **Simple infra** — works behind ALB or NLB, standard TLS termination at the load balancer, `mkcert` for local dev.

The key property — one ordered bidirectional channel replacing SSE + HTTP POST — holds for WebSocket exactly as it would for WebTransport.

### WebTransport as a future upgrade

The `TransportProvider` abstraction makes WebTransport a drop-in upgrade when the time is right. The upgrade path:

1. Add `WebTransportProvider` alongside `WebSocketProvider`
2. Client negotiation: try WebTransport, fall back to WebSocket
3. Server adds QUIC listener alongside TCP
4. Infra: NLB `TCP_QUIC` mode, server-side TLS

No sync logic changes, no wire format changes, no test changes. Pure transport swap.

### Transport architecture

```
                      ┌─────────────────────────┐
                      │    TransportProvider     │
                      │  (trait / interface)      │
                      │                           │
                      │  connect()                │
                      │  send(frame)              │
                      │  recv() -> frame          │
                      │  close()                  │
                      └────────┬─────────────────┘
                               │
                  ┌────────────┴────────────────┐
                  │                              │
         ┌───────▼──────┐              ┌────────▼───────┐
         │ WebTransport  │              │   WebSocket    │
         │ Provider      │              │   Provider     │
         └───────────────┘              └────────────────┘
```

**Negotiation flow:**

```
Client                                  Server
  │                                       │
  │─── Try WebTransport ─────────────────>│
  │    (HTTP/3 CONNECT)                   │
  │                                       │
  │<── WebTransport session ──────────────│  (success path)
  │    bidirectional stream opened        │
  │                                       │
  ├── OR timeout / fail ──────────────────┤
  │                                       │
  │─── Fallback: WebSocket ──────────────>│
  │    (HTTP/1.1 Upgrade)                 │
  │                                       │
  │<── WebSocket connection ──────────────│  (fallback path)
  │                                       │
```

**Single bidirectional stream for sync (both transports):**

Both transports use the same binary framing protocol Jazz already has (4-byte length-prefixed frames with JSON payloads). The existing `SyncBatchRequest` / `SyncBatchResponse` / `ServerEvent` types remain unchanged — only the transport underneath changes.

```
Client                             Server
  │                                  │
  │── frame: SyncBatchRequest ──────>│  (ordered, same stream)
  │── frame: SyncBatchRequest ──────>│  (guaranteed after previous)
  │                                  │
  │<── frame: ServerEvent ───────────│  (Connected, SyncUpdate, etc.)
  │<── frame: ServerEvent ───────────│
  │                                  │
```

The critical change: instead of SSE (read) + POST (write) as two separate channels, **one bidirectional stream carries both directions**. Writes go out in the order they're enqueued. The server processes them in arrival order. Done.

### Server side (Rust)

The Rust server (`crates/jazz-tools/src/routes.rs`) currently exposes `GET /events` (SSE) and `POST /sync`. Both get **deleted** and replaced with:

- `/.well-known/webtransport` — WebTransport session endpoint
- `/ws` — WebSocket upgrade endpoint (via `axum`'s built-in WebSocket support)

Rust WebTransport libraries to evaluate:
- [`wtransport`](https://github.com/BiagioFesta/wtransport) — pure Rust, built on `quinn`, async-native
- [`web-transport`](https://github.com/moq-dev/web-transport) — from the moq-dev project, also `quinn`-based

Both feed into the same `SyncManager` pipeline. The broadcast channel currently used for SSE events gets replaced with per-connection channels that work for either transport.

### Client side (TypeScript)

`packages/jazz-tools/src/runtime/sync-transport.ts` currently manages:
- `StreamController` for SSE
- `sendSyncPayloadBatch()` for POST

New `TransportProvider` interface with implementations for WebTransport and WebSocket. The `StreamController` reconnection logic (exponential backoff, jitter) stays — it just reconnects the appropriate transport.

### Wire format

Both WebTransport streams and WebSocket carry raw binary natively — no base64 encoding, no UTF-8 constraint. Keep the existing binary framing (4-byte length prefix + payload) but the transport no longer forces text encoding. This:

1. **Eliminates the ~33% base64 overhead** on the server→client path immediately.
2. **Unblocks future binary payload formats** — switching from JSON payloads to MessagePack, FlatBuffers, or a custom binary encoding becomes a wire-format-only change, not a transport change. (Out of scope for v1, but the door is open.)

Future optimization: WebTransport datagrams for heartbeats (currently 30s SSE pings), freeing the main stream from keep-alive noise.

### Cloud architecture (AWS EC2/ECS)

Current Jazz infra runs on EC2/ECS. WebTransport requires QUIC (UDP), which changes the load balancer and TLS story.

#### Load balancer: NLB with TCP_QUIC

- **ALB does not support HTTP/3/QUIC** — cannot forward WebTransport traffic.
- **NLB supports QUIC passthrough** ([announced Nov 2025](https://aws.amazon.com/about-aws/whats-new/2025/11/aws-network-load-balancer-quic-passthrough-mode/)). `TCP_QUIC` listener forwards both UDP 443 (QUIC) and TCP 443 (WebSocket) to the same target group. No extra cost, all commercial regions.
- **Session stickiness** via QUIC Connection ID — survives network changes (mobile WiFi→cellular), NLB routes by CID not 5-tuple.
- **ECS integration**: target group with `ip` target type (`awsvpc` network mode), TCP_QUIC protocol. ECS supports [linear and canary deployments with NLB](https://aws.amazon.com/about-aws/whats-new/2026/02/amazon-ecs-nlb-linear-canary-deployments/) (Feb 2026).
- **Constraint**: NLB with QUIC listeners must have **no security groups** on the LB itself (passthrough requirement). Security groups go on the ECS tasks instead.

```
                         Internet
                            │
                   ┌────────▼────────┐
                   │       NLB       │
                   │  TCP_QUIC mode  │
                   │  (no sec groups)│
                   │                 │
                   │  UDP 443 (QUIC) │── WebTransport
                   │  TCP 443 (TLS)  │── WebSocket
                   └────────┬────────┘
                            │
              ┌─────────────┴─────────────┐
              │    ECS Service (Jazz)      │
              │    (sec groups: UDP+TCP)   │
              │                            │
              │  QUIC listener ── wtransport│
              │  TCP listener  ── axum(WS) │
              │  TCP :80       ── /health  │
              └────────────────────────────┘
```

#### TLS: server-side termination

NLB passthrough means TLS terminates in the Jazz server process, not at the load balancer. This is fundamentally different from ALB-terminated TLS.

**Certificate options for ECS:**

| Approach | Pros | Cons |
|----------|------|------|
| **Let's Encrypt + certbot** | Free, automated, trusted by all browsers | Needs ACME challenge solver (DNS-01 via Route53 API, or HTTP-01 on port 80). Renewal runs inside container or as a sidecar. |
| **AWS Secrets Manager** | Centralized, IAM-controlled access, easy to mount into ECS tasks | Manual rotation (or Lambda-based auto-renewal). Extra cost per secret. |
| **ACM (Certificate Manager)** | Free, auto-renewing | **Cannot export private keys** — only works with ALB/CloudFront/API Gateway. **Not usable** for server-side TLS. |

**Recommended path**: Let's Encrypt with DNS-01 challenge via Route53. The Jazz server (or an init container) runs certbot, stores certs on an EFS volume or tmpfs, and reloads on renewal. `wtransport` supports certificate hot-reload.

#### Health checks

NLB cannot do QUIC health checks — it probes TCP only. The Jazz server exposes a `/health` TCP endpoint on a separate port (e.g. 80). The QUIC listener health is inferred from the TCP side being alive.

#### What you lose vs ALB

NLB is L4 — no path-based routing, no WAF, no request-level access logs, no HTTP header manipulation. If other services currently sit behind ALB, keep ALB for those and add a dedicated NLB for Jazz sync traffic.

#### Local development: Chrome + TLS gotcha

Chrome [does not support custom root CAs for WebTransport](https://moq.dev/blog/tls-and-quic/) despite supporting them for HTTPS. This means `mkcert`-style local dev doesn't work for WebTransport. Options:
- **Self-signed certs with fingerprint verification** — WebTransport API supports `serverCertificateHashes`, but certs must be <2 weeks validity.
- **Chrome flags** — `--ignore-certificate-errors-spki-list` for development.
- **Fallback to WebSocket locally** — simplest DX. WebSocket works fine with mkcert. Use WebTransport only in staging/production.

The recommended local dev story: **WebSocket by default, WebTransport opt-in via env flag** when testing QUIC-specific behavior.

### Rust client

`crates/jazz-tools/src/transport.rs` (used for server-to-server and tests) switches from `reqwest` HTTP to [`wtransport`](https://github.com/BiagioFesta/wtransport), which supports both client and server side. Same library on both ends — shared types, consistent behavior, single dependency for QUIC/WebTransport. WebSocket fallback via `tokio-tungstenite`. Tests get ordered delivery for free.

## Rabbit Holes

1. **HTTP/3 server infrastructure** — Running WebTransport requires an HTTP/3-capable server. `axum` doesn't natively support HTTP/3. Two Rust libraries to evaluate: [`wtransport`](https://github.com/BiagioFesta/wtransport) and [`web-transport`](https://github.com/moq-dev/web-transport), both built on `quinn`. Spike early to pick one and validate it runs alongside the existing axum server (dual-stack HTTP/1.1+HTTP/3, or separate listeners).

2. **Certificate requirements** — WebTransport over HTTP/3 requires valid TLS certificates. Local development needs self-signed certs with browser trust configuration, or a `localhost` exception. This could hurt DX if not handled carefully.

3. **Proxy/CDN compatibility** — Many CDNs and reverse proxies don't support HTTP/3 yet or strip WebTransport. Cloudflare supports it, but AWS ALB doesn't. The fallback chain must be robust.

4. **Multiple streams vs single stream** — WebTransport can open many streams per session. We could use separate streams for control (subscriptions) vs data (object updates). This adds complexity but could improve flow control. Start with single bidirectional stream, optimize later.

5. **Backpressure** — WebSocket and WebTransport both support flow control, but the semantics differ. WebTransport uses QUIC flow control (stream and connection level). WebSocket relies on TCP backpressure. The `TransportProvider` abstraction needs to expose backpressure signals uniformly.

6. **Worker bridge** — The main-thread ↔ worker communication (`postMessage`) is a separate transport. It doesn't need WebTransport, but the `TransportProvider` trait should be general enough to eventually unify it (as noted in the reliability idea doc).

7. **React Native / Node.js** — Neither has native WebTransport. The fallback to WebSocket must work well in these environments, not just as a degraded path.

8. **ALB → NLB migration** — NLB has no WAF, no path routing, no HTTP-level logging. If other services share the current ALB, the migration is "add NLB for Jazz sync" not "replace ALB". Terraform/CDK changes needed. Evaluate blast radius early.

9. **TLS certificate lifecycle in ECS** — Let's Encrypt certs expire every 90 days. Renewal must be automated (certbot sidecar, Lambda, or init container). Cert hot-reload in `wtransport` needs validation — a server restart on renewal is acceptable as fallback but not ideal for long-lived connections.

10. **NLB no-security-group constraint** — QUIC listeners require the NLB to have no security groups. All network filtering moves to ECS task security groups and NACLs. Verify this doesn't conflict with existing VPC security posture.

## No-gos

1. **No custom protocol on raw UDP/QUIC** — We use WebTransport's HTTP/3 mapping, not a bespoke QUIC protocol. Standards compliance, browser support, and proxy traversal matter more than squeezing out protocol-level optimizations.

2. **No payload format changes in this project** — The binary framing stays, payloads remain JSON. The base64 encoding overhead disappears naturally (binary-native transport), but switching to a compact binary payload format (MessagePack, etc.) is a separate effort. We're changing the *transport*, not the *serialization*.

3. **No multi-stream optimization in v1** — Start with one bidirectional stream per connection. Multi-stream (separate streams for subscriptions vs data) is a future optimization after the single-stream migration is stable.

4. **No backward compatibility with SSE+HTTP** — The legacy transport gets removed entirely. This is a prototype; we break backcompat freely. No migration path, no dual-stack period.

5. **No peer-to-peer WebTransport** — Server-mediated sync only. WebRTC DataChannel is a separate exploration for direct peer connectivity.

6. **No unreliable datagrams in v1** — Heartbeats and presence signals stay on the reliable stream initially. Datagram optimization comes after the core migration.

## Testing Strategy

Integration-first, realistic fixtures, human actor names.

**Ordering correctness (the primary goal):**
- Un-ignore `subscription_reflects_final_state_after_rapid_bulk_updates` — 500 rapid writes must arrive in order. This is the **north-star test**.
- Un-ignore `single_client_operations_reach_server_in_causal_order` — ownership transfer then write must land in causal order so policy enforcement is correct.
- New test: `concurrent_writers_maintain_per_client_order` — Alice and Bob both write rapidly; each client's writes arrive in their authored order (cross-client interleaving is fine).

**Transport fallback:**
- Test WebTransport → WebSocket fallback by simulating WebTransport connection failure.
- Test reconnection: kill connection mid-sync, verify ordered resume after reconnect.

**Parity:**
- All existing sync tests must pass identically on both WebTransport and WebSocket transports. Parameterize the test suite over transport type.

**Performance:**
- Benchmark: latency and throughput comparison between SSE+HTTP, WebSocket, and WebTransport for the 500-rapid-update scenario.
- Measure reconnection time after network interruption.

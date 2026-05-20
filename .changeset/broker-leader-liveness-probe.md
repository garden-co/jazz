---
"jazz-tools": patch
---

Add a leader-liveness probe to the SharedWorker broker so a stale `leaderPort` left behind by a previous app session (most commonly: a dev-server restart where Chrome reuses the existing SharedWorker while the prior leader tab is gone) no longer strands new followers with `Worker init timeout`.

Before minting a follower-tab `MessageChannel`, the broker now posts `leader-ping {seq}` to the cached `leaderPort` and waits up to 250 ms for a matching `leader-pong {seq}`. The supervisor responds synchronously while it holds the Web Lock. If no pong arrives, the broker evicts the stale leader, broadcasts `leader-changed`, and replies `no-leader` to the requester — letting the next `claim-leader` (the freshly booted tab that just won the lock) take over without waiting for a 15 s timeout.

Probe is on-demand only (no periodic heartbeat) and adds a sub-millisecond delay to legitimate follower handshakes against a live leader.

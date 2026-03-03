---
"jazz-tools": patch
---

Overhauled durability APIs to use a single `DurabilityTier` model across reads and writes.

- Reads now take `{ tier, localUpdates }`, where `localUpdates` defaults to `"immediate"` so local writes are reflected right away even when waiting for a more remote durability tier.
- Writes now use the base methods with optional `{ tier }` and environment-aware defaults (`"worker"` for clients, `"edge"` for backend contexts).
- Renamed the top tier from `"core"` to `"global"` for clearer semantics.
- Added multi-tier node identity support so single-node deployments (like CLI and cloud-server today) can acknowledge both `"edge"` and `"global"`.

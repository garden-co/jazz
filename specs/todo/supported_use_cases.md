# Supported Use Cases — TODO

Explicit scope definition: what Jazz is (and isn't) for at launch.

## In Scope

Jazz targets **collaborative, local-first SaaS applications**:

- Project management tools (Kanban, task trackers)
- Note-taking and document editing (collaborative, offline-capable)
- Chat / messaging applications
- CRM and internal tools
- Creative tools (design, music, drawing)
- Data collection / field apps (offline-first is the killer feature)

### What these have in common

- Multiple users reading/writing shared data
- Tolerance for eventual consistency (no global transactions needed)
- Benefit from offline access and instant local writes
- Modest per-user data volumes (MB–GB, not TB)
- UI-driven — users interact through a client app, not batch pipelines

## Not In Scope (Yet)

- **E-commerce / booking**: requires global transactions, SSR, inventory locks
- **Analytics / data warehousing**: Jazz is OLTP-shaped, not OLAP
- **IoT / high-frequency ingestion**: not optimized for append-heavy write patterns
- **Static sites / content management**: possible but not the sweet spot

These aren't permanent exclusions — they're scoping decisions for launch. Some (e.g., e-commerce) may become viable as features like transactions mature.

## Why This Matters

Explicit scoping lets us:

- Focus example apps and docs on the sweet spot
- Say "not yet" to feature requests that pull toward general-purpose DB territory
- Set honest expectations (avoid the "SQL database" comparison trap)
- Prioritize features that serve the core audience (collaboration, offline, sync)

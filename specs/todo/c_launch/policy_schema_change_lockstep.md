# Policy vs Schema Change Timing — TODO (Launch)

Clarify whether policy updates are applied in lockstep with schema changes, or if they can apply at different times.

## Overview

When a schema evolves (new table, renamed column, added policy), do policies take effect:

- **Atomically with the schema change** — old policy applies until the new schema is fully deployed, then new policy kicks in
- **Independently** — policies can be updated without a schema change, and vice versa

This matters for:

- Rolling deployments where some nodes have the new schema and some don't
- Policy hotfixes that shouldn't require a full schema migration
- Ordering guarantees when a schema change adds a column that a policy references

## Open Questions

- Are policies part of the catalogue (same versioning as schemas) or separate?
- Can a policy reference a column that doesn't exist yet (forward reference)?
- What happens if a policy update arrives before the schema it depends on?

# Schema Push Authorization — TODO (First Week)

**Priority: High — security gap, must fix before deploying multi-tenant server.**

Currently, any client can push schema/lens objects to the server via catalogue sync. This allows:

- Malicious clients injecting schemas with altered column types
- Attackers adding tables/columns to exfiltrate data
- Draft lenses pushed to cause server errors

## Required Fix

Schema/lens pushes should require an **app admin token** separate from the normal session token:

- Issued to developers/operators, not end users
- Required for `type=catalogue_schema` and `type=catalogue_lens` objects
- Validated server-side before accepting catalogue updates

Until implemented, treat schema sync as trusted-network-only.

> `crates/groove/src/schema_manager/manager.rs` — catalogue processing has no auth check
> See also: `CatalogueWriteDenied` error exists for User role, but Admin/Peer bypass

Remaining schema_manager items: see `../b_mvp/schema_manager.md` (MVP) and later sections within.

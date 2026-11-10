# Catalogue identity operand v1

`CatalogueTableIdentityV1` is a reserved policy value. Its public representation
is `SessionRef(["__jazz_table_identity_v1", table])`; its lowered query operand is
`Claim("\0jazz-table-identity-v1:" + table)`. The existing variants and field
layouts are unchanged. A byte fixture in `catalogue_identity_operands.rs` pins
both forms independently.

The SQL compiler resolves this value through accepted catalogue metadata for the
policy schema. It never reads caller claims for this namespace. A schema without
accepted metadata, a malformed path or an unknown table produces a typed query
error. Missing metadata cannot become a true null check. Schema views use their
governing policy schema, so a rename preserves the physical table identity.

An older runtime cannot evaluate this newly supported policy value. Publish such
policies only to runtimes that support the operand; this change supplies no
fallback to claims or compatibility bridge. Existing policies, schema encodings,
row/chunk storage and semantic wire versions keep their current layouts.

# SQLite authority catalogue storage v1

An authority can install its authored `CatalogueSnapshot` and resolve accepted
physical table and column UUIDs. Opening a schema or registering a typed view does
not publish a catalogue or allocate accepted UUIDs. A standalone authority can
explicitly initialise its genesis once; reopening it retains those identities.

These additive tables live in a Jazz SQLite format-1 database:

| Table                        | Value encoding                     | Key                         |
| ---------------------------- | ---------------------------------- | --------------------------- |
| `_jazz_catalogue_head_v1`    | `jazz.sqlite.catalogue-head.v1`    | slot 1                      |
| `_jazz_catalogue_schema_v1`  | `jazz.sqlite.catalogue-schema.v1`  | raw schema UUID             |
| `_jazz_catalogue_table_v1`   | `jazz.sqlite.catalogue-table.v1`   | raw schema UUID, table name |
| `_jazz_policy_descriptor_v1` | `jazz.sqlite.policy-descriptor.v1` | slot 1                      |

The v1 encodings use postcard fields in the following fixed order. Counters and
lengths are unsigned LEB128; UUIDs are a length of 16 followed by the raw UUID;
strings are a byte length followed by UTF-8. Maps are ordered by their string key.

- Head: 32 raw namespace bytes, publication sequence, active revision, current
  schema UUID. A namespace must be nonzero and the schema UUID must be non-nil.
- Schema record: `SchemaVersion` (schema UUID, length-prefixed canonical public
  schema JSON), followed by the physical identity manifest. Its schema has no
  policies. The manifest is a map from table name to table record.
- Table record: table UUID, then a map from column name to column UUID and the
  enum-variant map (structural occurrence path to an ordered vector of variant
  UUIDs). UUIDs must be non-nil and
  unique across the manifest.
- Policy descriptor: head followed by `SchemaVersion`, including its policies.
  It must have the accepted current schema's structure and exact accepted head.

The namespace is BLAKE3 derive-key mode with context
`jazz.sqlite.catalogue-namespace.v1`, over postcard fields `(genesis schema UUID,
genesis physical identity manifest)`. Policies do not affect that namespace.
The fixture's fixed namespace digest is `2915a6f28499cdc57591b4cf3b110e902448fabdedb3066c364fe031739470b0`.

`sqlite_catalogue_storage_v1.json` specifies bytes independently: one `notes`
table, a `text` column, table UUID `11111111-1111-1111-1111-111111111111`, column
UUID `22222222-2222-2222-2222-222222222222`, no enum variants, publication sequence
0, active revision 128, and a policy allowing SELECT. The schema UUID is
`66db23d1-af6f-5dd0-b955-450ee172c3cd`. Byte tests compare the actual persisted
values with this fixture, as well as checking decoded fields. Malformed and
suffixed records fail with a storage error; installation never repairs them.

Publication validates the complete lineage before writing. One savepoint admits
all immutable descriptors, the monotone head and its mutable policy descriptor.
A rejection preserves every previously accepted record. Changing policies at the
same revision is rejected; advancing the revision may change policies without
changing structural identities. A publication cannot change the namespace,
rebind an accepted identity, move either counter backwards or omit accepted
history. A rename retains the original physical IDs; a fixed old schema view
retains its original names.

## Compatibility

This extraction changes no existing row, history, pending-write, chunk or sync
encoding. Existing format-1 files acquire the four empty metadata tables on open;
existing rows remain readable. No sync messages are added. The historical wire
and storage corpora stay unchanged.

Older format-1 engines ignore and preserve these `_jazz_` tables. They cannot
publish or consume this catalogue API. They must not serve as catalogue
publishers: schema changes made by such an engine do not update the accepted
catalogue. Applications must publish a complete validated snapshot before relying
on the identities after an older writer changed the schema. Publication itself
does not activate the schema or apply policies to query execution; those existing
operations remain explicit.

This is a storage foundation, not a negotiated network feature. It introduces no
remote coverage guarantee, delegated read, compiler identity expression, private
owner mode or encryption behaviour.

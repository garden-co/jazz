# Shared native publication ABI before the v1 freeze (#2548, #2558, #2461)

Both alternatives use the same ordered postcard field-name enum:

| Tag | Payload, in order                               | Native visibility                       |
| --- | ----------------------------------------------- | --------------------------------------- |
| 0   | `StoredColumn { id: u64, output_name: string }` | application cell                        |
| 1   | `ResultField { name: string }`                  | application result or public provenance |
| 2   | `HiddenMetadata { name: string }`               | suppressed                              |

Each field-name envelope is followed by the existing recursive `ValueType`.
Stored IDs come from the selected schema's physical catalogue, never a parsed
carrier or compiler slot. The contained recursive descriptor encoding, physical
index namespace, persisted record bytes, row keys and hash inputs are unchanged.
This change is a native host publication ABI uplift, not a durable storage rewrite.

Producers own visibility. Physical current-row constructors identify the metadata
slots they construct and preserve the four public magic provenance fields.
Terminal layouts initialize non-public slots as hidden and explicitly mark their
public fields. Schema-aware materialization supplies catalogue IDs and carries
those roles through current rows, projection, cached records and root resets.
Logical result constructors remain public even without an output-name override.

The internal roles distinguish application cells, public provenance and hidden
metadata. Public `$createdAt`, `$createdBy`, `$updatedAt` and `$updatedBy` serialize
as tag 1 but do not participate in application-cell subscription equality.
Consumers suppress only tag 2. A visible `COUNT(*) AS schema_version` is tag 1;
a hidden metadata field of the same name is tag 2. Their names and value types can
be identical without changing visibility or losing the user result.

The shared golden update changes exactly five metadata tag bytes from 1 to 2:
relation snapshot case 1 at offsets 9, 123 and 190, and subscription delta case 0
at offsets 8 and 80 (zero-based). All other golden bytes remain unchanged.
This component contract alone does not establish bidirectional database reopen
or peer VersionRecord compatibility with the typed implementation. Those remain
separate freeze requirements.

# Address policy nodes: encodings and compatibility

Status: draft for review by the jazz-core owners, for the policy nodes that
let a row reach another row by a two-column address (a table identity and a
row ID): `PolicyExpr::InheritsFrom`, `PolicyExpr::InsertedByThisTransaction`,
`PolicyValue::TableIdentity` and its lowered form `Operand::TableIdentity`
(`crates/jazz/SPEC/E2EE_CORE_HOOKS.md`, "System tables, derived policies and
`InheritsFrom`"). Jazz derives policies with these
nodes for its system tables; an application writes only `table_id('T')`.

## 1. What is stored or sent

| Encoding                    | Where it lives                                                                                                                                  | Change                    |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------- |
| `PolicyExpr` (serde JSON)   | Permissions bundles (`jazz-bundle-v1`: canonical JSON, bundle ID over it); the public schema JSON a stored schema version keeps its policies in | Two additive variants     |
| `PolicyValue` (serde JSON)  | Inside `PolicyExpr`, as above                                                                                                                   | One additive variant      |
| `Operand` (postcard, serde) | Part of `Query`, which the sync wire carries and canonical request keys hash                                                                    | One variant, never sent   |
| Lowered policies (`Query`)  | In memory only (`RuntimeSchema`, rebuilt from the public schema on decode); a Debug digest of read policies feeds the activation record         | New forms in new policies |

## 2. The variants, named and versioned

The bundle definition encoding stays `jazz-bundle-v1`: no existing variant
changes, and a bundle without the new nodes is byte for byte what it was,
with the same bundle ID. The new variants are additive extensions of it.

- `{"type":"InheritsFrom","operation":…,"table_column":…,"id_column":…,"tables":[…]}`.
  `tables` holds table names within the bundle's schema; the authority
  resolves each to its catalogue identity when it compiles the policy. The
  `Update` operation of this node means both clauses of the candidate's
  update policy (`USING`, and `WITH CHECK` or `USING` without one); that
  meaning is part of the variant.
- `{"type":"InsertedByThisTransaction"}`: no fields.
- `{"type":"TableIdentity","table":…}` as a `PolicyValue`, written
  `table_id('T')` in `jazz.permissions.json`.
- `Operand::TableIdentity(name)`: the lowering of `table_id('T')`. Postcard
  appends it as variant index 4 of `Operand`, after `Literal`; the indexes of
  `Column`, `Param`, `Claim` and `Literal` are unchanged. Its canonical
  request key is the tag byte `t` followed by the length-prefixed name.

## 3. Byte-level fixtures

- `address_policy_json_is_pinned`
  (`crates/jazz/layers/model/src/model/public_api/policy.rs`): the serde
  JSON of each new variant, read back exactly, and the missing-field
  refusals.
- `crates/jazz/tests/address_policy_encoding.rs`: the canonical bundle form
  (sorted keys, no whitespace) and its SHA-256 for `IF(Select)` with
  `tables = ["projects"]` (116 bytes,
  `c2aead25ca360dafc60aed57ce264c42ec8ae17e8f52ee70e5a01dc17c05bd4b`), for
  `InsertedByThisTransaction` and for `table_id('projects')`; a bundle ID
  that holds none of them is unchanged; and a query carrying a table
  identity is refused.
- `a_table_identity_operand_has_its_own_canonical_key`
  (`crates/jazz/layers/model/src/query/canonical_request.rs`): the operand's
  canonical key, apart from every operand naming the same text.

## 4. Compatibility

- **Sync wire.** Unchanged. Catalogue sync carries schemas without
  permissions (`without_permissions`), so no peer decodes a bundle or a
  stored policy from the wire. `Operand::TableIdentity` is produced only by
  the lowering of policies, which is never sent; a query that carries it
  (from an application or a peer) is refused by the query compiler ("a table
  identity is not supported in a query").
- **Row storage.** Unchanged: the nodes are policy, not data.
- **Stored permissions.** A server binary that predates these variants fails
  to decode a bundle or stored schema version holding one (`unknown
variant`) and refuses it, so a downgrade fails closed rather than dropping
  a policy. Bundles and schema versions without them decode as before.
- **Activation digest.** The digest recorded when a schema becomes current
  hashes the Debug form of the lowered read policies. Only a policy that
  uses a new node prints differently; existing policies keep their digest.
- **Replicas.** A replica that compiles a policy with a table identity binds
  it only once its catalogue mirror has confirmed the schema version's names
  (as `Db::table_identity` answers); until then the identity is no value, a
  comparison with it holds for no row, and the policy grants nothing through
  it. Policies are compiled per check and per read, so the identity binds as
  soon as the names are confirmed. The authority always knows it.
- **Clients.** A client never evaluates write policies. `table_id('T')` is
  read by the permissions reader of this version and later; an older reader
  does not accept it, so a permissions file using it needs this version's
  tools.

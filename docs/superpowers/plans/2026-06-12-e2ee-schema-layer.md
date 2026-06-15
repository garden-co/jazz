# E2EE Schema Layer Implementation Plan (Plan 2 of 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Schema-level E2EE support: `encryption_space` tables and `encrypted_with` columns in the Rust schema model, auto-generated `<table>$keys` companion tables with v1 policies, schema validation, and the TS DSL (`.encryptionSpace()` / `.encrypted(ref)`) that emits the same wasm-schema JSON.

**Architecture:** New fields on `ColumnDescriptor` / `TableSchema` (serde- and hash-backward-compatible: absent fields hash exactly as before). A pure normalization function injects `$keys` companion tables and excludes encrypted columns from indexing; it runs in Rust's `SchemaBuilder::build()` and is mirrored in TS `schemaToWasm` so both producers emit identical schemas (the schema hash is computed only in Rust — TS delegates to the WASM runtime — so byte-identical JSON means identical hashes). Validation is exposed as `validate_e2ee_schema` in Rust and enforced at `defineApp` time in TS. Spec: `docs/superpowers/specs/2026-06-12-e2ee-shared-keys-design.md` (§2, §3).

**Tech Stack:** Rust (`crates/jazz-tools/src/query_manager/types/`), TS (`packages/jazz-tools/src/`: `dsl.ts`, `typed-app.ts`, `schema.ts`, `codegen/schema-reader.ts`, `drivers/types.ts`).

**Plan series:** 1 = crypto core (done). **2 = this plan.** 3 = runtime integration. 4 = TS bindings + E2E.

**Branch:** continue on `guido/e2ee-crypto-core`.

**Normative formats (from spec §3):**

`projects$keys` companion table (column order is normative — it feeds the schema hash):

| #   | column                 | type                   | notes        |
| --- | ---------------------- | ---------------------- | ------------ |
| 1   | `space_id`             | Uuid, FK → space table | non-nullable |
| 2   | `key_id`               | Uuid                   | non-nullable |
| 3   | `recipient_user_id`    | Uuid                   | non-nullable |
| 4   | `recipient_public_key` | Text                   | non-nullable |
| 5   | `sealed_key`           | Bytea                  | non-nullable |

v1 policies on `$keys`: select = `True` (world-readable); insert = `SessionIsNotNull(["user_id"])` (any authenticated user); **update = absent** (enforcing runtimes deny missing clauses — key rows are immutable); delete = `SessionIsNotNull(["user_id"])`.

> **Spec deviation (document, don't silently diverge):** the spec says delete is "own rows plus space creator", but creator identity needs the `created_by` permissions work that hasn't landed. v1 uses open authenticated delete (same trust posture as open insert; a delete is a recoverable annoyance — re-share heals it). Step 6.4 updates the spec to record this.

**Conventions:** black-box tests; no AI attribution in commits; commands run from repo root.

---

### Task 1: Rust schema fields (`encrypted_with`, `encryption_space`)

**Files:**

- Modify: `crates/jazz-tools/src/query_manager/types/schema.rs` (`ColumnDescriptor` ~line 225, `TableSchema` ~line 380, `TableSchemaBuilder` ~line 447)
- Modify: `crates/jazz-tools/src/query_manager/types/branch.rs` (`SchemaHash::compute` ~line 76, `hash_column_descriptor` ~line 143)
- Test: `crates/jazz-tools/tests/e2ee_schema.rs` (new)

- [ ] **Step 1.1: Write the failing tests**

Create `crates/jazz-tools/tests/e2ee_schema.rs`:

```rust
//! Black-box tests for E2EE schema support (public API only).

use jazz_tools::query_manager::types::{
    ColumnType, SchemaBuilder, TableSchemaBuilder,
};

fn base_schema() -> SchemaBuilder {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("projects")
                .column("name", ColumnType::Text)
                .encryption_space(),
        )
        .table(
            TableSchemaBuilder::new("todos")
                .encrypted_column("title", ColumnType::Text, "projectId")
                .column("done", ColumnType::Boolean)
                .fk_column("projectId", "projects"),
        )
}

#[test]
fn builder_sets_e2ee_fields() {
    let schema = base_schema().build();
    let projects = &schema[&"projects".into()];
    assert!(projects.encryption_space);

    let todos = &schema[&"todos".into()];
    assert!(!todos.encryption_space);
    let title = todos
        .columns
        .columns
        .iter()
        .find(|c| c.name.as_str() == "title")
        .unwrap();
    assert_eq!(
        title.encrypted_with.as_ref().map(|c| c.as_str()),
        Some("projectId")
    );
}

#[test]
fn e2ee_fields_serialize_only_when_set() {
    let schema = base_schema().build();
    let json = serde_json::to_value(&schema).unwrap();
    assert_eq!(json["projects"]["encryption_space"], true);
    assert!(json["todos"].get("encryption_space").is_none());

    let todos_cols = json["todos"]["columns"].as_array().unwrap();
    let title = todos_cols
        .iter()
        .find(|c| c["name"] == "title")
        .unwrap();
    assert_eq!(title["encrypted_with"], "projectId");
    let done = todos_cols.iter().find(|c| c["name"] == "done").unwrap();
    assert!(done.get("encrypted_with").is_none());
}

#[test]
fn pre_e2ee_schema_json_still_deserializes() {
    let json = r#"{
        "plain": {
            "columns": [
                {"name": "title", "column_type": {"type": "Text"}, "nullable": false}
            ]
        }
    }"#;
    let schema: jazz_tools::query_manager::types::Schema =
        serde_json::from_str(json).unwrap();
    let plain = &schema[&"plain".into()];
    assert!(!plain.encryption_space);
    assert!(plain.columns.columns[0].encrypted_with.is_none());
}

#[test]
fn e2ee_markers_change_the_schema_hash() {
    let without = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("projects").column("name", ColumnType::Text))
        .hash();
    let with = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("projects")
                .column("name", ColumnType::Text)
                .encryption_space(),
        )
        .hash();
    assert_ne!(without, with);
}
```

- [ ] **Step 1.2: Run tests to verify they fail**

Run: `cargo test -p jazz-tools --test e2ee_schema`
Expected: FAIL to compile (`encryption_space` / `encrypted_column` not found).

- [ ] **Step 1.3: Add the fields and builder methods**

In `crates/jazz-tools/src/query_manager/types/schema.rs`:

To `ColumnDescriptor` (after the `merge_strategy` field):

```rust
    /// E2EE: name of the sibling ref column that scopes this column's
    /// encryption to a space row. `Some` marks the column as encrypted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encrypted_with: Option<ColumnName>,
```

To `TableSchema` (after the `policies` field):

```rust
    /// E2EE: rows of this table own a shared encryption key.
    #[serde(default, skip_serializing_if = "encryption_space_is_unset")]
    pub encryption_space: bool,
```

Near `table_policies_are_default`:

```rust
fn encryption_space_is_unset(value: &bool) -> bool {
    !*value
}
```

To `TableSchemaBuilder` (struct gains `encryption_space: bool`, initialized `false` in `new`); add methods alongside `column`:

```rust
    /// Mark rows of this table as owners of a shared E2EE key (spec §3).
    pub fn encryption_space(mut self) -> Self {
        self.encryption_space = true;
        self
    }

    /// Add a non-nullable encrypted column scoped to the space referenced by
    /// the sibling ref column `space_ref`.
    pub fn encrypted_column(
        mut self,
        name: &str,
        column_type: ColumnType,
        space_ref: &str,
    ) -> Self {
        let mut descriptor = ColumnDescriptor::new(name, column_type);
        descriptor.encrypted_with = Some(space_ref.into());
        self.columns.push(descriptor);
        self
    }
```

- [ ] **Step 1.4: Fix construction sites compiler-guided**

Run: `cargo check -p jazz-tools 2>&1 | head -40`
Every error is a struct-literal construction of `ColumnDescriptor` (add `encrypted_with: None`) or `TableSchema` (add `encryption_space: false`), including `ColumnDescriptor::new`, `TableSchema::new`, `TableSchema::with_policies`, and `TableSchemaBuilder::build_named` (which must thread `encryption_space: self.encryption_space` into the built `TableSchema`). Repeat until `cargo check -p jazz-tools` passes.

- [ ] **Step 1.5: Extend the schema hash (backward-compatibly)**

In `crates/jazz-tools/src/query_manager/types/branch.rs`, append to `hash_column_descriptor` (after the `merge_strategy` block):

```rust
    // E2EE marker. Absent must hash exactly like pre-E2EE schemas.
    if let Some(ref space_ref) = col.encrypted_with {
        hasher.update(&[2]);
        hasher.update(space_ref.as_str().as_bytes());
        hasher.update(&[0]);
    }
```

In `SchemaHash::compute`, after the `indexed_columns` block inside the table loop:

```rust
            // E2EE marker. Absent must hash exactly like pre-E2EE schemas.
            if table_schema.encryption_space {
                hasher.update(&[2]);
            }
```

- [ ] **Step 1.6: Run tests to verify they pass**

Run: `cargo test -p jazz-tools --test e2ee_schema`
Expected: PASS, 4 tests.
Then run: `cargo test -p jazz-tools` — expected: all existing tests still pass (hash stability for schemas without E2EE fields is implicitly covered by every existing schema-hash test).

- [ ] **Step 1.7: Commit**

```bash
git add crates/jazz-tools/src/query_manager/types/schema.rs crates/jazz-tools/src/query_manager/types/branch.rs crates/jazz-tools/tests/e2ee_schema.rs
git commit -m "feat(jazz-tools): encryption_space and encrypted_with schema fields"
```

---

### Task 2: `$keys` companion-table generation (Rust)

**Files:**

- Create: `crates/jazz-tools/src/query_manager/types/e2ee_schema.rs`
- Modify: `crates/jazz-tools/src/query_manager/types/mod.rs` (add `pub mod e2ee_schema;` and re-export, matching how sibling modules are wired)
- Modify: `crates/jazz-tools/src/query_manager/types/schema.rs` (`SchemaBuilder::build` ~line 584)
- Test: `crates/jazz-tools/tests/e2ee_schema.rs` (append)

- [ ] **Step 2.1: Write the failing tests**

Append to `crates/jazz-tools/tests/e2ee_schema.rs`:

```rust
use jazz_tools::query_manager::types::e2ee_schema::{
    e2ee_keys_table_name, E2EE_KEYS_TABLE_SUFFIX,
};

#[test]
fn keys_table_name_appends_suffix() {
    assert_eq!(E2EE_KEYS_TABLE_SUFFIX, "$keys");
    assert_eq!(e2ee_keys_table_name("projects"), "projects$keys");
}

#[test]
fn build_expands_companion_keys_table() {
    let schema = base_schema().build();
    let keys = schema
        .get(&"projects$keys".into())
        .expect("companion table generated");

    let names: Vec<&str> = keys
        .columns
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(
        names,
        ["space_id", "key_id", "recipient_user_id", "recipient_public_key", "sealed_key"]
    );
    let space_id = &keys.columns.columns[0];
    assert_eq!(space_id.references.as_ref().map(|t| t.as_str()), Some("projects"));
    assert!(!space_id.nullable);
    assert!(!keys.encryption_space);

    // v1 policies: world-read, authenticated insert/delete, NO update clause.
    let policies = serde_json::to_value(&keys.policies).unwrap();
    assert_eq!(policies["select"]["using"]["type"], "True");
    assert_eq!(policies["insert"]["with_check"]["type"], "SessionIsNotNull");
    assert_eq!(policies["insert"]["with_check"]["path"][0], "user_id");
    assert_eq!(policies["delete"]["using"]["type"], "SessionIsNotNull");
    assert!(policies["update"]["using"].is_null());
    assert!(policies["update"]["with_check"].is_null());
}

#[test]
fn expansion_is_idempotent_and_hash_stable() {
    let a = base_schema().build();
    let b = base_schema().build();
    assert_eq!(
        jazz_tools::query_manager::types::SchemaHash::compute(&a),
        jazz_tools::query_manager::types::SchemaHash::compute(&b),
    );
    assert_eq!(a.len(), 3); // projects, todos, projects$keys
}

#[test]
fn tables_without_spaces_get_no_companion() {
    let schema = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("plain").column("name", ColumnType::Text))
        .build();
    assert_eq!(schema.len(), 1);
}
```

If `SchemaHash` is not re-exported from `jazz_tools::query_manager::types`, adjust the import to its actual public path (`jazz_tools::query_manager::types::branch::SchemaHash` or similar — check `types/mod.rs`).

- [ ] **Step 2.2: Run tests to verify they fail**

Run: `cargo test -p jazz-tools --test e2ee_schema`
Expected: FAIL to compile (`e2ee_schema` module not found).

- [ ] **Step 2.3: Implement the companion generator**

Create `crates/jazz-tools/src/query_manager/types/e2ee_schema.rs`:

```rust
//! E2EE schema normalization: `$keys` companion tables and validation.
//!
//! Mirrored by the TS side in `packages/jazz-tools/src/codegen/schema-reader.ts`;
//! the two must emit identical schemas (column order is normative — it feeds
//! the schema hash).

use crate::query_manager::policy::PolicyExpr;

use super::policy::{OperationPolicy, TablePolicies};
use super::schema::{
    ColumnType, Schema, TableName, TableSchema, TableSchemaBuilder,
};

/// Suffix of framework-generated sealed-key companion tables.
pub const E2EE_KEYS_TABLE_SUFFIX: &str = "$keys";

/// Companion table name for a space table.
pub fn e2ee_keys_table_name(space_table: &str) -> String {
    format!("{space_table}{E2EE_KEYS_TABLE_SUFFIX}")
}

fn session_authenticated() -> PolicyExpr {
    PolicyExpr::SessionIsNotNull {
        path: vec!["user_id".to_string()],
    }
}

/// Build the `$keys` companion table for a space table (spec §3).
pub fn e2ee_keys_table_schema(space_table: &str) -> (TableName, TableSchema) {
    let (name, mut table) = TableSchemaBuilder::new(&e2ee_keys_table_name(space_table))
        .fk_column("space_id", space_table)
        .column("key_id", ColumnType::Uuid)
        .column("recipient_user_id", ColumnType::Uuid)
        .column("recipient_public_key", ColumnType::Text)
        .column("sealed_key", ColumnType::Bytea)
        .build_named();

    let mut policies = TablePolicies::default();
    // World-readable: sealed copies are useless without the recipient's
    // private key, and open reads keep sync trivial.
    policies.select = OperationPolicy::using(PolicyExpr::True);
    // Open authenticated insert; bogus rows are ignored on unseal failure.
    // Tighten to members-only once created_by permissions land.
    policies.insert = OperationPolicy::with_check(session_authenticated());
    // No update clause: key rows are immutable (share = insert,
    // revoke = delete). Enforcing runtimes deny missing clauses.
    // v1 delete is open-authenticated; "own rows + creator" needs created_by.
    policies.delete = OperationPolicy::using(session_authenticated());
    table.policies = policies;

    (name, table)
}

/// Inject `$keys` companions for every `encryption_space` table.
/// Idempotent: existing entries are left untouched.
pub fn expand_e2ee_keys_tables(schema: &mut Schema) {
    let space_tables: Vec<String> = schema
        .iter()
        .filter(|(_, table)| table.encryption_space)
        .map(|(name, _)| name.as_str().to_string())
        .collect();
    for space_table in space_tables {
        let (name, table) = e2ee_keys_table_schema(&space_table);
        schema.entry(name).or_insert(table);
    }
}
```

Wire the module in `crates/jazz-tools/src/query_manager/types/mod.rs` the same way sibling modules (`schema`, `policy`, `branch`) are declared and re-exported.

In `schema.rs`, change `SchemaBuilder::build`:

```rust
    /// Build the complete schema.
    pub fn build(self) -> Schema {
        let mut schema: Schema = self.tables.into_iter().map(|t| t.build_named()).collect();
        super::e2ee_schema::expand_e2ee_keys_tables(&mut schema);
        schema
    }
```

(Adjust the `super::` path to match how `mod.rs` exposes the module.) If `TableSchemaBuilder::fk_column` produces anything other than a non-nullable Uuid FK column, fix the test expectation to reality only if reality is _also_ non-nullable Uuid — otherwise construct the `space_id` descriptor explicitly with `ColumnDescriptor`.

- [ ] **Step 2.4: Run tests to verify they pass**

Run: `cargo test -p jazz-tools --test e2ee_schema`
Expected: PASS, 8 tests. Run `cargo test -p jazz-tools` — no existing test may break (schemas without `encryption_space` expand to themselves).

- [ ] **Step 2.5: Commit**

```bash
git add crates/jazz-tools/src/query_manager/types/e2ee_schema.rs crates/jazz-tools/src/query_manager/types/mod.rs crates/jazz-tools/src/query_manager/types/schema.rs crates/jazz-tools/tests/e2ee_schema.rs
git commit -m "feat(jazz-tools): generate \$keys companion tables for encryption spaces"
```

---

### Task 3: E2EE schema validation (Rust)

**Files:**

- Modify: `crates/jazz-tools/src/query_manager/types/e2ee_schema.rs` (append)
- Test: `crates/jazz-tools/tests/e2ee_schema.rs` (append)

- [ ] **Step 3.1: Write the failing tests**

Append to `crates/jazz-tools/tests/e2ee_schema.rs`:

```rust
use jazz_tools::query_manager::types::e2ee_schema::validate_e2ee_schema;

fn expect_invalid(builder: SchemaBuilder, needle: &str) {
    let schema = builder.build();
    let err = validate_e2ee_schema(&schema).expect_err("schema should be invalid");
    assert!(
        err.contains(needle),
        "error {err:?} should mention {needle:?}"
    );
}

#[test]
fn valid_e2ee_schema_passes() {
    assert_eq!(validate_e2ee_schema(&base_schema().build()), Ok(()));
}

#[test]
fn encrypted_column_must_name_existing_ref() {
    expect_invalid(
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("name", ColumnType::Text)
                    .encryption_space(),
            )
            .table(
                TableSchemaBuilder::new("todos")
                    .encrypted_column("title", ColumnType::Text, "missing"),
            ),
        "missing",
    );
}

#[test]
fn encrypted_ref_must_be_non_nullable() {
    expect_invalid(
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("name", ColumnType::Text)
                    .encryption_space(),
            )
            .table(
                TableSchemaBuilder::new("todos")
                    .encrypted_column("title", ColumnType::Text, "projectId")
                    .nullable_fk_column("projectId", "projects"),
            ),
        "non-nullable",
    );
}

#[test]
fn encrypted_ref_target_must_be_encryption_space() {
    expect_invalid(
        SchemaBuilder::new()
            .table(TableSchemaBuilder::new("projects").column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("todos")
                    .encrypted_column("title", ColumnType::Text, "projectId")
                    .fk_column("projectId", "projects"),
            ),
        "encryption space",
    );
}

#[test]
fn user_tables_may_not_use_dollar_names() {
    expect_invalid(
        SchemaBuilder::new()
            .table(TableSchemaBuilder::new("nope$keys").column("name", ColumnType::Text)),
        "reserved",
    );
}

#[test]
fn encrypted_columns_are_excluded_from_indexing() {
    // Build's normalization must populate indexed_columns excluding encrypted
    // columns when the table previously indexed everything (None).
    let schema = base_schema().build();
    let todos = &schema[&"todos".into()];
    let indexed = todos.indexed_columns.as_ref().expect("normalized");
    assert!(indexed.iter().all(|c| c.as_str() != "title"));
    assert!(indexed.iter().any(|c| c.as_str() == "done"));
    assert!(indexed.iter().any(|c| c.as_str() == "projectId"));
}

#[test]
fn explicitly_indexed_encrypted_column_is_rejected() {
    let mut schema = base_schema().build();
    let todos = schema.get_mut(&"todos".into()).unwrap();
    todos.indexed_columns = Some(vec!["title".into()]);
    let err = validate_e2ee_schema(&schema).expect_err("indexed encrypted column");
    assert!(err.contains("index"));
}

#[test]
fn policies_may_not_reference_encrypted_columns() {
    use jazz_tools::query_manager::policy::{CmpOp, PolicyExpr, PolicyValue};
    use jazz_tools::query_manager::types::OperationPolicy;

    let mut schema = base_schema().build();
    let todos = schema.get_mut(&"todos".into()).unwrap();
    todos.policies.select = OperationPolicy::using(PolicyExpr::Cmp {
        column: "title".to_string(),
        op: CmpOp::Eq,
        value: PolicyValue::Literal {
            value: jazz_tools::query_manager::types::Value::Text("x".into()),
        },
    });
    let err = validate_e2ee_schema(&schema).expect_err("policy on encrypted column");
    assert!(err.contains("policy"));
}
```

Adjust import paths (`OperationPolicy`, `Value`, `PolicyValue` variants) to the actual public re-exports if they differ — `cargo test` output names the right paths; the _shape_ of each test is normative, not the import lines. If `PolicyValue` is a struct-variant vs tuple-variant, match its real definition in `crates/jazz-tools/src/query_manager/policy.rs`.

- [ ] **Step 3.2: Run tests to verify they fail**

Run: `cargo test -p jazz-tools --test e2ee_schema`
Expected: FAIL to compile (`validate_e2ee_schema` not found).

- [ ] **Step 3.3: Implement validation + index normalization**

Append to `crates/jazz-tools/src/query_manager/types/e2ee_schema.rs`:

```rust
/// Exclude encrypted columns from indexing. Tables that indexed everything
/// (`indexed_columns: None`) get an explicit subset without encrypted columns;
/// explicit subsets are left alone (validation rejects bad ones).
pub fn normalize_e2ee_indexes(schema: &mut Schema) {
    for table in schema.values_mut() {
        let has_encrypted = table
            .columns
            .columns
            .iter()
            .any(|c| c.encrypted_with.is_some());
        if !has_encrypted || table.indexed_columns.is_some() {
            continue;
        }
        table.indexed_columns = Some(
            table
                .columns
                .columns
                .iter()
                .filter(|c| c.encrypted_with.is_none())
                .map(|c| c.name.clone())
                .collect(),
        );
    }
}

fn policy_expr_references_column(expr: &PolicyExpr, column: &str) -> bool {
    match expr {
        PolicyExpr::Cmp { column: c, .. }
        | PolicyExpr::IsNull { column: c }
        | PolicyExpr::IsNotNull { column: c }
        | PolicyExpr::Contains { column: c, .. }
        | PolicyExpr::In { column: c, .. }
        | PolicyExpr::InList { column: c, .. } => c == column,
        PolicyExpr::Inherits { via_column, .. } => via_column == column,
        PolicyExpr::Exists { condition, .. } => {
            policy_expr_references_column(condition, column)
        }
        PolicyExpr::And(exprs) | PolicyExpr::Or(exprs) => exprs
            .iter()
            .any(|e| policy_expr_references_column(e, column)),
        PolicyExpr::Not(inner) => policy_expr_references_column(inner, column),
        // Session-only predicates touch no row columns. ExistsRel carries
        // relation IR whose column references are validated by the relation
        // layer; encrypted columns there are a known v1 gap (spec §11).
        PolicyExpr::SessionCmp { .. }
        | PolicyExpr::SessionIsNull { .. }
        | PolicyExpr::SessionIsNotNull { .. }
        | PolicyExpr::SessionContains { .. }
        | PolicyExpr::SessionInList { .. }
        | PolicyExpr::ExistsRel { .. }
        | PolicyExpr::True => false,
    }
}

/// Validate E2EE schema rules (spec §2):
/// - user table names must not contain `$` (only generated `$keys` companions);
/// - `encrypted_with` must name a non-nullable sibling ref column whose target
///   is an `encryption_space` table;
/// - encrypted columns cannot be indexed or referenced by policies.
pub fn validate_e2ee_schema(schema: &Schema) -> Result<(), String> {
    for (table_name, table) in schema {
        let name = table_name.as_str();
        if name.contains('$') {
            let base = name.strip_suffix(E2EE_KEYS_TABLE_SUFFIX);
            let is_generated_companion = base.is_some_and(|base| {
                !base.contains('$')
                    && schema
                        .get(&TableName::new(base))
                        .is_some_and(|t| t.encryption_space)
            });
            if !is_generated_companion {
                return Err(format!(
                    "table '{name}': '$' is reserved for framework tables"
                ));
            }
        }

        for col in &table.columns.columns {
            let Some(space_ref) = &col.encrypted_with else {
                continue;
            };
            let col_name = col.name.as_str();
            let Some(ref_col) = table
                .columns
                .columns
                .iter()
                .find(|c| c.name.as_str() == space_ref.as_str())
            else {
                return Err(format!(
                    "table '{name}': encrypted column '{col_name}' names unknown ref column '{space_ref}'"
                ));
            };
            if ref_col.nullable {
                return Err(format!(
                    "table '{name}': encrypted column '{col_name}' requires a non-nullable ref column '{space_ref}'"
                ));
            }
            let Some(target) = &ref_col.references else {
                return Err(format!(
                    "table '{name}': encrypted column '{col_name}' requires '{space_ref}' to be a ref column"
                ));
            };
            let Some(target_table) = schema.get(target) else {
                return Err(format!(
                    "table '{name}': encrypted column '{col_name}' references unknown table '{target}'"
                ));
            };
            if !target_table.encryption_space {
                return Err(format!(
                    "table '{name}': encrypted column '{col_name}' references '{target}', which is not an encryption space"
                ));
            }
            if let Some(indexed) = &table.indexed_columns {
                if indexed.iter().any(|c| c.as_str() == col_name) {
                    return Err(format!(
                        "table '{name}': encrypted column '{col_name}' cannot be indexed"
                    ));
                }
            }

            let policies = [
                &table.policies.select,
                &table.policies.insert,
                &table.policies.update,
                &table.policies.delete,
            ];
            for policy in policies {
                for expr in [&policy.using, &policy.with_check].into_iter().flatten() {
                    if policy_expr_references_column(expr, col_name) {
                        return Err(format!(
                            "table '{name}': policy references encrypted column '{col_name}'"
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}
```

And call the index normalization from `SchemaBuilder::build` (before companion expansion, order is not significant — companions have no encrypted columns):

```rust
    pub fn build(self) -> Schema {
        let mut schema: Schema = self.tables.into_iter().map(|t| t.build_named()).collect();
        super::e2ee_schema::normalize_e2ee_indexes(&mut schema);
        super::e2ee_schema::expand_e2ee_keys_tables(&mut schema);
        schema
    }
```

If `PolicyExpr::And`/`Or`/`Not` are struct variants rather than tuple variants, match their real shape (see `crates/jazz-tools/src/query_manager/policy.rs:283`).

- [ ] **Step 3.4: Run tests to verify they pass**

Run: `cargo test -p jazz-tools --test e2ee_schema`
Expected: PASS, 16 tests. Then `cargo test -p jazz-tools` — all green.

- [ ] **Step 3.5: Commit**

```bash
git add crates/jazz-tools/src/query_manager/types/e2ee_schema.rs crates/jazz-tools/src/query_manager/types/schema.rs crates/jazz-tools/tests/e2ee_schema.rs
git commit -m "feat(jazz-tools): validate E2EE schema constraints"
```

---

### Task 4: TS DSL (`.encrypted(ref)`, `.encryptionSpace()`)

**Files:**

- Modify: `packages/jazz-tools/src/schema.ts` (`Column` interface ~line 83, `Table` interface ~line 214, `PolicyExpr` union ~line 95)
- Modify: `packages/jazz-tools/src/dsl.ts` (`ColumnBuilder` interface ~line 78 and the concrete builder class that implements `_build`)
- Modify: `packages/jazz-tools/src/typed-app.ts` (`DefinedTable` ~line 26, `definitionToSchema` ~line 1269)
- Test: `packages/jazz-tools/src/e2ee-schema.test.ts` (new — written in Task 5, the DSL is exercised through `schemaToWasm`)

- [ ] **Step 4.1: Extend the AST types**

In `packages/jazz-tools/src/schema.ts`:

`Column` gains:

```ts
  /** E2EE: sibling ref column that scopes this column's encryption (spec §2). */
  encryptedWith?: string;
```

`Table` gains:

```ts
  /** E2EE: rows of this table own a shared encryption key. */
  encryptionSpace?: boolean;
```

`PolicyExpr` union gains (needed for the `$keys` world-read policy emitted in Task 5):

```ts
  | {
      type: "True";
    }
```

- [ ] **Step 4.2: Add `.encrypted(ref)` to column builders**

In `packages/jazz-tools/src/dsl.ts`:

The `ColumnBuilder` interface gains:

```ts
  encrypted(spaceRef: string): this;
  _encryptedWith?: string;
```

The concrete scalar builder class (`ScalarBuilder`, the one implementing `_build(name)`) gains a field and method:

```ts
  _encryptedWith?: string;

  encrypted(spaceRef: string): this {
    this._encryptedWith = spaceRef;
    return this;
  }
```

and its `_build(name)` adds `encryptedWith` to the produced `Column` when set:

```ts
    ...(this._encryptedWith ? { encryptedWith: this._encryptedWith } : {}),
```

Mirror the same three additions on any other builder classes that implement `_build` directly (`EnumBuilder`, `JsonBuilder`, `ArrayBuilder`, `RefBuilder`) — `.encrypted()` on a ref/array column will be _rejected by validation_ in Task 5, but the method must exist so the type surface is uniform and the error is a clear validation error rather than a TS compile mystery. On the typed surface, add to the `TypedColumnBuilder` type:

```ts
  /**
   * Encrypt this column end-to-end, scoped to the space row referenced by the
   * sibling ref column `spaceRef`. The server only ever sees ciphertext.
   */
  encrypted(spaceRef: string): this;
```

- [ ] **Step 4.3: Add `.encryptionSpace()` to tables**

In `packages/jazz-tools/src/typed-app.ts`, extend `DefinedTable`:

```ts
  constructor(
    public readonly columns: TColumns,
    public readonly indexedColumns?: readonly Extract<keyof TColumns, string>[],
    public readonly isEncryptionSpace?: boolean,
  ) {}

  /** Mark rows of this table as owners of a shared E2EE key (spec §3). */
  encryptionSpace(): DefinedTable<TColumns> {
    return new DefinedTable(this.columns, this.indexedColumns, true);
  }
```

and make `indexOnly` thread the flag: its trailing `return new DefinedTable(this.columns, normalizedColumns)` becomes `return new DefinedTable(this.columns, normalizedColumns, this.isEncryptionSpace)`.

Add a sibling of `tableIndexedColumns` (~line 1220):

```ts
function tableIsEncryptionSpace(
  definition: TableDefinition | DefinedTable<TableDefinition>,
): boolean {
  if (definition instanceof DefinedTable) {
    return definition.isEncryptionSpace === true;
  }

  if (typeof definition === "object" && definition !== null) {
    const maybeDefinedTable = definition as {
      __jazzTableDefinition?: unknown;
      isEncryptionSpace?: boolean;
    };
    if (maybeDefinedTable.__jazzTableDefinition === true) {
      return maybeDefinedTable.isEncryptionSpace === true;
    }
  }

  return false;
}
```

and thread it through `definitionToSchema`:

```ts
function definitionToSchema<TSchema extends SchemaDefinition>(definition: TSchema): SchemaAst {
  return {
    tables: Object.entries(definition).map(([tableName, tableDefinition]) => {
      const indexedColumns = tableIndexedColumns(tableDefinition);
      const encryptionSpace = tableIsEncryptionSpace(tableDefinition);
      return {
        name: tableName,
        columns: definitionToColumns(tableDefinition),
        ...(indexedColumns ? { indexedColumns } : {}),
        ...(encryptionSpace ? { encryptionSpace: true } : {}),
      };
    }),
  };
}
```

- [ ] **Step 4.4: Typecheck**

Run: `cd packages/jazz-tools && pnpm exec tsc --noEmit -p tsconfig.json`
Expected: clean (or only pre-existing errors — compare with `git stash; pnpm exec tsc --noEmit -p tsconfig.json; git stash pop` if unsure).

- [ ] **Step 4.5: Commit**

```bash
git add packages/jazz-tools/src/schema.ts packages/jazz-tools/src/dsl.ts packages/jazz-tools/src/typed-app.ts
git commit -m "feat(jazz-tools): encrypted() and encryptionSpace() schema DSL"
```

---

### Task 5: TS wasm-schema emission, expansion, and validation

**Files:**

- Modify: `packages/jazz-tools/src/drivers/types.ts` (wasm `ColumnDescriptor` + `TableSchema` types ~lines 80–155)
- Modify: `packages/jazz-tools/src/codegen/schema-reader.ts` (`schemaToWasm` ~line 232)
- Modify: `packages/jazz-tools/src/schema-permissions.ts` (`mergePermissionsIntoWasmSchema` ~line 529)
- Test: `packages/jazz-tools/src/e2ee-schema.test.ts` (new)

- [ ] **Step 5.1: Write the failing tests**

Create `packages/jazz-tools/src/e2ee-schema.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { schema as s } from "./index.js";

const validDefinition = () => ({
  projects: s.table({ name: s.string() }).encryptionSpace(),
  todos: s.table({
    title: s.string().encrypted("projectId"),
    done: s.boolean(),
    projectId: s.ref("projects"),
  }),
});

describe("E2EE schema DSL", () => {
  it("emits encryption markers and the $keys companion table", () => {
    const app = s.defineApp(validDefinition());
    const wasm = (app as unknown as { wasmSchema: Record<string, any> }).wasmSchema;

    expect(wasm.projects.encryption_space).toBe(true);
    const title = wasm.todos.columns.find((c: any) => c.name === "title");
    expect(title.encrypted_with).toBe("projectId");

    const keys = wasm["projects$keys"];
    expect(keys).toBeDefined();
    expect(keys.columns.map((c: any) => c.name)).toEqual([
      "space_id",
      "key_id",
      "recipient_user_id",
      "recipient_public_key",
      "sealed_key",
    ]);
    expect(keys.columns[0].references).toBe("projects");
    expect(keys.policies.select.using.type).toBe("True");
    expect(keys.policies.insert.with_check).toEqual({
      type: "SessionIsNotNull",
      path: ["user_id"],
    });
    expect(keys.policies.update).toBeUndefined();
    expect(keys.policies.delete.using.type).toBe("SessionIsNotNull");
  });

  it("excludes encrypted columns from indexing", () => {
    const app = s.defineApp(validDefinition());
    const wasm = (app as unknown as { wasmSchema: Record<string, any> }).wasmSchema;
    expect(wasm.todos.indexed_columns).toEqual(["done", "projectId"]);
  });

  it("rejects encrypted() pointing at a missing column", () => {
    expect(() =>
      s.defineApp({
        projects: s.table({ name: s.string() }).encryptionSpace(),
        todos: s.table({ title: s.string().encrypted("missing") }),
      }),
    ).toThrow(/missing/);
  });

  it("rejects encrypted() pointing at a nullable ref", () => {
    expect(() =>
      s.defineApp({
        projects: s.table({ name: s.string() }).encryptionSpace(),
        todos: s.table({
          title: s.string().encrypted("projectId"),
          projectId: s.ref("projects").optional(),
        }),
      }),
    ).toThrow(/non-nullable/);
  });

  it("rejects encrypted() pointing at a non-space table", () => {
    expect(() =>
      s.defineApp({
        projects: s.table({ name: s.string() }),
        todos: s.table({
          title: s.string().encrypted("projectId"),
          projectId: s.ref("projects"),
        }),
      }),
    ).toThrow(/encryption space/);
  });

  it("rejects user tables containing $", () => {
    expect(() => s.defineApp({ nope$keys: s.table({ name: s.string() }) })).toThrow(/reserved/);
  });

  it("rejects explicitly indexing an encrypted column", () => {
    expect(() =>
      s.defineApp({
        projects: s.table({ name: s.string() }).encryptionSpace(),
        todos: s
          .table({
            title: s.string().encrypted("projectId"),
            projectId: s.ref("projects"),
          })
          .indexOnly(["title"]),
      }),
    ).toThrow(/index/);
  });
});
```

- [ ] **Step 5.2: Run tests to verify they fail**

Run: `cd packages/jazz-tools && pnpm exec vitest run src/e2ee-schema.test.ts --config vitest.config.ts`
Expected: FAIL (markers not emitted, no companion, no validation).

- [ ] **Step 5.3: Extend the wasm types**

In `packages/jazz-tools/src/drivers/types.ts`, the wasm `ColumnDescriptor` interface gains:

```ts
  encrypted_with?: string;
```

and the wasm `TableSchema` interface gains:

```ts
  encryption_space?: boolean;
```

- [ ] **Step 5.4: Emit, validate, and expand in `schemaToWasm`**

In `packages/jazz-tools/src/codegen/schema-reader.ts`:

Inside the column mapping in `schemaToWasm` (next to the `references` handling):

```ts
if (col.encryptedWith) {
  descriptor.encrypted_with = col.encryptedWith;
}
```

Inside the table mapping (next to `indexed_columns`):

```ts
      ...(table.encryptionSpace ? { encryption_space: true } : {}),
```

After the `for (const table of schema.tables)` loop, before `return tables;`:

```ts
normalizeE2eeIndexes(tables);
expandE2eeKeysTables(tables);
validateE2eeSchema(tables);
```

Add at module level (mirrors `crates/jazz-tools/src/query_manager/types/e2ee_schema.rs` — column order is normative, it feeds the schema hash):

```ts
const E2EE_KEYS_TABLE_SUFFIX = "$keys";

const sessionAuthenticated = () => ({
  type: "SessionIsNotNull" as const,
  path: ["user_id"],
});

function e2eeKeysTable(spaceTable: string): TableSchema {
  return {
    columns: [
      {
        name: "space_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: spaceTable,
      },
      { name: "key_id", column_type: { type: "Uuid" }, nullable: false },
      { name: "recipient_user_id", column_type: { type: "Uuid" }, nullable: false },
      { name: "recipient_public_key", column_type: { type: "Text" }, nullable: false },
      { name: "sealed_key", column_type: { type: "Bytea" }, nullable: false },
    ],
    policies: {
      select: { using: { type: "True" } },
      insert: { with_check: sessionAuthenticated() },
      // update intentionally absent: key rows are immutable.
      delete: { using: sessionAuthenticated() },
    },
  };
}

function normalizeE2eeIndexes(tables: Record<string, TableSchema>): void {
  for (const table of Object.values(tables)) {
    const hasEncrypted = table.columns.some((c) => c.encrypted_with);
    if (!hasEncrypted || table.indexed_columns) {
      continue;
    }
    table.indexed_columns = table.columns.filter((c) => !c.encrypted_with).map((c) => c.name);
  }
}

function expandE2eeKeysTables(tables: Record<string, TableSchema>): void {
  for (const [name, table] of Object.entries(tables)) {
    const keysName = `${name}${E2EE_KEYS_TABLE_SUFFIX}`;
    if (table.encryption_space && !tables[keysName]) {
      tables[keysName] = e2eeKeysTable(name);
    }
  }
}

function validateE2eeSchema(tables: Record<string, TableSchema>): void {
  for (const [name, table] of Object.entries(tables)) {
    if (name.includes("$")) {
      const base = name.endsWith(E2EE_KEYS_TABLE_SUFFIX)
        ? name.slice(0, -E2EE_KEYS_TABLE_SUFFIX.length)
        : null;
      const isGeneratedCompanion =
        base !== null && !base.includes("$") && tables[base]?.encryption_space === true;
      if (!isGeneratedCompanion) {
        throw new Error(`Table "${name}": "$" is reserved for framework tables.`);
      }
    }

    for (const col of table.columns) {
      const spaceRef = col.encrypted_with;
      if (!spaceRef) continue;
      const refCol = table.columns.find((c) => c.name === spaceRef);
      if (!refCol) {
        throw new Error(
          `Table "${name}": encrypted column "${col.name}" names unknown ref column "${spaceRef}".`,
        );
      }
      if (refCol.nullable) {
        throw new Error(
          `Table "${name}": encrypted column "${col.name}" requires a non-nullable ref column "${spaceRef}".`,
        );
      }
      if (!refCol.references) {
        throw new Error(
          `Table "${name}": encrypted column "${col.name}" requires "${spaceRef}" to be a ref column.`,
        );
      }
      const target = tables[refCol.references];
      if (!target) {
        throw new Error(
          `Table "${name}": encrypted column "${col.name}" references unknown table "${refCol.references}".`,
        );
      }
      if (!target.encryption_space) {
        throw new Error(
          `Table "${name}": encrypted column "${col.name}" references "${refCol.references}", which is not an encryption space.`,
        );
      }
      if (table.indexed_columns?.includes(col.name)) {
        throw new Error(`Table "${name}": encrypted column "${col.name}" cannot be indexed.`);
      }
    }
  }
}
```

(Use the actual wasm `TableSchema` / `ColumnDescriptor` types from `../drivers/types.js` for the signatures; if `TableSchema.policies` is typed more strictly than these literals, fix the type, not the literals — the JSON shape matches the Rust serde format and is normative.)

- [ ] **Step 5.5: Re-validate after permission merging**

In `packages/jazz-tools/src/schema-permissions.ts`, `mergePermissionsIntoWasmSchema` (~line 529) merges `permissions.ts` policies into the wasm schema. Export `validateE2eeSchemaPolicies` from `schema-reader.ts`:

```ts
export function validateE2eeSchemaPolicies(tables: Record<string, TableSchema>): void {
  const encryptedColumns = (table: TableSchema): string[] =>
    table.columns.filter((c) => c.encrypted_with).map((c) => c.name);

  const referencesColumn = (expr: any, column: string): boolean => {
    if (!expr || typeof expr !== "object") return false;
    if (expr.column === column) return true;
    if (expr.via_column === column) return true;
    if (Array.isArray(expr.exprs)) {
      return expr.exprs.some((e: any) => referencesColumn(e, column));
    }
    if (expr.expr) return referencesColumn(expr.expr, column);
    if (expr.condition) return referencesColumn(expr.condition, column);
    return false;
  };

  for (const [name, table] of Object.entries(tables)) {
    for (const column of encryptedColumns(table)) {
      const policies = table.policies ?? {};
      for (const op of ["select", "insert", "update", "delete"] as const) {
        const policy = (policies as any)[op];
        for (const clause of [policy?.using, policy?.with_check]) {
          if (clause && referencesColumn(clause, column)) {
            throw new Error(`Table "${name}": policy references encrypted column "${column}".`);
          }
        }
      }
    }
  }
}
```

and call it at the end of `mergePermissionsIntoWasmSchema` on the merged result.

- [ ] **Step 5.6: Run tests to verify they pass**

Run: `cd packages/jazz-tools && pnpm exec vitest run src/e2ee-schema.test.ts --config vitest.config.ts`
Expected: PASS, 7 tests.

- [ ] **Step 5.7: Commit**

```bash
git add packages/jazz-tools/src/drivers/types.ts packages/jazz-tools/src/codegen/schema-reader.ts packages/jazz-tools/src/schema-permissions.ts packages/jazz-tools/src/e2ee-schema.test.ts
git commit -m "feat(jazz-tools): emit and validate E2EE markers in wasm schema"
```

---

### Task 6: Cross-layer verification

**Files:** none new (Step 6.4 touches the spec doc).

- [ ] **Step 6.1: Full Rust suite**

Run: `cargo test -p jazz-tools`
Expected: PASS. The TS-emitted `$keys` JSON and the Rust-built `$keys` table must describe identical schemas; the Rust test `build_expands_companion_keys_table` and the TS test `emits encryption markers...` pin both sides to the same normative column list, so divergence shows up as a test failure on whichever side drifted.

- [ ] **Step 6.2: Full TS suite for the package**

Run: `cd packages/jazz-tools && pnpm test`
Expected: PASS (pre-existing failures, if any, must also fail on `main` — verify before touching anything unrelated).

- [ ] **Step 6.3: Hash parity smoke check**

Run (from repo root):

```bash
cargo test -p jazz-tools --test e2ee_schema expansion_is_idempotent_and_hash_stable
```

Expected: PASS. (True TS↔Rust hash parity is exercised end-to-end in plan 4 when a TS-defined app boots a WASM runtime against a Rust-served schema; at this layer, byte-identical JSON emission is the contract and is covered by the shape tests.)

- [ ] **Step 6.4: Record the delete-policy deviation in the spec**

In `docs/superpowers/specs/2026-06-12-e2ee-shared-keys-design.md` §3, change the policy bullet sentence "delete is restricted to one's own rows plus the space creator" to "delete is open to any authenticated user in v1 (own-rows-plus-creator needs `created_by`, which hasn't landed; a malicious delete is a recoverable annoyance — re-sharing heals it)". Keep the rest of the bullet intact.

- [ ] **Step 6.5: Commit**

```bash
git add docs/superpowers/specs/2026-06-12-e2ee-shared-keys-design.md
git commit -m "docs: record v1 delete-policy decision for e2ee key tables"
```

---

## Out of scope (plans 3–4)

- Runtime: key cache, transparent encrypt/decrypt, `Value::Locked`, `share_key`/`unshare_key`/`key_holders`, `E2EEKeyUnavailable`, wiring `validate_e2ee_schema` into runtime schema ingestion (plan 3)
- Physical representation of encrypted values (envelope BYTEA at the row-format boundary; the schema keeps the _logical_ column type — plan 3 consumes `encrypted_with` to switch representations)
- `db.e2ee.publicKey()`, typed `shareKey`/`unshareKey`/`keyHolders`, `Locked` sentinel, E2E tests (plan 4)
- Policy `ExistsRel` relation-IR scans for encrypted-column references (documented v1 gap, spec §11)

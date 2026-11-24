//! What the deploy, `generate` and the loaders check of an app's encryption
//! declarations (E2EE protocol §1.5, E2EE_SCHEMA_CHANGES.md §3): each entry
//! against its schema version, each migration against the declarations of
//! its two versions, and each space table's read rule.
//!
//! The declarations stay outside the schema and its hash. They travel with
//! the files' history (one set per schema version) and in the deploy
//! request as [`DeclaredEncryption`].

use std::collections::{BTreeMap, BTreeSet};

pub use jazz_model::model::public_api::types::{EQUALITY_PREFIX, equality_column};

use serde::{Deserialize, Serialize};

use crate::catalogue::Permissions;
use crate::records::ValueType;
use crate::tools::public_schema::{
    CmpOp, ColumnDescriptor, ColumnType, EncryptedColumn, EncryptionEntry, EncryptionSettings,
    PolicyExpr, PolicyValue, Schema, TableName, TablePolicies, Value,
};

use super::super::published::{PublishLensOp, PublishMigrationRequest, PublishTableLens};
use super::system_tables::{MEMBERS, is_system_table};

/// How an existing plaintext app can adopt encryption.
pub const COPY_GUIDE: &str = "to encrypt existing data, copy it into a new encrypted table (docs/e2ee/encrypting-existing-data)";

/// A schema version's encryption entries, by table.
pub type Entries = BTreeMap<String, EncryptionEntry>;

/// An app's encryption declarations: the app-wide settings and every
/// schema version's entries. What the deploy request carries and generated
/// bindings embed.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeclaredEncryption {
    /// The top-level `encryption` object of the app file.
    #[serde(default)]
    pub settings: EncryptionSettings,
    /// Each schema version's entries, by the version's schema hash (which
    /// covers Jazz's system tables). A version that declares none is
    /// absent.
    #[serde(default)]
    pub versions: BTreeMap<String, Entries>,
}

impl DeclaredEncryption {
    /// The entries of the version whose schema hash is `hash`.
    pub fn entries(&self, hash: &str) -> Option<&Entries> {
        self.versions.get(hash).filter(|e| !e.is_empty())
    }

    /// Whether nothing is declared.
    pub fn is_empty(&self) -> bool {
        self.versions.values().all(BTreeMap::is_empty)
            && self.settings == EncryptionSettings::default()
    }
}

/// A refusal inside an entry: the path below the entry (`.scope`,
/// `.columns.title`) and why.
pub type EntryRefusal = (String, String);

fn column<'a>(schema: &'a Schema, table: &str, column: &str) -> Option<&'a ColumnDescriptor> {
    schema
        .get(&TableName::from(table))?
        .columns
        .columns
        .iter()
        .find(|c| c.name_str() == column)
}

/// Whether `name` is an equality token column declared for this table.
pub fn is_equality_token_column(entry: Option<&EncryptionEntry>, name: &str) -> bool {
    entry
        .zip(name.strip_prefix(EQUALITY_PREFIX))
        .is_some_and(|(entry, column)| {
            entry
                .equality
                .iter()
                .any(|indexed| indexed.as_str() == column)
        })
}

/// Whether `column` is a stored ciphertext column: non-null `bytes`.
fn is_cipher(column: &ColumnDescriptor) -> bool {
    column.column_type == ColumnType::Bytea && !column.nullable
}

/// Check that `space` names the space row of `scope` from `table`: `id`
/// when `table` is the scope itself, else a `uuid` column referencing it.
fn check_space(
    table: &str,
    space: &str,
    scope: &str,
    schema: &Schema,
    at: &str,
) -> Result<(), EntryRefusal> {
    if is_system_table(scope) {
        return Err((
            format!("{at}.scope"),
            format!("`{scope}` is one of Jazz's system tables and can't be a space table"),
        ));
    }
    if schema.get(&TableName::from(scope)).is_none() {
        return Err((
            format!("{at}.scope"),
            format!(
                "`scope` must be a table with an `id`; this schema version has no table {scope}{}",
                super::expr::did_you_mean(scope, schema.keys().map(|t| t.as_str()))
            ),
        ));
    }
    if space == "id" {
        if scope != table {
            return Err((
                format!("{at}.space"),
                format!(
                    "`space` is `id` only when the table is its own space table; for rows in \
                     spaces of {scope}, name the column referencing {scope}"
                ),
            ));
        }
        return Ok(());
    }
    match column(schema, table, space) {
        Some(c)
            if c.column_type == ColumnType::Uuid
                && c.references.is_some_and(|t| t.as_str() == scope) =>
        {
            Ok(())
        }
        Some(_) => Err((
            format!("{at}.space"),
            format!(
                "`space` must be `id` or a `uuid` column referencing {scope}; column {space} \
                 isn't one"
            ),
        )),
        None => Err((
            format!("{at}.space"),
            format!("table {table} has no column {space}"),
        )),
    }
}

/// Check one table's entry against its schema version (protocol §1.5,
/// E2EE_SCHEMA_CHANGES.md §3.1). Deploy, `generate` and the loaders share it.
pub fn check_entry(
    table: &str,
    entry: &EncryptionEntry,
    schema: &Schema,
) -> Result<(), EntryRefusal> {
    check_version_entry(table, entry, schema, true)
}

pub(crate) fn check_version_entry(
    table: &str,
    entry: &EncryptionEntry,
    schema: &Schema,
    current: bool,
) -> Result<(), EntryRefusal> {
    check_space(table, &entry.space, &entry.scope, schema, "")?;
    for (name, declared) in &entry.columns {
        let at = format!(".columns.{name}");
        let plain = &declared.plain;
        if declared
            .sealed_type
            .as_ref()
            .is_some_and(|sealed| sealed.default.is_some())
        {
            return Err((
                format!("{at}.sealedType.default"),
                format!(
                    "{table}.{name}: sealedType names only the stored type; declare the column's default beside it"
                ),
            ));
        }
        if let Some(default) = &plain.default {
            let path = format!("{at}.default");
            if !(default == &Value::Null && plain.nullable
                || fits_default(&plain.column_type, default))
            {
                return Err((
                    path,
                    format!("{table}.{name}: the declared default is not a value of its type"),
                ));
            }
            crate::tools::public_schema::validate_json_value(default, &plain.column_type, name)
                .map_err(|message| (path, message))?;
        }
        // The `__e2ee_` column names are Jazz's: an equality column's token
        // column (`__e2ee_eq_<column>`) is Jazz's, not an encrypted column
        // of the app's, and neither is any other reserved name.
        if name.starts_with("__e2ee_") {
            return Err((
                at,
                format!("`{name}` starts with `__e2ee_`, which Jazz reserves for its own columns"),
            ));
        }
        let Some(stored) = column(schema, table, name) else {
            return Err((at, format!("table {table} has no column {name}")));
        };
        if name == "id" || *name == entry.space {
            return Err((
                at,
                "the space column and `id` stay plaintext: the server routes rows by them".into(),
            ));
        }
        if !is_cipher(stored) {
            return Err((
                at,
                format!(
                    "an encrypted column is stored as required `bytes` (its ciphertext); column \
                     {name} isn't"
                ),
            ));
        }
        if stored
            .default
            .as_ref()
            .is_some_and(|default| !is_empty_cell(default))
        {
            return Err((
                at,
                format!(
                    "an encrypted column's stored default is the empty ciphertext (rows older \
                     than the column read as never written); column {name} has another. \
                     Declare the plaintext default in the entry instead (`{{ \"type\": …, \
                     \"default\": … }}`)"
                ),
            ));
        }
        if stored.merge_strategy.is_some() {
            return Err((
                at,
                "encrypted columns are last-writer-wins: the server can't merge ciphertext, so \
                 a merge strategy is refused"
                    .into(),
            ));
        }
    }
    let mut seen = BTreeSet::new();
    for name in &entry.equality {
        let at = ".equality".to_string();
        if !seen.insert(name) {
            return Err((at, format!("`{name}` is listed twice")));
        }
        if !entry.columns.contains_key(name) {
            return Err((
                at,
                format!("`{name}` is not an encrypted column of the entry"),
            ));
        }
        if let Some(declared) = entry.columns.get(name)
            && let Some(unsupported) = without_equality_encoding(&declared.plain.column_type)
        {
            return Err((
                at,
                format!(
                    "equality is not supported for {unsupported} columns: only boolean, integer, \
                     bigint, timestamp, double, text, bytes, uuid, enum and json values, and \
                     arrays of them, have an equality token encoding (equality format, column \
                     types)"
                ),
            ));
        }
        let tag = equality_column(name);
        let token = column(schema, table, &tag);
        if token
            .and_then(|c| c.default.as_ref())
            .is_some_and(|v| !is_empty_cell(v))
        {
            return Err((
                at,
                format!("the stored default of token column {tag} must be an empty byte string"),
            ));
        }
        if !token.is_some_and(is_cipher) {
            return Err((
                at,
                format!(
                    "an equality column's tokens are stored in a required `bytes` column {tag}"
                ),
            ));
        }
    }
    if current {
        for (name, declared) in &entry.columns {
            let at = format!(".columns.{name}");
            encrypted_value_type(&declared.plain.column_type)
                .map_err(|reason| (at.clone(), reason.to_owned()))?;
            if let Some(sealed) = &declared.sealed_type {
                encrypted_value_type(&sealed.column_type)
                    .map_err(|reason| (format!("{at}.sealedType"), reason.to_owned()))?;
            }
        }
    }
    Ok(())
}

/// The record type that a client can encode for this encrypted column.
pub fn encrypted_value_type(ty: &ColumnType) -> Result<ValueType, &'static str> {
    Ok(match ty {
        ColumnType::Integer => ValueType::I32,
        ColumnType::BigInt => ValueType::I64,
        ColumnType::Boolean => ValueType::Bool,
        ColumnType::Text | ColumnType::Enum { .. } => ValueType::String,
        ColumnType::Json { schema } => {
            if schema
                .as_ref()
                .is_some_and(|s| !schema_numbers_are_integers(s))
            {
                return Err(
                    "the encrypted cell type cannot encode a JSON schema with a non-integer number",
                );
            }
            ValueType::String
        }
        ColumnType::Timestamp => ValueType::U64,
        ColumnType::Double => ValueType::F64,
        ColumnType::Uuid => ValueType::Uuid,
        ColumnType::Bytea => ValueType::Bytes,
        ColumnType::Array { element } => encrypted_value_type(element)?.array_of(),
        ColumnType::LegacyEnumPayload { .. } => {
            return Err("the encrypted cell type cannot encode a payload enum");
        }
        ColumnType::Row { .. } => return Err("the encrypted cell type cannot encode a row"),
        ColumnType::ScalarEnum { .. } => {
            return Err("the encrypted cell type cannot encode a catalogue scalar enum");
        }
        ColumnType::TransactionId => {
            return Err("the encrypted cell type cannot encode a transaction ID");
        }
    })
}

/// The integer represented by a number in the cell type's canonical JSON.
pub fn cell_type_integer(number: &serde_json::Number) -> Option<i128> {
    number
        .as_i64()
        .map(i128::from)
        .or_else(|| number.as_u64().map(i128::from))
}

fn schema_numbers_are_integers(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Number(number) => cell_type_integer(number).is_some(),
        serde_json::Value::Array(items) => items.iter().all(schema_numbers_are_integers),
        serde_json::Value::Object(fields) => fields.values().all(schema_numbers_are_integers),
        _ => true,
    }
}

fn fits_default(ty: &ColumnType, value: &Value) -> bool {
    match (ty, value) {
        (ColumnType::Enum { variants }, Value::Text(text)) => variants.contains(text),
        (ColumnType::Json { .. }, Value::Text(_)) => true,
        (ColumnType::Array { element }, Value::Array(items)) => {
            items.iter().all(|item| fits_default(element, item))
        }
        (ColumnType::Array { .. } | ColumnType::TransactionId, _) => false,
        _ => value.column_type().as_ref() == Some(ty),
    }
}

fn is_empty_cell(value: &Value) -> bool {
    matches!(value, Value::Bytea(bytes) if bytes.is_empty())
}

/// What stops the equality format encoding values of `column_type`, if
/// anything: a row or transaction-ID column, or an array of either (an array
/// has an encoding when its element type does). JSON and the scalar and
/// enum types have one.
fn without_equality_encoding(column_type: &ColumnType) -> Option<String> {
    match column_type {
        ColumnType::Boolean
        | ColumnType::Integer
        | ColumnType::BigInt
        | ColumnType::Timestamp
        | ColumnType::Double
        | ColumnType::Text
        | ColumnType::Bytea
        | ColumnType::Uuid
        | ColumnType::Enum { .. }
        | ColumnType::Json { .. } => None,
        ColumnType::Array { element } => {
            without_equality_encoding(element).map(|inner| format!("array of {inner}"))
        }
        ColumnType::Row { .. } => Some("row".to_owned()),
        ColumnType::TransactionId => Some("transaction-ID".to_owned()),
        ColumnType::LegacyEnumPayload { .. } => Some("payload enum".to_owned()),
        ColumnType::ScalarEnum { .. } => Some("catalogue scalar enum".to_owned()),
    }
}

// ---------------------------------------------------------------------------
// The transition check
// ---------------------------------------------------------------------------

/// A migration's view of one table: which old column each new one comes
/// from, and where each old one goes.
struct TableMap<'a> {
    lens: Option<&'a PublishTableLens>,
}

impl TableMap<'_> {
    /// The old name of new column `column`, or `None` for one the
    /// migration introduces.
    fn old_name(&self, column: &str) -> Option<String> {
        let Some(lens) = self.lens else {
            return Some(column.to_owned());
        };
        if lens.added {
            return None;
        }
        for op in &lens.operations {
            match op {
                PublishLensOp::Rename { column: was, value } if value == column => {
                    return Some(was.clone());
                }
                PublishLensOp::Introduce { column: c, .. } if c == column => return None,
                _ => {}
            }
        }
        Some(column.to_owned())
    }

    /// The new name of old column `column`, or `None` when the migration
    /// drops it.
    fn new_name(&self, column: &str) -> Option<String> {
        let Some(lens) = self.lens else {
            return Some(column.to_owned());
        };
        for op in &lens.operations {
            match op {
                PublishLensOp::Rename { column: was, value } if was == column => {
                    return Some(value.clone());
                }
                PublishLensOp::Drop { column: c, .. } if c == column => return None,
                _ => {}
            }
        }
        Some(column.to_owned())
    }
}

/// How a migration names the tables and columns on either side of it.
pub struct MigrationNames<'a> {
    pub migration: &'a PublishMigrationRequest,
    /// The schema it leads to.
    pub to: &'a Schema,
}

impl MigrationNames<'_> {
    fn lens_of(&self, table: &str) -> Option<&PublishTableLens> {
        self.migration.forward.iter().find(|l| l.table == table)
    }

    /// The old name of new table `table`, or `None` when the migration adds it.
    pub fn old_table(&self, table: &str) -> Option<String> {
        match self.lens_of(table) {
            Some(lens) if lens.added => None,
            Some(lens) => Some(
                lens.renamed_from
                    .clone()
                    .unwrap_or_else(|| table.to_owned()),
            ),
            None => Some(table.to_owned()),
        }
    }

    /// The new name of old table `table`, or `None` when the new schema
    /// leaves it out.
    pub fn new_table(&self, table: &str) -> Option<String> {
        if let Some(lens) = self
            .migration
            .forward
            .iter()
            .find(|l| l.renamed_from.as_deref() == Some(table))
        {
            return Some(lens.table.clone());
        }
        match self.lens_of(table) {
            Some(lens) if lens.removed => None,
            _ => self
                .to
                .contains_key(&TableName::from(table))
                .then(|| table.to_owned()),
        }
    }

    /// The old name of `column` in new table `table`.
    pub fn old_name(&self, table: &str, column: &str) -> Option<String> {
        TableMap {
            lens: self.lens_of(table),
        }
        .old_name(column)
    }

    /// The new name of `column` in old table `table`.
    pub fn new_name(&self, table: &str, column: &str) -> Option<String> {
        TableMap {
            lens: self.lens_of(&self.new_table(table)?),
        }
        .new_name(column)
    }
}

/// Refuse a migration that changes an encrypted column's type, scope or
/// space, or renames an equality column apart from
/// its column (E2EE_SCHEMA_CHANGES.md §3.2). `from`/`to` are the two
/// versions' schemas and entries.
pub fn check_transition(
    from: (&Schema, Option<&Entries>),
    to: (&Schema, Option<&Entries>),
    migration: &PublishMigrationRequest,
) -> Result<(), String> {
    let empty = Entries::new();
    let (old_schema, old_entries) = (from.0, from.1.unwrap_or(&empty));
    let (new_schema, new_entries) = (to.0, to.1.unwrap_or(&empty));
    let names = MigrationNames {
        migration,
        to: new_schema,
    };
    for lens in &migration.forward {
        let was = lens.renamed_from.as_deref().unwrap_or(&lens.table);
        let old = old_entries.get(was);
        let new = new_entries.get(&lens.table);
        if (old.is_none() && new.is_none())
            || lens.added
            || !old_schema.contains_key(&TableName::from(was))
        {
            continue;
        }
        for op in &lens.operations {
            let (entry, column, value, field) = match op {
                PublishLensOp::Introduce { column, value, .. } => (new, column, value, "default"),
                PublishLensOp::Drop { column, value, .. } => {
                    (old, column, value, "backwardsDefault")
                }
                PublishLensOp::Rename { .. } => continue,
            };
            if !is_empty_cell(value)
                && (entry.is_some_and(|e| e.columns.contains_key(column))
                    || is_equality_token_column(entry, column))
            {
                return Err(format!(
                    "{}.{column}: the migration's {field} must be an empty byte string",
                    lens.table
                ));
            }
        }
    }
    check_mapped_transition(
        (old_schema, old_entries),
        (new_schema, new_entries),
        |table| names.old_table(table),
        |table| names.new_table(table),
        |table, name| names.old_name(table, name),
        |table, name| names.new_name(table, name),
    )
}

/// Check the same transition after catalogue admission has resolved the
/// table and column identities of both versions. Each mapping takes names in its source
/// version and returns their names in the destination version, if kept.
pub fn check_mapped_transition(
    (old_schema, old_entries): (&Schema, &Entries),
    (_, new_entries): (&Schema, &Entries),
    old_table: impl Fn(&str) -> Option<String>,
    new_table: impl Fn(&str) -> Option<String>,
    old_name: impl Fn(&str, &str) -> Option<String>,
    new_name: impl Fn(&str, &str) -> Option<String>,
) -> Result<(), String> {
    check_mapped_columns(
        (old_schema, old_entries),
        new_entries,
        old_table,
        new_table,
        old_name,
        new_name,
        false,
    )
}

/// An index edit keeps every stored column. Apply the storage checks of an
/// identity migration, allowing sibling declarations of either enum prefix.
pub fn check_index_transition(
    from: (&Schema, &Entries),
    new_entries: &Entries,
) -> Result<(), String> {
    let table = |name: &str| Some(name.to_owned());
    let column = |_: &str, name: &str| Some(name.to_owned());
    check_mapped_columns(from, new_entries, table, table, column, column, true)
}

fn check_mapped_columns(
    (old_schema, old_entries): (&Schema, &Entries),
    new_entries: &Entries,
    old_table: impl Fn(&str) -> Option<String>,
    new_table: impl Fn(&str) -> Option<String>,
    old_name: impl Fn(&str, &str) -> Option<String>,
    new_name: impl Fn(&str, &str) -> Option<String>,
    siblings: bool,
) -> Result<(), String> {
    // Every old encrypted column the new version keeps stays encrypted,
    // as long as its stored column remains.
    for (table, old) in old_entries {
        let Some(renamed) = new_table(table) else {
            continue;
        };
        let new = new_entries.get(&renamed);
        for name in old.columns.keys() {
            let kept = new_name(table, name);
            if let Some(kept) = &kept
                && !new.is_some_and(|entry| entry.columns.contains_key(kept))
            {
                return Err(format!(
                    "column {renamed}.{kept} holds ciphertext but is no longer declared \
                     encrypted; keep its declaration \
                     or drop the column (E2EE_SCHEMA_CHANGES.md §3.2)"
                ));
            }
            if old.equality.contains(name)
                && !kept
                    .as_ref()
                    .is_some_and(|name| new.is_some_and(|entry| entry.equality.contains(name)))
                && let Some(tag) = new_name(table, &equality_column(name))
            {
                let column = kept.as_ref().unwrap_or(name);
                return Err(format!(
                    "{renamed}.{column}: the token column {tag} stays after equality is removed; drop it in the same migration (E2EE_SCHEMA_CHANGES.md §3.2)"
                ));
            }
        }
    }

    for (table, new) in new_entries {
        let Some(was) = old_table(table) else {
            continue;
        };
        let old = old_entries.get(&was);
        let old_column =
            |name: &str| old_name(table, name).and_then(|n| column(old_schema, &was, &n));
        if let Some(old) = old {
            check_stored_space(
                table,
                old,
                new,
                new_table(&old.scope),
                new_name(&was, &old.space),
            )?;
        }
        for (name, declared) in &new.columns {
            let path = format!("{table}.{name}");
            // Existing rows may lack tokens for a newly tagged column.
            let tagged_before =
                old_name(table, name).is_some_and(|n| old.is_some_and(|o| o.equality.contains(&n)));
            if new.equality.contains(name) && !tagged_before {
                return Err(format!(
                    "{path}: the table existed in the earlier version, whose rows have no \
                     equality token for it; adding equality to an existing table is unsupported \
                     (E2EE_SCHEMA_CHANGES.md §3.2)"
                ));
            }
            let previous = old_name(table, name)
                .and_then(|n| old.and_then(|o| o.columns.get(&n).map(|c| (n, c))));
            match previous {
                Some((was_name, previous)) => {
                    check_stored_column_type(&path, previous, declared, siblings)?;
                    let tagged_before = old.is_some_and(|o| o.equality.contains(&was_name));
                    if tagged_before
                        && new.equality.contains(name)
                        && old_name(table, &equality_column(name))
                            != Some(equality_column(&was_name))
                    {
                        return Err(format!(
                            "{path}: renaming the encrypted column {was_name} also renames \
                             {} to {}",
                            equality_column(&was_name),
                            equality_column(name)
                        ));
                    }
                }
                None => {
                    // A column the old version stored as plaintext.
                    if old_column(name).is_some() {
                        return Err(format!(
                            "{path}: column {name} held plaintext in the earlier version; \
                             copy existing data into a new encrypted table \
                             (docs/e2ee/encrypting-existing-data)"
                        ));
                    }
                    // A column added to a table that may hold rows: they
                    // read it as never written (the empty cell), which a
                    // required column answers with its declared default.
                    if !declared.plain.nullable && declared.plain.default.is_none() {
                        return Err(format!(
                            "{path}: the migration adds a required encrypted column, which the \
                             rows stored before it read as never written; declare its default \
                             (`{{ \"type\": …, \"default\": … }}`) or make it optional"
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Check a version against an earlier one that no single migration leads
/// from, after mapping the stored identities of both. Migration descendants
/// can only append. Siblings may use either prefix of the same enum.
/// `precedes` says whether the new version descends from the old one; it
/// runs only for a column whose plaintext type differs, and its error is
/// returned as the outer one. Each mapping is as in
/// [`check_mapped_transition`].
pub async fn check_unlinked_transition<E>(
    old_entries: &Entries,
    new_entries: &Entries,
    old_table: impl Fn(&str) -> Option<String>,
    new_table: impl Fn(&str) -> Option<String>,
    old_name: impl Fn(&str, &str) -> Option<String>,
    new_name: impl Fn(&str, &str) -> Option<String>,
    precedes: impl AsyncFnMut() -> Result<bool, E>,
) -> Result<Result<(), String>, E> {
    check_unlinked_columns(
        old_entries,
        new_entries,
        old_table,
        old_name,
        |table, was, old, new| {
            check_stored_space(
                table,
                old,
                new,
                new_table(&old.scope),
                new_name(was, &old.space),
            )
        },
        precedes,
    )
    .await
}

/// Compare the declarations whose old names are known. The caller checks
/// the space using the identities available from the catalogue or the file.
pub async fn check_unlinked_columns<E>(
    old_entries: &Entries,
    new_entries: &Entries,
    old_table: impl Fn(&str) -> Option<String>,
    old_name: impl Fn(&str, &str) -> Option<String>,
    check_space: impl Fn(&str, &str, &EncryptionEntry, &EncryptionEntry) -> Result<(), String>,
    mut precedes: impl AsyncFnMut() -> Result<bool, E>,
) -> Result<Result<(), String>, E> {
    for (table, new) in new_entries {
        let Some(was) = old_table(table) else {
            continue;
        };
        let Some(old) = old_entries.get(&was) else {
            continue;
        };
        if let Err(refusal) = check_space(table, &was, old, new) {
            return Ok(Err(refusal));
        }
        for (name, declared) in &new.columns {
            let Some(previous) = old_name(table, name).and_then(|column| old.columns.get(&column))
            else {
                continue;
            };
            let path = format!("{table}.{name}");
            let siblings = !previous.plain.same_type(&declared.plain) && !precedes().await?;
            if let Err(refusal) = check_stored_column_type(&path, previous, declared, siblings) {
                return Ok(Err(refusal));
            }
        }
    }
    Ok(Ok(()))
}

/// Check the scope table and space column after mapping their stored identities into the new version.
pub fn check_stored_space(
    table: &str,
    old: &EncryptionEntry,
    new: &EncryptionEntry,
    mapped_scope: Option<String>,
    mapped_space: Option<String>,
) -> Result<(), String> {
    check_space_matches(
        table,
        old,
        new,
        mapped_scope.as_deref() == Some(&new.scope),
        mapped_space.as_deref() == Some(&new.space),
    )
}

/// Check the scope and space comparisons the caller can resolve. The
/// implicit row id always maps to the row id.
pub fn check_space_matches(
    table: &str,
    old: &EncryptionEntry,
    new: &EncryptionEntry,
    scope_matches: bool,
    space_matches: bool,
) -> Result<(), String> {
    if !scope_matches {
        return Err(format!(
            "table {table} moves its space table (`scope`) from {} to {}, which \
             changes every cell's context; copy the data into a new encrypted table \
             (docs/e2ee/encrypting-existing-data)",
            old.scope, new.scope
        ));
    }
    let same_space = if old.space == "id" {
        new.space == "id"
    } else {
        space_matches
    };
    if !same_space {
        return Err(format!(
            "table {table} changes its space column from {} to {}; copy the data into a \
             new encrypted table (docs/e2ee/encrypting-existing-data)",
            old.space, new.space
        ));
    }
    Ok(())
}

/// Whether a new declaration can read the values of a previously declared
/// encrypted column with the same stored identity.
pub fn check_stored_column_type(
    path: &str,
    previous: &EncryptedColumn,
    declared: &EncryptedColumn,
    siblings: bool,
) -> Result<(), String> {
    if !declared.cell_type().same_type(previous.cell_type()) {
        return Err(format!(
            "{path}: the encrypted column's type changes from {:?} to {:?}, \
             which its cells were sealed under; only append-only enum widening can pin \
             the old type with sealedType; {COPY_GUIDE}",
            previous.cell_type(),
            declared.cell_type()
        ));
    }
    // Rows older than the column read its default: another
    // default would read them differently on clients of
    // each version.
    if declared.plain.default != previous.plain.default {
        return Err(format!(
            "{path}: the encrypted column's default changes, and the rows \
             stored before the column read it; {COPY_GUIDE}"
        ));
    }
    let compatible = widens(&previous.plain, &declared.plain)
        || (siblings && widens(&declared.plain, &previous.plain));
    if !widens(declared.cell_type(), &declared.plain) || !compatible {
        return Err(format!(
            "{path}: only appending enum variants preserves existing encrypted values in place; \
             {COPY_GUIDE}"
        ));
    }
    Ok(())
}

/// The same native encoding and nullability, allowing new enum names at the end.
fn widens(
    from: &crate::tools::public_schema::PlainType,
    to: &crate::tools::public_schema::PlainType,
) -> bool {
    let types = match (&from.column_type, &to.column_type) {
        (ColumnType::Enum { variants: a }, ColumnType::Enum { variants: b }) => b.starts_with(a),
        (a, b) => a == b,
    };
    types && from.nullable == to.nullable
}

// ---------------------------------------------------------------------------
// Space tables' read rules and the deploy's warnings
// ---------------------------------------------------------------------------

const OUTER_ROW: &str = "__jazz_outer_row";

/// Whether `expr` is exactly `M(table)`: `exists __e2ee_members(scopeId =
/// table_id('table'), identifier = this.id, accountId = session.user)`,
/// its conditions in any order (protocol R13).
pub fn is_members_rule(expr: &PolicyExpr, table: &str) -> bool {
    members_rule_column(expr, table) == Some("id")
}

/// The checked row's column in an exact membership check for `scope`.
fn members_rule_column<'a>(expr: &'a PolicyExpr, scope: &str) -> Option<&'a str> {
    let PolicyExpr::Exists {
        table: members,
        condition,
    } = expr
    else {
        return None;
    };
    let PolicyExpr::And(conditions) = condition.as_ref() else {
        return None;
    };
    if members != MEMBERS || conditions.len() != 3 {
        return None;
    }
    let equality = |name: &str| {
        conditions.iter().find_map(|expr| match expr {
            PolicyExpr::Cmp {
                column,
                op: CmpOp::Eq,
                value,
            } if column == name => Some(value),
            _ => None,
        })
    };
    if let PolicyValue::TableIdentity { table } = equality("scopeId")?
        && table == scope
        && let PolicyValue::SessionRef(account) = equality("accountId")?
        && account.as_slice() == ["user"]
        && let PolicyValue::SessionRef(identifier) = equality("identifier")?
        && let [row, column] = identifier.as_slice()
        && row == OUTER_ROW
    {
        Some(column)
    } else {
        None
    }
}

/// Whether a read rule comes from the members table: `M(table)`, or an
/// `And` with `M(table)` as one of its direct conjuncts.
fn comes_from_members(rule: Option<&PolicyExpr>, table: &str) -> bool {
    match rule {
        Some(PolicyExpr::And(conjuncts)) => conjuncts.iter().any(|c| is_members_rule(c, table)),
        Some(rule) => is_members_rule(rule, table),
        None => false,
    }
}

/// The space tables of `entries`: each `scope`, and whether some entry
/// naming it encrypts at least one column.
fn scopes(entries: &Entries) -> BTreeMap<&str, bool> {
    let mut scopes = BTreeMap::new();
    for entry in entries.values() {
        *scopes.entry(entry.scope.as_str()).or_insert(false) |= !entry.columns.is_empty();
    }
    scopes
}

/// Own entries have already been checked against their schema. Inherited
/// entries can name a space that no longer exists under its original name.
pub fn check_space_tables(schema: &Schema, entries: &Entries) -> Result<(), String> {
    for entry in entries.values() {
        let scope = entry.scope.as_str();
        if !schema.contains_key(&TableName::from(scope)) {
            return Err(format!(
                "space table {scope} is not in this version (dropped or renamed). This version \
                 declares no encryption entries, so it follows the nearest earlier declarations, \
                 which use {scope}. Either keep {scope} under its original name or declare encryption in this version. \
                 Even one entry with no encrypted columns makes this version follow its own declarations. \
                 If this version has already been deployed to a server without declarations, they can't be added to it: \
                 make the change in a new version."
            ));
        }
    }
    Ok(())
}

/// Refuse a space table whose read rule doesn't come from the members
/// table (protocol R13), naming it. Only space tables of an entry that
/// encrypts a column are checked: an entry without encrypted columns lets
/// an app write member rows first (E2EE_SCHEMA_CHANGES.md §4).
pub fn check_space_read_rules(permissions: &Permissions, entries: &Entries) -> Result<(), String> {
    for (scope, encrypts) in scopes(entries) {
        let rule = permissions
            .get(&TableName::from(scope))
            .and_then(TablePolicies::select_policy);
        if encrypts && !comes_from_members(rule, scope) {
            return Err(format!(
                "space table {scope}: its read rule must come from the members table, so that \
                 only members read its rows and every member gets their keys. Write it as \
                 `exists __e2ee_members(scopeId = table_id('{scope}'), identifier = this.id, \
                 accountId = session.user)`, alone or conjoined (`allOf`) with further \
                 conditions"
            ));
        }
    }
    Ok(())
}

fn conjuncts(expr: &PolicyExpr) -> Vec<&PolicyExpr> {
    match expr {
        PolicyExpr::And(items) => items.iter().flat_map(conjuncts).collect(),
        other => vec![other],
    }
}

/// Whether `expr` reads a session value or `$createdBy` (which tie an
/// update to who makes it). Inheritance and relation reads may, so they
/// count.
fn reads_session(expr: &PolicyExpr) -> bool {
    let value = |v: &PolicyValue| matches!(v, PolicyValue::SessionRef(path) if path.first().map(String::as_str) != Some(OUTER_ROW));
    match expr {
        PolicyExpr::Cmp {
            column, value: v, ..
        }
        | PolicyExpr::Contains { column, value: v } => value(v) || column.starts_with("$createdBy"),
        PolicyExpr::InList { column, values } => {
            values.iter().any(value) || column.starts_with("$createdBy")
        }
        PolicyExpr::IsNull { column } | PolicyExpr::IsNotNull { column } => {
            column.starts_with("$createdBy")
        }
        PolicyExpr::SessionCmp { .. }
        | PolicyExpr::SessionIsNull { .. }
        | PolicyExpr::SessionIsNotNull { .. }
        | PolicyExpr::SessionContains { .. }
        | PolicyExpr::SessionInList { .. }
        | PolicyExpr::In { .. }
        | PolicyExpr::ExistsRel { .. }
        | PolicyExpr::Inherits { .. }
        | PolicyExpr::InheritsReferencing { .. }
        | PolicyExpr::InheritsFrom { .. } => true,
        PolicyExpr::Exists { condition, .. } => reads_session(condition),
        PolicyExpr::And(items) | PolicyExpr::Or(items) => items.iter().any(reads_session),
        PolicyExpr::Not(inner) => reads_session(inner),
        _ => false,
    }
}

/// Whether `expr` reads a member row of the row being checked: an `exists`
/// over the members table with `identifier = this.id`.
fn reads_own_member_row(expr: &PolicyExpr) -> bool {
    match expr {
        PolicyExpr::Exists { table, condition } if table == MEMBERS => conjuncts(condition)
            .iter()
            .any(|c| {
                matches!(c, PolicyExpr::Cmp { column, op: CmpOp::Eq, value: PolicyValue::SessionRef(path) }
                    if column == "identifier" && path == &[OUTER_ROW.to_owned(), "id".to_owned()])
            }),
        PolicyExpr::Exists { condition, .. } => reads_own_member_row(condition),
        PolicyExpr::And(items) | PolicyExpr::Or(items) => items.iter().any(reads_own_member_row),
        PolicyExpr::Not(inner) => reads_own_member_row(inner),
        _ => false,
    }
}

/// Whether an update policy can only pass on a change: a column compared
/// with one literal in `USING` and with another in `WITH CHECK`, as direct
/// conjuncts.
fn requires_transition(using: &PolicyExpr, check: &PolicyExpr) -> bool {
    let literals = |expr: &PolicyExpr| -> Vec<(String, CmpOp, PolicyValue)> {
        conjuncts(expr)
            .into_iter()
            .filter_map(|c| match c {
                PolicyExpr::Cmp {
                    column,
                    op,
                    value: value @ PolicyValue::Literal(_),
                } => Some((column.clone(), op.clone(), value.clone())),
                _ => None,
            })
            .collect()
    };
    let (old, new) = (literals(using), literals(check));
    old.iter().any(|(column, op, value)| {
        new.iter().any(|(c, o, v)| {
            c == column
                && match (op, o) {
                    (CmpOp::Eq, CmpOp::Eq) => v != value,
                    (CmpOp::Eq, CmpOp::Ne) | (CmpOp::Ne, CmpOp::Eq) => v == value,
                    _ => false,
                }
        })
    })
}

/// Whether `expr` involves the space row of an encrypted table whose
/// space column is `space` in table `scope`: inheritance through the space
/// column, an `exists` over the space table, or the members table.
fn involves_space_row(expr: &PolicyExpr, space: &str, scope: &str) -> bool {
    match expr {
        PolicyExpr::Inherits { via_column, .. } => via_column == space,
        PolicyExpr::Exists { table, condition } => {
            table == scope || table == MEMBERS || involves_space_row(condition, space, scope)
        }
        PolicyExpr::ExistsRel { .. } => true,
        PolicyExpr::And(items) | PolicyExpr::Or(items) => {
            items.iter().any(|i| involves_space_row(i, space, scope))
        }
        PolicyExpr::Not(inner) => involves_space_row(inner, space, scope),
        _ => false,
    }
}

/// The deploy's warnings about what it can see of an encrypted app's
/// policies (protocol §1.5). Each names the table; none claims more than
/// the syntax shows.
pub fn warnings(permissions: &Permissions, entries: &Entries) -> Vec<String> {
    let mut warnings = Vec::new();
    let policies = |table: &str| permissions.get(&TableName::from(table));
    for (scope, encrypts) in scopes(entries) {
        let space_policies = policies(scope);
        let read = space_policies.and_then(TablePolicies::select_policy);
        if !encrypts && !comes_from_members(read, scope) {
            warnings.push(format!(
                "space table {scope}: its read rule doesn't come from the members table yet; \
                 write its member rows, then change it before encrypting columns in its spaces"
            ));
        }
        let Some(space_policies) = space_policies else {
            continue;
        };
        let (using, check) = (
            space_policies.update_using_policy(),
            space_policies.update_check_policy(),
        );
        if let (Some(using), Some(check)) = (using, check)
            && requires_transition(using, check)
        {
            warnings.push(format!(
                "space table {scope}: its update rule needs a column to change, so no save that \
                 changes nothing passes it: nobody can add members, create the space or rotate \
                 its key"
            ));
        }
        let broad = [using, check].into_iter().flatten().any(|clause| {
            matches!(clause, PolicyExpr::True)
                || is_members_rule(clause, scope)
                || !reads_session(clause)
        });
        if broad {
            warnings.push(format!(
                "space table {scope}: its update rule is broad (true, the members rule, or no \
                 session value or $createdBy), and whoever may update a space row manages its \
                 members, creates its space and rotates its key"
            ));
        }
        if space_policies
            .insert_policy()
            .is_some_and(reads_own_member_row)
        {
            warnings.push(format!(
                "space table {scope}: its insert rule reads the members table for the row being \
                 inserted, which no member row can satisfy yet; let the creator insert it, e.g. \
                 `$createdBy.account = session.user`"
            ));
        }
    }
    for (table, entry) in entries {
        if entry.columns.is_empty() || *table == entry.scope {
            continue;
        }
        let (Some(read), Some(space_read)) = (
            policies(table).and_then(TablePolicies::select_policy),
            policies(&entry.scope).and_then(TablePolicies::select_policy),
        ) else {
            continue;
        };
        let space_conjuncts = conjuncts(space_read);
        let own = conjuncts(read);
        let membership_columns: BTreeSet<_> = own
            .iter()
            .filter_map(|clause| members_rule_column(clause, &entry.scope))
            .collect();
        for column in &membership_columns {
            if *column != entry.space {
                warnings.push(format!(
                    "table {table}: its read rule checks {} membership through {column}, but its space is {}",
                    entry.scope, entry.space
                ));
            }
        }
        if own.len() > space_conjuncts.len()
            && space_conjuncts.iter().all(|clause| {
                if is_members_rule(clause, &entry.scope) {
                    membership_columns.contains(entry.space.as_str())
                } else {
                    own.contains(clause)
                }
            })
        {
            warnings.push(format!(
                "table {table}: its read rule is its space table {}'s plus further conditions, \
                 so every member of a space can decrypt rows of {table} that Jazz doesn't send \
                 them",
                entry.scope
            ));
        }
        if !involves_space_row(read, &entry.space, &entry.scope) {
            warnings.push(format!(
                "table {table}: its read rule doesn't involve its space row in {}, so a session \
                 that isn't a member may receive its ciphertext and never a key \
                 (`key-unavailable`)",
                entry.scope
            ));
        }
    }
    warnings
}

/// Refuse a policy that reads an encrypted column's value: the server
/// sees only its ciphertext, so no comparison with it can mean what it says
/// (E2EE_SCHEMA_CHANGES.md §3.1). Columns are read where a policy names them: its own
/// table's, an `exists` table's, and the checked row's through `this.col`.
/// A relation (`exists (…)`) is judged conservatively: a column it names
/// counts as a column of every table it might belong to.
pub fn check_encrypted_reads(permissions: &Permissions, entries: &Entries) -> Result<(), String> {
    fn encrypted(entries: &Entries, table: &str, column: &str) -> bool {
        entries
            .get(table)
            .is_some_and(|entry| entry.columns.contains_key(column))
    }
    fn reads<'a>(
        expr: &'a PolicyExpr,
        table: &'a str,
        outer: Option<&'a str>,
        entries: &Entries,
    ) -> Option<(String, String)> {
        let column = |name: &str| {
            encrypted(entries, table, name).then(|| (table.to_owned(), name.to_owned()))
        };
        let value = |value: &PolicyValue| match value {
            PolicyValue::SessionRef(path) => match (path.as_slice(), outer) {
                ([row, name], Some(outer))
                    if row == OUTER_ROW && encrypted(entries, outer, name) =>
                {
                    Some((outer.to_owned(), name.clone()))
                }
                _ => None,
            },
            _ => None,
        };
        match expr {
            PolicyExpr::Cmp {
                column: name,
                value: v,
                ..
            }
            | PolicyExpr::Contains {
                column: name,
                value: v,
            } => column(name).or_else(|| value(v)),
            PolicyExpr::InList {
                column: name,
                values,
            } => column(name).or_else(|| values.iter().find_map(value)),
            PolicyExpr::IsNull { column: name }
            | PolicyExpr::IsNotNull { column: name }
            | PolicyExpr::In { column: name, .. } => column(name),
            PolicyExpr::Exists {
                table: inner,
                condition,
            } => reads(condition, inner, Some(table), entries),
            PolicyExpr::And(items) | PolicyExpr::Or(items) => items
                .iter()
                .find_map(|item| reads(item, table, outer, entries)),
            PolicyExpr::Not(inner) => reads(inner, table, outer, entries),
            PolicyExpr::ExistsRel { rel } => relation_reads(rel, table, entries),
            _ => None,
        }
    }
    /// A relation's column references, conservatively: a scoped one names
    /// the table its scope aliases (or, unresolved, any table the relation
    /// or the policy reads); an unscoped one any of them.
    fn relation_reads(
        rel: &crate::tools::public_schema::RelationExpr,
        table: &str,
        entries: &Entries,
    ) -> Option<(String, String)> {
        fn walk(
            value: &serde_json::Value,
            scans: &mut Vec<(String, Option<String>)>,
            columns: &mut Vec<(Option<String>, String)>,
        ) {
            match value {
                serde_json::Value::Object(map) => {
                    if let Some(serde_json::Value::Object(scan)) = map.get("TableScan")
                        && let Some(serde_json::Value::String(t)) = scan.get("table")
                    {
                        let alias = scan
                            .get("alias")
                            .and_then(|a| a.as_str())
                            .map(str::to_owned);
                        scans.push((t.clone(), alias));
                    }
                    if let Some(serde_json::Value::String(column)) = map.get("column") {
                        let scope = map.get("scope").and_then(|s| s.as_str()).map(str::to_owned);
                        columns.push((scope, column.clone()));
                    }
                    map.values().for_each(|item| walk(item, scans, columns));
                }
                serde_json::Value::Array(items) => {
                    items.iter().for_each(|item| walk(item, scans, columns));
                }
                _ => {}
            }
        }
        let (mut scans, mut columns) = (Vec::new(), Vec::new());
        walk(
            &serde_json::to_value(rel).unwrap_or_default(),
            &mut scans,
            &mut columns,
        );
        let every: Vec<&str> = scans
            .iter()
            .map(|(t, _)| t.as_str())
            .chain(std::iter::once(table))
            .collect();
        columns.into_iter().find_map(|(scope, column)| {
            let scoped: Option<&str> = scope.as_deref().and_then(|scope| {
                scans
                    .iter()
                    .find(|(t, alias)| alias.as_deref() == Some(scope) || t == scope)
                    .map(|(t, _)| t.as_str())
            });
            let candidates = match scoped {
                Some(one) => vec![one],
                None => every.clone(),
            };
            candidates
                .into_iter()
                .find(|t| encrypted(entries, t, &column))
                .map(|t| (t.to_owned(), column.clone()))
        })
    }
    for (table, policies) in permissions {
        let clauses = [
            ("read", &policies.select),
            ("insert", &policies.insert),
            ("update", &policies.update),
            ("delete", &policies.delete),
        ];
        for (op, policy) in clauses {
            if let Some((owner, column)) = [&policy.using, &policy.with_check]
                .into_iter()
                .flatten()
                .find_map(|expr| reads(expr, table.as_str(), None, entries))
            {
                return Err(format!(
                    "tables.{table}.{op}: the policy reads {owner}.{column}, an encrypted \
                     column; the server sees only its ciphertext. Write the policy over \
                     plaintext columns"
                ));
            }
        }
    }
    Ok(())
}

/// Refuse an app policy that reaches a system table: `exists` over any but
/// the members table, inheritance from one, or a relation reading one
/// (protocol §1.3). `InheritsFrom` and `InsertedByThisTransaction` are the
/// permissions reader's to refuse.
pub fn reaches_system_table(expr: &PolicyExpr) -> Option<String> {
    fn relation_tables(value: &serde_json::Value, out: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, item) in map {
                    match (key.as_str(), item) {
                        ("table", serde_json::Value::String(t)) => out.push(t.clone()),
                        _ => relation_tables(item, out),
                    }
                }
            }
            serde_json::Value::Array(items) => {
                items.iter().for_each(|i| relation_tables(i, out));
            }
            _ => {}
        }
    }
    match expr {
        PolicyExpr::Exists { table, condition } => {
            if is_system_table(table) && table != MEMBERS {
                return Some(table.clone());
            }
            reaches_system_table(condition)
        }
        PolicyExpr::InheritsReferencing { source_table, .. } if is_system_table(source_table) => {
            Some(source_table.clone())
        }
        PolicyExpr::ExistsRel { rel } => {
            let mut tables = Vec::new();
            relation_tables(&serde_json::to_value(rel).unwrap_or_default(), &mut tables);
            tables
                .into_iter()
                .find(|t| is_system_table(t) && t != MEMBERS)
        }
        PolicyExpr::And(items) | PolicyExpr::Or(items) => {
            items.iter().find_map(reaches_system_table)
        }
        PolicyExpr::Not(inner) => reaches_system_table(inner),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Declarations the server keeps per schema version
// ---------------------------------------------------------------------------

/// The name and version of the stored form of one schema version's
/// declarations: this line, a newline, then the entries as canonical JSON
/// (keys sorted at every level, no whitespace). The empty object records
/// a deployment without declarations. Nonempty v1 records read as before,
/// and a version deployed before empty records were written can still be
/// identified in the catalogue. Older hosts decode the empty record and
/// require declarations on redeploy because they treat every record as
/// nonempty.
pub const STORED_DECLARATIONS_V1: &str = "jazz-e2ee-declarations-v1";

/// One schema version's entries in their stored form
/// ([`STORED_DECLARATIONS_V1`]).
pub fn encode_stored(entries: &Entries) -> Result<Vec<u8>, String> {
    let json = crate::catalogue::canonical_json(entries).map_err(|error| error.to_string())?;
    Ok(format!("{STORED_DECLARATIONS_V1}\n{json}").into_bytes())
}

/// The entries [`encode_stored`] wrote. Another form is refused, never
/// guessed at.
pub fn decode_stored(bytes: &[u8]) -> Result<Entries, String> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| "stored encryption declarations are not text".to_owned())?;
    let Some(json) = text
        .strip_prefix(STORED_DECLARATIONS_V1)
        .and_then(|rest| rest.strip_prefix('\n'))
    else {
        return Err(format!(
            "stored encryption declarations are not in the `{STORED_DECLARATIONS_V1}` form; a \
             newer Jazz wrote them"
        ));
    };
    serde_json::from_str(json)
        .map_err(|error| format!("stored encryption declarations don't read: {error}"))
}

/// A deployed version keeps its first encryption declarations. When a change
/// would also invalidate existing cells, report the column and the remedy.
pub fn check_redeclaration(
    schema: &Schema,
    stored: &Entries,
    given: &Entries,
) -> Result<(), String> {
    if stored == given {
        return Ok(());
    }
    let same_version = PublishMigrationRequest {
        from_hash: String::new(),
        to_hash: String::new(),
        forward: Vec::new(),
    };
    check_transition((schema, Some(stored)), (schema, Some(given)), &same_version).map_err(
        |reason| {
            if stored.is_empty() {
                format!(
                    "this schema version was deployed without encryption declarations: {reason}"
                )
            } else {
                format!(
                    "this schema version was deployed with other encryption declarations, and \
                     its cells are sealed under them: {reason}"
                )
            }
        },
    )?;
    Err("the encryption declarations of a deployed schema version are immutable; deploy a new schema version to change them".to_owned())
}

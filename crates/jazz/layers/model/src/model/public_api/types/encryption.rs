//! Encryption declarations: what an app file says about its encrypted
//! tables (E2EE protocol §1.5, migrations §3.2).
//!
//! These are plain data. They live outside [`Schema`](super::Schema), so they
//! never change the schema hash. The app-file reader produces them, the
//! loaders return them with each schema version, the deploy request carries
//! them, and the client's encryption hooks read them. Core never interprets
//! the cryptographic settings: [`EncryptionSettings::mechanisms`] is passed
//! through unread to the client runtime.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use jazz_types::app_id::AppId;

use super::ColumnType;

/// Everything a client needs to know about one app's encryption, for the
/// schema version it runs.
#[derive(Debug, Clone, PartialEq)]
pub struct E2eeDeclarations {
    /// The application, part of every cell's domain.
    pub app_id: AppId,
    /// The environment label, part of every cell's domain.
    pub environment: String,
    /// The app-wide settings (the top-level `encryption` object).
    pub settings: EncryptionSettings,
    /// One entry per encrypted table, keyed by table name.
    pub tables: BTreeMap<String, EncryptionEntry>,
}

/// The app-wide settings: the top-level `encryption` object of the app file.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EncryptionSettings {
    /// Whether an account may be reset (protocol §2.11). Off by default.
    #[serde(default)]
    pub allow_account_reset: bool,
    /// The mechanism selection and allow-list (protocol §0), as written.
    /// Core does not read it; the client runtime does. `None` means the
    /// built-in mechanisms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mechanisms: Option<serde_json::Value>,
}

/// A table's `encryption` entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EncryptionEntry {
    /// The column holding the space row's ID: `id` when the table's rows
    /// are their own spaces.
    pub space: String,
    /// The space table.
    pub scope: String,
    /// The encrypted columns, keyed by column name.
    pub columns: BTreeMap<String, EncryptedColumn>,
    /// The encrypted columns that carry equality tokens.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub equality: Vec<String>,
    /// Equality columns whose tokens are still being written
    /// (migrations §4.3).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backfill: Vec<String>,
    /// During a re-home, the space the rows are moving from
    /// (migrations §3.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rehome: Option<Rehome>,
    /// Who sweeps conversions in the background (migrations §3.5).
    #[serde(default, skip_serializing_if = "SweepBy::is_default")]
    pub sweep: SweepBy,
}

/// One encrypted column: its plaintext type and, during a conversion, the
/// conversion fields of migrations §3.2.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EncryptedColumn {
    /// The plaintext type the application reads and writes.
    #[serde(rename = "type")]
    pub plain: PlainType,
    /// The type the column's cells are bound to, when a later schema widened
    /// the type in an encoding-compatible way (migrations §4.3). `None`
    /// means the declared type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed_type: Option<PlainType>,
    /// The source of an encrypt or re-encrypt conversion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// The plaintext target of a decrypt conversion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    /// The function converting the source type to this column's type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convert: Option<String>,
    /// The function listing every source value that converts to a target
    /// value (at most 16), for equality queries during a conversion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reverse: Option<String>,
}

impl EncryptedColumn {
    /// The type the column's cells are bound to (protocol §4.1).
    pub fn cell_type(&self) -> &PlainType {
        self.sealed_type.as_ref().unwrap_or(&self.plain)
    }
}

/// A plaintext type with its nullability, as an app file states it
/// (`"string"`, `"string?"`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlainType {
    /// The column type.
    pub column_type: ColumnType,
    /// Whether the value may be null.
    #[serde(default)]
    pub nullable: bool,
}

/// The old space of a re-home.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Rehome {
    /// The old space column.
    pub space: String,
    /// The old space table.
    pub scope: String,
}

/// Who runs a table's background conversion sweep.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SweepBy {
    /// Sessions that may update the space row and hold its key.
    #[default]
    Updaters,
    /// Every member holding the key.
    Members,
}

impl SweepBy {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

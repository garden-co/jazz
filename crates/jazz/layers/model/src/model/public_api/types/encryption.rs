//! Encryption declarations: what an app file says about its encrypted
//! tables (E2EE protocol §1.5, E2EE_SCHEMA_CHANGES.md §3.1).
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

use super::{ColumnType, Value};

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
}

/// One encrypted column and the type its stored cells were sealed under.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EncryptedColumn {
    /// The plaintext type the application reads and writes.
    #[serde(rename = "type")]
    pub plain: PlainType,
    /// The type the column's cells are bound to, when a later schema widened
    /// the type in an encoding-compatible way (E2EE_SCHEMA_CHANGES.md §3.2). `None`
    /// means the declared type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed_type: Option<PlainType>,
}

impl EncryptedColumn {
    /// The type the column's cells are bound to (protocol §4.1).
    pub fn cell_type(&self) -> &PlainType {
        self.sealed_type.as_ref().unwrap_or(&self.plain)
    }
}

/// A plaintext type with its nullability, as an app file states it
/// (`"string"`, `"string?"`), and the column's declared plaintext default
/// (`{ "type": "string", "default": "untitled" }`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlainType {
    /// The column type.
    #[serde(deserialize_with = "read_column_type")]
    pub column_type: ColumnType,
    /// Whether the value may be null.
    #[serde(default)]
    pub nullable: bool,
    /// What a cell that was never written reads as (protocol §4.1): rows
    /// stored before a migration added the column, say. `None`: no
    /// default. Not part of the type the cells are sealed under, and
    /// absent from the stored form when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
}

// Serde accepts extra fields on the shared ColumnType's unit variants. An
// encrypted declaration must account for every field, including inside arrays.
fn read_column_type<'de, D: serde::Deserializer<'de>>(reader: D) -> Result<ColumnType, D::Error> {
    if !reader.is_human_readable() {
        return ColumnType::deserialize(reader);
    }
    // Read the tagged type directly as well as its fields: a JSON map alone
    // would discard duplicate keys before the normal type reader sees them.
    #[derive(Deserialize)]
    struct DeclarationType {
        #[serde(flatten)]
        fields: serde_json::Value,
        #[serde(flatten)]
        parsed: ColumnType,
    }
    let value = DeclarationType::deserialize(reader)?;
    check_type_keys(&value.fields, "columnType").map_err(serde::de::Error::custom)?;
    Ok(value.parsed)
}

fn check_type_keys(raw: &serde_json::Value, path: &str) -> Result<(), String> {
    let Some(fields) = raw.as_object() else {
        return Ok(());
    };
    match raw["type"].as_str() {
        Some("Array") => check_type_keys(&raw["element"], &format!("{path}.element"))?,
        Some("Row") => check_field_types(&raw["columns"], &format!("{path}.columns"))?,
        Some("EnumPayload") => {
            if let Some(cases) = raw["cases"].as_array() {
                for (i, case) in cases.iter().enumerate() {
                    check_field_types(&case["fields"], &format!("{path}.cases[{i}].fields"))?;
                }
            }
        }
        Some("Enum" | "ScalarEnum" | "Json") => (),
        Some(_) => {
            if let Some(key) = fields.keys().find(|name| *name != "type") {
                return Err(format!("unknown field `{key}` in {path}; expected `type`"));
            }
        }
        None => (),
    }
    Ok(())
}

fn check_field_types(fields: &serde_json::Value, path: &str) -> Result<(), String> {
    if let Some(fields) = fields.as_array() {
        for (i, field) in fields.iter().enumerate() {
            check_type_keys(&field["column_type"], &format!("{path}[{i}].column_type"))?;
        }
    }
    Ok(())
}

impl PlainType {
    /// A type without a declared default.
    pub fn new(column_type: ColumnType, nullable: bool) -> Self {
        Self {
            column_type,
            nullable,
            default: None,
        }
    }

    /// Whether `other` is the same type, its default aside: what cells are
    /// sealed under (protocol §4.1).
    pub fn same_type(&self, other: &PlainType) -> bool {
        self.column_type == other.column_type && self.nullable == other.nullable
    }
}

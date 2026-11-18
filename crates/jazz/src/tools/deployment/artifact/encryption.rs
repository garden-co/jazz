//! An app's encryption declarations as the deploy carries them (E2EE
//! protocol §1.5), and what the deploy refuses of an app policy beside
//! Jazz's system tables.
//!
//! The declarations stay outside the schema and its hash. They travel in the
//! deploy request as [`DeclaredEncryption`], one set per schema version.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::tools::public_schema::{EncryptionEntry, EncryptionSettings, PolicyExpr};

use super::system_tables::{MEMBERS, is_system_table};

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

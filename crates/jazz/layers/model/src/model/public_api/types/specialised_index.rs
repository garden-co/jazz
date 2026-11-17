//! Specialised index declarations (SPEC 20.1).
//!
//! A table declares text, vector and geo indexes next to its B-tree
//! indexes. They are derived state: part of the table's version id, not of
//! its compatibility id, and never of how rows are stored or synced. The
//! declaration lives here, shared by the public and the runtime schema;
//! the shadow tables and their maintenance live in `jazz-db`, the kinds'
//! pure functions in `jazz-index`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Longest specialised index name, in bytes.
pub const SPECIALISED_INDEX_NAME_MAX: usize = 32;

/// Text analyzers this version knows (SPEC 20.10.1). Mirrors
/// `jazz_index::text::ANALYZERS` (jazz-model cannot depend on jazz-index):
/// change both together.
pub const TEXT_ANALYZERS: &[&str] = &["jazz-text/1:en", "jazz-text/1:simple"];

/// Highest weight of a text index field.
pub const TEXT_WEIGHT_MAX: u8 = 15;

/// A text field's reference length in terms when it declares none.
/// Mirrors `jazz_index::text::DEFAULT_FIELD_LENGTH`.
pub const TEXT_FIELD_LENGTH_DEFAULT: u16 = 32;

/// Longest reference length a text field may declare. Mirrors
/// `jazz_index::text::MAX_FIELD_LENGTH`.
pub const TEXT_FIELD_LENGTH_MAX: u16 = 4096;

/// One field of a text index: its weight and its reference length in terms
/// (SPEC 20.10.1). `"title": 4` is `{ weight: 4, length: 32 }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TextField {
    /// 1–[`TEXT_WEIGHT_MAX`].
    pub weight: u8,
    /// 1–[`TEXT_FIELD_LENGTH_MAX`].
    pub length: u16,
}

impl TextField {
    /// A field of `weight` with the default reference length.
    pub fn weight(weight: u8) -> Self {
        Self {
            weight,
            length: TEXT_FIELD_LENGTH_DEFAULT,
        }
    }
}

impl From<u8> for TextField {
    fn from(weight: u8) -> Self {
        Self::weight(weight)
    }
}

/// Domain tag of [`SpecialisedIndex::canonical_bytes`].
pub const SPECIALISED_INDEX_DECL_ENCODING: &str = "jazz-ix-decl/1";

/// One specialised index of a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecialisedIndex {
    /// Unique within the table: `[a-z][a-z0-9_]*`, at most
    /// [`SPECIALISED_INDEX_NAME_MAX`] bytes.
    pub name: String,
    pub kind: SpecialisedIndexKind,
    /// Not maintained on clients; queries using it run at `Remote` tier
    /// only (SPEC 20.9).
    #[serde(default, skip_serializing_if = "is_false")]
    pub server_only: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// What a specialised index indexes, by kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpecialisedIndexKind {
    /// Word search over string columns.
    Text {
        /// Column → its weight and reference length.
        fields: BTreeMap<String, TextField>,
        /// One of [`TEXT_ANALYZERS`].
        analyzer: String,
        /// The last query term also matches as a prefix.
        #[serde(default, skip_serializing_if = "is_false")]
        prefix: bool,
    },
    /// Nearest-neighbour search over a vector column.
    Vector {
        column: String,
        metric: VectorMetric,
        #[serde(default)]
        ann: VectorAnn,
    },
    /// Points on the Earth, from two `f64` columns in degrees (WGS84).
    Geo {
        /// A `point` column.
        column: String,
    },
}

/// A vector index's distance (SPEC 20.10.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VectorMetric {
    Cosine,
    L2,
    Dot,
}

impl VectorMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cosine => "cosine",
            Self::L2 => "l2",
            Self::Dot => "dot",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "cosine" => Some(Self::Cosine),
            "l2" => Some(Self::L2),
            "dot" => Some(Self::Dot),
            _ => None,
        }
    }
}

/// How a vector index chooses candidates. Only exact search exists yet;
/// approximate kinds will choose candidates but never change scores.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VectorAnn {
    #[default]
    Exact,
}

/// The kind of a specialised index, without its options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SpecialisedIndexKindName {
    Text,
    Vector,
    Geo,
}

impl SpecialisedIndexKindName {
    /// The kind's key in `jazz.app.json` and its `_jazz_ix.kind`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Vector => "vector",
            Self::Geo => "geo",
        }
    }
}

impl std::fmt::Display for SpecialisedIndexKindName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl SpecialisedIndexKind {
    pub fn kind_name(&self) -> SpecialisedIndexKindName {
        match self {
            Self::Text { .. } => SpecialisedIndexKindName::Text,
            Self::Vector { .. } => SpecialisedIndexKindName::Vector,
            Self::Geo { .. } => SpecialisedIndexKindName::Geo,
        }
    }

    /// The columns whose values the index reads, in a fixed order: text
    /// fields by name, the vector column, geo `lat` then `lng`.
    pub fn columns(&self) -> Vec<&str> {
        match self {
            Self::Text { fields, .. } => fields.keys().map(String::as_str).collect(),
            Self::Vector { column, .. } => vec![column.as_str()],
            Self::Geo { column } => vec![column.as_str()],
        }
    }

    /// The same declaration with each column renamed by `rename` (to stored
    /// names, say).
    pub fn map_columns(&self, mut rename: impl FnMut(&str) -> String) -> Self {
        match self {
            Self::Text {
                fields,
                analyzer,
                prefix,
            } => Self::Text {
                fields: fields
                    .iter()
                    .map(|(column, field)| (rename(column), *field))
                    .collect(),
                analyzer: analyzer.clone(),
                prefix: *prefix,
            },
            Self::Vector {
                column,
                metric,
                ann,
            } => Self::Vector {
                column: rename(column),
                metric: *metric,
                ann: *ann,
            },
            Self::Geo { column } => Self::Geo {
                column: rename(column),
            },
        }
    }
}

impl SpecialisedIndex {
    /// The declaration's canonical bytes, encoding `jazz-ix-decl/1`: the
    /// domain tag, then the name, the kind's tag byte (text 0, vector 1,
    /// geo 2), its options, and the `server_only` byte. Strings are
    /// length-prefixed (`u64` big-endian), as in the schema id encoding.
    ///
    /// - text: field count, then per field (by name) the name, the
    ///   weight byte and the reference length (`u16` big-endian); the
    ///   analyzer; the prefix byte;
    /// - vector: the column; the metric byte (cosine 0, l2 1, dot 2); the
    ///   ann byte (exact 0);
    /// - geo: the point column.
    ///
    /// Pinned by fixtures: a change is a new encoding.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        put_str(&mut bytes, SPECIALISED_INDEX_DECL_ENCODING);
        put_str(&mut bytes, &self.name);
        match &self.kind {
            SpecialisedIndexKind::Text {
                fields,
                analyzer,
                prefix,
            } => {
                bytes.push(0);
                put_u64(&mut bytes, fields.len() as u64);
                for (column, field) in fields {
                    put_str(&mut bytes, column);
                    bytes.push(field.weight);
                    bytes.extend_from_slice(&field.length.to_be_bytes());
                }
                put_str(&mut bytes, analyzer);
                bytes.push(u8::from(*prefix));
            }
            SpecialisedIndexKind::Vector {
                column,
                metric,
                ann,
            } => {
                bytes.push(1);
                put_str(&mut bytes, column);
                bytes.push(match metric {
                    VectorMetric::Cosine => 0,
                    VectorMetric::L2 => 1,
                    VectorMetric::Dot => 2,
                });
                bytes.push(match ann {
                    VectorAnn::Exact => 0,
                });
            }
            SpecialisedIndexKind::Geo { column } => {
                bytes.push(2);
                put_str(&mut bytes, column);
            }
        }
        bytes.push(u8::from(self.server_only));
        bytes
    }
}

impl SpecialisedIndex {
    /// The SHA-256 of [`Self::canonical_bytes`]: what a search names the
    /// declaration it was written against by (SPEC 20.4).
    pub fn declaration_hash(&self) -> [u8; 32] {
        *jazz_types::sha256::hash(&self.canonical_bytes()).as_bytes()
    }
}

/// Whether `name` is a valid specialised index name.
pub fn is_specialised_index_name(name: &str) -> bool {
    let mut chars = name.chars();
    name.len() <= SPECIALISED_INDEX_NAME_MAX
        && chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Sort a table's specialised indexes by name, their canonical order.
pub fn canonical_specialised_index_order(indexes: &mut [SpecialisedIndex]) {
    indexes.sort_by(|left, right| left.name.cmp(&right.name));
}

fn put_str(bytes: &mut Vec<u8>, value: &str) {
    put_u64(bytes, value.len() as u64);
    bytes.extend_from_slice(value.as_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

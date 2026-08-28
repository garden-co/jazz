use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use jazz::tools::ObjectId;
use jazz::tools::metadata::MetadataKey;
use jazz::tools::public_schema::{ColumnDescriptor, ColumnType, RowDescriptor, Value};

use jazz::tools::admin_catalogue_row_format::{decode_row, encode_row};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogueEntry {
    pub object_id: ObjectId,
    pub metadata: HashMap<String, String>,
    pub content: Vec<u8>,
}

impl CatalogueEntry {
    pub fn object_type(&self) -> Option<&str> {
        self.metadata
            .get(MetadataKey::Type.as_str())
            .map(String::as_str)
    }

    pub(crate) fn encode_storage_row(&self) -> Result<Vec<u8>, String> {
        let descriptor = storage_descriptor();
        let metadata_json = canonical_metadata_json(&self.metadata)?;
        let values = vec![
            Value::Bytea(metadata_json),
            Value::Bytea(self.content.clone()),
        ];
        encode_row(&descriptor, &values).map_err(|err| err.to_string())
    }

    pub(crate) fn decode_storage_row(object_id: ObjectId, bytes: &[u8]) -> Result<Self, String> {
        let descriptor = storage_descriptor();
        let values = decode_row(&descriptor, bytes).map_err(|err| err.to_string())?;
        let [Value::Bytea(metadata_json), Value::Bytea(content)] = values.as_slice() else {
            return Err("unexpected catalogue row shape".to_string());
        };
        let metadata: HashMap<String, String> =
            serde_json::from_slice(metadata_json).map_err(|err| err.to_string())?;
        if canonical_metadata_json(&metadata)?.as_slice() != metadata_json {
            return Err("noncanonical catalogue metadata JSON".to_owned());
        }

        Ok(Self {
            object_id,
            metadata,
            content: content.clone(),
        })
    }
}

/// The catalogue row is an epoch-pinned durable payload. `HashMap` iteration
/// deliberately has no stable order, so serialize a key-sorted map instead
/// and reject a decodable alternate spelling at recovery. This is a local
/// catalogue encoding rule; it does not make arbitrary JSON a canonical Jazz
/// value encoding.
fn canonical_metadata_json(metadata: &HashMap<String, String>) -> Result<Vec<u8>, String> {
    let sorted = metadata.iter().collect::<BTreeMap<_, _>>();
    serde_json::to_vec(&sorted).map_err(|err| err.to_string())
}

fn storage_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new("metadata", ColumnType::Bytea),
        ColumnDescriptor::new("content", ColumnType::Bytea),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(metadata: impl IntoIterator<Item = (&'static str, &'static str)>) -> CatalogueEntry {
        CatalogueEntry {
            object_id: ObjectId::from_uuid(uuid::Uuid::from_bytes([0x4a; 16])),
            metadata: metadata
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
            content: b"catalogue-content".to_vec(),
        }
    }

    // This intentionally reaches the storage-row codec directly: the rule is
    // about byte identity at the durable server-catalogue boundary, which a
    // public catalogue query cannot expose.
    #[test]
    fn catalogue_entry_metadata_json_is_sorted_and_noncanonical_spelling_is_rejected() {
        let first = entry([("zeta", "two"), ("alpha", "one")]);
        let second = entry([("alpha", "one"), ("zeta", "two")]);
        let first_bytes = first.encode_storage_row().expect("entry encodes");
        let expected = b"\x1c\0\0\0{\"alpha\":\"one\",\"zeta\":\"two\"}catalogue-content";
        assert_eq!(
            first_bytes, expected,
            "the epoch-one catalogue-entry fixture must remain byte-identical"
        );
        assert_eq!(
            first_bytes,
            second.encode_storage_row().expect("entry encodes"),
            "equivalent metadata must not inherit HashMap iteration order"
        );

        let canonical_metadata = br#"{"alpha":"one","zeta":"two"}"#;
        let noncanonical_metadata = br#"{"zeta":"two","alpha":"one"}"#;
        assert_eq!(canonical_metadata.len(), noncanonical_metadata.len());
        let offset = first_bytes
            .windows(canonical_metadata.len())
            .position(|window| window == canonical_metadata)
            .expect("row contains its canonical metadata bytes");
        let mut noncanonical_bytes = first_bytes.clone();
        noncanonical_bytes[offset..offset + canonical_metadata.len()]
            .copy_from_slice(noncanonical_metadata);
        assert_eq!(
            CatalogueEntry::decode_storage_row(first.object_id, &noncanonical_bytes)
                .expect_err("alternate map order is not an epoch-one spelling"),
            "noncanonical catalogue metadata JSON"
        );
    }
}

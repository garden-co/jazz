use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::metadata::MetadataKey;
use crate::object::ObjectId;
#[cfg(any(test, all(feature = "rocksdb", not(target_arch = "wasm32"))))]
use crate::{
    admin_catalogue_row_format::{decode_row, encode_row},
    public_api::types::{ColumnDescriptor, ColumnType, RowDescriptor, Value},
};

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

    #[cfg(any(test, all(feature = "rocksdb", not(target_arch = "wasm32"))))]
    pub(crate) fn encode_storage_row(&self) -> Result<Vec<u8>, String> {
        let descriptor = storage_descriptor();
        let metadata_json = serde_json::to_vec(&self.metadata).map_err(|err| err.to_string())?;
        let values = vec![
            Value::Bytea(metadata_json),
            Value::Bytea(self.content.clone()),
        ];
        encode_row(&descriptor, &values).map_err(|err| err.to_string())
    }

    #[cfg(any(test, all(feature = "rocksdb", not(target_arch = "wasm32"))))]
    pub(crate) fn decode_storage_row(object_id: ObjectId, bytes: &[u8]) -> Result<Self, String> {
        let descriptor = storage_descriptor();
        let values = decode_row(&descriptor, bytes).map_err(|err| err.to_string())?;
        let [Value::Bytea(metadata_json), Value::Bytea(content)] = values.as_slice() else {
            return Err("unexpected catalogue row shape".to_string());
        };
        let metadata: HashMap<String, String> =
            serde_json::from_slice(metadata_json).map_err(|err| err.to_string())?;

        Ok(Self {
            object_id,
            metadata,
            content: content.clone(),
        })
    }
}

#[cfg(any(test, all(feature = "rocksdb", not(target_arch = "wasm32"))))]
fn storage_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new("metadata", ColumnType::Bytea),
        ColumnDescriptor::new("content", ColumnType::Bytea),
    ])
}

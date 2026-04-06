use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::metadata::{MetadataKey, ObjectType};
use crate::object::ObjectId;
use crate::query_manager::encoding::{decode_row, encode_row};
use crate::query_manager::types::{ColumnDescriptor, ColumnType, RowDescriptor, Value};

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

    pub fn is_catalogue(&self) -> bool {
        self.object_type()
            .is_some_and(ObjectType::is_catalogue_type_str)
    }

    pub fn is_structural_schema_catalogue(&self) -> bool {
        self.object_type() == Some(ObjectType::CatalogueSchema.as_str())
    }

    pub fn encode_storage_row(&self) -> Result<Vec<u8>, String> {
        let descriptor = storage_descriptor();
        let values = vec![
            Value::Text(self.object_type().unwrap_or_default().to_string()),
            nullable_text(MetadataKey::AppId.as_str(), &self.metadata),
            nullable_text(MetadataKey::SchemaHash.as_str(), &self.metadata),
            nullable_text(MetadataKey::SourceHash.as_str(), &self.metadata),
            nullable_text(MetadataKey::TargetHash.as_str(), &self.metadata),
            Value::Bytea(self.content.clone()),
        ];
        encode_row(&descriptor, &values).map_err(|err| err.to_string())
    }

    pub fn decode_storage_row(object_id: ObjectId, bytes: &[u8]) -> Result<Self, String> {
        let descriptor = storage_descriptor();
        let values = decode_row(&descriptor, bytes).map_err(|err| err.to_string())?;
        let [
            Value::Text(type_str),
            app_id,
            schema_hash,
            source_hash,
            target_hash,
            Value::Bytea(content),
        ] = values.as_slice()
        else {
            return Err("unexpected catalogue row shape".to_string());
        };

        let mut metadata = HashMap::new();
        metadata.insert(MetadataKey::Type.to_string(), type_str.clone());
        insert_nullable_text(&mut metadata, MetadataKey::AppId, app_id);
        insert_nullable_text(&mut metadata, MetadataKey::SchemaHash, schema_hash);
        insert_nullable_text(&mut metadata, MetadataKey::SourceHash, source_hash);
        insert_nullable_text(&mut metadata, MetadataKey::TargetHash, target_hash);

        Ok(Self {
            object_id,
            metadata,
            content: content.clone(),
        })
    }
}

fn storage_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new(MetadataKey::Type.as_str(), ColumnType::Text),
        ColumnDescriptor::new(MetadataKey::AppId.as_str(), ColumnType::Text).nullable(),
        ColumnDescriptor::new(MetadataKey::SchemaHash.as_str(), ColumnType::Text).nullable(),
        ColumnDescriptor::new(MetadataKey::SourceHash.as_str(), ColumnType::Text).nullable(),
        ColumnDescriptor::new(MetadataKey::TargetHash.as_str(), ColumnType::Text).nullable(),
        ColumnDescriptor::new("content", ColumnType::Bytea),
    ])
}

fn nullable_text(key: &str, metadata: &HashMap<String, String>) -> Value {
    metadata
        .get(key)
        .cloned()
        .map(Value::Text)
        .unwrap_or(Value::Null)
}

fn insert_nullable_text(metadata: &mut HashMap<String, String>, key: MetadataKey, value: &Value) {
    if let Value::Text(text) = value {
        metadata.insert(key.to_string(), text.clone());
    }
}

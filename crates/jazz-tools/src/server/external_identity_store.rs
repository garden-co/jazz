use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::query_manager::query::QueryBuilder;
use crate::query_manager::types::{
    ColumnType, RowDescriptor, SchemaBuilder, TableName, TableSchema, Value,
};
use crate::runtime_core::ReadDurabilityOptions;
use crate::runtime_tokio::TokioRuntime;
use crate::schema_manager::{AppId, SchemaManager};
use crate::server::DynStorage;
use crate::sync_manager::{DurabilityTier, SyncManager};

const EXTERNAL_IDENTITIES_TABLE: &str = "external_identities";

#[derive(Debug, Clone)]
pub struct ExternalIdentityRow {
    pub issuer: String,
    pub subject: String,
    pub principal_id: String,
}

/// Persistent storage for external identity -> principal mappings.
pub struct ExternalIdentityStore {
    runtime: TokioRuntime<DynStorage>,
    insert_descriptor: RowDescriptor,
    read_descriptor: RowDescriptor,
}

impl ExternalIdentityStore {
    pub fn new_with_storage(storage: DynStorage) -> Result<Self, String> {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder(EXTERNAL_IDENTITIES_TABLE)
                    .column("app_id", ColumnType::Uuid)
                    .column("issuer", ColumnType::Text)
                    .column("subject", ColumnType::Text)
                    .column("principal_id", ColumnType::Text)
                    .column("created_at", ColumnType::Timestamp)
                    .column("updated_at", ColumnType::Timestamp),
            )
            .build();

        let insert_descriptor = schema
            .get(&TableName::new(EXTERNAL_IDENTITIES_TABLE))
            .ok_or_else(|| "meta schema missing external_identities table".to_string())?
            .columns
            .clone();
        let mut read_descriptor = insert_descriptor.clone();
        normalize_row_descriptor(&mut read_descriptor);

        let sync_manager = SyncManager::new()
            .with_durability_tiers([DurabilityTier::EdgeServer, DurabilityTier::GlobalServer]);
        let schema_manager = SchemaManager::new(
            sync_manager,
            schema,
            AppId::from_name("jazz-tools-meta"),
            "meta",
            "main",
        )
        .map_err(|e| format!("failed to initialize meta schema manager: {e:?}"))?;

        let runtime = TokioRuntime::new(schema_manager, storage, |_entry| {});
        Ok(Self {
            runtime,
            insert_descriptor,
            read_descriptor,
        })
    }

    pub async fn list_external_identities(
        &self,
        app_id: AppId,
    ) -> Result<Vec<ExternalIdentityRow>, String> {
        let query = QueryBuilder::new(EXTERNAL_IDENTITIES_TABLE)
            .filter_eq("app_id", Value::Uuid(app_id.as_object_id()))
            .build();

        let future = self
            .runtime
            .query(query, None, ReadDurabilityOptions::default())
            .map_err(|e| format!("external identity query error: {e}"))?;
        let rows = future
            .await
            .map_err(|e| format!("external identity query await error: {e}"))?;

        rows.into_iter()
            .map(|(_, values)| self.decode_external_identity_row(&values))
            .collect()
    }

    pub async fn get_external_identity(
        &self,
        app_id: AppId,
        issuer: &str,
        subject: &str,
    ) -> Result<Option<ExternalIdentityRow>, String> {
        let query = QueryBuilder::new(EXTERNAL_IDENTITIES_TABLE)
            .filter_eq("app_id", Value::Uuid(app_id.as_object_id()))
            .filter_eq("issuer", Value::Text(issuer.to_string()))
            .filter_eq("subject", Value::Text(subject.to_string()))
            .build();

        let future = self
            .runtime
            .query(query, None, ReadDurabilityOptions::default())
            .map_err(|e| format!("external identity query error: {e}"))?;
        let mut rows = future
            .await
            .map_err(|e| format!("external identity query await error: {e}"))?;

        if let Some((_object_id, values)) = rows.pop() {
            Ok(Some(self.decode_external_identity_row(&values)?))
        } else {
            Ok(None)
        }
    }

    pub async fn create_external_identity(
        &self,
        app_id: AppId,
        issuer: &str,
        subject: &str,
        principal_id: &str,
    ) -> Result<(), String> {
        let now = now_timestamp_us();
        let values: HashMap<String, Value> = self
            .insert_descriptor
            .columns
            .iter()
            .map(|column| {
                let value = match column.name.as_str() {
                    "app_id" => Value::Uuid(app_id.as_object_id()),
                    "created_at" => Value::Timestamp(now),
                    "issuer" => Value::Text(issuer.to_string()),
                    "principal_id" => Value::Text(principal_id.to_string()),
                    "subject" => Value::Text(subject.to_string()),
                    "updated_at" => Value::Timestamp(now),
                    other => panic!("unexpected external identity column {other}"),
                };
                (column.name.to_string(), value)
            })
            .collect();

        self.runtime
            .insert(EXTERNAL_IDENTITIES_TABLE, values, None)
            .map_err(|e| format!("failed to insert external identity: {e}"))?;
        Ok(())
    }

    pub async fn close(&self) -> Result<(), String> {
        self.runtime
            .flush()
            .await
            .map_err(|e| format!("failed to flush external identity store: {e}"))?;
        self.runtime
            .with_storage(|storage| {
                storage.flush();
                storage.flush_wal();
                let _ = storage.close();
            })
            .map_err(|e| format!("failed to close external identity store: {e}"))?;
        Ok(())
    }

    fn decode_external_identity_row(
        &self,
        values: &[Value],
    ) -> Result<ExternalIdentityRow, String> {
        let issuer = match descriptor_value(&self.read_descriptor, values, "issuer") {
            Some(Value::Text(s)) => s.clone(),
            other => {
                return Err(format!(
                    "external identity field issuer expected text, got {other:?}"
                ));
            }
        };

        let subject = match descriptor_value(&self.read_descriptor, values, "subject") {
            Some(Value::Text(s)) => s.clone(),
            other => {
                return Err(format!(
                    "external identity field subject expected text, got {other:?}"
                ));
            }
        };

        let principal_id = match descriptor_value(&self.read_descriptor, values, "principal_id") {
            Some(Value::Text(s)) => s.clone(),
            other => {
                return Err(format!(
                    "external identity field principal_id expected text, got {other:?}"
                ));
            }
        };

        Ok(ExternalIdentityRow {
            issuer,
            subject,
            principal_id,
        })
    }
}

fn normalize_row_descriptor(descriptor: &mut RowDescriptor) {
    descriptor
        .columns
        .sort_unstable_by(|left, right| left.name.as_str().cmp(right.name.as_str()));
}

fn descriptor_value<'a>(
    descriptor: &RowDescriptor,
    values: &'a [Value],
    column: &str,
) -> Option<&'a Value> {
    descriptor
        .column_index(column)
        .and_then(|index| values.get(index))
}

fn now_timestamp_us() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_micros().min(u128::from(u64::MAX)) as u64,
        Err(_) => 0,
    }
}

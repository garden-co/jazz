//! Runtime E2EE state and storage integration.
//!
//! Pure crypto lives in `crate::e2ee`; this module owns mutable runtime state
//! and the local `$keys` lookup/write helpers.

use std::collections::HashMap;

use uuid::Uuid;

use crate::e2ee::{
    self, E2eeKeypair, E2eePublicKey, EncryptionContext, SpaceKey, derive_e2ee_keypair,
    seal_space_key, unseal_space_key,
};
use crate::identity::derive_user_id;
use crate::object::ObjectId;
use crate::query_manager::types::e2ee_schema::e2ee_keys_table_name;
use crate::query_manager::types::{RowDescriptor, TableName, Value};
use crate::row_format::decode_row;
use crate::storage::Storage;

use super::{RuntimeCore, RuntimeError, Scheduler};

#[derive(Default)]
pub struct E2eeService {
    keypair: Option<E2eeKeypair>,
    user_id: Option<ObjectId>,
    /// Space row id -> (key_id, unsealed key). v1: exactly one active key.
    space_keys: HashMap<ObjectId, (Uuid, SpaceKey)>,
    /// Key id -> unsealed key, for decrypting rows once the envelope is read.
    keys_by_id: HashMap<Uuid, SpaceKey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct E2eeKeyHolder {
    pub row_id: ObjectId,
    pub space_id: ObjectId,
    pub key_id: Uuid,
    pub recipient_user_id: ObjectId,
    pub recipient_public_key: String,
}

impl E2eeService {
    pub fn enable(&mut self, seed: &[u8; 32]) {
        self.keypair = Some(derive_e2ee_keypair(seed));
        self.user_id = Some(ObjectId::from_uuid(derive_user_id(seed)));
    }

    pub fn is_enabled(&self) -> bool {
        self.keypair.is_some()
    }

    pub fn public_key(&self) -> Option<&E2eePublicKey> {
        self.keypair.as_ref().map(|kp| &kp.public)
    }

    pub fn keypair(&self) -> Option<&E2eeKeypair> {
        self.keypair.as_ref()
    }

    pub fn user_id(&self) -> Option<ObjectId> {
        self.user_id
    }

    pub fn cached_space_key(&self, space_id: &ObjectId) -> Option<&(Uuid, SpaceKey)> {
        self.space_keys.get(space_id)
    }

    pub fn cached_key_by_id(&self, key_id: &Uuid) -> Option<&SpaceKey> {
        self.keys_by_id.get(key_id)
    }

    pub fn cache_space_key(&mut self, space_id: ObjectId, key_id: Uuid, key: SpaceKey) {
        self.keys_by_id.insert(key_id, key.clone());
        self.space_keys.insert(space_id, (key_id, key));
    }

    pub fn clear(&mut self) {
        self.keypair = None;
        self.user_id = None;
        self.space_keys.clear();
        self.keys_by_id.clear();
    }
}

impl<S: Storage, Sch: Scheduler> RuntimeCore<S, Sch> {
    pub fn enable_e2ee(&mut self, seed: &[u8; 32]) {
        self.e2ee.enable(seed);
    }

    pub fn clear_e2ee(&mut self) {
        self.e2ee.clear();
    }

    pub fn e2ee_public_key(&self) -> Option<E2eePublicKey> {
        self.e2ee.public_key().cloned()
    }

    pub(crate) fn space_key_for(
        &mut self,
        space_table: &str,
        space_id: ObjectId,
    ) -> Result<(Uuid, SpaceKey), RuntimeError> {
        if let Some((key_id, key)) = self.e2ee.cached_space_key(&space_id) {
            return Ok((*key_id, key.clone()));
        }

        let keys_table = e2ee_keys_table_name(space_table);
        let descriptor = self
            .schema_manager
            .current_schema()
            .get(&TableName::new(&keys_table))
            .ok_or_else(|| {
                RuntimeError::WriteError(format!("missing E2EE keys table `{keys_table}`"))
            })?
            .columns
            .clone();

        let Some(space_idx) = descriptor.column_index("space_id") else {
            return Err(RuntimeError::WriteError(format!(
                "E2EE keys table `{keys_table}` is missing space_id"
            )));
        };
        let Some(key_idx) = descriptor.column_index("key_id") else {
            return Err(RuntimeError::WriteError(format!(
                "E2EE keys table `{keys_table}` is missing key_id"
            )));
        };
        let Some(sealed_idx) = descriptor.column_index("sealed_key") else {
            return Err(RuntimeError::WriteError(format!(
                "E2EE keys table `{keys_table}` is missing sealed_key"
            )));
        };

        // Local `$keys` lookup deliberately scans visible rows: sealed copies
        // are ordinary rows, and bogus/unrelated rows are ignored below.
        let found = {
            let keypair = self
                .e2ee
                .keypair()
                .ok_or_else(|| RuntimeError::E2eeKeyUnavailable {
                    table: space_table.to_string(),
                    space_id: space_id.to_string(),
                })?;
            let mut found = None;
            for branch in self.schema_manager.all_branches() {
                let rows = self
                    .storage
                    .scan_visible_region(&keys_table, branch.as_str())
                    .map_err(|err| {
                        RuntimeError::QueryError(format!("scan `{keys_table}`: {err}"))
                    })?;
                for row in rows {
                    let Ok(values) = decode_row(&descriptor, &row.data) else {
                        continue;
                    };
                    if values.get(space_idx) != Some(&Value::Uuid(space_id)) {
                        continue;
                    }
                    let (Some(Value::Uuid(key_object_id)), Some(Value::Bytea(sealed))) =
                        (values.get(key_idx), values.get(sealed_idx))
                    else {
                        continue;
                    };
                    let Ok(key) = unseal_space_key(keypair, sealed) else {
                        continue;
                    };
                    found = Some((*key_object_id.uuid(), key));
                    break;
                }
                if found.is_some() {
                    break;
                }
            }
            found
        };

        let Some((key_id, key)) = found else {
            return Err(RuntimeError::E2eeKeyUnavailable {
                table: space_table.to_string(),
                space_id: space_id.to_string(),
            });
        };
        self.e2ee.cache_space_key(space_id, key_id, key.clone());
        Ok((key_id, key))
    }

    pub(crate) fn encrypt_values_for_write(
        &mut self,
        table: &str,
        row_id: ObjectId,
        values: &mut HashMap<String, Value>,
        lookup_values: &HashMap<String, Value>,
    ) -> Result<(), RuntimeError> {
        let Some(table_schema) = self
            .schema_manager
            .current_schema()
            .get(&TableName::new(table))
            .cloned()
        else {
            return Ok(());
        };

        for column in &table_schema.columns.columns {
            let Some(space_ref) = &column.encrypted_with else {
                continue;
            };
            let Some(value) = values.get_mut(column.name_str()) else {
                continue;
            };
            if value.is_null() {
                continue;
            }
            let space_ref_name = space_ref.as_str();
            let Some(space_ref_column) = table_schema.columns.column(space_ref_name) else {
                return Err(RuntimeError::WriteError(format!(
                    "encrypted column `{}` references unknown `{space_ref_name}`",
                    column.name
                )));
            };
            let Some(space_table) = space_ref_column.references.as_ref() else {
                return Err(RuntimeError::WriteError(format!(
                    "encrypted column `{}` references non-ref `{space_ref_name}`",
                    column.name
                )));
            };
            let Some(Value::Uuid(space_id)) = lookup_values.get(space_ref_name).cloned() else {
                return Err(RuntimeError::E2eeKeyUnavailable {
                    table: space_table.as_str().to_string(),
                    space_id: format!("missing `{space_ref_name}`"),
                });
            };
            let (key_id, key) = self.space_key_for(space_table.as_str(), space_id)?;
            let plaintext = postcard::to_allocvec(value).map_err(|err| {
                RuntimeError::WriteError(format!("serialize encrypted value: {err}"))
            })?;
            let envelope = e2ee::encrypt_value(
                &key,
                &key_id,
                &EncryptionContext {
                    table,
                    column: column.name_str(),
                    row_id: row_id.uuid().as_bytes(),
                },
                &plaintext,
            )
            .map_err(|err| RuntimeError::WriteError(format!("encrypt value: {err}")))?;
            *value = Value::Bytea(envelope);
        }

        Ok(())
    }

    pub(crate) fn decrypt_row_values(
        &mut self,
        table: &str,
        row_id: ObjectId,
        mut values: Vec<Value>,
    ) -> Vec<Value> {
        let Some(table_schema) = self
            .schema_manager
            .current_schema()
            .get(&TableName::new(table))
            .cloned()
        else {
            return values;
        };

        for (index, column) in table_schema.columns.columns.iter().enumerate() {
            let Some(space_ref) = &column.encrypted_with else {
                continue;
            };
            let Value::Bytea(envelope) = &values[index] else {
                continue;
            };
            let key = match e2ee::envelope_key_id(envelope)
                .ok()
                .and_then(|key_id| self.e2ee.cached_key_by_id(&key_id).cloned())
            {
                Some(key) => key,
                None => {
                    let Some(space_idx) = table_schema.columns.column_index(space_ref.as_str())
                    else {
                        values[index] = Value::Locked;
                        continue;
                    };
                    let Value::Uuid(space_id) = values[space_idx] else {
                        values[index] = Value::Locked;
                        continue;
                    };
                    let Some(space_table) = table_schema
                        .columns
                        .column(space_ref.as_str())
                        .and_then(|c| c.references.as_ref())
                        .map(|t| t.as_str().to_string())
                    else {
                        values[index] = Value::Locked;
                        continue;
                    };
                    match self.space_key_for(&space_table, space_id) {
                        Ok((_key_id, key)) => key,
                        Err(_) => {
                            values[index] = Value::Locked;
                            continue;
                        }
                    }
                }
            };
            let plaintext = match e2ee::decrypt_value(
                &key,
                &EncryptionContext {
                    table,
                    column: column.name_str(),
                    row_id: row_id.uuid().as_bytes(),
                },
                envelope,
            ) {
                Ok(plaintext) => plaintext,
                Err(_) => {
                    values[index] = Value::Locked;
                    continue;
                }
            };
            values[index] = postcard::from_bytes(&plaintext).unwrap_or(Value::Locked);
        }

        values
    }

    pub(crate) fn decrypt_query_row_values(
        &mut self,
        descriptor: &RowDescriptor,
        row_id: ObjectId,
        values: Vec<Value>,
    ) -> Vec<Value> {
        let table_name =
            self.schema_manager
                .current_schema()
                .iter()
                .find_map(|(table_name, table_schema)| {
                    (table_schema.columns == *descriptor).then(|| table_name.as_str().to_string())
                });
        match table_name {
            Some(table_name) => self.decrypt_row_values(&table_name, row_id, values),
            None => values,
        }
    }

    pub fn share_key(
        &mut self,
        space_table: &str,
        space_id: ObjectId,
        recipient_user_id: ObjectId,
        recipient_public_key: &str,
        write_context: Option<&crate::query_manager::session::WriteContext>,
    ) -> Result<crate::row_histories::BatchId, RuntimeError> {
        let (key_id, key) = self.space_key_for(space_table, space_id)?;
        let recipient = E2eePublicKey::from_base64url(recipient_public_key)
            .map_err(|err| RuntimeError::WriteError(format!("parse recipient E2EE key: {err}")))?;
        let sealed = seal_space_key(&recipient, &key)
            .map_err(|err| RuntimeError::WriteError(format!("seal space key: {err}")))?;
        let keys_table = e2ee_keys_table_name(space_table);
        let values = HashMap::from([
            ("space_id".to_string(), Value::Uuid(space_id)),
            (
                "key_id".to_string(),
                Value::Uuid(ObjectId::from_uuid(key_id)),
            ),
            (
                "recipient_user_id".to_string(),
                Value::Uuid(recipient_user_id),
            ),
            (
                "recipient_public_key".to_string(),
                Value::Text(recipient_public_key.to_string()),
            ),
            ("sealed_key".to_string(), Value::Bytea(sealed)),
        ]);
        let ((_, _), batch_id) = self.insert_with_id(&keys_table, values, None, write_context)?;
        Ok(batch_id)
    }

    pub fn unshare_key(
        &mut self,
        key_row_id: ObjectId,
        write_context: Option<&crate::query_manager::session::WriteContext>,
    ) -> Result<crate::row_histories::BatchId, RuntimeError> {
        self.delete(key_row_id, write_context)
    }

    pub fn key_holders(
        &self,
        space_table: &str,
        space_id: ObjectId,
    ) -> Result<Vec<E2eeKeyHolder>, RuntimeError> {
        let keys_table = e2ee_keys_table_name(space_table);
        let descriptor = self
            .schema_manager
            .current_schema()
            .get(&TableName::new(&keys_table))
            .ok_or_else(|| {
                RuntimeError::QueryError(format!("missing E2EE keys table `{keys_table}`"))
            })?
            .columns
            .clone();
        let space_idx = descriptor
            .column_index("space_id")
            .ok_or_else(|| RuntimeError::QueryError(format!("`{keys_table}` missing space_id")))?;
        let key_idx = descriptor
            .column_index("key_id")
            .ok_or_else(|| RuntimeError::QueryError(format!("`{keys_table}` missing key_id")))?;
        let user_idx = descriptor
            .column_index("recipient_user_id")
            .ok_or_else(|| {
                RuntimeError::QueryError(format!("`{keys_table}` missing recipient_user_id"))
            })?;
        let public_key_idx = descriptor
            .column_index("recipient_public_key")
            .ok_or_else(|| {
                RuntimeError::QueryError(format!("`{keys_table}` missing recipient_public_key"))
            })?;

        let mut holders = Vec::new();
        for branch in self.schema_manager.all_branches() {
            let rows = self
                .storage
                .scan_visible_region(&keys_table, branch.as_str())
                .map_err(|err| RuntimeError::QueryError(format!("scan `{keys_table}`: {err}")))?;
            for row in rows {
                let Ok(values) = decode_row(&descriptor, &row.data) else {
                    continue;
                };
                if values.get(space_idx) != Some(&Value::Uuid(space_id)) {
                    continue;
                }
                let (
                    Some(Value::Uuid(key_object_id)),
                    Some(Value::Uuid(recipient_user_id)),
                    Some(Value::Text(recipient_public_key)),
                ) = (
                    values.get(key_idx),
                    values.get(user_idx),
                    values.get(public_key_idx),
                )
                else {
                    continue;
                };
                holders.push(E2eeKeyHolder {
                    row_id: row.row_id,
                    space_id,
                    key_id: *key_object_id.uuid(),
                    recipient_user_id: *recipient_user_id,
                    recipient_public_key: recipient_public_key.clone(),
                });
            }
        }
        holders.sort_by_key(|holder| holder.row_id);
        Ok(holders)
    }
}

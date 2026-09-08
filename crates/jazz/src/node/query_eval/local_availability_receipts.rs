//! Local availability record v1 uses Groove's native typed-record codec.
//! It is never a content tombstone, a replicated row, or a policy proof.

use super::*;
use crate::ids::GlobalPhysicalTableId;
use crate::protocol::PolicyBindingKey;
use crate::schema::{AUTHORITY_POLICY_BINDINGS_STORE, LOCAL_ROW_AVAILABILITY_STORE};

/// Sequence is comparable only within one admitted Core connection epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LocalAvailabilityWatermark {
    pub core: NodeUuid,
    pub core_epoch: u64,
    pub claims_revision: u64,
    pub policy_epoch: u64,
    pub settled_through: GlobalTime,
    pub authorization_progress: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LocalRowAvailability {
    Readable,
    CurrentUnavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LocalAvailabilityRecord {
    pub status: LocalRowAvailability,
    pub watermark: LocalAvailabilityWatermark,
}

pub(crate) fn local_availability_record_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([
        ("format_v1", ValueType::U8),
        ("unavailable", ValueType::Bool),
        ("core", ValueType::Uuid),
        ("core_epoch", ValueType::U64),
        ("claims_revision", ValueType::U64),
        ("policy_epoch", ValueType::U64),
        ("settled_through", ValueType::U64),
        ("evaluation_seq", ValueType::U64),
    ])
}

impl LocalAvailabilityRecord {
    fn values(self) -> Vec<Value> {
        let w = self.watermark;
        vec![
            Value::U8(1),
            Value::Bool(self.status == LocalRowAvailability::CurrentUnavailable),
            Value::Uuid(w.core.0),
            Value::U64(w.core_epoch),
            Value::U64(w.claims_revision),
            Value::U64(w.policy_epoch),
            Value::U64(w.settled_through.0),
            Value::U64(w.authorization_progress),
        ]
    }

    fn decode(record: &groove::records::Record<'_>) -> Result<Self, Error> {
        let values = record.to_values()?;
        if local_availability_record_descriptor().create(&values)? != record.raw() {
            return Err(Error::InvalidStoredValue(
                "noncanonical local availability record v1",
            ));
        }
        let [
            Value::U8(1),
            Value::Bool(unavailable),
            Value::Uuid(core),
            Value::U64(core_epoch),
            Value::U64(claims_revision),
            Value::U64(policy_epoch),
            Value::U64(cut),
            Value::U64(seq),
        ] = values.as_slice()
        else {
            return Err(Error::InvalidStoredValue(
                "invalid local availability record v1",
            ));
        };
        if *seq == 0 {
            return Err(Error::InvalidStoredValue(
                "local availability evaluation sequence must be positive",
            ));
        }
        Ok(Self {
            status: if *unavailable {
                LocalRowAvailability::CurrentUnavailable
            } else {
                LocalRowAvailability::Readable
            },
            watermark: LocalAvailabilityWatermark {
                core: NodeUuid(*core),
                core_epoch: *core_epoch,
                claims_revision: *claims_revision,
                policy_epoch: *policy_epoch,
                settled_through: GlobalTime(*cut),
                authorization_progress: *seq,
            },
        })
    }

    fn follows(self, prior: Self) -> Result<bool, Error> {
        let (next, old) = (self.watermark, prior.watermark);
        if next.policy_epoch < old.policy_epoch || next.settled_through < old.settled_through {
            return Ok(false);
        }
        if next.core != old.core {
            return Ok(true);
        } // Explicit route-owner admission is required separately.
        if next.core_epoch != old.core_epoch {
            return Ok(true);
        }
        if next.claims_revision < old.claims_revision
            || next.authorization_progress < old.authorization_progress
        {
            return Ok(false);
        }
        if next.authorization_progress == old.authorization_progress {
            if self != prior {
                return Err(Error::InvalidStoredValue(
                    "conflicting local availability evaluation receipt",
                ));
            }
            return Ok(false);
        }
        Ok(true)
    }
}

impl<S: OrderedKvStorage> NodeState<S> {
    /// The selected route owner calls this at admission, never from an
    /// uncorrelated reply. Reopen deliberately restores no live authority.
    #[allow(dead_code)] // Network route integration is a separate change.
    pub(crate) fn activate_local_availability_authority(
        &mut self,
        scope: PolicyBindingKey,
        core: NodeUuid,
        epoch: u64,
    ) -> Result<(), Error> {
        if scope.identity == AuthorSubject::SYSTEM {
            return Err(Error::InvalidStoredValue(
                "SYSTEM does not own local availability exclusions",
            ));
        }
        self.require_local_availability_context_capacity(&scope)?;
        self.query
            .local_availability_authorities
            .insert(scope, (core, epoch));
        Ok(())
    }

    /// Apply one already-verified, complete bounded row evaluation after its
    /// native carriers have been ingested. Unknown is deliberately not an
    /// accepted status. An inactive/stale reply does not mutate durable state.
    #[allow(dead_code)] // Network receipt integration is a separate change.
    pub(crate) async fn apply_verified_local_row_availability(
        &mut self,
        scope: &PolicyBindingKey,
        watermark: LocalAvailabilityWatermark,
        outcomes: &[(GlobalPhysicalTableId, RowUuid, LocalRowAvailability)],
    ) -> Result<bool, Error> {
        if self.query.local_availability_authorities.get(scope)
            != Some(&(watermark.core, watermark.core_epoch))
        {
            return Ok(false);
        }
        if outcomes.len() > MAX_KNOWN_STATE_EXACT_REFS || watermark.authorization_progress == 0 {
            return Err(Error::InvalidStoredValue(
                "invalid local availability receipt bounds",
            ));
        }
        let mut seen = BTreeSet::new();
        let mut updates = Vec::new();
        for &(table, row, status) in outcomes {
            if !seen.insert((table, row)) {
                return Err(Error::InvalidStoredValue(
                    "duplicate local availability row coordinate",
                ));
            }
            if !self.catalogue.physical_mappings.values().any(|mapping| {
                mapping
                    .identities
                    .tables
                    .values()
                    .any(|identity| identity.id == table)
            }) {
                return Err(Error::InvalidStoredValue(
                    "local availability receipt has unknown global table identity",
                ));
            }
            let key = (scope.clone(), table, row);
            let record = LocalAvailabilityRecord { status, watermark };
            if let Some(prior) = self.query.local_availability_records.get(&key)
                && !record.follows(*prior)?
            {
                continue;
            }
            updates.push((key, record));
        }
        if updates.is_empty() {
            return Ok(false);
        }
        // The exact directory comparison rejects digest collisions. Its
        // harmless orphan is valid if the subsequent row batch fails.
        self.persist_policy_binding_directory(scope).await?;
        let digest = scope.directory_digest();
        let writes = updates
            .iter()
            .map(|((_, table, row), record)| DirectRecordStoreWrite::Set {
                key: vec![
                    Value::Bytes(digest.to_vec()),
                    Value::Uuid(table.0),
                    Value::Uuid(row.0),
                ],
                value: record.values(),
            })
            .collect::<Vec<_>>();
        self.database
            .direct_record_store(LOCAL_ROW_AVAILABILITY_STORE)?
            .write_many(&writes)
            .await?;
        let changes = updates
            .iter()
            .map(|((_, table, row), record)| {
                (
                    *table,
                    *row,
                    record.status == LocalRowAvailability::CurrentUnavailable,
                )
            })
            .collect::<Vec<_>>();
        // NodeState's mutable owner excludes concurrent source retirement or
        // runtime replacement across this apply. IDs are runtime-token checked;
        // row UUID records use the same fixed descriptor as source allocation.
        // Thus Groove's non-poisoning descriptor/ownership prevalidation errors
        // indicate an internal invariant violation. Operational IVM/storage
        // failures poison Groove, blocking further query/subscription delivery
        // until reopen replays the already durable receipt.
        let changed = self.update_local_unavailable_rows(scope, &changes).await?;
        for (key, record) in updates {
            self.query.local_availability_records.insert(key, record);
        }
        Ok(changed)
    }

    pub(crate) async fn recover_local_availability_records(&mut self) -> Result<(), Error> {
        let store = self
            .database
            .direct_record_store(LOCAL_ROW_AVAILABILITY_STORE)?;
        let entries = store.prefix_entries(&[]).await?;
        let policy_store = self
            .database
            .direct_record_store(AUTHORITY_POLICY_BINDINGS_STORE)?;
        let mut policies = BTreeMap::<[u8; 32], PolicyBindingKey>::new();
        let mut recovered = BTreeMap::new();
        for entry in entries {
            let [Value::Bytes(digest), Value::Uuid(table), Value::Uuid(row)] = entry.key.as_slice()
            else {
                return Err(Error::InvalidStoredValue(
                    "invalid local availability record key",
                ));
            };
            let digest: [u8; 32] = digest.as_slice().try_into().map_err(|_| {
                Error::InvalidStoredValue("invalid local availability policy digest")
            })?;
            if let std::collections::btree_map::Entry::Vacant(entry) = policies.entry(digest) {
                let policy = policy_store
                    .get(&[Value::Bytes(digest.to_vec())])
                    .await?
                    .ok_or(Error::InvalidStoredValue(
                        "local availability policy directory entry is missing",
                    ))?;
                let Value::String(subject) = policy.get_idx(0)? else {
                    return Err(Error::InvalidStoredValue(
                        "invalid local availability policy subject",
                    ));
                };
                let identity = AuthorSubject::from_canonical(&subject)
                    .map_err(|_| Error::InvalidStoredValue("invalid local availability subject"))?;
                let policy = PolicyBindingKey::from_directory_value(identity, policy.get_idx(1)?)
                    .map_err(|_| {
                    Error::InvalidStoredValue("invalid local availability policy claims")
                })?;
                if policy.identity == AuthorSubject::SYSTEM || policy.directory_digest() != digest {
                    return Err(Error::InvalidStoredValue(
                        "local availability policy directory identity mismatch",
                    ));
                }
                entry.insert(policy);
            }
            let record = LocalAvailabilityRecord::decode(&entry.value)?;
            recovered.insert(
                (
                    policies[&digest].clone(),
                    GlobalPhysicalTableId(*table),
                    RowUuid(*row),
                ),
                record,
            );
        }
        // Recovery publishes no prefix if a later record is malformed.
        for ((scope, table, row), record) in &recovered {
            self.update_local_unavailable_rows(
                scope,
                &[(
                    *table,
                    *row,
                    record.status == LocalRowAvailability::CurrentUnavailable,
                )],
            )
            .await?;
        }
        self.query.local_availability_records = recovered;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Internal codec test: public reads cannot pin the native metadata bytes.
    #[test]
    fn local_availability_v1_native_bytes_and_rejection_are_pinned() {
        let descriptor = local_availability_record_descriptor();
        let record = LocalAvailabilityRecord {
            status: LocalRowAvailability::CurrentUnavailable,
            watermark: LocalAvailabilityWatermark {
                core: NodeUuid::from_bytes([0x11; 16]),
                core_epoch: 2,
                claims_revision: 3,
                policy_epoch: 4,
                settled_through: GlobalTime(5),
                authorization_progress: 6,
            },
        };
        // Groove typed records use fixed little-endian U64 fields here.
        let expected: Vec<u8> = vec![
            1, 1, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 2, 0, 0, 0, 0, 0,
            0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0,
            0, 0, 0, 0, 0,
        ];
        assert_eq!(descriptor.create(&record.values()).unwrap(), expected);
        let native = groove::records::Record::new(expected.clone(), &descriptor);
        assert_eq!(LocalAvailabilityRecord::decode(&native).unwrap(), record);
        let mut future = expected.clone();
        future[0] = 2;
        assert!(
            LocalAvailabilityRecord::decode(&groove::records::Record::new(future, &descriptor))
                .is_err()
        );
        let mut trailing = expected.clone();
        trailing.push(0);
        assert!(
            LocalAvailabilityRecord::decode(&groove::records::Record::new(trailing, &descriptor))
                .is_err()
        );
        let mut zero_sequence = expected;
        zero_sequence[50] = 0;
        assert!(
            LocalAvailabilityRecord::decode(&groove::records::Record::new(
                zero_sequence,
                &descriptor
            ))
            .is_err()
        );
    }
}

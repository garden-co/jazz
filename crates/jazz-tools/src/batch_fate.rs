use serde::{Deserialize, Serialize};

use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::{ColumnDescriptor, ColumnType, RowDescriptor, Value};
use crate::row_format::{decode_row, encode_row};
use crate::row_histories::BatchId;
use crate::sync_manager::DurabilityTier;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatchMode {
    Direct,
    Transactional,
}

impl BatchMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Transactional => "transactional",
        }
    }

    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "direct" => Ok(Self::Direct),
            "transactional" => Ok(Self::Transactional),
            other => Err(format!("unknown batch mode '{other}'")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisibleBatchMember {
    pub object_id: ObjectId,
    pub branch_name: BranchName,
    pub batch_id: BatchId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatchSettlement {
    Missing {
        batch_id: BatchId,
    },
    Rejected {
        batch_id: BatchId,
        code: String,
        reason: String,
    },
    DurableDirect {
        batch_id: BatchId,
        confirmed_tier: DurabilityTier,
        visible_members: Vec<VisibleBatchMember>,
    },
    AcceptedTransaction {
        batch_id: BatchId,
        confirmed_tier: DurabilityTier,
        visible_members: Vec<VisibleBatchMember>,
    },
}

impl BatchSettlement {
    pub fn batch_id(&self) -> BatchId {
        match self {
            Self::Missing { batch_id }
            | Self::Rejected { batch_id, .. }
            | Self::DurableDirect { batch_id, .. }
            | Self::AcceptedTransaction { batch_id, .. } => *batch_id,
        }
    }

    pub fn confirmed_tier(&self) -> Option<DurabilityTier> {
        match self {
            Self::DurableDirect { confirmed_tier, .. }
            | Self::AcceptedTransaction { confirmed_tier, .. } => Some(*confirmed_tier),
            Self::Missing { .. } | Self::Rejected { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalBatchRecord {
    pub batch_id: BatchId,
    pub mode: BatchMode,
    pub requested_tier: DurabilityTier,
    pub latest_settlement: Option<BatchSettlement>,
}

impl LocalBatchRecord {
    pub fn new(
        batch_id: BatchId,
        mode: BatchMode,
        requested_tier: DurabilityTier,
        latest_settlement: Option<BatchSettlement>,
    ) -> Self {
        Self {
            batch_id,
            mode,
            requested_tier,
            latest_settlement,
        }
    }

    pub fn apply_settlement(&mut self, settlement: BatchSettlement) {
        assert_eq!(
            settlement.batch_id(),
            self.batch_id,
            "settlement batch id should match record batch id"
        );

        match (&self.latest_settlement, &settlement) {
            (Some(BatchSettlement::Rejected { .. }), _) => {}
            (_, BatchSettlement::Rejected { .. }) => {
                self.latest_settlement = Some(settlement);
            }
            (
                Some(BatchSettlement::DurableDirect {
                    confirmed_tier: current_tier,
                    ..
                }),
                BatchSettlement::DurableDirect { confirmed_tier, .. },
            )
            | (
                Some(BatchSettlement::AcceptedTransaction {
                    confirmed_tier: current_tier,
                    ..
                }),
                BatchSettlement::AcceptedTransaction { confirmed_tier, .. },
            ) => {
                if confirmed_tier >= current_tier {
                    self.latest_settlement = Some(settlement);
                }
            }
            (
                Some(BatchSettlement::DurableDirect { .. })
                | Some(BatchSettlement::AcceptedTransaction { .. }),
                BatchSettlement::Missing { .. },
            ) => {}
            _ => {
                self.latest_settlement = Some(settlement);
            }
        }
    }

    pub fn encode_storage_row(&self) -> Result<Vec<u8>, String> {
        let latest_settlement = self
            .latest_settlement
            .as_ref()
            .map(postcard::to_allocvec)
            .transpose()
            .map_err(|err| format!("encode settlement: {err}"))?;
        let values = vec![
            Value::Bytea(self.batch_id.0.as_bytes().to_vec()),
            Value::Text(self.mode.as_str().to_string()),
            Value::Text(durability_tier_to_str(self.requested_tier).to_string()),
            latest_settlement.map(Value::Bytea).unwrap_or(Value::Null),
        ];
        encode_row(&storage_descriptor(), &values).map_err(|err| format!("encode batch row: {err}"))
    }

    pub fn decode_storage_row(bytes: &[u8]) -> Result<Self, String> {
        let values = decode_row(&storage_descriptor(), bytes)
            .map_err(|err| format!("decode batch row: {err}"))?;
        let [batch_id, mode, requested_tier, latest_settlement] = values.as_slice() else {
            return Err("unexpected local batch record shape".to_string());
        };

        let batch_id = match batch_id {
            Value::Bytea(bytes) => {
                let uuid = uuid::Uuid::from_slice(bytes)
                    .map_err(|err| format!("decode batch id uuid: {err}"))?;
                BatchId(uuid)
            }
            other => return Err(format!("expected batch id bytes, got {other:?}")),
        };
        let mode = match mode {
            Value::Text(raw) => BatchMode::parse(raw)?,
            other => return Err(format!("expected batch mode text, got {other:?}")),
        };
        let requested_tier = match requested_tier {
            Value::Text(raw) => durability_tier_from_str(raw)?,
            other => return Err(format!("expected requested tier text, got {other:?}")),
        };
        let latest_settlement = match latest_settlement {
            Value::Null => None,
            Value::Bytea(bytes) => Some(
                postcard::from_bytes(bytes)
                    .map_err(|err| format!("decode latest settlement: {err}"))?,
            ),
            other => {
                return Err(format!(
                    "expected latest settlement bytes or null, got {other:?}"
                ));
            }
        };

        Ok(Self {
            batch_id,
            mode,
            requested_tier,
            latest_settlement,
        })
    }
}

fn durability_tier_to_str(tier: DurabilityTier) -> &'static str {
    match tier {
        DurabilityTier::Worker => "worker",
        DurabilityTier::EdgeServer => "edge",
        DurabilityTier::GlobalServer => "global",
    }
}

fn durability_tier_from_str(raw: &str) -> Result<DurabilityTier, String> {
    match raw {
        "worker" => Ok(DurabilityTier::Worker),
        "edge" => Ok(DurabilityTier::EdgeServer),
        "global" => Ok(DurabilityTier::GlobalServer),
        other => Err(format!("unknown durability tier '{other}'")),
    }
}

fn storage_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new("batch_id", ColumnType::Bytea),
        ColumnDescriptor::new("mode", ColumnType::Text),
        ColumnDescriptor::new("requested_tier", ColumnType::Text),
        ColumnDescriptor::new("latest_settlement", ColumnType::Bytea).nullable(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_batch_record_storage_row_roundtrips() {
        let batch_id = BatchId::new();
        let record = LocalBatchRecord::new(
            batch_id,
            BatchMode::Direct,
            DurabilityTier::EdgeServer,
            Some(BatchSettlement::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::Worker,
                visible_members: vec![VisibleBatchMember {
                    object_id: ObjectId::from_uuid(uuid::Uuid::from_u128(7)),
                    branch_name: BranchName::new("main"),
                    batch_id,
                }],
            }),
        );

        let bytes = record.encode_storage_row().expect("encode record");
        let decoded = LocalBatchRecord::decode_storage_row(&bytes).expect("decode record");

        assert_eq!(decoded, record);
    }

    #[test]
    fn local_batch_record_keeps_highest_durable_direct_tier() {
        let batch_id = BatchId::new();
        let mut record = LocalBatchRecord::new(
            batch_id,
            BatchMode::Direct,
            DurabilityTier::GlobalServer,
            Some(BatchSettlement::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::EdgeServer,
                visible_members: Vec::new(),
            }),
        );

        record.apply_settlement(BatchSettlement::DurableDirect {
            batch_id,
            confirmed_tier: DurabilityTier::Worker,
            visible_members: Vec::new(),
        });

        assert_eq!(
            record.latest_settlement,
            Some(BatchSettlement::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::EdgeServer,
                visible_members: Vec::new(),
            })
        );
    }
}

use crate::commit::CommitId;
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

    pub fn encode_storage_row(&self) -> Result<Vec<u8>, String> {
        postcard::to_allocvec(self).map_err(|err| format!("encode batch settlement: {err}"))
    }

    pub fn decode_storage_row(bytes: &[u8]) -> Result<Self, String> {
        postcard::from_bytes(bytes).map_err(|err| format!("decode batch settlement: {err}"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalBatchRecord {
    pub batch_id: BatchId,
    pub mode: BatchMode,
    pub requested_tier: DurabilityTier,
    pub sealed: bool,
    pub sealed_submission: Option<SealedBatchSubmission>,
    pub latest_settlement: Option<BatchSettlement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedBatchSubmission {
    pub batch_id: BatchId,
    pub target_branch_name: BranchName,
    pub members: Vec<SealedBatchMember>,
    pub captured_frontier: Vec<CapturedFrontierMember>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedBatchMember {
    pub object_id: ObjectId,
    pub branch_name: BranchName,
    pub version_id: CommitId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedFrontierMember {
    pub object_id: ObjectId,
    pub branch_name: BranchName,
    pub version_id: CommitId,
}

impl LocalBatchRecord {
    pub fn new(
        batch_id: BatchId,
        mode: BatchMode,
        requested_tier: DurabilityTier,
        sealed: bool,
        latest_settlement: Option<BatchSettlement>,
    ) -> Self {
        Self {
            batch_id,
            mode,
            requested_tier,
            sealed,
            sealed_submission: None,
            latest_settlement,
        }
    }

    pub fn mark_sealed(&mut self, submission: SealedBatchSubmission) {
        self.sealed = true;
        self.sealed_submission = Some(submission);
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
        let sealed_submission = self
            .sealed_submission
            .as_ref()
            .map(postcard::to_allocvec)
            .transpose()
            .map_err(|err| format!("encode sealed submission: {err}"))?;
        let values = vec![
            Value::Bytea(self.batch_id.0.as_bytes().to_vec()),
            Value::Text(self.mode.as_str().to_string()),
            Value::Text(durability_tier_to_str(self.requested_tier).to_string()),
            Value::Boolean(self.sealed),
            sealed_submission.map(Value::Bytea).unwrap_or(Value::Null),
            latest_settlement.map(Value::Bytea).unwrap_or(Value::Null),
        ];
        encode_row(&storage_descriptor(), &values).map_err(|err| format!("encode batch row: {err}"))
    }

    pub fn decode_storage_row(bytes: &[u8]) -> Result<Self, String> {
        let values = decode_row(&storage_descriptor(), bytes)
            .map_err(|err| format!("decode batch row: {err}"))?;
        let [
            batch_id,
            mode,
            requested_tier,
            sealed,
            sealed_submission,
            latest_settlement,
        ] = values.as_slice()
        else {
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
        let sealed = match sealed {
            Value::Boolean(value) => *value,
            other => return Err(format!("expected sealed boolean, got {other:?}")),
        };
        let sealed_submission = match sealed_submission {
            Value::Null => None,
            Value::Bytea(bytes) => Some(
                postcard::from_bytes(bytes)
                    .map_err(|err| format!("decode sealed batch submission: {err}"))?,
            ),
            other => {
                return Err(format!(
                    "expected sealed submission bytes or null, got {other:?}"
                ));
            }
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
            sealed,
            sealed_submission,
            latest_settlement,
        })
    }
}

impl SealedBatchSubmission {
    pub fn new(
        batch_id: BatchId,
        target_branch_name: BranchName,
        mut members: Vec<SealedBatchMember>,
        mut captured_frontier: Vec<CapturedFrontierMember>,
    ) -> Self {
        members.sort_by(|left, right| {
            left.object_id
                .uuid()
                .as_bytes()
                .cmp(right.object_id.uuid().as_bytes())
                .then_with(|| left.branch_name.as_str().cmp(right.branch_name.as_str()))
                .then_with(|| left.version_id.0.cmp(&right.version_id.0))
        });
        members.dedup();
        captured_frontier.sort_by(|left, right| {
            left.object_id
                .uuid()
                .as_bytes()
                .cmp(right.object_id.uuid().as_bytes())
                .then_with(|| left.branch_name.as_str().cmp(right.branch_name.as_str()))
                .then_with(|| left.version_id.0.cmp(&right.version_id.0))
        });
        captured_frontier.dedup();
        Self {
            batch_id,
            target_branch_name,
            members,
            captured_frontier,
        }
    }

    pub fn encode_storage_row(&self) -> Result<Vec<u8>, String> {
        let values = vec![
            Value::Bytea(self.batch_id.0.as_bytes().to_vec()),
            Value::Text(self.target_branch_name.as_str().to_string()),
            Value::Array(
                self.members
                    .iter()
                    .map(|member| Value::Row {
                        id: None,
                        values: vec![
                            Value::Bytea(member.object_id.uuid().as_bytes().to_vec()),
                            Value::Text(member.branch_name.as_str().to_string()),
                            Value::Bytea(member.version_id.0.to_vec()),
                        ],
                    })
                    .collect(),
            ),
            Value::Array(
                self.captured_frontier
                    .iter()
                    .map(|member| Value::Row {
                        id: None,
                        values: vec![
                            Value::Bytea(member.object_id.uuid().as_bytes().to_vec()),
                            Value::Text(member.branch_name.as_str().to_string()),
                            Value::Bytea(member.version_id.0.to_vec()),
                        ],
                    })
                    .collect(),
            ),
        ];
        encode_row(&sealed_batch_submission_storage_descriptor(), &values)
            .map_err(|err| format!("encode sealed batch submission row: {err}"))
    }

    pub fn decode_storage_row(bytes: &[u8]) -> Result<Self, String> {
        let values = decode_row(&sealed_batch_submission_storage_descriptor(), bytes)
            .map_err(|err| format!("decode sealed batch submission row: {err}"))?;
        let [batch_id, target_branch_name, members, captured_frontier] = values.as_slice() else {
            return Err("unexpected sealed batch submission shape".to_string());
        };

        let batch_id = match batch_id {
            Value::Bytea(bytes) => {
                let uuid = uuid::Uuid::from_slice(bytes)
                    .map_err(|err| format!("decode sealed batch submission uuid: {err}"))?;
                BatchId(uuid)
            }
            other => return Err(format!("expected batch id bytes, got {other:?}")),
        };
        let target_branch_name = match target_branch_name {
            Value::Text(raw) => BranchName::new(raw),
            other => return Err(format!("expected target branch text, got {other:?}")),
        };

        let members = match members {
            Value::Array(elements) => elements
                .iter()
                .map(|element| match element {
                    Value::Row { values, .. } => {
                        let [object_id, branch_name, version_id] = values.as_slice() else {
                            return Err(
                                "expected sealed batch member row to have three values".to_string(),
                            );
                        };
                        let object_id = match object_id {
                            Value::Bytea(bytes) => uuid::Uuid::from_slice(bytes)
                                .map(ObjectId::from_uuid)
                                .map_err(|err| {
                                    format!("decode sealed batch object id uuid: {err}")
                                })?,
                            other => {
                                return Err(format!(
                                    "expected sealed batch member object id bytes, got {other:?}"
                                ));
                            }
                        };
                        let branch_name = match branch_name {
                            Value::Text(raw) => BranchName::new(raw),
                            other => {
                                return Err(format!(
                                    "expected sealed batch member branch text, got {other:?}"
                                ));
                            }
                        };
                        let version_id = match version_id {
                            Value::Bytea(bytes) => CommitId(bytes.as_slice().try_into().map_err(
                                |_| {
                                    format!(
                                        "expected sealed batch member version id to be 32 bytes, got {}",
                                        bytes.len()
                                    )
                                },
                            )?),
                            other => {
                                return Err(format!(
                                    "expected sealed batch member version id bytes, got {other:?}"
                                ));
                            }
                        };
                        Ok(SealedBatchMember {
                            object_id,
                            branch_name,
                            version_id,
                        })
                    }
                    other => Err(format!("expected sealed batch member row, got {other:?}")),
                })
                .collect::<Result<Vec<_>, _>>()?,
            other => return Err(format!("expected sealed batch member array, got {other:?}")),
        };

        let captured_frontier = match captured_frontier {
            Value::Array(elements) => elements
                .iter()
                .map(|element| match element {
                    Value::Row { values, .. } => {
                        let [object_id, branch_name, version_id] = values.as_slice() else {
                            return Err(
                                "expected captured frontier row to have three values".to_string(),
                            );
                        };
                        let object_id = match object_id {
                            Value::Bytea(bytes) => uuid::Uuid::from_slice(bytes)
                                .map(ObjectId::from_uuid)
                                .map_err(|err| {
                                    format!("decode captured frontier object id uuid: {err}")
                                })?,
                            other => {
                                return Err(format!(
                                    "expected captured frontier object id bytes, got {other:?}"
                                ));
                            }
                        };
                        let branch_name = match branch_name {
                            Value::Text(raw) => BranchName::new(raw),
                            other => {
                                return Err(format!(
                                    "expected captured frontier branch text, got {other:?}"
                                ));
                            }
                        };
                        let version_id = match version_id {
                            Value::Bytea(bytes) => CommitId(bytes.as_slice().try_into().map_err(
                                |_| {
                                    format!(
                                        "expected captured frontier version id to be 32 bytes, got {}",
                                        bytes.len()
                                    )
                                },
                            )?),
                            other => {
                                return Err(format!(
                                    "expected captured frontier version id bytes, got {other:?}"
                                ));
                            }
                        };
                        Ok(CapturedFrontierMember {
                            object_id,
                            branch_name,
                            version_id,
                        })
                    }
                    other => Err(format!("expected captured frontier row, got {other:?}")),
                })
                .collect::<Result<Vec<_>, _>>()?,
            other => return Err(format!("expected captured frontier array, got {other:?}")),
        };

        Ok(Self::new(
            batch_id,
            target_branch_name,
            members,
            captured_frontier,
        ))
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
        ColumnDescriptor::new("sealed", ColumnType::Boolean),
        ColumnDescriptor::new("sealed_submission", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("latest_settlement", ColumnType::Bytea).nullable(),
    ])
}

fn sealed_batch_submission_storage_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new("batch_id", ColumnType::Bytea),
        ColumnDescriptor::new("target_branch_name", ColumnType::Text),
        ColumnDescriptor::new(
            "members",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(RowDescriptor::new(vec![
                        ColumnDescriptor::new("object_id", ColumnType::Bytea),
                        ColumnDescriptor::new("branch_name", ColumnType::Text),
                        ColumnDescriptor::new("version_id", ColumnType::Bytea),
                    ])),
                }),
            },
        ),
        ColumnDescriptor::new(
            "captured_frontier",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(RowDescriptor::new(vec![
                        ColumnDescriptor::new("object_id", ColumnType::Bytea),
                        ColumnDescriptor::new("branch_name", ColumnType::Text),
                        ColumnDescriptor::new("version_id", ColumnType::Bytea),
                    ])),
                }),
            },
        ),
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
            true,
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
            true,
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

    #[test]
    fn local_batch_record_storage_row_roundtrips_with_sealed_submission() {
        let batch_id = BatchId::new();
        let mut record = LocalBatchRecord::new(
            batch_id,
            BatchMode::Transactional,
            DurabilityTier::GlobalServer,
            false,
            None,
        );
        record.mark_sealed(SealedBatchSubmission::new(
            batch_id,
            BranchName::new("dev-aaaaaaaaaaaa-main"),
            vec![SealedBatchMember {
                object_id: ObjectId::from_uuid(uuid::Uuid::from_u128(42)),
                branch_name: BranchName::new("dev-aaaaaaaaaaaa-main"),
                version_id: CommitId([4; 32]),
            }],
            vec![CapturedFrontierMember {
                object_id: ObjectId::from_uuid(uuid::Uuid::from_u128(7)),
                branch_name: BranchName::new("dev-bbbbbbbbbbbb-main"),
                version_id: CommitId([8; 32]),
            }],
        ));

        let bytes = record.encode_storage_row().expect("encode record");
        let decoded = LocalBatchRecord::decode_storage_row(&bytes).expect("decode record");

        assert_eq!(decoded, record);
    }

    #[test]
    fn sealed_batch_submission_storage_row_roundtrips() {
        let batch_id = BatchId::new();
        let object_id = ObjectId::new();
        let version_id = CommitId([7; 32]);
        let submission = SealedBatchSubmission::new(
            batch_id,
            BranchName::new("main"),
            vec![
                SealedBatchMember {
                    object_id,
                    branch_name: BranchName::new("main"),
                    version_id,
                },
                SealedBatchMember {
                    object_id,
                    branch_name: BranchName::new("main"),
                    version_id,
                },
            ],
            vec![CapturedFrontierMember {
                object_id,
                branch_name: BranchName::new("dev-aaaaaaaaaaaa-main"),
                version_id: CommitId([9; 32]),
            }],
        );

        let bytes = submission
            .encode_storage_row()
            .expect("encode sealed batch submission");
        let decoded = SealedBatchSubmission::decode_storage_row(&bytes)
            .expect("decode sealed batch submission");

        assert_eq!(
            decoded,
            SealedBatchSubmission {
                batch_id,
                target_branch_name: BranchName::new("main"),
                members: vec![SealedBatchMember {
                    object_id,
                    branch_name: BranchName::new("main"),
                    version_id,
                }],
                captured_frontier: vec![CapturedFrontierMember {
                    object_id,
                    branch_name: BranchName::new("dev-aaaaaaaaaaaa-main"),
                    version_id: CommitId([9; 32]),
                }],
            }
        );
    }
}

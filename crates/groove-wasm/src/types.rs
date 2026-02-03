//! Type bridges for WASM boundary.
//!
//! Serializable versions of key Groove types for crossing the WASM/JS boundary.
//! Types with `Tsify` derive automatically generate TypeScript definitions.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use tsify::Tsify;

// ============================================================================
// Value Serialization
// ============================================================================

/// Value type for WASM boundary (mirrors groove::query_manager::types::Value).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
#[serde(tag = "type", content = "value")]
pub enum WasmValue {
    Integer(i32),
    BigInt(i64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(String), // UUID as string for JS compatibility
    Array(Vec<WasmValue>),
    Row(Vec<WasmValue>),
    Null,
}

impl From<groove::query_manager::types::Value> for WasmValue {
    fn from(v: groove::query_manager::types::Value) -> Self {
        use groove::query_manager::types::Value;
        match v {
            Value::Integer(i) => WasmValue::Integer(i),
            Value::BigInt(i) => WasmValue::BigInt(i),
            Value::Boolean(b) => WasmValue::Boolean(b),
            Value::Text(s) => WasmValue::Text(s),
            Value::Timestamp(t) => WasmValue::Timestamp(t),
            Value::Uuid(id) => WasmValue::Uuid(id.uuid().to_string()),
            Value::Array(arr) => WasmValue::Array(arr.into_iter().map(Into::into).collect()),
            Value::Row(row) => WasmValue::Row(row.into_iter().map(Into::into).collect()),
            Value::Null => WasmValue::Null,
        }
    }
}

impl TryFrom<WasmValue> for groove::query_manager::types::Value {
    type Error = String;

    fn try_from(v: WasmValue) -> Result<Self, Self::Error> {
        use groove::object::ObjectId;
        use groove::query_manager::types::Value;

        Ok(match v {
            WasmValue::Integer(i) => Value::Integer(i),
            WasmValue::BigInt(i) => Value::BigInt(i),
            WasmValue::Boolean(b) => Value::Boolean(b),
            WasmValue::Text(s) => Value::Text(s),
            WasmValue::Timestamp(t) => Value::Timestamp(t),
            WasmValue::Uuid(s) => {
                let uuid = uuid::Uuid::parse_str(&s)
                    .map_err(|e| format!("Invalid UUID: {}", e))?;
                Value::Uuid(ObjectId::from_uuid(uuid))
            }
            WasmValue::Array(arr) => {
                let converted: Result<Vec<_>, _> = arr.into_iter().map(TryInto::try_into).collect();
                Value::Array(converted?)
            }
            WasmValue::Row(row) => {
                let converted: Result<Vec<_>, _> = row.into_iter().map(TryInto::try_into).collect();
                Value::Row(converted?)
            }
            WasmValue::Null => Value::Null,
        })
    }
}

// ============================================================================
// Row Delta Serialization
// ============================================================================

/// Serializable row for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmRow {
    pub id: String, // ObjectId as UUID string
    pub values: Vec<WasmValue>,
}

/// Delta for row-level changes (mirrors groove::query_manager::types::RowDelta).
#[derive(Debug, Clone, Default, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmRowDelta {
    pub added: Vec<WasmRow>,
    pub removed: Vec<WasmRow>,
    pub updated: Vec<(WasmRow, WasmRow)>,
    pub pending: bool,
}

// ============================================================================
// Storage Request/Response Serialization
// ============================================================================

/// Load depth for branch loading operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub enum WasmLoadDepth {
    /// Just CommitIds of tips.
    TipIdsOnly,
    /// Full Commit structs for tips.
    TipsOnly,
    /// All commits in branch.
    AllCommits,
}

impl From<groove::storage::LoadDepth> for WasmLoadDepth {
    fn from(depth: groove::storage::LoadDepth) -> Self {
        match depth {
            groove::storage::LoadDepth::TipIdsOnly => WasmLoadDepth::TipIdsOnly,
            groove::storage::LoadDepth::TipsOnly => WasmLoadDepth::TipsOnly,
            groove::storage::LoadDepth::AllCommits => WasmLoadDepth::AllCommits,
        }
    }
}

impl From<WasmLoadDepth> for groove::storage::LoadDepth {
    fn from(depth: WasmLoadDepth) -> Self {
        match depth {
            WasmLoadDepth::TipIdsOnly => groove::storage::LoadDepth::TipIdsOnly,
            WasmLoadDepth::TipsOnly => groove::storage::LoadDepth::TipsOnly,
            WasmLoadDepth::AllCommits => groove::storage::LoadDepth::AllCommits,
        }
    }
}

/// Serializable storage request for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
#[serde(tag = "type")]
pub enum WasmStorageRequest {
    CreateObject {
        id: String,
        #[tsify(type = "Record<string, string>")]
        metadata: HashMap<String, String>,
    },
    AppendCommit {
        object_id: String,
        branch_name: String,
        commit: WasmCommit,
    },
    LoadObjectBranch {
        object_id: String,
        branch_name: String,
        depth: WasmLoadDepth,
    },
    StoreBlob {
        content_hash: String, // hex encoded
        #[serde(with = "serde_bytes")]
        #[tsify(type = "Uint8Array")]
        data: Vec<u8>,
    },
    LoadBlob {
        content_hash: String,
    },
    AssociateBlob {
        content_hash: String,
        object_id: String,
        branch_name: String,
        commit_id: String,
    },
    LoadBlobAssociations {
        content_hash: String,
    },
    DeleteCommit {
        object_id: String,
        branch_name: String,
        commit_id: String,
    },
    DissociateAndMaybeDeleteBlob {
        content_hash: String,
        object_id: String,
        branch_name: String,
        commit_id: String,
    },
    SetBranchTails {
        object_id: String,
        branch_name: String,
        #[tsify(optional)]
        tails: Option<Vec<String>>,
    },
    LoadIndexPage {
        table: String,
        column: String,
        page_id: u64,
    },
    StoreIndexPage {
        table: String,
        column: String,
        page_id: u64,
        #[serde(with = "serde_bytes")]
        #[tsify(type = "Uint8Array")]
        data: Vec<u8>,
    },
    DeleteIndexPage {
        table: String,
        column: String,
        page_id: u64,
    },
    LoadIndexMeta {
        table: String,
        column: String,
    },
    StoreIndexMeta {
        table: String,
        column: String,
        #[serde(with = "serde_bytes")]
        #[tsify(type = "Uint8Array")]
        data: Vec<u8>,
    },
}

/// Serializable commit for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmCommit {
    pub parents: Vec<String>, // CommitIds as hex strings
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub content: Vec<u8>,
    pub timestamp: u64,
    pub author: String, // ObjectId as UUID string
    #[tsify(optional, type = "Record<string, string>")]
    pub metadata: Option<BTreeMap<String, String>>,
}

/// Serializable loaded branch for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmLoadedBranch {
    pub tips: Vec<String>,
    #[tsify(optional)]
    pub tails: Option<Vec<String>>,
    #[tsify(type = "Record<string, WasmCommit>")]
    pub commits: HashMap<String, WasmCommit>,
    #[tsify(optional, type = "Record<string, string>")]
    pub metadata: Option<HashMap<String, String>>,
}

/// Serializable blob association for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmBlobAssociation {
    pub object_id: String,
    pub branch_name: String,
    pub commit_id: String,
}

/// Serializable storage response for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
#[serde(tag = "type")]
pub enum WasmStorageResponse {
    CreateObject {
        id: String,
        success: bool,
        #[tsify(optional)]
        error: Option<String>,
    },
    AppendCommit {
        object_id: String,
        commit_id: String,
        success: bool,
        #[tsify(optional)]
        error: Option<String>,
    },
    LoadObjectBranch {
        object_id: String,
        branch_name: String,
        #[tsify(optional)]
        branch: Option<WasmLoadedBranch>,
        #[tsify(optional)]
        error: Option<String>,
    },
    StoreBlob {
        content_hash: String,
        success: bool,
        #[tsify(optional)]
        error: Option<String>,
    },
    LoadBlob {
        content_hash: String,
        #[tsify(optional, type = "Uint8Array")]
        data: Option<Vec<u8>>,
        #[tsify(optional)]
        error: Option<String>,
    },
    AssociateBlob {
        content_hash: String,
        success: bool,
        #[tsify(optional)]
        error: Option<String>,
    },
    LoadBlobAssociations {
        content_hash: String,
        #[tsify(optional)]
        associations: Option<Vec<WasmBlobAssociation>>,
        #[tsify(optional)]
        error: Option<String>,
    },
    DeleteCommit {
        object_id: String,
        branch_name: String,
        commit_id: String,
        success: bool,
        #[tsify(optional)]
        error: Option<String>,
    },
    DissociateAndMaybeDeleteBlob {
        content_hash: String,
        object_id: String,
        branch_name: String,
        commit_id: String,
        #[tsify(optional)]
        blob_deleted: Option<bool>,
        #[tsify(optional)]
        error: Option<String>,
    },
    SetBranchTails {
        object_id: String,
        branch_name: String,
        success: bool,
        #[tsify(optional)]
        error: Option<String>,
    },
    LoadIndexPage {
        table: String,
        column: String,
        page_id: u64,
        #[tsify(optional, type = "Uint8Array")]
        data: Option<Vec<u8>>,
        #[tsify(optional)]
        error: Option<String>,
    },
    StoreIndexPage {
        table: String,
        column: String,
        page_id: u64,
        success: bool,
        #[tsify(optional)]
        error: Option<String>,
    },
    DeleteIndexPage {
        table: String,
        column: String,
        page_id: u64,
        success: bool,
        #[tsify(optional)]
        error: Option<String>,
    },
    LoadIndexMeta {
        table: String,
        column: String,
        #[tsify(optional, type = "Uint8Array")]
        data: Option<Vec<u8>>,
        #[tsify(optional)]
        error: Option<String>,
    },
    StoreIndexMeta {
        table: String,
        column: String,
        success: bool,
        #[tsify(optional)]
        error: Option<String>,
    },
}

// ============================================================================
// Conversion Functions
// ============================================================================

/// Encode bytes as hex string.
pub fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Decode hex string to bytes.
pub fn hex_to_bytes(s: &str) -> Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err("Hex string must have even length".to_string());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

/// Convert a Groove StorageRequest to a WasmStorageRequest.
pub fn storage_request_to_wasm(req: groove::storage::StorageRequest) -> WasmStorageRequest {
    use groove::storage::StorageRequest;

    match req {
        StorageRequest::CreateObject { id, metadata } => WasmStorageRequest::CreateObject {
            id: id.uuid().to_string(),
            metadata,
        },
        StorageRequest::AppendCommit {
            object_id,
            branch_name,
            commit,
        } => WasmStorageRequest::AppendCommit {
            object_id: object_id.uuid().to_string(),
            branch_name: branch_name.as_str().to_string(),
            commit: WasmCommit {
                parents: commit.parents.iter().map(|p| bytes_to_hex(&p.0)).collect(),
                content: commit.content.clone(),
                timestamp: commit.timestamp,
                author: commit.author.uuid().to_string(),
                metadata: commit.metadata.clone(),
            },
        },
        StorageRequest::LoadObjectBranch {
            object_id,
            branch_name,
            depth,
        } => WasmStorageRequest::LoadObjectBranch {
            object_id: object_id.uuid().to_string(),
            branch_name: branch_name.as_str().to_string(),
            depth: depth.into(),
        },
        StorageRequest::StoreBlob { content_hash, data } => WasmStorageRequest::StoreBlob {
            content_hash: bytes_to_hex(&content_hash.0),
            data,
        },
        StorageRequest::LoadBlob { content_hash } => WasmStorageRequest::LoadBlob {
            content_hash: bytes_to_hex(&content_hash.0),
        },
        StorageRequest::AssociateBlob {
            content_hash,
            object_id,
            branch_name,
            commit_id,
        } => WasmStorageRequest::AssociateBlob {
            content_hash: bytes_to_hex(&content_hash.0),
            object_id: object_id.uuid().to_string(),
            branch_name: branch_name.as_str().to_string(),
            commit_id: bytes_to_hex(&commit_id.0),
        },
        StorageRequest::LoadBlobAssociations { content_hash } => {
            WasmStorageRequest::LoadBlobAssociations {
                content_hash: bytes_to_hex(&content_hash.0),
            }
        }
        StorageRequest::DeleteCommit {
            object_id,
            branch_name,
            commit_id,
        } => WasmStorageRequest::DeleteCommit {
            object_id: object_id.uuid().to_string(),
            branch_name: branch_name.as_str().to_string(),
            commit_id: bytes_to_hex(&commit_id.0),
        },
        StorageRequest::DissociateAndMaybeDeleteBlob {
            content_hash,
            object_id,
            branch_name,
            commit_id,
        } => WasmStorageRequest::DissociateAndMaybeDeleteBlob {
            content_hash: bytes_to_hex(&content_hash.0),
            object_id: object_id.uuid().to_string(),
            branch_name: branch_name.as_str().to_string(),
            commit_id: bytes_to_hex(&commit_id.0),
        },
        StorageRequest::SetBranchTails {
            object_id,
            branch_name,
            tails,
        } => WasmStorageRequest::SetBranchTails {
            object_id: object_id.uuid().to_string(),
            branch_name: branch_name.as_str().to_string(),
            tails: tails.map(|t| t.into_iter().map(|c| bytes_to_hex(&c.0)).collect()),
        },
        StorageRequest::LoadIndexPage {
            table,
            column,
            page_id,
        } => WasmStorageRequest::LoadIndexPage {
            table,
            column,
            page_id,
        },
        StorageRequest::StoreIndexPage {
            table,
            column,
            page_id,
            data,
        } => WasmStorageRequest::StoreIndexPage {
            table,
            column,
            page_id,
            data,
        },
        StorageRequest::DeleteIndexPage {
            table,
            column,
            page_id,
        } => WasmStorageRequest::DeleteIndexPage {
            table,
            column,
            page_id,
        },
        StorageRequest::LoadIndexMeta { table, column } => {
            WasmStorageRequest::LoadIndexMeta { table, column }
        }
        StorageRequest::StoreIndexMeta {
            table,
            column,
            data,
        } => WasmStorageRequest::StoreIndexMeta {
            table,
            column,
            data,
        },
    }
}

/// Convert a WasmStorageResponse to a Groove StorageResponse.
pub fn wasm_response_to_storage(
    resp: WasmStorageResponse,
) -> Result<groove::storage::StorageResponse, String> {
    use groove::commit::{Commit, CommitId};
    use groove::object::{BranchName, ObjectId};
    use groove::storage::{
        BlobAssociation, ContentHash, LoadedBranch, StorageError, StorageResponse,
    };
    use smallvec::SmallVec;

    fn parse_object_id(s: &str) -> Result<ObjectId, String> {
        let uuid =
            uuid::Uuid::parse_str(s).map_err(|e| format!("Invalid ObjectId UUID: {}", e))?;
        Ok(ObjectId::from_uuid(uuid))
    }

    fn parse_commit_id(s: &str) -> Result<CommitId, String> {
        let bytes = hex_to_bytes(s)?;
        if bytes.len() != 32 {
            return Err("CommitId must be 32 bytes".to_string());
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(CommitId(arr))
    }

    fn parse_content_hash(s: &str) -> Result<ContentHash, String> {
        let bytes = hex_to_bytes(s)?;
        if bytes.len() != 32 {
            return Err("ContentHash must be 32 bytes".to_string());
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(ContentHash(arr))
    }

    fn to_storage_error(error: Option<String>) -> Result<(), StorageError> {
        match error {
            None => Ok(()),
            Some(e) if e == "NotFound" => Err(StorageError::NotFound),
            Some(e) => Err(StorageError::IoError(e)),
        }
    }

    Ok(match resp {
        WasmStorageResponse::CreateObject { id, error, .. } => StorageResponse::CreateObject {
            id: parse_object_id(&id)?,
            result: to_storage_error(error),
        },
        WasmStorageResponse::AppendCommit {
            object_id,
            commit_id,
            error,
            ..
        } => StorageResponse::AppendCommit {
            object_id: parse_object_id(&object_id)?,
            commit_id: parse_commit_id(&commit_id)?,
            result: to_storage_error(error),
        },
        WasmStorageResponse::LoadObjectBranch {
            object_id,
            branch_name,
            branch,
            error,
        } => {
            let result = if let Some(e) = error {
                if e == "NotFound" {
                    Err(StorageError::NotFound)
                } else {
                    Err(StorageError::IoError(e))
                }
            } else if let Some(b) = branch {
                let tips: HashSet<CommitId> = b
                    .tips
                    .into_iter()
                    .map(|s| parse_commit_id(&s))
                    .collect::<Result<_, _>>()?;
                let tails: Option<HashSet<CommitId>> = b
                    .tails
                    .map(|t| t.into_iter().map(|s| parse_commit_id(&s)).collect())
                    .transpose()?;
                let metadata = b.metadata.clone();
                let commits: HashMap<CommitId, Commit> = b
                    .commits
                    .into_iter()
                    .map(|(k, v)| {
                        let commit_id = parse_commit_id(&k)?;
                        let parents: SmallVec<[CommitId; 2]> = v
                            .parents
                            .into_iter()
                            .map(|p| parse_commit_id(&p))
                            .collect::<Result<_, _>>()?;
                        let author = parse_object_id(&v.author)?;
                        Ok((
                            commit_id,
                            Commit {
                                parents,
                                content: v.content,
                                timestamp: v.timestamp,
                                author,
                                metadata: v.metadata,
                                stored_state: groove::commit::StoredState::Stored,
                            },
                        ))
                    })
                    .collect::<Result<_, String>>()?;
                Ok(LoadedBranch {
                    tips,
                    tails,
                    commits,
                    metadata,
                })
            } else {
                Err(StorageError::NotFound)
            };
            StorageResponse::LoadObjectBranch {
                object_id: parse_object_id(&object_id)?,
                branch_name: BranchName::new(&branch_name),
                result,
            }
        }
        WasmStorageResponse::StoreBlob {
            content_hash,
            error,
            ..
        } => StorageResponse::StoreBlob {
            content_hash: parse_content_hash(&content_hash)?,
            result: to_storage_error(error),
        },
        WasmStorageResponse::LoadBlob {
            content_hash,
            data,
            error,
        } => {
            let result = if let Some(e) = error {
                if e == "NotFound" {
                    Err(StorageError::NotFound)
                } else {
                    Err(StorageError::IoError(e))
                }
            } else {
                data.ok_or(StorageError::NotFound)
            };
            StorageResponse::LoadBlob {
                content_hash: parse_content_hash(&content_hash)?,
                result,
            }
        }
        WasmStorageResponse::AssociateBlob {
            content_hash,
            error,
            ..
        } => StorageResponse::AssociateBlob {
            content_hash: parse_content_hash(&content_hash)?,
            result: to_storage_error(error),
        },
        WasmStorageResponse::LoadBlobAssociations {
            content_hash,
            associations,
            error,
        } => {
            let result = if let Some(e) = error {
                if e == "NotFound" {
                    Err(StorageError::NotFound)
                } else {
                    Err(StorageError::IoError(e))
                }
            } else if let Some(assocs) = associations {
                let parsed: Vec<BlobAssociation> = assocs
                    .into_iter()
                    .map(|a| {
                        Ok(BlobAssociation {
                            object_id: parse_object_id(&a.object_id)?,
                            branch_name: BranchName::new(&a.branch_name),
                            commit_id: parse_commit_id(&a.commit_id)?,
                        })
                    })
                    .collect::<Result<_, String>>()?;
                Ok(parsed)
            } else {
                Err(StorageError::NotFound)
            };
            StorageResponse::LoadBlobAssociations {
                content_hash: parse_content_hash(&content_hash)?,
                result,
            }
        }
        WasmStorageResponse::DeleteCommit {
            object_id,
            branch_name,
            commit_id,
            error,
            ..
        } => StorageResponse::DeleteCommit {
            object_id: parse_object_id(&object_id)?,
            branch_name: BranchName::new(&branch_name),
            commit_id: parse_commit_id(&commit_id)?,
            result: to_storage_error(error),
        },
        WasmStorageResponse::DissociateAndMaybeDeleteBlob {
            content_hash,
            object_id,
            branch_name,
            commit_id,
            blob_deleted,
            error,
        } => {
            let result = if let Some(e) = error {
                if e == "NotFound" {
                    Err(StorageError::NotFound)
                } else {
                    Err(StorageError::IoError(e))
                }
            } else {
                Ok(blob_deleted.unwrap_or(false))
            };
            StorageResponse::DissociateAndMaybeDeleteBlob {
                content_hash: parse_content_hash(&content_hash)?,
                object_id: parse_object_id(&object_id)?,
                branch_name: BranchName::new(&branch_name),
                commit_id: parse_commit_id(&commit_id)?,
                blob_deleted: result,
            }
        }
        WasmStorageResponse::SetBranchTails {
            object_id,
            branch_name,
            error,
            ..
        } => StorageResponse::SetBranchTails {
            object_id: parse_object_id(&object_id)?,
            branch_name: BranchName::new(&branch_name),
            result: to_storage_error(error),
        },
        WasmStorageResponse::LoadIndexPage {
            table,
            column,
            page_id,
            data,
            error,
        } => {
            let result = if let Some(e) = error {
                Err(StorageError::IoError(e))
            } else {
                Ok(data)
            };
            StorageResponse::LoadIndexPage {
                table,
                column,
                page_id,
                result,
            }
        }
        WasmStorageResponse::StoreIndexPage {
            table,
            column,
            page_id,
            error,
            ..
        } => StorageResponse::StoreIndexPage {
            table,
            column,
            page_id,
            result: to_storage_error(error),
        },
        WasmStorageResponse::DeleteIndexPage {
            table,
            column,
            page_id,
            error,
            ..
        } => StorageResponse::DeleteIndexPage {
            table,
            column,
            page_id,
            result: to_storage_error(error),
        },
        WasmStorageResponse::LoadIndexMeta {
            table,
            column,
            data,
            error,
        } => {
            let result = if let Some(e) = error {
                Err(StorageError::IoError(e))
            } else {
                Ok(data)
            };
            StorageResponse::LoadIndexMeta {
                table,
                column,
                result,
            }
        }
        WasmStorageResponse::StoreIndexMeta {
            table,
            column,
            error,
            ..
        } => StorageResponse::StoreIndexMeta {
            table,
            column,
            result: to_storage_error(error),
        },
    })
}

// ============================================================================
// Schema Serialization
// ============================================================================

/// Serializable column type for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
#[serde(tag = "type")]
pub enum WasmColumnType {
    Integer,
    BigInt,
    Boolean,
    Text,
    Timestamp,
    Uuid,
    Array { element: Box<WasmColumnType> },
    Row { columns: Vec<WasmColumnDescriptor> },
}

/// Serializable column descriptor for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmColumnDescriptor {
    pub name: String,
    pub column_type: WasmColumnType,
    pub nullable: bool,
    #[tsify(optional)]
    pub references: Option<String>,
}

/// Serializable table schema for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmTableSchema {
    pub columns: Vec<WasmColumnDescriptor>,
}

/// Serializable schema for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmSchema {
    #[tsify(type = "Record<string, WasmTableSchema>")]
    pub tables: HashMap<String, WasmTableSchema>,
}

impl From<groove::query_manager::types::ColumnType> for WasmColumnType {
    fn from(ct: groove::query_manager::types::ColumnType) -> Self {
        use groove::query_manager::types::ColumnType;
        match ct {
            ColumnType::Integer => WasmColumnType::Integer,
            ColumnType::BigInt => WasmColumnType::BigInt,
            ColumnType::Boolean => WasmColumnType::Boolean,
            ColumnType::Text => WasmColumnType::Text,
            ColumnType::Timestamp => WasmColumnType::Timestamp,
            ColumnType::Uuid => WasmColumnType::Uuid,
            ColumnType::Array(elem) => WasmColumnType::Array {
                element: Box::new((*elem).into()),
            },
            ColumnType::Row(desc) => WasmColumnType::Row {
                columns: desc
                    .columns
                    .into_iter()
                    .map(|c| WasmColumnDescriptor {
                        name: c.name.as_str().to_string(),
                        column_type: c.column_type.into(),
                        nullable: c.nullable,
                        references: c.references.map(|r| r.as_str().to_string()),
                    })
                    .collect(),
            },
        }
    }
}

impl From<&groove::query_manager::types::Schema> for WasmSchema {
    fn from(schema: &groove::query_manager::types::Schema) -> Self {
        let tables = schema
            .iter()
            .map(|(name, ts)| {
                let columns = ts
                    .descriptor
                    .columns
                    .iter()
                    .map(|c| WasmColumnDescriptor {
                        name: c.name.as_str().to_string(),
                        column_type: c.column_type.clone().into(),
                        nullable: c.nullable,
                        references: c.references.map(|r| r.as_str().to_string()),
                    })
                    .collect();
                (name.as_str().to_string(), WasmTableSchema { columns })
            })
            .collect();
        WasmSchema { tables }
    }
}

/// Convert WasmSchema back to Groove Schema.
impl TryFrom<WasmSchema> for groove::query_manager::types::Schema {
    type Error = String;

    fn try_from(ws: WasmSchema) -> Result<Self, Self::Error> {
        use groove::query_manager::types::{
            ColumnDescriptor, ColumnType, RowDescriptor, TableName, TableSchema,
        };

        fn wasm_type_to_groove(wt: WasmColumnType) -> ColumnType {
            match wt {
                WasmColumnType::Integer => ColumnType::Integer,
                WasmColumnType::BigInt => ColumnType::BigInt,
                WasmColumnType::Boolean => ColumnType::Boolean,
                WasmColumnType::Text => ColumnType::Text,
                WasmColumnType::Timestamp => ColumnType::Timestamp,
                WasmColumnType::Uuid => ColumnType::Uuid,
                WasmColumnType::Array { element } => {
                    ColumnType::Array(Box::new(wasm_type_to_groove(*element)))
                }
                WasmColumnType::Row { columns } => {
                    let cols = columns
                        .into_iter()
                        .map(|c| {
                            let mut cd =
                                ColumnDescriptor::new(&c.name, wasm_type_to_groove(c.column_type));
                            if c.nullable {
                                cd = cd.nullable();
                            }
                            if let Some(ref_table) = c.references {
                                cd = cd.references(&ref_table);
                            }
                            cd
                        })
                        .collect();
                    ColumnType::Row(Box::new(RowDescriptor::new(cols)))
                }
            }
        }

        let mut schema = groove::query_manager::types::Schema::new();
        for (table_name, table_schema) in ws.tables {
            let columns = table_schema
                .columns
                .into_iter()
                .map(|c| {
                    let mut cd = ColumnDescriptor::new(&c.name, wasm_type_to_groove(c.column_type));
                    if c.nullable {
                        cd = cd.nullable();
                    }
                    if let Some(ref_table) = c.references {
                        cd = cd.references(&ref_table);
                    }
                    cd
                })
                .collect();
            schema.insert(
                TableName::new(&table_name),
                TableSchema::new(RowDescriptor::new(columns)),
            );
        }
        Ok(schema)
    }
}

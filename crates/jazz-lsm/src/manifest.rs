use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub(crate) format_version: u32,
    pub(crate) next_file_id: u64,
    pub(crate) next_seq: u64,
    pub(crate) levels: Vec<Vec<SstMeta>>,
    pub(crate) required_merge_ops: Vec<u32>,
}

impl Manifest {
    pub(crate) fn new(num_levels: usize) -> Self {
        Self {
            format_version: 1,
            next_file_id: 1,
            next_seq: 1,
            levels: vec![Vec::new(); num_levels],
            required_merge_ops: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SstMeta {
    pub(crate) id: u64,
    pub(crate) level: usize,
    pub(crate) path: String,
    pub(crate) min_key: Vec<u8>,
    pub(crate) max_key: Vec<u8>,
    pub(crate) bytes: u64,
    pub(crate) records: u64,
}
